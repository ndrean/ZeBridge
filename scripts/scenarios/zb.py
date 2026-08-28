"""Shared helpers for the scenario scripts.

Names come from `grammar.json`, never from literals here — the whole point of that
file is that one rename moves the bridge, `nats-init` and every client together, and a
test harness that hardcodes `cdc.` is one more place to forget.

Python because the decode side needs MessagePack and these grew out of live debugging.
The natural alternative is `emitter/`, which already has `gnat` and a Postgres pool, and
would not need the `psql` shell-out below; if this harness grows, that is where it goes.
"""

import asyncio
import json
import os
import pathlib
import subprocess
import sys

import msgpack
import nats

ROOT = pathlib.Path(__file__).resolve().parents[2]
TOPOLOGY = json.loads((ROOT / "grammar.json").read_text())

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
NKEY_SEED = os.environ.get("NATS_NKEY_SEED")

# How to reach psql. Defaults to the compose stack; override for a remote database.
PSQL = os.environ.get("ZB_PSQL", "docker exec -i postgres-primary psql -U postgres")


def subject(*parts: str) -> str:
    return ".".join(parts)


def tenants() -> list[str]:
    """The live tenant list. Tenants are DATA (zebridge_user_tenants), not config —
    grammar.json no longer carries a `tenants` key, and the bridge derives the same
    list at boot for its stream reconciliation."""
    out = psql("SELECT DISTINCT tenant_id FROM public.zebridge_user_tenants ORDER BY 1", quiet=True)
    return [t for t in out.splitlines() if t]


async def connect():
    """Connect with whichever credential the server is configured for.

    Two shapes now, because the server carries per-principal permissions:

      nkey            `NATS_NKEY_SEED` — the bridge's own key, authorised for everything.
                      Right for scenarios that inspect streams or act as the bridge.
      user/password   userinfo in `NATS_URL`, e.g.
                      `NATS_URL=nats://alice:s3cret@127.0.0.1:4222`. Right for anything
                      testing what a *client* can do, because it is confined to
                      `mutation.alice.>` exactly as a real client is.

    ⚠️ Prefer the URL when it carries credentials: a scenario that means to exercise a
    client's permissions but silently connects as the bridge would pass while proving
    nothing — every permissions violation it exists to catch is one the bridge is allowed
    to perform.
    """
    creds = os.environ.get("NATS_CREDS")
    if creds:
        return await nats.connect(NATS_URL.split("://")[0] + "://" +
                                  NATS_URL.split("://", 1)[-1].rsplit("@", 1)[-1],
                                  user_credentials=creds)
    if "@" in NATS_URL.split("://", 1)[-1]:
        return await nats.connect(NATS_URL)
    if not NKEY_SEED:
        sys.exit(
            "No NATS credentials.\n"
            "  set -a && . ./.env && set +a          # bridge nkey, full access\n"
            "  NATS_URL=nats://alice:s3cret@127.0.0.1:4222   # as a client principal"
        )
    return await nats.connect(NATS_URL, nkeys_seed_str=NKEY_SEED)


def psql(sql: str, quiet: bool = False) -> str:
    """Run one statement and return stdout. Uses -tA so output is parseable."""
    res = subprocess.run(PSQL.split() + ["-tA", "-c", sql], capture_output=True, text=True)
    if res.returncode != 0 and not quiet:
        print(f"  psql failed: {res.stderr.strip()}", file=sys.stderr)
    return res.stdout.strip()


def decode(data: bytes):
    """CDC and snapshot payloads are MessagePack unless the bridge ran with --json."""
    try:
        return msgpack.unpackb(data, raw=False, strict_map_key=False)
    except Exception:
        return json.loads(data.decode())


def ok(msg: str):
    print(f"  \033[32m✓\033[0m {msg}")


def bad(msg: str):
    print(f"  \033[31m✗\033[0m {msg}")


def run(main):
    """Entry point: the coroutine's return value is the exit code (0 = pass)."""
    sys.exit(asyncio.run(main()) or 0)


# ─── Driving a bridge process ───────────────────────────────────────────────────
#
# The fault probes need a bridge of their own: they misconfigure it or break the
# broker under it, which no long-running instance should have to survive.

BRIDGE = ROOT / "zig-out" / "bin" / "bridge"
BRIDGE_ARGS = os.environ.get(
    "ZB_BRIDGE_ARGS", "--slot zb_probe --pub my_pub --port 9096"
).split()


def bridge_env(**overrides) -> dict:
    """Inherit the shell's env, then override.

    DATABASE_READER_URL is required rather than assembled: the bridge dropped its
    PG_HOST/PG_USER fallback precisely so that no component quietly builds a connection
    out of parts, and a test harness that did it anyway would be testing a path that no
    longer exists.
    """
    if not os.environ.get("DATABASE_READER_URL"):
        sys.exit(
            "DATABASE_READER_URL is not set — the probes start a real bridge.\n"
            "  set -a && . ./.env.bridge && set +a\n"
            '  export NATS_NKEY_SEED="SU..."'
        )
    env = dict(os.environ)
    env.setdefault("LOG_LEVEL", "info")
    env.update({k: v for k, v in overrides.items() if v is not None})
    return env


class Bridge:
    """A bridge subprocess with its stderr captured to a file.

    Context manager so a probe cannot leave one running when an assertion raises.
    """

    def __init__(self, log_path, **env_overrides):
        self.log_path = pathlib.Path(log_path)
        self.env = bridge_env(**env_overrides)
        self.proc = None

    def __enter__(self):
        if not BRIDGE.exists():
            sys.exit(f"{BRIDGE} not found — run `zig build -Doptimize=ReleaseFast`")
        self.log = open(self.log_path, "w")
        self.proc = subprocess.Popen(
            [str(BRIDGE), *BRIDGE_ARGS], env=self.env, stdout=subprocess.DEVNULL, stderr=self.log
        )
        return self

    def __exit__(self, *exc):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.log.close()

    def text(self) -> str:
        self.log.flush()
        return self.log_path.read_text(errors="replace")

    def wait_for_log(self, needle: str, timeout: float = 30) -> bool:
        import time

        deadline = time.time() + timeout
        while time.time() < deadline:
            if needle in self.text():
                return True
            if self.proc.poll() is not None:
                return needle in self.text()
            time.sleep(0.5)
        return False

    def wait_for_exit(self, timeout: float = 40):
        """Returns the exit code, or None if it is still running."""
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None


def nats_cli(*args, seed_file=None) -> subprocess.CompletedProcess:
    """Shell out to the `nats` CLI, which is the only thing that can create a stream
    with the exact config nats-init uses — nats-py's add_stream would round-trip it
    through its own defaults."""
    if seed_file is None:
        seed_file = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_seed.nk"
        seed_file.write_text(NKEY_SEED or "")
    # ⚠️ Strip credentials from the URL. This passes `--nkey`, so the seed *is* the
    # credential — but if NATS_URL also carries `user:pass@` (which it does whenever a
    # scenario is run as a client principal), those win and every administrative command
    # is denied. The failure is opaque: `could not pick a Stream to operate on: context
    # deadline exceeded`, because a denied JetStream API publish never answers.
    scheme, _, rest = NATS_URL.partition("://")
    address = rest.rsplit("@", 1)[-1] if "@" in rest else rest
    server = f"{scheme}://{address}" if scheme else address

    creds = os.environ.get("NATS_CREDS")
    auth = ["--creds", creds] if creds else ["--nkey", str(seed_file)]
    return subprocess.run(
        ["nats", "--server", server, *auth, *args],
        capture_output=True,
        text=True,
    )

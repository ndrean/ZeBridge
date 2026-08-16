"""Shared helpers for the scenario scripts.

Names come from `topology.json`, never from literals here — the whole point of that
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
TOPOLOGY = json.loads((ROOT / "topology.json").read_text())

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
NKEY_SEED = os.environ.get("NATS_NKEY_SEED")

# How to reach psql. Defaults to the compose stack; override for a remote database.
PSQL = os.environ.get("ZB_PSQL", "docker exec -i postgres-primary psql -U postgres")


def subject(*parts: str) -> str:
    return ".".join(parts)


async def connect():
    if not NKEY_SEED:
        sys.exit("NATS_NKEY_SEED is not set.\n  set -a && . ./.env && set +a")
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

    DATABASE_URL is required rather than assembled: the bridge dropped its
    PG_HOST/PG_USER fallback precisely so that no component quietly builds a connection
    out of parts, and a test harness that did it anyway would be testing a path that no
    longer exists.
    """
    if not os.environ.get("DATABASE_URL"):
        sys.exit(
            "DATABASE_URL is not set — the probes start a real bridge.\n"
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
    return subprocess.run(
        ["nats", "--server", NATS_URL, "--nkey", str(seed_file), *args],
        capture_output=True,
        text=True,
    )

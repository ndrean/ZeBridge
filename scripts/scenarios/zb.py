"""Shared helpers for the scenario scripts.

Names come from `grammar.json`, never from literals here — the whole point of that
file is that one rename moves the bridge, `nats-init` and every client together, and a
test harness that hardcodes `cdc.` is one more place to forget.

Python because the decode side needs MessagePack and these grew out of live debugging.

The stack is the NATIVE one (`scripts/native/up.sh`: Postgres 127.0.0.1:5432, NATS
127.0.0.1:4222, JWT/operator auth with `scripts/native/creds/<principal>.creds`).
Everything below derives from that, and from `grammar.json`, `zebridge_catalogue` and
`zebridge_user_tenants` — never from literals in a scenario. Run a battery with
`scripts/scenarios/run.py`.
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
NKEY_SEED = os.environ.get("NATS_BRIDGE_NKEY_SEED")  # legacy; creds win when present
CREDS_DIR = ROOT / "scripts" / "native" / "creds"


def _psql_binary() -> str:
    """`ZB_PGBIN/psql` if set, else the Homebrew PostgreSQL 18 the native stack uses when
    it exists, else whatever `psql` is on PATH."""
    pgbin = os.environ.get("ZB_PGBIN")
    if pgbin:
        return str(pathlib.Path(pgbin) / "psql")
    brew = pathlib.Path("/opt/homebrew/opt/postgresql@18/bin/psql")
    return str(brew) if brew.exists() else "psql"


# How to reach psql AS ADMIN. The native stack, unless `ZB_PSQL` says otherwise — the
# docker default this used to carry made every scenario read "" from a dead
# connection and score it as "no rows".
PSQL = os.environ.get(
    "ZB_PSQL",
    f"{_psql_binary()} -h {os.environ.get('ZB_PG_HOST', '127.0.0.1')} "
    f"-p {os.environ.get('ZB_PG_PORT', '5432')} -U {os.environ.get('ZB_PG_USER', 'postgres')} -d postgres",
)


def nats_server() -> str:
    """`NATS_URL` without any userinfo — the creds file is the credential."""
    scheme, _, rest = NATS_URL.partition("://")
    address = rest.rsplit("@", 1)[-1] if "@" in rest else rest
    return f"{scheme}://{address}" if scheme else address


def principal() -> str | None:
    """Who a scenario acts as: `ZB_PRINCIPAL`, else the creds file's name
    (`scripts/native/creds/<principal>.creds`), else the URL's userinfo (legacy),
    else None. Every scenario used to carry its own copy of this — and default to
    "alice", which under creds auth published on a subject the connection was not
    allowed to use and turned every check into a timeout."""
    if os.environ.get("ZB_PRINCIPAL"):
        return os.environ["ZB_PRINCIPAL"]
    creds = os.environ.get("NATS_CREDS")
    if creds:
        return pathlib.Path(creds).stem
    rest = NATS_URL.split("://", 1)[-1]
    if "@" in rest:
        return rest.rsplit("@", 1)[0].split(":", 1)[0]
    return None


def require_principal() -> str:
    who = principal()
    if not who:
        sys.exit(
            "This scenario acts as a CLIENT principal and cannot tell which.\n"
            "  export NATS_CREDS=scripts/native/creds/omar.creds   # or alice/bob/mary/nina\n"
            "  export ZB_PRINCIPAL=omar                             # when the creds file is named otherwise"
        )
    return who


def creds_for(who: str) -> str:
    path = CREDS_DIR / f"{who}.creds"
    if not path.exists():
        sys.exit(f"no creds for '{who}': {path} — scripts/native/jwt-bootstrap.sh mints them")
    return str(path)


async def connect_as(who: str):
    """Connect as one named principal, confined exactly as a real client is."""
    return await nats.connect(nats_server(), user_credentials=creds_for(who))


def tenant_of(who: str) -> str:
    """The tenant a principal is mapped to (zebridge_user_tenants), or the open tenant."""
    out = psql(f"SELECT tenant_id FROM public.zebridge_user_tenants WHERE principal = '{who}'", quiet=True)
    return out.splitlines()[0] if out else TOPOLOGY.get("open_tenant", "_default")


def rules(table: str) -> dict:
    """The LWW columns for a table: version, tombstone, tiebreak, tenant — from
    `zebridge_catalogue`, with a `SYNC_RULES`/`TENANT_RULES` env entry overriding per
    table exactly as the bridge honours it. Absent keys are None. Scenarios used to
    read the env alone and `sys.exit` when it was empty, which on a catalogue-configured
    stack is always."""
    r = {"version": None, "tombstone": None, "tiebreak": None, "tenant": None}
    out = psql(
        "SELECT version_col::text, COALESCE(tombstone_col::text, ''), COALESCE(tiebreak_col::text, ''), "
        f"COALESCE(tenant_col::text, '') FROM public.zebridge_catalogue WHERE tbl = '{table}'", quiet=True)
    if out:
        v, t, k, tc = (out.splitlines()[0].split("|") + ["", "", "", ""])[:4]
        r.update(version=v or None, tombstone=t or None, tiebreak=k or None, tenant=tc or None)
    for entry in os.environ.get("SYNC_RULES", "").split(";"):
        if entry.strip().startswith(table + ":"):
            cols = (entry.split(":", 1)[1].split(",") + ["", "", ""])[:3]
            r.update(version=cols[0] or r["version"], tombstone=cols[1] or None, tiebreak=cols[2] or None)
    for entry in os.environ.get("TENANT_RULES", "").split(";"):
        if entry.strip().startswith(table + ":"):
            r["tenant"] = entry.split(":", 1)[1].split(",")[0] or r["tenant"]
    return r


def require_rules(table: str, *needed: str) -> dict:
    r = rules(table)
    missing = [n for n in needed if not r.get(n)]
    if missing:
        sys.exit(f"'{table}' has no {', '.join(missing)} column declared — "
                 f"zebridge_enable('{table}', ...) declares them in zebridge_catalogue")
    return r


def publication() -> str:
    """The publication under test — named, never guessed.

    Scenarios used to write `os.environ.get("BRIDGE_CDC_PUBLICATION", "my_pub")`,
    which is the same defaulting the bridge and `zebridge_enable` both stopped
    doing (NOTES §10ad/§10ae): a run pointed at another publication would quietly
    inspect `my_pub` instead and report on a feed nobody was testing.
    """
    return os.environ["BRIDGE_CDC_PUBLICATION"]


def subject(*parts: str) -> str:
    return ".".join(parts)


def tenants() -> list[str]:
    """The live tenant list. Tenants are DATA (zebridge_user_tenants), not config —
    grammar.json no longer carries a `tenants` key, and the bridge derives the same
    list at boot for its stream reconciliation."""
    out = psql("SELECT DISTINCT tenant_id FROM public.zebridge_user_tenants ORDER BY 1", quiet=True)
    return [t for t in out.splitlines() if t]


async def connect():
    """Connect with the credential in `NATS_CREDS` — the JWT/operator stack's one shape.

    `scripts/native/creds/bridge.creds` is the bridge's own identity (inspect streams,
    act as the bridge); `<principal>.creds` is a client, confined to `mutation.<p>.>`
    exactly as a real client is. ⚠️ A scenario that means to exercise a CLIENT's
    permissions must connect as that client (`connect_as(require_principal())`):
    connected as the bridge it passes while proving nothing, because every violation it
    exists to catch is one the bridge is allowed to perform.

    Legacy shapes, kept only for a pre-JWT server: userinfo in `NATS_URL`, or the nkey
    seed in `NATS_BRIDGE_NKEY_SEED`.
    """
    creds = os.environ.get("NATS_CREDS")
    if creds:
        if not pathlib.Path(creds).exists():
            sys.exit(f"NATS_CREDS={creds} does not exist")
        return await nats.connect(nats_server(), user_credentials=creds)
    if "@" in NATS_URL.split("://", 1)[-1]:
        return await nats.connect(NATS_URL)
    if not NKEY_SEED:
        sys.exit(
            "No NATS credentials.\n"
            "  export NATS_CREDS=scripts/native/creds/bridge.creds   # as the bridge\n"
            "  export NATS_CREDS=scripts/native/creds/omar.creds     # as a client principal"
        )
    return await nats.connect(NATS_URL, nkeys_seed_str=NKEY_SEED)


def psql(sql: str, quiet: bool = False) -> str:
    """Run one statement and return stdout. Uses -tA so output is parseable."""
    res = subprocess.run(PSQL.split() + ["-tA", "-c", sql], capture_output=True, text=True)
    if res.returncode != 0 and not quiet:
        print(f"  psql failed: {res.stderr.strip()}", file=sys.stderr)
    return res.stdout.strip()


def decode(data: bytes):
    """CDC events and mutation payloads are MessagePack; verdicts and KV descriptors are
    JSON. Chain objects are msgpack under zstd (a per-era dictionary) — decode those with
    the chain's dictionary, not with this."""
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
SWEEPER = ROOT / "zig-out" / "bin" / "bridge_sweeper"
# `--pub` comes from BRIDGE_CDC_PUBLICATION (publication(), no default): the literal
# `my_pub` this used to carry was the very defaulting the bridge stopped doing.
BRIDGE_ARGS = os.environ.get("ZB_BRIDGE_ARGS", "--slot zb_probe --port 9096").split()
if "--pub" not in BRIDGE_ARGS:
    BRIDGE_ARGS += ["--pub", os.environ.get("BRIDGE_CDC_PUBLICATION", "")]


def bridge_port() -> int:
    """The HTTP port of the bridge a scenario drives: `--port` in ZB_BRIDGE_ARGS for a
    probe, else `BRIDGE_PORT`, else the long-running bridge's 9090."""
    if "--port" in BRIDGE_ARGS:
        return int(BRIDGE_ARGS[BRIDGE_ARGS.index("--port") + 1])
    return int(os.environ.get("BRIDGE_PORT", "9090"))


def http_base(probe: bool = False) -> str:
    port = bridge_port() if probe else int(os.environ.get("BRIDGE_PORT", "9090"))
    return f"http://127.0.0.1:{port}"


def leaks_available() -> bool:
    """macOS `leaks` — the scenarios that audit memory skip loudly elsewhere."""
    import shutil
    return sys.platform == "darwin" and shutil.which("leaks") is not None


def another_bridge_running() -> bool:
    # Anchored: `zig-out/bin/bridge_sweeper` is always running and is not a bridge —
    # unanchored, every owns-group scenario refused to start (measured 2026-08-29).
    r = subprocess.run(["pgrep", "-f", r"zig-out/bin/bridge$"], capture_output=True, text=True)
    return bool(r.stdout.strip())


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
            "  export NATS_CREDS=scripts/native/creds/bridge.creds"
        )
    if not os.environ.get("BRIDGE_CDC_PUBLICATION"):
        sys.exit("BRIDGE_CDC_PUBLICATION is not set — the publication is named, never guessed (NOTES §10ad)")
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
    creds = os.environ.get("NATS_CREDS")
    if creds:
        auth = ["--creds", creds]
    else:
        # Legacy nkey path: the seed file is a secret — 0600, and gone at exit.
        if seed_file is None:
            import atexit
            seed_file = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / f"zb_seed_{os.getpid()}.nk"
            seed_file.write_text(NKEY_SEED or "")
            seed_file.chmod(0o600)
            atexit.register(lambda: seed_file.unlink(missing_ok=True))
        auth = ["--nkey", str(seed_file)]
    # ⚠️ The server address only: userinfo in NATS_URL would win over the credential
    # and every administrative command would be denied with an opaque timeout.
    return subprocess.run(
        ["nats", "--server", nats_server(), *auth, *args],
        capture_output=True,
        text=True,
    )


def kv_bucket(bucket_key: str) -> str:
    """A KV bucket's name from grammar.json: `kv.<key>` (schemas, tenants) or the
    generations bucket, which the grammar keeps under `generations.kv`."""
    if bucket_key in TOPOLOGY.get("kv", {}):
        return TOPOLOGY["kv"][bucket_key]
    if bucket_key == "generations":
        return TOPOLOGY["generations"]["kv"]
    raise KeyError(f"no KV bucket named {bucket_key!r} in grammar.json")


def kv_get(bucket_key: str, key: str) -> str:
    """`$KV.<bucket>.<key>` raw, with `bucket_key` resolved by `kv_bucket`."""
    r = nats_cli("kv", "get", kv_bucket(bucket_key), key, "--raw")
    return r.stdout.strip() if r.returncode == 0 else ""

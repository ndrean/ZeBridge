"""`kill -9` the CLIENT's host mid-seed: the SQLite file must survive its process
(NOTES §10ce).

    scripts/scenarios/run.py owns -k client_kill      (owns the bridge; builds a big fixture)

Every kill in the chaos program was server-side. But you cannot kill a library — you
kill its HOST, and libzb's host is a Python/Node process or a browser tab, all of which
die without cleanup (SIGKILL, tab close, OOM). The client's only durable organ is the
SQLite file, so the claims are the file's:

  1. a host killed MID-SEED (the fixture is big enough that applyChain takes seconds)
     leaves a database a fresh host can OPEN — SQLite's journal makes the torn write
     invisible
  2. the interrupted seed is IDEMPOTENT on retry: re-applied, never doubled
     (count == count DISTINCT uid)
  3. the fresh host converges to PostgreSQL exactly, and a second kill mid-RE-seed
     changes nothing
"""
import asyncio
import json
import os
import pathlib
import random
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402

TABLE = "zb_client_kill"
TENANT = "acme"
PRINCIPAL = "alice"
ROWS = 120_000
KILLS = 2
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_client_kill_bridge.log"
DB = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_client_kill.sqlite3"

HOST = r'''
import ctypes, json, sys, pathlib
sys.path.insert(0, sys.argv[1] + "/libzb/python")
import _env
lib = _env.load_lib()
lib.zb_free.argtypes = [ctypes.c_void_p]
lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
lib.zb_client_close.argtypes = [ctypes.c_uint64]
for n, a in (("sync", []), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
    f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a
def take(p):
    try: return json.loads(ctypes.string_at(p).decode())
    finally: lib.zb_free(p)
cfg = json.loads(sys.argv[2])
h = lib.zb_client_open(json.dumps(cfg).encode())
if not h:
    print("OPEN-FAILED", flush=True); sys.exit(2)
print("SYNC-START", flush=True)      # the parent's kill window opens here
take(lib.zb_client_sync(h))
q = lambda s: take(lib.zb_client_query(h, s.encode(), b"[]"))
rows = q(f"SELECT count(*), count(DISTINCT uid) FROM {sys.argv[3]}").get("rows") or [[-1, -1]]
print(f"DONE {rows[0][0]} {rows[0][1]}", flush=True)
lib.zb_client_close(h)
'''


def slot_name() -> str:
    return zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"


def cleanup() -> None:
    zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{TABLE}'", quiet=True)
    zb.psql(f"ALTER PUBLICATION {zb.publication()} DROP TABLE public.{TABLE}", quiet=True)
    zb.psql(f"DROP TABLE IF EXISTS public.{TABLE}", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_generations WHERE tbl = '{TABLE}'", quiet=True)
    zb.nats_cli("kv", "del", zb.kv_bucket("generations"), f"{TENANT}.{TABLE}", "-f")
    bucket = zb.TOPOLOGY["generations"]["bucket_prefix"] + TENANT
    for suffix in ("full", "dict", "delta"):
        zb.nats_cli("obj", "rm", bucket, f"{TABLE}-g1-{suffix}", "-f")
    zb.psql(f"SELECT pg_drop_replication_slot('{slot_name()}') FROM pg_replication_slots "
            f"WHERE slot_name = '{slot_name()}' AND NOT active", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_limits WHERE slot = '{slot_name()}'", quiet=True)
    for f in (DB, DB.with_suffix(".sqlite3-wal"), DB.with_suffix(".sqlite3-shm"),
              pathlib.Path(str(DB) + "-wal"), pathlib.Path(str(DB) + "-shm")):
        f.unlink(missing_ok=True)


async def main() -> int:
    try:
        return await run()
    finally:
        cleanup()


async def run() -> int:
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")
    failed = 0
    cleanup()
    zb.psql(f"CREATE TABLE public.{TABLE} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), "
            "tenant_id varchar(255) NOT NULL, payload text, deleted_at timestamptz, "
            "inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now())")
    zb.psql(f"INSERT INTO public.{TABLE} (tenant_id, payload) "
            f"SELECT '{TENANT}', repeat(md5(g::text), 6) FROM generate_series(1, {ROWS}) g")
    out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable("
                  f"'{TABLE}', tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', "
                  f"tombstone_col => 'deleted_at', publication => '{zb.publication()}', dry_run => false)")
    if "ERROR" in out:
        zb.bad(f"could not enable the fixture: {out[:140]}"); return 1

    host_py = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_client_kill_host.py"
    host_py.write_text(HOST)
    cfg = json.dumps({"url": zb.nats_server(), "credsPath": zb.creds_for(PRINCIPAL),
                      "grammarPath": str(zb.ROOT / "grammar.json"), "dbPath": str(DB),
                      "principal": PRINCIPAL, "clientId": "py-client-kill", "tables": [TABLE]})

    def spawn():
        return subprocess.Popen([sys.executable, str(host_py), str(zb.ROOT), cfg, TABLE],
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)

    # a chain must exist to seed from
    env = {"GENERATIONS_ENABLED": "1", "GENERATION_CADENCE_SECONDS": "5",
           "GENERATION_RULES": f"{TABLE}:{TENANT}"}
    rnd = random.Random(20260903)
    with zb.Bridge(LOG, **env) as br:
        if not br.wait_for_log(f"🗜️ '{TENANT}'/'{TABLE}'", timeout=120):
            zb.bad("the chain never built"); return 1
        zb.ok(f"fixture: {ROWS} rows, chain built — the seed will take seconds, which is the kill window")

        # ── kill the host mid-seed, twice ────────────────────────────────────
        for i in range(KILLS):
            host = spawn()
            line = host.stdout.readline().strip()
            if line != "SYNC-START":
                zb.bad(f"kill {i + 1}: host never reached sync ({line!r})"); return failed + 1
            await asyncio.sleep(rnd.uniform(0.8, 2.5))   # inside applyChain, statistically
            host.kill()                                   # SIGKILL: no cleanup, like a dead tab
            host.wait(timeout=10)
            if not DB.exists():
                zb.bad(f"kill {i + 1}: no database file at all — the seed never touched disk before dying")
        zb.ok(f"{KILLS} hosts SIGKILLed mid-seed — no cleanup ran, the SQLite file is the survivor")

        # ── a fresh host must open the corpse's file and converge ────────────
        host = spawn()
        deadline = time.monotonic() + 240
        done = ""
        while time.monotonic() < deadline:
            line = host.stdout.readline().strip()
            if line.startswith("DONE"):
                done = line
                break
            if not line and host.poll() is not None:
                break
        host.wait(timeout=10)
        truth = int(zb.psql(f"SELECT count(*) FROM public.{TABLE}"))
        if not done:
            zb.bad("the fresh host never finished syncing the corpse's database"); return failed + 1
        _, total, distinct = done.split()
        if int(total) == truth and total == distinct:
            zb.ok(f"the fresh host opened the torn database and converged: {total} rows, all "
                  f"distinct, equal to PostgreSQL — the interrupted seed re-applied, never doubled")
        else:
            zb.bad(f"corruption or double-apply: client {total} rows / {distinct} distinct, "
                   f"PostgreSQL {truth}")
            failed += 1

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)

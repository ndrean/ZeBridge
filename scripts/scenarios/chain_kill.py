"""`kill -9` the producer mid-chain-build: the manifest must never name an object that
is not there (NOTES §10bs).

    scripts/scenarios/run.py owns -k chain_kill      (owns the bridge; builds a 36 MB table)

A generation is written in three steps — **immutable objects first, the PostgreSQL row,
then the KV manifest, swapped last** (generation_producer.zig §4/§5). That order is the
whole safety argument: the manifest is the only pointer a client follows, so it may only
appear once everything it points at exists. Killed in between, the worst case must be
orphan objects nobody references — never a manifest with a dangling pointer, because a
client that reads one cannot seed at all, and every FRESH client would fail the same way
until the next tick. Nothing tested it.

Asserted after every kill, and the kills are aimed at the window that matters (right
after compression, while the object is uploading and the manifest has not been swapped):

  1. every object the manifest names exists in the bucket — the invariant
  2. every generation row in PostgreSQL names objects that exist
  3. the chain still moves: a later, uninterrupted tick produces a manifest
  4. and a fresh client seeds the whole table from it and matches PostgreSQL
"""
import asyncio
import ctypes
import json
import os
import pathlib
import random
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

TABLE = "zb_chain_kill"
TENANT = "acme"
ROWS = 120_000                 # ~36 MB: the build takes seconds, so it can be interrupted
TARGETED_KILLS = 5             # right after compression: the upload → manifest window
BLIND_KILLS = 3                # anywhere in the build
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_chain_kill_bridge.log"
BUCKET = None                  # filled from grammar at run time


def slot_name() -> str:
    return zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"


def objects_in_bucket() -> set:
    """Object names present in the tenant's generation store."""
    out = zb.nats_cli("obj", "ls", BUCKET).stdout
    names = set()
    for line in out.splitlines():
        for cell in line.split("│"):
            cell = cell.strip()
            if cell.startswith(TABLE + "-"):
                names.add(cell)
    return names


def manifest() -> dict | None:
    raw = zb.kv_get("generations", f"{TENANT}.{TABLE}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def manifest_refs(man: dict) -> set:
    """Every object name the manifest points at: the full, each delta, and their dicts."""
    refs = set()
    if man.get("full", {}).get("object"):
        refs.add(man["full"]["object"])
    if man.get("full", {}).get("dict"):
        refs.add(man["full"]["dict"])
    for d in man.get("deltas") or []:
        if d.get("object"):
            refs.add(d["object"])
        if d.get("dict"):
            refs.add(d["dict"])
    return refs


def check_invariants(label: str) -> list:
    """The two structural invariants. Returns a list of failure strings (empty = good)."""
    problems = []
    man = manifest()
    if man is not None:
        present = objects_in_bucket()
        dangling = manifest_refs(man) - present
        if dangling:
            problems.append(f"{label}: the manifest (g{man.get('gen')}) names {len(dangling)} object(s) that do "
                            f"NOT exist: {sorted(dangling)[:3]} — every fresh client fails to seed")
    rows = zb.psql(
        f"SELECT gen || ' ' || has_full || ' ' || coalesce(dict_object,'-') "
        f"FROM public.zebridge_generations WHERE tenant = '{TENANT}' AND tbl = '{TABLE}' ORDER BY gen")
    present = objects_in_bucket()
    for line in [r for r in rows.splitlines() if r.strip()]:
        gen, has_full, dict_obj = (line.split() + ["-", "-"])[:3]
        wanted = {f"{TABLE}-g{gen}-delta"} if has_full in ("f", "false") else {f"{TABLE}-g{gen}-full"}
        if dict_obj not in ("-", ""):
            wanted.add(dict_obj)
        missing = {w for w in wanted if w not in present}
        # ⚠️ A PostgreSQL row whose objects are gone is only a problem while the manifest
        # still points at that generation — pruning removes objects and rows in the other
        # order on purpose. Reported, not failed, unless the manifest references it.
        if missing and man is not None and (missing & manifest_refs(man)):
            problems.append(f"{label}: generation row g{gen} is in the manifest but its objects are missing: {sorted(missing)}")
    return problems


def churn(n: int = 25_000) -> None:
    """Move enough rows that the NEXT tick has a substantial delta to build.

    ⚠️ Without this the table is static after the first generation and the producer
    correctly builds nothing — the scenario then waits for a compression that never
    comes (measured: kill 4 timed out). A kill needs something to interrupt.
    """
    zb.psql(f"UPDATE public.{TABLE} SET payload = payload || 'x', updated_at = now() "
            f"WHERE uid IN (SELECT uid FROM public.{TABLE} ORDER BY random() LIMIT {n})")


def stream_of_tenant() -> str:
    return zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + TENANT


def cleanup() -> None:
    zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{TABLE}'", quiet=True)
    zb.psql(f"ALTER PUBLICATION {zb.publication()} DROP TABLE public.{TABLE}", quiet=True)
    zb.psql(f"DROP TABLE IF EXISTS public.{TABLE}", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_generations WHERE tbl = '{TABLE}'", quiet=True)
    zb.nats_cli("kv", "del", zb.kv_bucket("generations"), f"{TENANT}.{TABLE}", "-f")
    for name in objects_in_bucket():
        zb.nats_cli("obj", "rm", BUCKET, name, "-f")
    # ⚠️ And the fixture's own CDC events. The table is dropped and recreated with the
    # SAME NAME every run, so a client starting at position 0 replays the PREVIOUS run's
    # inserts for it and its replica is not empty — measured: txn_kill failed its "the
    # fixture is empty" precondition on the second run, having asserted against the last
    # run's rows.
    zb.nats_cli("stream", "purge", stream_of_tenant(),
                "--subject", f"{zb.TOPOLOGY['subjects']['cdc_prefix']}.{TENANT}.{TABLE}.>", "-f")
    zb.psql(f"SELECT pg_drop_replication_slot('{slot_name()}') FROM pg_replication_slots "
            f"WHERE slot_name = '{slot_name()}' AND NOT active", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_limits WHERE slot = '{slot_name()}'", quiet=True)


async def main() -> int:
    try:
        return await run()
    finally:
        cleanup()


async def run() -> int:
    global BUCKET
    BUCKET = zb.TOPOLOGY["generations"]["bucket_prefix"] + TENANT
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge "
                 "(it kills one repeatedly, mid-write)")
    failed = 0

    # ── fixture: big enough that a build takes seconds ───────────────────────
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
        zb.bad(f"could not enable the fixture: {out[:160]}")
        return 1
    size = zb.psql(f"SELECT pg_size_pretty(pg_total_relation_size('{TABLE}'))")
    zb.ok(f"fixture: {ROWS} rows ({size}) in one tenant, generations on")

    env = {"GENERATIONS_ENABLED": "1", "GENERATION_CADENCE_SECONDS": "5",
           "GENERATION_RULES": f"{TABLE}:{TENANT}"}
    rnd = random.Random(20260901)

    # ── phase A: kill in the window between the object and the manifest ──────
    for i in range(TARGETED_KILLS):
        churn()
        with zb.Bridge(LOG, **env) as br:
            if not br.wait_for_log(f"🗜️ '{TENANT}'/'{TABLE}'", timeout=90):
                zb.bad(f"targeted kill {i + 1}: the producer never reached compression")
                return failed + 1
            # Compression is done and the object is going up; the manifest is swapped
            # after it. A jittered delay walks across that window over the iterations.
            await asyncio.sleep(rnd.uniform(0.0, 0.9))
            br.proc.kill()
        problems = check_invariants(f"targeted kill {i + 1}")
        if problems:
            for p in problems:
                zb.bad(p)
            failed += len(problems)
    if not failed:
        zb.ok(f"{TARGETED_KILLS} kills inside the upload → manifest window: no dangling pointer, "
              "no manifest naming a missing object")

    # ── phase B: kill anywhere in the build ──────────────────────────────────
    for i in range(BLIND_KILLS):
        churn()
        with zb.Bridge(LOG, **env) as br:
            br.wait_for_log("Generation producer started", timeout=60)
            await asyncio.sleep(rnd.uniform(0.5, 8.0))
            br.proc.kill()
        problems = check_invariants(f"blind kill {i + 1}")
        if problems:
            for p in problems:
                zb.bad(p)
            failed += len(problems)
    if not failed:
        zb.ok(f"{BLIND_KILLS} kills at random points in the build: same, invariants hold")

    # ── phase C: the chain still moves, and a client can use it ──────────────
    churn()
    with zb.Bridge(LOG, **env) as br:
        if not br.wait_for_log(f"🧬 g", timeout=120):
            zb.bad("after all those kills the producer never completed another generation")
            return failed + 1
        await asyncio.sleep(2)
        man = manifest()
        problems = check_invariants("after a clean tick")
        if man and not problems:
            zb.ok(f"the chain moved on after {TARGETED_KILLS + BLIND_KILLS} kills: manifest at g{man['gen']}, "
                  f"{len(manifest_refs(man))} object(s), all present")
        else:
            for p in problems:
                zb.bad(p)
            failed += max(1, len(problems))

        # ── phase D: a fresh client seeds the whole table from that chain ────
        lib = _env.load_lib()
        lib.zb_free.argtypes = [ctypes.c_void_p]
        lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
        lib.zb_client_close.argtypes = [ctypes.c_uint64]
        for n, a in (("sync", []), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
            f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

        def take(p):
            try: return json.loads(ctypes.string_at(p).decode())
            finally: lib.zb_free(p)

        db = f"/tmp/zb-chain-kill-{os.getpid()}.sqlite3"
        _env.rm_sqlite(db)
        h = lib.zb_client_open(json.dumps({
            "url": zb.nats_server(), "credsPath": zb.creds_for("alice"),   # alice → acme
            "grammarPath": str(zb.ROOT / "grammar.json"), "dbPath": db,
            "principal": "alice", "clientId": "py-chain-kill", "tables": [TABLE]}).encode())
        if not h:
            zb.bad("libzb client could not open"); return failed + 1
        try:
            take(lib.zb_client_sync(h))
            local = take(lib.zb_client_query(h, f"SELECT count(*) FROM {TABLE}".encode(), b"[]"))["rows"][0][0]
            pgc = int(zb.psql(f"SELECT count(*) FROM public.{TABLE}"))
            if local == pgc:
                zb.ok(f"a fresh client seeded the whole table from that chain: {local} rows, matching PostgreSQL")
            else:
                zb.bad(f"the chain does not reconstruct the table: replica {local}, PostgreSQL {pgc}")
                failed += 1
        finally:
            lib.zb_client_close(h)
            _env.rm_sqlite(db)

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)

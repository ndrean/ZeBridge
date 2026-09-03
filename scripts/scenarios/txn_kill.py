"""`kill -9` the bridge while it is delivering ONE large transaction (NOTES §10bt).

    scripts/scenarios/run.py owns -k txn_kill      (owns the bridge)

⚠️ The property is NOT "nothing partial escapes". A long transaction is handed to the
publisher in batches as it is decoded (`releaseTxSlots`: "hand this batch over so the
publisher can drain") precisely so a huge transaction cannot exhaust the ring — so part
of it IS on the wire before the commit is seen. What holds the line is the LSN: it is
acked only at `.commit`. Kill the bridge mid-delivery and PostgreSQL replays the WHOLE
transaction from the slot, so the already-published half arrives twice.

That is the design's own trade, stated in the producer's header: *"duplicates are
absorbed by the client's version-guarded upsert, gaps are unrecoverable"*. This asserts
both halves of it:

  1. NO LOSS — every row of the transaction reaches a client that was connected
     throughout, across the kill and the restart
  2. CONVERGENCE — that client's replica equals PostgreSQL afterwards, duplicates and all
  3. and the duplicates are counted and reported, because they are expected, bounded by
     JetStream's dedup window, and absorbed rather than prevented
"""
import asyncio
import ctypes
import json
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

TABLE = "zb_txn_kill"
TENANT = "acme"
PRINCIPAL = "alice"            # mapped to acme
ROWS = 30_000                  # one transaction, seconds of delivery
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_txn_kill_bridge.log"


def slot_name() -> str:
    return zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"


def stream() -> str:
    return zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + TENANT


def stream_seq() -> int:
    try:
        return json.loads(zb.nats_cli("stream", "info", stream(), "-j").stdout)["state"]["last_seq"]
    except Exception:  # noqa: BLE001
        return -1


def stream_of_tenant() -> str:
    return zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + TENANT


def cleanup() -> None:
    zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{TABLE}'", quiet=True)
    zb.psql(f"ALTER PUBLICATION {zb.publication()} DROP TABLE public.{TABLE}", quiet=True)
    zb.psql(f"DROP TABLE IF EXISTS public.{TABLE}", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_generations WHERE tbl = '{TABLE}'", quiet=True)
    zb.nats_cli("kv", "del", zb.kv_bucket("generations"), f"{TENANT}.{TABLE}", "-f")
    # ⚠️ NOT the schema key. `kv del` writes a DELETE tombstone, and a Direct Get returns
    # the newest revision — so the NEXT run's client, reading between this tombstone and
    # its own boot's fresh PUT, gets the tombstone, treats the schema as missing, and its
    # "fixture is empty" precondition fails on a phantom (measured 2026-09-03). The table
    # is recreated with the same shape every run and boot re-publishes the schema, so a
    # left-behind PUT is harmless and overwritten; a tombstone is not.
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
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge (it kills one mid-write)")
    failed = 0

    cleanup()
    zb.psql(f"CREATE TABLE public.{TABLE} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), "
            "tenant_id varchar(255) NOT NULL, payload text, deleted_at timestamptz, "
            "inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now())")
    out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable("
                  f"'{TABLE}', tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', "
                  f"tombstone_col => 'deleted_at', publication => '{zb.publication()}', dry_run => false)")
    if "ERROR" in out:
        zb.bad(f"could not enable the fixture: {out[:160]}"); return 1

    lib = _env.load_lib()
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

    def take(p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: lib.zb_free(p)

    db = f"/tmp/zb-txn-kill-{os.getpid()}.sqlite3"
    _env.rm_sqlite(db)
    h = 0
    try:
        with zb.Bridge(LOG) as br:
            if not br.wait_for_log("Replication started successfully", timeout=40):
                zb.bad("probe bridge did not start"); return 1
            h = lib.zb_client_open(json.dumps({
                "url": zb.nats_server(), "credsPath": zb.creds_for(PRINCIPAL),
                "grammarPath": str(zb.ROOT / "grammar.json"), "dbPath": db,
                "principal": PRINCIPAL, "clientId": "py-txn-kill", "tables": [TABLE]}).encode())
            if not h:
                zb.bad("libzb client could not open"); return 1

            def rows_local() -> int:
                take(lib.zb_client_poll(h, 300))
                r = take(lib.zb_client_query(h, f"SELECT count(*) FROM {TABLE}".encode(), b"[]"))
                return r["rows"][0][0] if r.get("rows") else -1

            take(lib.zb_client_sync(h))
            if rows_local() != 0:
                zb.bad("the fixture is not empty at the client"); return 1
            seq_before = stream_seq()
            zb.ok(f"client {PRINCIPAL}/{TENANT} connected and caught up on an empty {TABLE} "
                  f"({stream()} at {seq_before})")

            # ── ONE transaction ──────────────────────────────────────────────
            # PostgreSQL decodes a transaction only after it COMMITS, so "mid-transaction"
            # for the bridge means mid-DELIVERY of an already-committed one.
            zb.psql(f"INSERT INTO public.{TABLE} (tenant_id, payload) "
                    f"SELECT '{TENANT}', repeat(md5(g::text), 2) FROM generate_series(1, {ROWS}) g")
            committed = int(zb.psql(f"SELECT count(*) FROM public.{TABLE}"))
            if committed != ROWS:
                zb.bad(f"the transaction did not commit whole: {committed}"); return 1

            # ── kill while it is PART WAY through that delivery ──────────────
            deadline = time.monotonic() + 60
            seen = 0
            while time.monotonic() < deadline:
                seen = rows_local()
                if 0 < seen < ROWS * 0.8:
                    break
                if seen >= ROWS:
                    break
                await asyncio.sleep(0.2)
            if not 0 < seen < ROWS:
                zb.bad(f"could not catch the delivery mid-flight (client saw {seen} of {ROWS}) — "
                       "the transaction is too small to interrupt on this machine")
                return 1
            br.proc.kill()
            seq_at_kill = stream_seq()
            zb.ok(f"killed the bridge mid-delivery: the client had {seen} of {ROWS} rows, "
                  f"{stream()} at {seq_at_kill} — part of the transaction is already on the wire")

        # ── restart: PostgreSQL replays the whole transaction ────────────────
        with zb.Bridge(LOG.with_suffix(".2.log")) as br:
            if not br.wait_for_log("Replication started successfully", timeout=40):
                zb.bad("bridge did not come back"); return failed + 1
            deadline = time.monotonic() + 120
            local = 0
            while time.monotonic() < deadline:
                local = rows_local()
                if local >= ROWS:
                    break
                await asyncio.sleep(0.5)
            pgc = int(zb.psql(f"SELECT count(*) FROM public.{TABLE}"))
            if local == ROWS == pgc:
                zb.ok(f"NO LOSS: every one of the {ROWS} rows reached the client across the kill "
                      f"and the restart, and the replica equals PostgreSQL ({local} = {pgc})")
            else:
                zb.bad(f"rows were lost or never redelivered: replica {local}, PostgreSQL {pgc}, expected {ROWS} "
                       "— an unacked transaction must replay whole")
                failed += 1

            distinct = take(lib.zb_client_query(h, f"SELECT count(DISTINCT uid) FROM {TABLE}".encode(), b"[]"))["rows"][0][0]
            if distinct == local:
                zb.ok(f"and no row was duplicated INTO the replica: {distinct} distinct uids — "
                      "the version-guarded upsert absorbed the replayed half")
            else:
                zb.bad(f"the replica has duplicate rows: {local} rows for {distinct} uids")
                failed += 1

            seq_end = stream_seq()
            replayed = seq_end - seq_at_kill
            print(f"  ⓘ  the stream carries the replay: {seq_before} → {seq_at_kill} → {seq_end} "
                  f"({replayed} message(s) after the kill) — duplicates on the wire are expected and "
                  "absorbed by the client, JetStream dedup covers a 120 s window")
    finally:
        if h:
            lib.zb_client_close(h)
        _env.rm_sqlite(db)

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)

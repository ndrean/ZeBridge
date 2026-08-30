"""The replication slot is INVALIDATED (bridge down past max_slot_wal_keep_size) —
the one outage PostgreSQL cannot retain through. What must happen (NOTES §10bm):

  1. a libzb client is in sync (position P on its tenant stream), then the bridge stops
  2. with the bridge down, more WAL is written than the slot may keep → wal_status = lost
  3. the bridge REFUSES to boot: a FATAL naming the slot and the recovery, then exit —
     not a reconnect loop retrying a dead slot every two seconds
  4. recovery: drop the slot; the bridge, started once with ZB_FEED_RESTART=1, creates a
     new one — a NEW FEED: it recreates the
     CDC streams (numbering restarts), drops the stale chain manifests and forces fresh
     fulls, because nothing between the old slot's last LSN and now was ever published
     and the streams' numbering would show no hole
  5. the client syncs: its position is BEYOND the recreated stream's last_seq — the gap
     rule fires, it re-seeds from a fresh full, and the replica equals PostgreSQL,
     rows written during the loss included

    scripts/scenarios/run.py owns -k slot_loss     (owns the bridge; changes a PG setting and restores it)

⚠️ max_slot_wal_keep_size is cluster-wide: every INACTIVE slot that is behind is lost with
the probe's — the long-running bridge's slot included when it is stopped for this run.
Its recovery is the same one this scenario proves (drop the slot, ZB_FEED_RESTART=1 once).
"""
import ctypes
import json
import os
import pathlib
import subprocess
import sys
import time
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

TABLE = "test_types"
KEEP = "64MB"
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_slot_loss_bridge.log"
SLOT = "zb_probe"


def slot_status() -> str:
    return zb.psql(f"SELECT wal_status FROM pg_replication_slots WHERE slot_name = '{SLOT}'", quiet=True).strip()


def stream_state(stream: str) -> dict:
    return json.loads(zb.nats_cli("stream", "info", stream, "-j").stdout)["state"]


def main() -> int:
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge and invalidates a slot")
    who = os.environ.get("ZB_CLIENT_PRINCIPAL", "omar")
    tenant = zb.tenant_of(who)
    stream = zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + tenant
    marker = str(uuid.uuid4())[:8]
    failed = 0

    lib = _env.load_lib()
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

    def take(p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: lib.zb_free(p)

    def q1(h, sql, params=()):
        r = take(lib.zb_client_query(h, sql.encode(), json.dumps(list(params)).encode()))
        return r["rows"][0][0] if r.get("rows") else None

    db = f"/tmp/zb-slot-loss-{os.getpid()}.sqlite3"
    old_keep = zb.psql("SHOW max_slot_wal_keep_size").strip()
    h = 0
    try:
        # ── 1. a client in sync, then the bridge goes away ────────────────────
        with zb.Bridge(LOG) as bridge:
            if not bridge.wait_for_log("Replication started successfully", timeout=40):
                zb.bad("probe bridge did not start"); return 1
            h = lib.zb_client_open(json.dumps({"url": zb.nats_server(), "credsPath": zb.creds_for(who), "grammarPath": str(zb.ROOT / "grammar.json"),
                                               "dbPath": db, "principal": who, "clientId": "py-slot-loss", "tables": ["users", TABLE]}).encode())
            if not h:
                zb.bad("libzb client could not open"); return 1
            take(lib.zb_client_sync(h)); take(lib.zb_client_poll(h, 500))
            # A position exists only once something was drained: right after a feed restart
            # the tenant stream is empty. One row through the pipe gives the client a
            # position to be stranded on.
            zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                    f"VALUES (gen_random_uuid(), 'anchor {marker}', '{tenant}', now(), now())")
            for _ in range(40):
                if take(lib.zb_client_poll(h, 500)).get("applied", 0): break
            pos_raw = q1(h, "SELECT last_seq FROM _zbz_stream_seq WHERE stream = ?", [stream])
            if pos_raw is None:
                zb.bad(f"the client never got a position on {stream} — is the probe bridge routing {TABLE}?"); return 1
            pos = int(pos_raw)
            zb.ok(f"client {who}/{tenant} in sync at position {pos} on {stream}; bridge stopping")
        # ── 2. more WAL than the slot may keep ────────────────────────────────
        zb.psql(f"ALTER SYSTEM SET max_slot_wal_keep_size = '{KEEP}'"); zb.psql("SELECT pg_reload_conf()")
        zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                f"SELECT gen_random_uuid(), 'lost {marker} ' || g, '{tenant}', now(), now() FROM generate_series(1, 3) g")
        zb.psql("CREATE TABLE IF NOT EXISTS public.zb_wal_filler (id bigserial PRIMARY KEY, pad text)")
        for _ in range(6):
            zb.psql("INSERT INTO public.zb_wal_filler (pad) SELECT repeat('x', 1000) FROM generate_series(1, 20000)")
            zb.psql("SELECT pg_switch_wal()")
        zb.psql("CHECKPOINT")
        for _ in range(20):
            if slot_status() == "lost": break
            zb.psql("INSERT INTO public.zb_wal_filler (pad) SELECT repeat('y', 1000) FROM generate_series(1, 20000)")
            zb.psql("SELECT pg_switch_wal()"); zb.psql("CHECKPOINT")
        st = slot_status()
        if st == "lost":
            zb.ok(f"slot '{SLOT}' invalidated by PostgreSQL (wal_status = lost) while the bridge was down; 3 rows committed in the hole")
        else:
            zb.bad(f"could not invalidate the slot (wal_status = {st!r}) — raise the filler or lower KEEP"); return 1

        # ── 3. the bridge refuses, once, loudly ───────────────────────────────
        with zb.Bridge(LOG.with_suffix(".2.log")) as bridge:
            refused = bridge.wait_for_log("INVALIDATED", timeout=40)
            rc = bridge.wait_for_exit(timeout=15)
            if refused and rc not in (None, 0):
                zb.ok(f"bridge refused to boot on the lost slot (exit {rc}) with the recovery in its FATAL line — no reconnect loop")
            else:
                zb.bad(f"bridge did not refuse the lost slot (refused={refused}, exit={rc})"); failed += 1

        # ── 4. recovery: a new slot is a new feed ─────────────────────────────
        before = stream_state(stream)
        zb.psql(f"SELECT pg_drop_replication_slot('{SLOT}')")
        with zb.Bridge(LOG.with_suffix(".3.log"), ZB_FEED_RESTART="1") as bridge:
            if not bridge.wait_for_log("Replication started successfully", timeout=40):
                zb.bad("bridge did not come back on a fresh slot"); return failed + 1
            restarted = bridge.wait_for_log("new feed", timeout=10)
            after = stream_state(stream)
            if restarted and after["last_seq"] < before["last_seq"]:
                zb.ok(f"new slot → new feed: {stream} recreated (last_seq {before['last_seq']} → {after['last_seq']})")
            else:
                zb.bad(f"the feed did not restart: log={restarted}, {stream} last_seq {before['last_seq']} → {after['last_seq']}"); failed += 1
            # a fresh full must exist before a client can re-seed: the producer's first tick
            cadence = int(os.environ.get("GENERATION_CADENCE_SECONDS", "60"))
            deadline = time.monotonic() + cadence + 30
            fresh = False
            while time.monotonic() < deadline:
                man = zb.kv_get("generations", f"{tenant}.{TABLE}")
                if man and int(json.loads(man).get("cutoff_seq") or 0) <= stream_state(stream)["last_seq"]:
                    fresh = True; break
                time.sleep(2)
            if fresh:
                zb.ok("a fresh chain was built on the new feed (manifest cutoff_seq within the recreated stream)")
            else:
                zb.bad("no fresh chain within one cadence — a client could only re-seed from a stale manifest"); failed += 1

            # ── 5. the client returns to a feed that restarted under it ───────
            take(lib.zb_client_sync(h))
            newpos = int(q1(h, "SELECT last_seq FROM _zbz_stream_seq WHERE stream = ?", [stream]) or 0)
            got = q1(h, "SELECT count(*) FROM test_types WHERE some_text LIKE ?", [f"lost {marker} %"])
            pg = int(zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE some_text LIKE 'lost {marker} %'"))
            total_local = q1(h, "SELECT count(*) FROM test_types WHERE deleted_at IS NULL AND tenant_id = ?", [tenant])
            total_pg = int(zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE deleted_at IS NULL AND tenant_id = '{tenant}'"))
            if got == pg == 3 and total_local == total_pg and newpos <= after["last_seq"] + 50:
                zb.ok(f"client took the gap (position {pos} was beyond the new feed), re-seeded: {got}/{pg} hole rows, {total_local} = {total_pg} live rows, position now {newpos}")
            else:
                zb.bad(f"client did not converge: hole rows {got}/{pg}, live {total_local} vs {total_pg}, position {pos} → {newpos}"); failed += 1
    finally:
        if h: lib.zb_client_close(h)
        _env.rm_sqlite(db)
        zb.psql(f"ALTER SYSTEM SET max_slot_wal_keep_size = '{old_keep}'" if old_keep else "ALTER SYSTEM RESET max_slot_wal_keep_size", quiet=True)
        zb.psql("SELECT pg_reload_conf()", quiet=True)
        zb.psql("DROP TABLE IF EXISTS public.zb_wal_filler", quiet=True)
        zb.psql(f"DELETE FROM public.{TABLE} WHERE some_text LIKE 'lost {marker} %' OR some_text = 'anchor {marker}'", quiet=True)
        zb.psql(f"SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name = '{SLOT}' AND NOT active", quiet=True)
        # ⚠️ Collateral: max_slot_wal_keep_size is cluster-wide, so EVERY inactive slot that
        # was behind is lost too — the long-running bridge's, if it was stopped for this
        # run (measured: my_slot, 2026-08-29). Say so, with the recovery.
        others = [l for l in zb.psql("SELECT slot_name FROM pg_replication_slots WHERE wal_status = 'lost'", quiet=True).splitlines() if l]
        if others:
            print(f"  ⚠️  other slot(s) invalidated by this run: {others} — for each: SELECT pg_drop_replication_slot(...); "
                  f"then start its bridge ONCE with ZB_FEED_RESTART=1 (its clients re-seed)")
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

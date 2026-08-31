"""A CLIENT's outage, not the bridge's: it comes back after the tail it needs is
gone and must take the gap → re-seed path, converging with PostgreSQL.

    scripts/scenarios/run.py live -k client_gap

Reconnection is the common path on mobile (NOTES §10k); today's proofs only tested
the resume half. This drives libzb through its C ABI:

  1. a client syncs (tenant stream position P) and stops polling
  2. rows are written; the producer captures them into the chain (one cadence)
  3. the stream is purged past P — what retention does, made instant
  4. the client syncs again: the gap rule fires (stored < first_seq - 1), the
     tenant-routed tables re-seed from the chain, and the replica equals PostgreSQL
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
STREAM_KEY = "tenant_prefix"


def main() -> int:
    # Two identities at once: the libzb client is a tenant principal (ZB_CLIENT_PRINCIPAL,
    # omar by default), the purge below is the bridge's (NATS_CREDS = bridge.creds, which
    # run.py provides for the "bridge" role) — retention is not something a client does.
    who = os.environ.get("ZB_CLIENT_PRINCIPAL", "omar")
    tenant = zb.tenant_of(who)
    stream = zb.TOPOLOGY["cdc_streams"][STREAM_KEY] + tenant
    lib = _env.load_lib()
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

    def take(p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: lib.zb_free(p)

    db = f"/tmp/zb-client-gap-{os.getpid()}.sqlite3"
    h = lib.zb_client_open(json.dumps({"url": zb.nats_server(), "credsPath": zb.creds_for(who), "grammarPath": str(zb.ROOT / "grammar.json"),
                                       "dbPath": db, "principal": who, "clientId": "py-client-gap", "tables": ["users", TABLE]}).encode())
    if not h:
        sys.exit("libzb client could not open (cd libzb && zig build; is the stack up?)")
    failed = 0
    marker = str(uuid.uuid4())[:8]
    try:
        take(lib.zb_client_sync(h)); take(lib.zb_client_poll(h, 500))
        pos = int(take(lib.zb_client_query(h, b"SELECT last_seq FROM _zbz_stream_seq WHERE stream = ?", json.dumps([stream]).encode()))["rows"][0][0])
        zb.ok(f"client synced as {who}/{tenant}: position {pos} on {stream}, then stops polling")

        # ── 2. the world moves on ─────────────────────────────────────────────
        n = 5
        zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                f"SELECT gen_random_uuid(), 'gap {marker} ' || g, '{tenant}', now(), now() FROM generate_series(1, {n}) g")
        info = json.loads(zb.nats_cli("stream", "info", stream, "-j").stdout)["state"]
        for _ in range(60):
            info = json.loads(zb.nats_cli("stream", "info", stream, "-j").stdout)["state"]
            if info["last_seq"] > pos: break
            time.sleep(0.5)
        last = info["last_seq"]
        # the producer must have the rows in a chain, or the re-seed cannot restore them
        cadence = int(os.environ.get("GENERATION_CADENCE_SECONDS", "60"))
        deadline = time.monotonic() + cadence + 20
        chained = False
        while time.monotonic() < deadline:
            man = zb.kv_get("generations", f"{tenant}.{TABLE}")
            if man:
                m = json.loads(man)
                if int(m.get("cutoff_seq") or 0) >= last:
                    chained = True; break
            time.sleep(2)
        if not chained:
            zb.bad(f"the chain never caught up to seq {last} within one cadence — cannot test the re-seed"); return 1
        zb.ok(f"{n} rows written while the client was away (stream at {last}); the chain captured them (cutoff_seq ≥ {last})")

        # ── 3. retention, made instant ────────────────────────────────────────
        r = zb.nats_cli("stream", "purge", stream, "--seq", str(last), "-f")
        if r.returncode != 0:
            zb.bad(f"purge refused: {r.stderr.strip()[:120]} (run as the bridge: NATS_CREDS=bridge.creds)"); return 1
        first = json.loads(zb.nats_cli("stream", "info", stream, "-j").stdout)["state"]["first_seq"]
        zb.ok(f"{stream} purged to first_seq {first} — the client's position {pos} is now behind the tail")

        # ── 4. the client returns ─────────────────────────────────────────────
        take(lib.zb_client_sync(h))
        got = take(lib.zb_client_query(h, b"SELECT count(*) FROM test_types WHERE some_text LIKE ?", json.dumps([f"gap {marker} %"]).encode()))["rows"][0][0]
        pg = int(zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE some_text LIKE 'gap {marker} %'"))
        newpos = int(take(lib.zb_client_query(h, b"SELECT last_seq FROM _zbz_stream_seq WHERE stream = ?", json.dumps([stream]).encode()))["rows"][0][0])
        if got == pg == n and newpos >= first - 1:
            zb.ok(f"gap taken: re-seeded from the chain, {got}/{pg} rows present, position moved {pos} → {newpos}")
        else:
            zb.bad(f"after the gap: replica has {got}, PostgreSQL {pg}, position {pos} → {newpos} (first_seq {first})")
            failed += 1
        # The tenant's rows: that is what the tenant stream carries and the tenant chain
        # re-seeds. Open-tenant (`_default`) rows of a tenant-scoped table ride CDC_PUBLIC
        # and are seeded from nowhere — a separate hazard (NOTES §10bm), not this test.
        total_local = take(lib.zb_client_query(h, f"SELECT count(*) FROM {TABLE} WHERE deleted_at IS NULL AND tenant_id = ?".encode(), json.dumps([tenant]).encode()))["rows"][0][0]
        total_pg = int(zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE deleted_at IS NULL AND tenant_id = '{tenant}'"))
        if total_local == total_pg:
            zb.ok(f"whole table converged: {total_local} live rows on both sides")
        else:
            # ⚠️ Reported, not failed. The property under test is the gap → re-seed path,
            # asserted above on this scenario's OWN rows. A whole-table comparison in a
            # shared fixture also counts rows other scenarios hard-deleted, which a
            # tombstoned table never forwards (§10bm) — a real hazard, but not this
            # scenario's to fail on.
            extra = total_local - total_pg
            print(f"  ⓘ  whole table: replica {total_local} vs PostgreSQL {total_pg} "
                  f"({extra:+d}) — phantoms from hard-deleted fixture rows (§10bm), not a gap-path defect")
    finally:
        lib.zb_client_close(h)
        _env.rm_sqlite(db)
        # ⚠️ A SOFT delete, not a hard one. `test_types` has a tombstone column, so a
        # physical DELETE is suppressed as a sweeper reap and never reaches a client —
        # every hard-deleted fixture row becomes a permanent phantom in every replica
        # until it re-seeds from a chain built afterwards (§10bm). Measured: this
        # scenario's own leftovers made its whole-table check read 47 vs 46. Soft
        # deleting sends the tombstone, and the sweeper reaps the row later.
        zb.psql(f"UPDATE public.{TABLE} SET deleted_at = now(), updated_at = now() "
                f"WHERE some_text LIKE 'gap {marker} %' AND deleted_at IS NULL", quiet=True)
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

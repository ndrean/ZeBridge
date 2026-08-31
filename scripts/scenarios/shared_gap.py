"""A tenant-scoped table's SHARED rows ride CDC_PUBLIC — so a gap there must re-seed it
(NOTES §10bq).

    scripts/scenarios/run.py live -k shared_gap

`zb_reader_all` admits `tenant_col = <open tenant>` as well as the reader's own tenant,
so a tenant-scoped table holds two kinds of row: the tenant's, published to
`CDC_<tenant>`, and the OPEN tenant's — shared, readable by everyone, published to
`CDC_PUBLIC`. The chain carries both (the producer reads under that same policy), but
the client scoped each table to ONE stream: a gap on CDC_PUBLIC re-seeded the public
TABLES and left every tenant-scoped table's shared rows silently stale. Both routes
count now (`scopeSeeding`'s `sharedRoute`).

  1. a client syncs and stops polling
  2. a shared row (`tenant_id = <open tenant>`) is written to the tenant-scoped table,
     and the chain captures it
  3. CDC_PUBLIC is purged past the client's position — what retention does, made instant
  4. the client syncs: the gap on CDC_PUBLIC re-seeds the TENANT-SCOPED table too, and
     the shared row is there

⚠️ Purges CDC_PUBLIC, so every client re-seeds from its chain afterwards — the same
disturbance `client_gap.py` causes on a tenant stream.
"""
import ctypes
import json
import os
import pathlib
import sys
import time
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

TABLE = "test_types"


def main() -> int:
    who = os.environ.get("ZB_CLIENT_PRINCIPAL", "omar")
    tenant = zb.tenant_of(who)
    open_tenant = zb.TOPOLOGY.get("open_tenant", "_default")
    public = zb.TOPOLOGY["cdc_streams"]["public"]
    if tenant == open_tenant:
        sys.exit(f"principal '{who}' resolves to the open tenant — this needs a tenant-scoped one")
    if not zb.rules(TABLE).get("tenant"):
        sys.exit(f"{TABLE} is not tenant-scoped — nothing to prove here")

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

    def stream_state():
        return json.loads(zb.nats_cli("stream", "info", public, "-j").stdout)["state"]

    db = f"/tmp/zb-shared-gap-{os.getpid()}.sqlite3"
    _env.rm_sqlite(db)
    marker = str(uuid.uuid4())[:8]
    shared_uid = str(uuid.uuid4())
    failed = 0
    h = lib.zb_client_open(json.dumps({
        "url": zb.nats_server(), "credsPath": zb.creds_for(who), "grammarPath": str(zb.ROOT / "grammar.json"),
        "dbPath": db, "principal": who, "clientId": "py-shared-gap", "tables": ["users", TABLE]}).encode())
    if not h:
        sys.exit("libzb client could not open (cd libzb && zig build; is the stack up?)")
    try:
        take(lib.zb_client_sync(h)); take(lib.zb_client_poll(h, 800))
        pos = int(q1(h, "SELECT last_seq FROM _zbz_stream_seq WHERE stream = ?", [public]) or 0)
        zb.ok(f"client {who}/{tenant} synced; position {pos} on {public}, then stops polling")

        # ── 2. a SHARED row, written while the client is away ────────────────
        zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                f"VALUES ('{shared_uid}', 'shared {marker}', '{open_tenant}', now(), now())")
        if zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE uid = '{shared_uid}'").strip() != "1":
            zb.bad("could not write a shared row"); return 1
        shared_seq = stream_state()["last_seq"]
        # ⚠️ Push the head well past that row. Purging only to it would leave its own
        # message in the stream (a purge removes what is BELOW the sequence) and the
        # client's position one short of a gap — it would then read the row straight off
        # CDC and the re-seed under test would never run. The first draft passed that way
        # (2026-08-31): position 15, first_seq 16, `15 < 15` is false.
        for i in range(5):
            zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                    f"VALUES (gen_random_uuid(), 'filler {marker} {i}', '{open_tenant}', now(), now())")
        cadence = int(os.environ.get("GENERATION_CADENCE_SECONDS", "60"))
        deadline = time.monotonic() + cadence + 45
        captured = False
        while time.monotonic() < deadline:
            man = zb.kv_get("generations", f"{tenant}.{TABLE}")
            if man and marker_in_chain(man, marker, tenant):
                captured = True
                break
            time.sleep(3)
        if not captured:
            zb.bad(f"the {tenant} chain never captured the shared row within a cadence — "
                   "without it the re-seed cannot restore it")
            return 1
        zb.ok(f"a shared row ({open_tenant}) was written and the {tenant} chain captured it "
              "— the chain carries both kinds, because the producer reads under zb_reader_all")

        # ── 3. retention on CDC_PUBLIC, made instant ─────────────────────────
        head = stream_state()["last_seq"]
        r = zb.nats_cli("stream", "purge", public, "--seq", str(head), "-f")
        if r.returncode != 0:
            zb.bad(f"purge refused: {r.stderr.strip()[:120]} (run as the bridge)"); return 1
        first = stream_state()["first_seq"]
        if first <= shared_seq or pos >= first - 1:
            zb.bad(f"the purge did not isolate the chain: shared row at seq {shared_seq}, first_seq {first}, "
                   f"client at {pos} — it could still read the row from CDC, so this proves nothing")
            return 1
        zb.ok(f"{public} purged to first_seq {first}: the shared row's own event (seq {shared_seq}) is GONE "
              f"and the client's position {pos} is behind the tail — only a re-seed can restore it")

        # ── 4. the client returns: the gap must re-seed the TENANT-SCOPED table
        take(lib.zb_client_sync(h))
        has_shared = q1(h, "SELECT count(*) FROM test_types WHERE uid = ?", [shared_uid])
        if has_shared == 1:
            zb.ok(f"the CDC_PUBLIC gap re-seeded '{TABLE}' as well: the shared row is in the replica "
                  "(before §10bq only public TABLES were re-seeded and this row stayed missing)")
        else:
            zb.bad(f"'{TABLE}' was not re-seeded by a gap on {public}: shared row present={has_shared} "
                   "— its shared rows are stale and nothing will correct them")
            failed += 1

        local = q1(h, "SELECT count(*) FROM test_types WHERE tenant_id = ? AND deleted_at IS NULL", [open_tenant])
        pgc = int(zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE tenant_id = '{open_tenant}' "
                          "AND deleted_at IS NULL").strip() or 0)
        if local == pgc:
            zb.ok(f"every shared row converged: {local} on both sides")
        else:
            zb.bad(f"shared rows diverge: replica {local}, PostgreSQL {pgc}")
            failed += 1
    finally:
        lib.zb_client_close(h)
        _env.rm_sqlite(db)
        # Soft delete: a physical one on a tombstoned table never reaches a client (§10bm).
        # `%marker%`, not `%marker`: the filler rows are 'filler <marker> <n>' and end in a
        # digit, so an anchored pattern left five shared rows behind every run.
        zb.psql(f"UPDATE public.{TABLE} SET deleted_at = now(), updated_at = now() "
                f"WHERE some_text LIKE '%{marker}%' AND deleted_at IS NULL", quiet=True)
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


def marker_in_chain(manifest_json: str, marker: str, tenant: str) -> bool:
    """Is the shared row in the tenant's chain yet? Decoded rather than assumed: the
    whole point is that a TENANT chain carries OPEN-TENANT rows."""
    import subprocess
    try:
        man = json.loads(manifest_json)
    except json.JSONDecodeError:
        return False
    steps = ([man["full"]] if man.get("full") else []) + (man.get("deltas") or [])
    for step in steps:
        out = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb-shared-chain"
        zb.nats_cli("obj", "get", man["bucket"], step["object"], "-O", str(out) + ".zst", "-f")
        args = ["zstd", "-d", "-q", "-f", str(out) + ".zst", "-o", str(out) + ".bin"]
        if step.get("dict"):
            zb.nats_cli("obj", "get", man["bucket"], step["dict"], "-O", str(out) + ".dict", "-f")
            args = ["zstd", "-d", "-q", "-f", "-D", str(out) + ".dict", str(out) + ".zst", "-o", str(out) + ".bin"]
        if subprocess.run(args, capture_output=True).returncode != 0:
            continue
        if marker.encode() in (out.with_suffix(".bin")).read_bytes():
            return True
    return False


if __name__ == "__main__":
    sys.exit(main())

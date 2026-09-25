"""A consumer filtered to its own tables jumps over the other tables' messages — and that
is not a gap (NOTES §10ja).

    scripts/scenarios/run.py live -k filtered_gap

A client's CDC consumer on a tenant stream is filtered to the tables it follows (§10gm),
so every write to another table of the same tenant is a jump in stream sequence. Until
2026-09-25 both clients read such a jump as "the stream pruned under me" and re-seeded:
a client following test_types_v7 re-fetched 3M rows three times because test_types_v4
got a burst. The rule now asks the stream whether it still holds what follows the
position; only a real prune is a gap.

  1. a libzb client follows note_t only, syncs, and records its position
  2. 200 single-row transactions go into test_types on the same tenant — 200+ messages
     the client's filtered consumer never sees
  3. one note_t row is written
  4. the client polls: the row arrives, nothing re-seeds, the position reaches the tail
"""
import ctypes
import json
import os
import pathlib
import sys
import tempfile
import time
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

FOLLOWED = "note_t"
NOISE = "test_types"
NOISE_TXNS = 200


def main() -> int:
    who = os.environ.get("ZB_CLIENT_PRINCIPAL", "omar")
    tenant = zb.tenant_of(who)
    stream = zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + tenant
    lib = _env.load_lib()
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

    def take(p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: lib.zb_free(p)

    def position() -> int:
        rows = take(lib.zb_client_query(h, b"SELECT last_seq FROM _zbz_stream_seq WHERE stream = ?", json.dumps([stream]).encode()))["rows"]
        return int(rows[0][0]) if rows else 0

    db = f"/tmp/zb-filtered-gap-{os.getpid()}.sqlite3"
    h = lib.zb_client_connect(json.dumps({"natsUrl": zb.nats_server(), "credsPath": zb.creds_for(who), "dbPath": db, "principal": who, "clientId": "py-filtered-gap", "tables": [FOLLOWED]}).encode())
    if not h:
        sys.exit("libzb client could not open (cd libzb && zig build; is the stack up?)")
    failed = 0
    marker = str(uuid.uuid4())[:8]
    try:
        take(lib.zb_client_sync(h)); take(lib.zb_client_poll(h, 500))
        pos = position()
        zb.ok(f"client follows {FOLLOWED} only, as {who}/{tenant}: position {pos} on {stream}")

        # ── 2. another table of the same tenant gets a burst ─────────────────
        for i in range(NOISE_TXNS):
            zb.psql(f"INSERT INTO public.{NOISE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                    f"VALUES (gen_random_uuid(), 'noise {marker} {i}', '{tenant}', now(), now())", quiet=True)
        # ── 3. then one row of the followed table ─────────────────────────────
        zb.psql(f"INSERT INTO public.{FOLLOWED} (uid, txt, tenant_id, updated_at) "
                f"VALUES (gen_random_uuid(), 'note {marker}', '{tenant}', now())", quiet=True)
        last = 0
        for _ in range(60):
            last = json.loads(zb.nats_cli("stream", "info", stream, "-j").stdout)["state"]["last_seq"]
            if last >= pos + NOISE_TXNS + 1: break
            time.sleep(0.5)
        zb.ok(f"{NOISE_TXNS} {NOISE} commits then one {FOLLOWED} row: {stream} at {last} (the client's consumer sees only the last)")

        # ── 4. the client polls ───────────────────────────────────────────────
        # libzb says what it concluded on stderr ("N message(s) pruned under the live
        # consumer … re-seeding"), and a small table's re-seed changes nothing a query
        # can see — measured: the unfixed library took the false gap and every other
        # check here still passed. So the poll runs with this process's fd 2 captured.
        reseeded = []
        got = 0
        cap = tempfile.TemporaryFile()
        saved = os.dup(2); sys.stderr.flush(); os.dup2(cap.fileno(), 2)
        try:
            for _ in range(20):
                r = take(lib.zb_client_poll(h, 500))
                reseeded += r.get("seeded") or []
                got = take(lib.zb_client_query(h, f"SELECT count(*) FROM {FOLLOWED} WHERE txt = ?".encode(), json.dumps([f"note {marker}"]).encode()))["rows"][0][0]
                if got: break
        finally:
            os.dup2(saved, 2); os.close(saved)
        cap.seek(0)
        said = [l for l in cap.read().decode(errors="replace").splitlines() if "pruned under" in l or "re-seeding" in l]
        for l in said: print(f"  libzb: {l}")
        newpos = position()
        if said:
            zb.bad(f"the jump over {newpos - pos - 1} other-table message(s) was taken for a gap: {said[0][:120]}")
            failed += 1
        elif got == 1 and not reseeded and newpos >= last:
            zb.ok(f"the {FOLLOWED} row arrived, nothing re-seeded, position {pos} → {newpos} (the jump over {newpos - pos - 1} other-table message(s) was no gap)")
        else:
            zb.bad(f"row present: {got}, re-seeded: {reseeded or 'nothing'}, position {pos} → {newpos} (stream at {last})")
            failed += 1
    finally:
        lib.zb_client_close(h)
        _env.rm_sqlite(db)
        # Soft deletes, like client_gap.py: both tables carry a tombstone column.
        zb.psql(f"UPDATE public.{NOISE} SET deleted_at = now(), updated_at = now() WHERE some_text LIKE 'noise {marker} %' AND deleted_at IS NULL", quiet=True)
        zb.psql(f"DELETE FROM public.{FOLLOWED} WHERE txt = 'note {marker}'", quiet=True)
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

"""The index card from Python (NOTES §10): open, sync, query, mutate, flush, close.

    cd libzb && zig build && python3 python/index_card.py

Drives the C ABI exactly as a host binding would — ctypes, five declarations, JSON in
and out — against the native stack as `omar`. Asserts the write round-trips into the
read-only connection, and that a write THROUGH that connection is refused.
"""
import ctypes, json, os, sys, time, uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
lib = ctypes.CDLL(os.path.join(ROOT, "zig-out", "lib", "libzbcore.dylib"))
lib.zb_free.argtypes = [ctypes.c_void_p]
lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
lib.zb_client_close.restype, lib.zb_client_close.argtypes = ctypes.c_int, [ctypes.c_uint64]
lib.zb_client_sync.restype, lib.zb_client_sync.argtypes = ctypes.c_void_p, [ctypes.c_uint64]
lib.zb_client_query.restype, lib.zb_client_query.argtypes = ctypes.c_void_p, [ctypes.c_uint64, ctypes.c_char_p, ctypes.c_char_p]
lib.zb_client_mutate.restype, lib.zb_client_mutate.argtypes = ctypes.c_void_p, [ctypes.c_uint64, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]
lib.zb_client_flush.restype, lib.zb_client_flush.argtypes = ctypes.c_void_p, [ctypes.c_uint64, ctypes.c_uint64]

def take(p):
    """Read a returned C string as JSON and free it — the caller owns every result."""
    if not p:
        raise RuntimeError("NULL from the library")
    try:
        return json.loads(ctypes.string_at(p).decode())
    finally:
        lib.zb_free(p)

def query(h, sql, params=()):
    r = take(lib.zb_client_query(h, sql.encode(), json.dumps(list(params)).encode()))
    if "error" in r:
        return r
    return [dict(zip(r["columns"], row)) for row in r["rows"]]

db = "/tmp/zb-index-card.sqlite3"
for f in (db, db + "-wal", db + "-shm"):
    try: os.unlink(f)
    except FileNotFoundError: pass

h = lib.zb_client_open(json.dumps({
    "url": "nats://127.0.0.1:4222",
    "credsPath": os.path.join(ROOT, "..", "scripts", "native", "creds", "omar.creds"),
    "grammarPath": os.path.join(ROOT, "..", "grammar.json"),
    "dbPath": db,
    "principal": "omar",
    "clientId": "py-index-card",
    "tables": ["users", "salaries", "test_types"],
}).encode())
assert h, "open failed (is the native stack up?)"
ok = True
def check(label, cond):
    global ok
    ok &= bool(cond)
    print(("  ✓ " if cond else "  ✗ ") + label)

try:
    s = take(lib.zb_client_sync(h))
    check(f"sync: tenant resolved ({s.get('tenant')}), first pass", s.get("first") is True and s.get("tenant"))
    before = query(h, "SELECT count(*) AS n FROM test_types WHERE deleted_at IS NULL")[0]["n"]
    check(f"query through the read-only connection: {before} live test_types row(s)", isinstance(before, int))

    refused = query(h, "DELETE FROM test_types")
    check("a write THROUGH the app connection is refused (read-only)", "error" in refused)

    uid = str(uuid.uuid4())  # fresh per run: a fixed uid is soft-deleted by the previous run, and an INSERT onto a tombstoned row echoes as deleted
    now = time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime())
    m = take(lib.zb_client_mutate(h, b"test_types", b"INSERT", json.dumps({"uid": uid}).encode(), json.dumps({
        "uid": uid, "some_text": "written by the python index card", "age": 7, "is_true": True,
        "tags": ["py", "index-card", "with,comma"], "matrix": [[1, 2], [3, 4]], "metadata": {"source": "index_card.py"},
        "price": "1234.56789012", "temperature": 36.6, "tenant_id": s.get("tenant"), "inserted_at": now, "updated_at": now,
    }).encode()))
    check(f"mutate: queued {m.get('msgId', m)}", "msgId" in m)
    f = take(lib.zb_client_flush(h, 4000))
    check(f"flush: sent={f.get('sent')} settled={f.get('settled')}", f.get("settled", 0) >= 1)
    take(lib.zb_client_sync(h))  # the CDC echo
    row = query(h, "SELECT some_text, tags, last_writer FROM test_types WHERE uid = ?", [uid])
    check(f"the echo is in the replica: {row[0] if row else None}", bool(row) and row[0]["last_writer"] == "py-index-card")
    take(lib.zb_client_mutate(h, b"test_types", b"DELETE", json.dumps({"uid": uid}).encode(), None))
    f2 = take(lib.zb_client_flush(h, 4000)); take(lib.zb_client_sync(h))
    gone = query(h, "SELECT count(*) AS n FROM test_types WHERE uid = ?", [uid])[0]["n"]
    check(f"DELETE settled ({f2.get('settled')}) and the tombstone rule removed the row locally: {gone} left", gone == 0)
    bad = take(lib.zb_client_sync(0))
    check("a bad handle answers an error, not a crash", bad.get("error") == "UnknownHandle")
finally:
    check("close", lib.zb_client_close(h) == 0)
print("PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)

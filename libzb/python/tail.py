"""Live tailing through the C ABI (NOTES §10bh): a host loop on `zb_client_poll`.

    cd libzb && zig build && python3 python/tail.py

Opens a client as omar, syncs, then polls while a write lands in PostgreSQL from
OUTSIDE the client (psql) — the poll must return early with the row applied. Also
measures the idle poll (must cost ~wait_ms, not spin, not overshoot) and that a
mutation queued by this client settles through `poll`'s verdict sweep, no `flush` wait.
"""
import ctypes, json, os, subprocess, sys, time, uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PSQL = ["/opt/homebrew/opt/postgresql@18/bin/psql", "-X", "-q", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-d", "postgres", "-c"]
lib = ctypes.CDLL(os.path.join(ROOT, "zig-out", "lib", "libzbcore.dylib"))
lib.zb_free.argtypes = [ctypes.c_void_p]
lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
lib.zb_client_close.restype, lib.zb_client_close.argtypes = ctypes.c_int, [ctypes.c_uint64]
for name, args in (("sync", []), ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                   ("mutate", [ctypes.c_char_p] * 4), ("flush", [ctypes.c_uint64]), ("poll", [ctypes.c_uint64])):
    f = getattr(lib, "zb_client_" + name); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + args

def take(p):
    if not p: raise RuntimeError("NULL")
    try: return json.loads(ctypes.string_at(p).decode())
    finally: lib.zb_free(p)

def q(h, sql, params=()):
    r = take(lib.zb_client_query(h, sql.encode(), json.dumps(list(params)).encode()))
    return r if "error" in r else [dict(zip(r["columns"], row)) for row in r["rows"]]

db = "/tmp/zb-tail.sqlite3"
for f in (db, db + "-wal", db + "-shm"):
    try: os.unlink(f)
    except FileNotFoundError: pass
h = lib.zb_client_open(json.dumps({
    "url": "nats://127.0.0.1:4222", "credsPath": os.path.join(ROOT, "..", "scripts", "native", "creds", "omar.creds"),
    "grammarPath": os.path.join(ROOT, "..", "grammar.json"), "dbPath": db, "principal": "omar",
    "clientId": "py-tail", "tables": ["users", "salaries", "test_types"]}).encode())
assert h
ok = True
def check(label, cond):
    global ok; ok &= bool(cond); print(("  ✓ " if cond else "  ✗ ") + label)

try:
    s = take(lib.zb_client_sync(h)); check(f"sync (tenant {s.get('tenant')})", s.get("first"))
    # 1. idle: a poll with nothing to do costs about wait_ms and applies nothing
    t0 = time.monotonic(); r = take(lib.zb_client_poll(h, 600)); dt = (time.monotonic() - t0) * 1000
    check(f"idle poll: applied={r.get('applied')} in {dt:.0f} ms (budget 600)", r.get("applied") == 0 and 500 <= dt <= 1500)

    # 2. an outside write: PG via psql → CDC → the tail; poll must return EARLY
    uid = str(uuid.uuid4())  # fresh per run: the previous one is soft-deleted, its uid still taken
    subprocess.run(PSQL + [f"INSERT INTO test_types (uid, some_text, tenant_id, inserted_at, updated_at) VALUES ('{uid}', 'from psql, outside the client', '{s['tenant']}', now(), now())"], check=True)
    t0 = time.monotonic(); applied = 0; polls = 0
    while applied == 0 and time.monotonic() - t0 < 10:
        r = take(lib.zb_client_poll(h, 1000)); polls += 1; applied += r.get("applied", 0)
    dt = (time.monotonic() - t0) * 1000
    row = q(h, "SELECT some_text FROM test_types WHERE uid = ?", [uid])
    check(f"outside INSERT tailed in {dt:.0f} ms over {polls} poll(s): {row[0]['some_text'] if row else None}", bool(row))

    subprocess.run(PSQL + [f"UPDATE test_types SET some_text = 'updated outside', updated_at = now() WHERE uid = '{uid}'"], check=True)
    t0 = time.monotonic(); r = take(lib.zb_client_poll(h, 3000)); dt = (time.monotonic() - t0) * 1000
    row = q(h, "SELECT some_text FROM test_types WHERE uid = ?", [uid])
    check(f"outside UPDATE tailed in one poll of {dt:.0f} ms (returned before the 3000 budget): {row[0]['some_text'] if row else None}",
          r.get("applied", 0) >= 1 and dt < 3000 and row and row[0]["some_text"] == "updated outside")

    # 3. this client's own write: mutate, flush with NO wait, let poll sweep the verdict and tail the echo
    take(lib.zb_client_mutate(h, b"test_types", b"DELETE", json.dumps({"uid": uid}).encode(), None))
    f = take(lib.zb_client_flush(h, 0))
    settled = f.get("settled", 0); t0 = time.monotonic()
    while settled == 0 and time.monotonic() - t0 < 10:
        r = take(lib.zb_client_poll(h, 1000)); settled += r.get("settled", 0)
    take(lib.zb_client_poll(h, 1000))
    left = q(h, "SELECT count(*) AS n FROM test_types WHERE uid = ?", [uid])[0]["n"]
    check(f"own DELETE: flush sent={f.get('sent')} settled-by-poll={settled}, row gone locally ({left} left)", settled >= 1 and left == 0)
    pg = subprocess.run(PSQL[:-1] + ["-tAc", f"SELECT count(*) FROM test_types WHERE uid = '{uid}' AND deleted_at IS NULL"], capture_output=True, text=True).stdout.strip()
    check(f"…and soft-deleted in PostgreSQL (live rows: {pg})", pg == "0")
finally:
    check("close", lib.zb_client_close(h) == 0)
print("PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)

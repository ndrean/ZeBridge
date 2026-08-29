"""Live schema migration (NOTES §10v → §10bi): ALTER TABLE in PostgreSQL while a
client runs; the bridge republishes the descriptor; the client's next `sync`
migrates the replica in place. ADD → INSERT with the new column → RENAME (hint) →
DROP, the value surviving the rename."""
import ctypes, json, os, subprocess, sys, time, uuid
R = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PSQL = ["/opt/homebrew/opt/postgresql@18/bin/psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-d", "postgres", "-c"]
lib = ctypes.CDLL(os.path.join(R, "zig-out", "lib", "libzbcore.dylib"))
lib.zb_free.argtypes = [ctypes.c_void_p]
lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
lib.zb_client_close.argtypes = [ctypes.c_uint64]
for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
    f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a
def take(p):
    try: return json.loads(ctypes.string_at(p).decode())
    finally: lib.zb_free(p)
def cols(h):
    return [r[0] for r in take(lib.zb_client_query(h, b"SELECT name FROM pragma_table_info('test_types')", b"[]"))["rows"]]
def pg(sql): subprocess.run(PSQL + [sql], check=True)
def sync_until(h, pred, label, budget=10):
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        take(lib.zb_client_sync(h))
        if pred(): return time.monotonic() - t0
        time.sleep(0.25)
    return None
ok = True
def check(label, cond):
    global ok; ok &= bool(cond); print(("  ✓ " if cond else "  ✗ ") + label)

db = "/tmp/zb-migrate.sqlite3"
for f in (db, db + "-wal", db + "-shm"):
    try: os.unlink(f)
    except FileNotFoundError: pass
h = lib.zb_client_open(json.dumps({"url": "nats://127.0.0.1:4222", "credsPath": R + "/../scripts/native/creds/omar.creds",
    "grammarPath": R + "/../grammar.json", "dbPath": db, "principal": "omar", "clientId": "py-migrate", "tables": ["users", "salaries", "test_types"]}).encode())
assert h
s = take(lib.zb_client_sync(h)); before = cols(h)
check(f"sync: {len(before)} columns in the replica's test_types", "note" not in before)
uid = str(uuid.uuid4())
try:
    pg("ALTER TABLE test_types ADD COLUMN note text")
    dt = sync_until(h, lambda: "note" in cols(h), "add")
    check(f"ADD COLUMN in PostgreSQL → column in the replica after {dt and round(dt,2)} s of syncs", dt is not None)

    pg(f"INSERT INTO test_types (uid, some_text, note, tenant_id, inserted_at, updated_at) VALUES ('{uid}', 'migrate', 'hello', '{s['tenant']}', now(), now())")
    t0 = time.monotonic()
    while take(lib.zb_client_poll(h, 1000)).get("applied", 0) == 0 and time.monotonic() - t0 < 10: pass
    v = take(lib.zb_client_query(h, b"SELECT note FROM test_types WHERE uid = ?", json.dumps([uid]).encode()))["rows"]
    check(f"a row written with the new column arrives with it: note = {v[0][0] if v else None}", v and v[0][0] == "hello")

    pg("ALTER TABLE test_types RENAME COLUMN note TO remark")
    dt = sync_until(h, lambda: "remark" in cols(h) and "note" not in cols(h), "rename")
    v = take(lib.zb_client_query(h, b"SELECT remark FROM test_types WHERE uid = ?", json.dumps([uid]).encode()))["rows"]
    check(f"RENAME COLUMN → renamed in the replica ({dt and round(dt,2)} s), value kept: remark = {v[0][0] if v else None}", dt is not None and v and v[0][0] == "hello")

    pg("ALTER TABLE test_types DROP COLUMN remark")
    dt = sync_until(h, lambda: "remark" not in cols(h), "drop")
    check(f"DROP COLUMN → gone from the replica ({dt and round(dt,2)} s); columns back to {len(cols(h))}", dt is not None and len(cols(h)) == len(before))
    v = take(lib.zb_client_query(h, b"SELECT some_text FROM test_types WHERE uid = ?", json.dumps([uid]).encode()))["rows"]
    check("the row survived every migration", v and v[0][0] == "migrate")
finally:
    subprocess.run(PSQL + ["ALTER TABLE test_types DROP COLUMN IF EXISTS note"], check=False)
    subprocess.run(PSQL + ["ALTER TABLE test_types DROP COLUMN IF EXISTS remark"], check=False)
    subprocess.run(PSQL + [f"DELETE FROM test_types WHERE uid = '{uid}'"], check=False)
    lib.zb_client_close(h)
print("PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)

"""ADD COLUMN ... DEFAULT x under a client that only polls (NOTES §10df). PostgreSQL
fills its existing rows from the catalog without a single WAL row; the descriptor now
carries the constant, the client adds the column WITH it, and its engine fills the
existing local rows the same way — converged, no re-seed."""
import ctypes, json, os, subprocess, sys, time, uuid
from _env import load_lib, psql_cmd, creds, GRAMMAR, rm_sqlite
PSQL = psql_cmd("-v", "ON_ERROR_STOP=1", "-c")
lib = load_lib()
lib.zb_free.argtypes = [ctypes.c_void_p]
lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
lib.zb_client_close.argtypes = [ctypes.c_uint64]
for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
    f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a
def take(p):
    try: return json.loads(ctypes.string_at(p).decode())
    finally: lib.zb_free(p)
def q(h, sql, params=()):
    return take(lib.zb_client_query(h, sql.encode(), json.dumps(list(params)).encode()))["rows"]
def cols(h): return [r[0] for r in q(h, "SELECT name FROM pragma_table_info('test_types')")]
def pg(sql): subprocess.run(PSQL + [sql], check=True)
def poll_until(h, pred, budget=12):
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        take(lib.zb_client_poll(h, 500))
        if pred(): return round(time.monotonic() - t0, 2)
    return None
ok = True
def check(label, cond):
    global ok; ok &= bool(cond); print(("  ✓ " if cond else "  ✗ ") + label)
db = "/tmp/zb-migrate-default.sqlite3"; rm_sqlite(db)
old_uid, new_uid = str(uuid.uuid4()), str(uuid.uuid4())
h = lib.zb_client_open(json.dumps({
    "url": os.environ.get("NATS_URL", "nats://127.0.0.1:4222"), "credsPath": creds("omar"),
    "grammarPath": GRAMMAR, "dbPath": db, "principal": "omar", "clientId": "py-migrate-default",
    "tables": ["users", "test_types"], "heartbeatMs": 0}).encode())
try:
    if not h: sys.exit("open failed")
    s = take(lib.zb_client_sync(h))
    pg(f"INSERT INTO test_types (uid, some_text, tenant_id, inserted_at, updated_at) VALUES ('{old_uid}', 'before', '{s['tenant']}', now(), now())")
    dt = poll_until(h, lambda: bool(q(h, "SELECT uid FROM test_types WHERE uid = ?", [old_uid])))
    check(f"a row exists on the replica BEFORE the migration ({dt} s)", dt is not None)

    pg("ALTER TABLE test_types ADD COLUMN kind text NOT NULL DEFAULT 'plain', ADD COLUMN n integer DEFAULT 5, ADD COLUMN flag boolean DEFAULT true")
    dt = poll_until(h, lambda: {"kind", "n", "flag"} <= set(cols(h)))
    check(f"ADD COLUMN x3 with constant defaults → columns in the replica by polls ({dt} s)", dt is not None)
    ddl = q(h, "SELECT sql FROM sqlite_master WHERE name = 'test_types'")[0][0]
    check(f"the local DDL carries the defaults: {'DEFAULT' in ddl}", "DEFAULT 'plain'" in ddl and "DEFAULT 5" in ddl)
    v = q(h, "SELECT kind, n, flag FROM test_types WHERE uid = ?", [old_uid])
    check(f"the PRE-EXISTING local row reads the defaults, no re-seed: {v[0] if v else None}", v and v[0][0] == "plain" and v[0][1] == 5 and v[0][2] in (1, True, "true"))
    pgv = subprocess.run(psql_cmd("-At", "-c") + [f"SELECT kind || '|' || n || '|' || flag FROM test_types WHERE uid = '{old_uid}'"], capture_output=True, text=True).stdout.strip()
    check(f"PostgreSQL's own old row agrees: {pgv}", pgv == "plain|5|true")

    pg(f"INSERT INTO test_types (uid, some_text, tenant_id, inserted_at, updated_at) VALUES ('{new_uid}', 'after', '{s['tenant']}', now(), now())")
    dt = poll_until(h, lambda: bool(q(h, "SELECT uid FROM test_types WHERE uid = ?", [new_uid])))
    v = q(h, "SELECT kind, n FROM test_types WHERE uid = ?", [new_uid])
    check(f"a row written AFTER, omitting the columns, arrives with the defaults ({dt} s): {v[0] if v else None}", dt is not None and v and v[0][0] == "plain" and v[0][1] == 5)
finally:
    for c in ("kind", "n", "flag"):
        subprocess.run(PSQL + [f"ALTER TABLE test_types DROP COLUMN IF EXISTS {c}"], check=False)
    subprocess.run(PSQL + [f"DELETE FROM test_types WHERE uid IN ('{old_uid}', '{new_uid}')"], check=False)
    if h: lib.zb_client_close(h)
    rm_sqlite(db)
print("PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)

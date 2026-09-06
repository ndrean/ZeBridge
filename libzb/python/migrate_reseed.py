"""The re-seed lever (NOTES §10df): a VOLATILE default (now()) diverges silently — PostgreSQL
rewrites its rows without a decoded event, the descriptor cannot carry now(), the replica
holds NULL. One `zebridge_reseed('test_types')`: the producer forces a full, the bridge
republishes the descriptor with the new epoch, the polling client forgets its watermark,
seeds the fresh full, and the NULLs become PostgreSQL's timestamps. Then the foreign-key
closure: reseeding `users` bumps `salaries` too."""
import ctypes, json, os, subprocess, sys, time, uuid
from _env import load_lib, psql_cmd, creds, GRAMMAR, rm_sqlite
PSQL = psql_cmd("-v", "ON_ERROR_STOP=1", "-c"); PSQLQ = psql_cmd("-At", "-c")
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
def pg(sql): subprocess.run(PSQL + [sql], check=True)
def pgq(sql): return subprocess.run(PSQLQ + [sql], capture_output=True, text=True).stdout.strip()
def poll_until(h, pred, budget=30):
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        take(lib.zb_client_poll(h, 500))
        if pred(): return round(time.monotonic() - t0, 2)
    return None
ok = True
def check(label, cond):
    global ok; ok &= bool(cond); print(("  ✓ " if cond else "  ✗ ") + label)
db = "/tmp/zb-migrate-reseed.sqlite3"; rm_sqlite(db)
uid = str(uuid.uuid4())
h = lib.zb_client_open(json.dumps({
    "url": os.environ.get("NATS_URL", "nats://127.0.0.1:4222"), "credsPath": creds("omar"),
    "grammarPath": GRAMMAR, "dbPath": db, "principal": "omar", "clientId": "py-migrate-reseed",
    "tables": ["users", "test_types"], "heartbeatMs": 0}).encode())
try:
    if not h: sys.exit("open failed")
    s = take(lib.zb_client_sync(h))
    pg(f"INSERT INTO test_types (uid, some_text, tenant_id, inserted_at, updated_at) VALUES ('{uid}', 'reseed', '{s['tenant']}', now(), now())")
    dt = poll_until(h, lambda: bool(q(h, "SELECT uid FROM test_types WHERE uid = ?", [uid])), 15)
    check(f"a row exists on the replica before the migration ({dt} s)", dt is not None)
    epoch0 = int(pgq("SELECT seed_epoch FROM zebridge_catalogue WHERE tbl = 'test_types'") or 0)

    pg("ALTER TABLE test_types ADD COLUMN stamp timestamptz DEFAULT now()")
    dt = poll_until(h, lambda: "stamp" in [r[0] for r in q(h, "SELECT name FROM pragma_table_info('test_types')")], 15)
    local = q(h, "SELECT stamp FROM test_types WHERE uid = ?", [uid]); pgv = pgq(f"SELECT stamp FROM test_types WHERE uid = '{uid}'")
    check(f"volatile default: column arrived ({dt} s) but the replica holds {local[0][0] if local else None} where PostgreSQL holds {pgv[:19]} — the silent divergence", dt is not None and local and local[0][0] is None and pgv)

    bumped = pgq("SELECT tbl || '=' || seed_epoch FROM zebridge_reseed('test_types')")
    check(f"zebridge_reseed('test_types') → {bumped}", bumped == f"test_types={epoch0 + 1}")
    dt = poll_until(h, lambda: (q(h, "SELECT stamp FROM test_types WHERE uid = ?", [uid]) or [[None]])[0][0] is not None, 60)
    local = q(h, "SELECT stamp, (SELECT seed_epoch FROM _zbz_generations WHERE tbl = 'test_types') FROM test_types WHERE uid = ?", [uid])
    check(f"after the bump ({dt} s of polls): the replica re-seeded from a fresh full, stamp = {str(local[0][0])[:19] if local else None}, stored epoch {local[0][1] if local else None}", dt is not None and local and local[0][1] == epoch0 + 1)

    closure = pgq("SELECT string_agg(tbl, ',' ORDER BY tbl) FROM zebridge_reseed('users')")
    check(f"the closure: zebridge_reseed('users') bumped {closure}", "users" in closure.split(",") and "salaries" in closure.split(","))
finally:
    subprocess.run(PSQL + ["ALTER TABLE test_types DROP COLUMN IF EXISTS stamp"], check=False)
    subprocess.run(PSQL + [f"DELETE FROM test_types WHERE uid = '{uid}'"], check=False)
    if h: lib.zb_client_close(h)
    rm_sqlite(db)
print("PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)

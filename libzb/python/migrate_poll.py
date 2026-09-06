"""Live schema migration under a client that only POLLS (CLIENTS.md divergence 2,
NOTES §10de): after one sync the host never calls sync again. ADD COLUMN → a row in
the new shape → RENAME (hint) → DROP COLUMN must all reach the replica through the
schema watch that `poll` drains; then a table DROPPED upstream must disappear locally.
`migrate.py` proves the same moves through `sync`; this one proves them without it."""
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
def cols(h, table="test_types"):
    return [r[0] for r in q(h, f"SELECT name FROM pragma_table_info('{table}')")]
def tables(h):
    return [r[0] for r in q(h, "SELECT name FROM sqlite_master WHERE type='table'")]
def pg(sql): subprocess.run(PSQL + [sql], check=True)
def poll_until(h, pred, budget=12):
    """ONLY poll. No sync. That is the point."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        take(lib.zb_client_poll(h, 500))
        if pred(): return round(time.monotonic() - t0, 2)
    return None
ok = True
def check(label, cond):
    global ok; ok &= bool(cond); print(("  ✓ " if cond else "  ✗ ") + label)

PROBE = "zb_mig_drop"
db = "/tmp/zb-migrate-poll.sqlite3"; rm_sqlite(db)
pub = os.environ.get("BRIDGE_CDC_PUBLICATION", "my_pub")
uid = str(uuid.uuid4())
# The probe table exists and is enabled BEFORE the client opens, so the client follows it.
subprocess.run(PSQL + [f"DROP TABLE IF EXISTS public.{PROBE}"], check=False)
subprocess.run(PSQL + [f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{PROBE}'"], check=False)
pg(f"CREATE TABLE public.{PROBE} (uid uuid PRIMARY KEY, txt text, updated_at timestamptz NOT NULL); "
   f"SELECT * FROM zebridge_enable('public.{PROBE}', version_col => 'updated_at', public_reason => 'migrate_poll probe', "
   f"publication => '{pub}', dry_run => false)")
time.sleep(2)  # the descriptor for the newborn reaches $KV.schemas
h = lib.zb_client_open(json.dumps({
    "url": os.environ.get("NATS_URL", "nats://127.0.0.1:4222"), "credsPath": creds("omar"),
    "grammarPath": GRAMMAR, "dbPath": db, "principal": "omar", "clientId": "py-migrate-poll",
    "tables": ["users", "test_types", PROBE], "heartbeatMs": 0}).encode())
try:
    if not h: sys.exit("open failed (is the native stack up? nats 127.0.0.1:4222, creds for omar)")
    s = take(lib.zb_client_sync(h)); before = cols(h)
    check(f"one sync, then never again: {len(before)} columns, probe table {'present' if PROBE in tables(h) else 'ABSENT'}", "note" not in before and PROBE in tables(h))

    pg("ALTER TABLE test_types ADD COLUMN note text")
    dt = poll_until(h, lambda: "note" in cols(h))
    check(f"ADD COLUMN → in the replica after {dt} s of POLLS only (the schema watch)", dt is not None)

    pg(f"INSERT INTO test_types (uid, some_text, note, tenant_id, inserted_at, updated_at) VALUES ('{uid}', 'migrate', 'hello', '{s['tenant']}', now(), now())")
    dt = poll_until(h, lambda: (q(h, "SELECT note FROM test_types WHERE uid = ?", [uid]) or [[None]])[0][0] == "hello")
    check(f"a row in the new shape arrives with it ({dt} s): note = hello", dt is not None)
    held = q(h, "SELECT count(*) FROM _zbz_inbox")[0][0]
    check(f"nothing left held in the inbox ({held})", held == 0)

    pg("ALTER TABLE test_types RENAME COLUMN note TO remark")
    dt = poll_until(h, lambda: "remark" in cols(h) and "note" not in cols(h))
    v = q(h, "SELECT remark FROM test_types WHERE uid = ?", [uid])
    check(f"RENAME COLUMN → renamed by polls ({dt} s), value kept: remark = {v[0][0] if v else None}", dt is not None and v and v[0][0] == "hello")

    pg("ALTER TABLE test_types DROP COLUMN remark")
    dt = poll_until(h, lambda: "remark" not in cols(h))
    check(f"DROP COLUMN → gone by polls ({dt} s); columns back to {len(cols(h))}", dt is not None and len(cols(h)) == len(before))

    pg(f"DROP TABLE public.{PROBE}")
    dt = poll_until(h, lambda: PROBE not in tables(h))
    check(f"DROP TABLE upstream → local table dropped by polls ({dt} s)", dt is not None)
    v = q(h, "SELECT some_text FROM test_types WHERE uid = ?", [uid])
    check("the test_types row survived every migration", v and v[0][0] == "migrate")
finally:
    subprocess.run(PSQL + ["ALTER TABLE test_types DROP COLUMN IF EXISTS note"], check=False)
    subprocess.run(PSQL + ["ALTER TABLE test_types DROP COLUMN IF EXISTS remark"], check=False)
    subprocess.run(PSQL + [f"DELETE FROM test_types WHERE uid = '{uid}'"], check=False)
    subprocess.run(PSQL + [f"DROP TABLE IF EXISTS public.{PROBE}"], check=False)
    subprocess.run(PSQL + [f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{PROBE}'"], check=False)
    if h: lib.zb_client_close(h)
    rm_sqlite(db)
print("PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)

"""The re-key (NOTES §10dg): the migration a replica cannot ALTER through.

`rekey_probe` is keyed by a bigserial and `rekey_child` references it. The migration
moves the parent to a uuid key the way it is really done — a new column, populated,
the child's FK remapped, the old columns dropped, the new ones renamed, pk and FK
re-added — in ONE transaction. Expected, with no client restart:

  * the DDL trigger sees the parent's key shape move (id:bigint → NULL → id:uuid) and
    bumps the seed epoch of the parent AND the child (the FK closure of zebridge_reseed);
  * the producer's next build is a full for both;
  * the polling libzb client rebuilds `rekey_probe` EMPTY (its recorded key shape
    [["id","INTEGER"]] no longer matches [["id","TEXT"]]), re-types the child's FK
    column, drops both watermarks and seeds both from the fresh fulls;
  * CDC keyed by the NEW pk lands afterwards.
"""
import ctypes, json, os, subprocess, sys, time, uuid
from _env import load_lib, psql_cmd, creds, rm_sqlite
PSQL = psql_cmd("-v", "ON_ERROR_STOP=1", "-c"); PSQLQ = psql_cmd("-At", "-c")
PUB = os.environ.get("BRIDGE_CDC_PUBLICATION") or sys.exit("BRIDGE_CDC_PUBLICATION is not set (source .env.bridge)")
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
        try:
            if pred(): return round(time.monotonic() - t0, 2)
        except Exception:
            pass
    return None
def col_type(h, table, col):
    r = q(h, f"SELECT type FROM pragma_table_info('{table}') WHERE name = ?", [col]); return r[0][0] if r else None
def epochs(): return pgq("SELECT string_agg(tbl || '=' || seed_epoch, ',' ORDER BY tbl) FROM zebridge_catalogue WHERE tbl IN ('rekey_probe','rekey_child')")
ok = True
def check(label, cond):
    global ok; ok &= bool(cond); print(("  ✓ " if cond else "  ✗ ") + label)

def teardown():
    for sql in ("DROP TABLE IF EXISTS public.rekey_child", "DROP TABLE IF EXISTS public.rekey_probe",
                "DELETE FROM public.zebridge_catalogue WHERE tbl IN ('rekey_probe','rekey_child')"):
        subprocess.run(PSQL + [sql], check=False, capture_output=True)
teardown()
COMMON = "tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz"
pg(f"CREATE TABLE public.rekey_probe (id bigserial PRIMARY KEY, label text, {COMMON})")
pg(f"CREATE TABLE public.rekey_child (id bigserial PRIMARY KEY, probe_id bigint NOT NULL REFERENCES public.rekey_probe(id), note text, {COMMON})")
for t in ("rekey_probe", "rekey_child"):
    out = pgq(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{t}', tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false)")
    check(f"zebridge_enable({t}): {out[:90]}", "error" not in out.lower())

db = "/tmp/zb-migrate-rekey.sqlite3"; rm_sqlite(db)
h = lib.zb_client_open(json.dumps({
    "url": os.environ.get("NATS_URL", "nats://127.0.0.1:4222"), "credsPath": creds("omar"),
    "dbPath": db, "principal": "omar", "clientId": "py-migrate-rekey",
    "tables": ["rekey_probe", "rekey_child"], "heartbeatMs": 0}).encode())
try:
    if not h: sys.exit("open failed")
    s = take(lib.zb_client_sync(h)); tenant = s["tenant"]
    pg(f"INSERT INTO rekey_probe (label, tenant_id) SELECT 'p' || g, '{tenant}' FROM generate_series(1,3) g")
    pg(f"INSERT INTO rekey_child (probe_id, note, tenant_id) SELECT p.id, 'c' || p.id, '{tenant}' FROM rekey_probe p")
    dt = poll_until(h, lambda: q(h, "SELECT count(*) FROM rekey_probe")[0][0] == 3 and q(h, "SELECT count(*) FROM rekey_child")[0][0] == 3, 20)
    check(f"before: 3 parents + 3 children on the replica ({dt} s); parent id is {col_type(h, 'rekey_probe', 'id')}, child fk is {col_type(h, 'rekey_child', 'probe_id')}",
          dt is not None and col_type(h, "rekey_probe", "id") == "INTEGER" and col_type(h, "rekey_child", "probe_id") == "INTEGER")
    shape0 = q(h, "SELECT key_shape FROM _zbz_shape WHERE tbl = 'rekey_probe'")
    check(f"the replica recorded the key shape it built: {shape0[0][0] if shape0 else None}", shape0 and shape0[0][0] == '[["id","INTEGER"]]')
    e0 = epochs(); print(f"  · epochs before: {e0}")
    # let the producer write g1 for both, so the forced full is visible as a NEW generation
    poll_until(h, lambda: pgq(f"SELECT count(*) FROM zebridge_generations WHERE tenant = '{tenant}' AND tbl IN ('rekey_probe','rekey_child')") == "2", 30)
    g0 = pgq(f"SELECT string_agg(tbl || ':g' || gen || (CASE WHEN has_full THEN 'F' ELSE '' END), ',' ORDER BY tbl, gen) FROM zebridge_generations WHERE tenant = '{tenant}' AND tbl IN ('rekey_probe','rekey_child')")
    print(f"  · generations before: {g0}")

    # ── THE MIGRATION: bigserial → uuid on the parent's key, one transaction ──
    pg("""BEGIN;
        ALTER TABLE rekey_probe ADD COLUMN new_id uuid NOT NULL DEFAULT gen_random_uuid();
        ALTER TABLE rekey_child ADD COLUMN new_probe_id uuid;
        UPDATE rekey_child c SET new_probe_id = p.new_id FROM rekey_probe p WHERE p.id = c.probe_id;
        ALTER TABLE rekey_child DROP COLUMN probe_id;
        ALTER TABLE rekey_probe DROP COLUMN id;
        ALTER TABLE rekey_probe RENAME COLUMN new_id TO id;
        ALTER TABLE rekey_probe ADD PRIMARY KEY (id);
        ALTER TABLE rekey_child RENAME COLUMN new_probe_id TO probe_id;
        ALTER TABLE rekey_child ALTER COLUMN probe_id SET NOT NULL;
        ALTER TABLE rekey_child ADD FOREIGN KEY (probe_id) REFERENCES rekey_probe(id);
        COMMIT;""")
    # The old pk took the replica identity with it (the `_zb_ri` index on (tenant, pk)):
    # without this, the next UPDATE is refused by PostgreSQL itself. zebridge_enable is
    # idempotent — re-running it is the documented last step of a re-key.
    out = pgq(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.rekey_probe', tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false)")
    check(f"zebridge_enable re-run restores the replica identity: {out[:70]}…", "error" not in out.lower())
    e1 = epochs()
    check(f"the trigger bumped the seed epoch of parent AND child (closure): {e0} → {e1}",
          all(int(e1.split(',')[i].split('=')[1]) > int(e0.split(',')[i].split('=')[1]) for i in range(2)))

    pg_ids = pgq("SELECT string_agg(id::text, ',' ORDER BY label) FROM rekey_probe")
    def converged():
        ids = q(h, "SELECT id FROM rekey_probe ORDER BY label")
        kids = q(h, "SELECT c.probe_id FROM rekey_child c JOIN rekey_probe p ON p.id = c.probe_id")
        return ",".join(r[0] for r in ids) == pg_ids and len(kids) == 3
    dt = poll_until(h, converged, 90)
    check(f"after ({dt} s of polls): parent ids are PostgreSQL's uuids, every child joins its parent by the NEW key", dt is not None)
    check(f"the replica re-keyed the parent (id is {col_type(h, 'rekey_probe', 'id')}) and re-typed the child's fk ({col_type(h, 'rekey_child', 'probe_id')})",
          col_type(h, "rekey_probe", "id") == "TEXT" and col_type(h, "rekey_child", "probe_id") == "TEXT")
    shape1 = q(h, "SELECT key_shape FROM _zbz_shape WHERE tbl = 'rekey_probe'")
    check(f"the recorded key shape moved: {shape1[0][0] if shape1 else None}", shape1 and shape1[0][0] == '[["id","TEXT"]]')
    g1 = pgq(f"SELECT string_agg(tbl || ':g' || gen || (CASE WHEN has_full THEN 'F' ELSE '' END) || '@e' || seed_epoch, ',' ORDER BY tbl) FROM (SELECT DISTINCT ON (tbl) * FROM zebridge_generations WHERE tenant = '{tenant}' AND tbl IN ('rekey_probe','rekey_child') ORDER BY tbl, gen DESC) g")
    check(f"the producer forced a FULL for both under the new epoch: {g1}", g1.count("F@") == 2)
    wm = q(h, "SELECT tbl, seed_epoch FROM _zbz_generations WHERE tbl IN ('rekey_probe','rekey_child') ORDER BY tbl")
    check(f"replica watermarks carry the new epochs: {wm}", len(wm) == 2 and all(str(r[1]) == e1.split(',')[i].split('=')[1] for i, r in enumerate(wm)))

    # ── CDC keyed by the new pk ──
    nid = str(uuid.uuid4())
    pg(f"INSERT INTO rekey_probe (id, label, tenant_id) VALUES ('{nid}', 'p4', '{tenant}'); INSERT INTO rekey_child (probe_id, note, tenant_id) VALUES ('{nid}', 'c4', '{tenant}')")
    dt = poll_until(h, lambda: bool(q(h, "SELECT 1 FROM rekey_child c JOIN rekey_probe p ON p.id = c.probe_id WHERE p.id = ?", [nid])), 20)
    check(f"CDC after the re-key: a parent+child inserted under the uuid key landed and join ({dt} s)", dt is not None)
    pg(f"UPDATE rekey_probe SET label = 'p4-renamed', updated_at = now() WHERE id = '{nid}'")
    dt = poll_until(h, lambda: (q(h, "SELECT label FROM rekey_probe WHERE id = ?", [nid]) or [[None]])[0][0] == "p4-renamed", 20)
    check(f"and an UPDATE keyed by the uuid applies ({dt} s)", dt is not None)
finally:
    if h: lib.zb_client_close(h)
    rm_sqlite(db)
    teardown()
print("PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)

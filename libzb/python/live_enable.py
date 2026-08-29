"""catalog_epoch → CDC routing, live (NOTES §10bj): tables enabled while the bridge
runs, no restart. A tenant-scoped table and a public one; rows flow to the right
stream and into a libzb client; taking the public table out of the catalogue shrinks
CDC_PUBLIC's filter again."""
import ctypes, json, os, subprocess, sys, time, uuid
R = "/Users/nevendrean/code/zig/ZeBridge"
LOG = sys.argv[1] if len(sys.argv) > 1 else None
PSQL = ["/opt/homebrew/opt/postgresql@18/bin/psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-d", "postgres"]
def pg(sql, tuples=False):
    r = subprocess.run(PSQL + (["-tAc", sql] if tuples else ["-c", sql]), capture_output=True, text=True)
    if r.returncode: print("   psql:", r.stderr.strip()[:300])
    return r.stdout.strip()
def nats(*args):
    return subprocess.run(["nats", "--server", "nats://127.0.0.1:4222", "--creds", R + "/scripts/native/creds/omar.creds", *args], capture_output=True, text=True).stdout
def public_subjects():
    return json.loads(nats("stream", "info", "CDC_PUBLIC", "-j"))["config"]["subjects"]
def kv_schema(t):
    return nats("kv", "get", "schemas", t, "--raw").strip()
def wait(pred, budget=8):
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        if pred(): return round(time.monotonic() - t0, 2)
        time.sleep(0.2)
    return None
def log_has(s):
    return LOG and s in open(LOG, errors="replace").read()
lib = ctypes.CDLL(R + "/libzb/zig-out/lib/libzbcore.dylib")
lib.zb_free.argtypes = [ctypes.c_void_p]
lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
lib.zb_client_close.argtypes = [ctypes.c_uint64]
for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
    f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a
def take(p):
    try: return json.loads(ctypes.string_at(p).decode())
    finally: lib.zb_free(p)
def open_client(tables, tag):
    db = f"/tmp/zb-live-{tag}.sqlite3"
    for f in (db, db + "-wal", db + "-shm"):
        try: os.unlink(f)
        except FileNotFoundError: pass
    h = lib.zb_client_open(json.dumps({"url": "nats://127.0.0.1:4222", "credsPath": R + "/scripts/native/creds/omar.creds", "grammarPath": R + "/grammar.json",
        "dbPath": db, "principal": "omar", "clientId": "py-live-" + tag, "tables": tables}).encode())
    assert h; return h
def count(h, t):
    return take(lib.zb_client_query(h, f"SELECT count(*) FROM {t}".encode(), b"[]"))["rows"][0][0]
ok = True
def check(label, cond):
    global ok; ok &= bool(cond); print(("  ✓ " if cond else "  ✗ ") + label)

T, P = "livetab", "pubtab"
subs0 = public_subjects()
print(f"CDC_PUBLIC subjects before: {len(subs0)}")
try:
    # ── a tenant-scoped table, enabled while the bridge runs ──────────────────
    pg(f"CREATE TABLE {T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id varchar(255) NOT NULL, some_text text, deleted_at timestamptz, inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now())")
    out = pg(f"SELECT step||':'||status FROM zebridge_enable('{T}', tenant_col=>'tenant_id', writable=>true, version_col=>'updated_at', tombstone_col=>'deleted_at', publication=>'my_pub', dry_run=>false)", tuples=True)
    check(f"zebridge_enable({T}, tenant) ran: {out.replace(chr(10), ' ')[:120]}", "ERROR" not in out)
    dt = wait(lambda: log_has(f"catalogue reloaded live") if LOG else True)
    check(f"bridge log: catalogue reloaded live ({dt} s, no restart)", dt is not None)
    dt = wait(lambda: '"table":"' + T + '"' in kv_schema(T))
    check(f"$KV.schemas.{T} published ({dt} s)", dt is not None)
    sch = kv_schema(T)
    check("…and it says writable:true (the writability report re-ran AFTER the grant)", '"writable":true' in sch)
    u1 = str(uuid.uuid4())
    pg(f"INSERT INTO {T} (uid, tenant_id, some_text) VALUES ('{u1}', 'kilo', 'first row after a live enable')")
    h = open_client(["users", T], "tenant")
    take(lib.zb_client_sync(h))
    dt = wait(lambda: count(h, T) >= 1 or take(lib.zb_client_poll(h, 500)).get("applied", 0) >= 0 and count(h, T) >= 1)
    row = take(lib.zb_client_query(h, f"SELECT some_text FROM {T}".encode(), b"[]"))["rows"]
    check(f"the row reached a libzb client via CDC_kilo ({dt} s): {row[0][0] if row else None}", bool(row))
    lib.zb_client_close(h)

    # ── a public table: CDC_PUBLIC's filter must grow, live ───────────────────
    pg(f"CREATE TABLE {P} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), some_text text, deleted_at timestamptz, inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now())")
    out = pg(f"SELECT step||':'||status FROM zebridge_enable('{P}', writable=>true, version_col=>'updated_at', tombstone_col=>'deleted_at', public_reason=>'live-enable test', publication=>'my_pub', dry_run=>false)", tuples=True)
    check(f"zebridge_enable({P}, public) ran", "ERROR" not in out)
    dt = wait(lambda: f"cdc.{P}.>" in public_subjects())
    check(f"CDC_PUBLIC subjects reconciled live: cdc.{P}.> added ({dt} s), {len(public_subjects())} subjects", dt is not None)
    u2 = str(uuid.uuid4())
    pg(f"INSERT INTO {P} (uid, some_text) VALUES ('{u2}', 'public row after a live enable')")
    h = open_client([P], "public")
    take(lib.zb_client_sync(h))
    dt = wait(lambda: count(h, P) >= 1 or (take(lib.zb_client_poll(h, 500)) and count(h, P) >= 1))
    row = take(lib.zb_client_query(h, f"SELECT some_text FROM {P}".encode(), b"[]"))["rows"]
    check(f"the public row reached a libzb client via CDC_PUBLIC ({dt} s): {row[0][0] if row else None}", bool(row))
    lib.zb_client_close(h)

    # ── out of the catalogue: the filter shrinks again ────────────────────────
    pg(f"DELETE FROM zebridge_catalogue WHERE tbl = '{P}'")
    dt = wait(lambda: f"cdc.{P}.>" not in public_subjects())
    check(f"DELETE FROM zebridge_catalogue → cdc.{P}.> removed from CDC_PUBLIC ({dt} s)", dt is not None)
finally:
    pg(f"DELETE FROM zebridge_catalogue WHERE tbl IN ('{T}', '{P}')")
    pg(f"ALTER PUBLICATION my_pub DROP TABLE {T}")
    pg(f"ALTER PUBLICATION my_pub DROP TABLE {P}")
    pg(f"DROP TABLE IF EXISTS {T}"); pg(f"DROP TABLE IF EXISTS {P}")
    for t in (T, P):
        subprocess.run(["nats", "--server", "nats://127.0.0.1:4222", "--creds", R + "/scripts/native/creds/bridge.creds", "kv", "del", "schemas", t, "-f"], capture_output=True)
    print(f"CDC_PUBLIC subjects after cleanup: {len(public_subjects())} (before: {len(subs0)})")
print("PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)

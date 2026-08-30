"""catalog_epoch → CDC routing, live (NOTES §10bj): tables enabled while the bridge
runs, no restart. A tenant-scoped table and a public one; rows flow to the right
stream and into a libzb client; taking the public table out of the catalogue shrinks
CDC_PUBLIC's filter again.

    cd libzb && zig build && python3 python/live_enable.py --log <bridge log>

--log is REQUIRED: the "catalogue reloaded live" check reads the bridge's log, and
without one it would pass vacuously. BRIDGE_CDC_PUBLICATION names the publication
(default my_pub, announced when defaulted).
"""
import argparse, ctypes, json, os, subprocess, sys, time, uuid
from _env import load_lib, psql_cmd, creds, GRAMMAR, rm_sqlite

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("--log", required=True, help="the running bridge's log file (for 'catalogue reloaded live')")
args = ap.parse_args()
LOG = args.log
if not os.path.exists(LOG):
    sys.exit(f"--log {LOG}: no such file")
PUB = os.environ.get("BRIDGE_CDC_PUBLICATION")
if not PUB:
    PUB = "my_pub"
    print(f"note: BRIDGE_CDC_PUBLICATION unset — using publication '{PUB}'")

PSQL = psql_cmd("-v", "ON_ERROR_STOP=1")
NATS_URL = "nats://127.0.0.1:4222"
def pg(sql, tuples=False):
    r = subprocess.run(PSQL + (["-tAc", sql] if tuples else ["-c", sql]), capture_output=True, text=True)
    if r.returncode: print("   psql:", r.stderr.strip()[:300])
    return r.stdout.strip()
def nats(*a, principal="bridge"):
    # stream-info and kv reads need the bridge's reach; a tenant's creds see an empty world
    return subprocess.run(["nats", "--server", NATS_URL, "--creds", creds(principal), *a], capture_output=True, text=True).stdout
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
    return s in open(LOG, errors="replace").read()
lib = load_lib()
lib.zb_free.argtypes = [ctypes.c_void_p]
lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
lib.zb_client_close.argtypes = [ctypes.c_uint64]
for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
    f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a
def take(p):
    try: return json.loads(ctypes.string_at(p).decode())
    finally: lib.zb_free(p)
DBS = []
def open_client(tables, tag):
    db = f"/tmp/zb-live-{tag}.sqlite3"
    rm_sqlite(db); DBS.append(db)
    h = lib.zb_client_open(json.dumps({"url": NATS_URL, "credsPath": creds("omar"), "grammarPath": GRAMMAR,
        "dbPath": db, "principal": "omar", "clientId": "py-live-" + tag, "tables": tables}).encode())
    if not h:
        sys.exit(f"open failed for client '{tag}' (is the native stack up?)")
    return h
def count(h, t):
    return take(lib.zb_client_query(h, f"SELECT count(*) FROM {t}".encode(), b"[]"))["rows"][0][0]
ok = True
def check(label, cond):
    global ok; ok &= bool(cond); print(("  ✓ " if cond else "  ✗ ") + label)

T, P = "livetab", "pubtab"
subs0 = public_subjects()
print(f"CDC_PUBLIC subjects before: {len(subs0)}")
log_marks = open(LOG, errors="replace").read().count("catalogue reloaded live")
try:
    # ── a tenant-scoped table, enabled while the bridge runs ──────────────────
    pg(f"CREATE TABLE {T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id varchar(255) NOT NULL, some_text text, deleted_at timestamptz, inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now())")
    out = pg(f"SELECT step||':'||status FROM zebridge_enable('{T}', tenant_col=>'tenant_id', writable=>true, version_col=>'updated_at', tombstone_col=>'deleted_at', publication=>'{PUB}', dry_run=>false)", tuples=True)
    check(f"zebridge_enable({T}, tenant) ran: {out.replace(chr(10), ' ')[:120]}", "ERROR" not in out)
    # a NEW mark, not one left by an earlier run against the same log
    dt = wait(lambda: open(LOG, errors="replace").read().count("catalogue reloaded live") > log_marks)
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
    out = pg(f"SELECT step||':'||status FROM zebridge_enable('{P}', writable=>true, version_col=>'updated_at', tombstone_col=>'deleted_at', public_reason=>'live-enable test', publication=>'{PUB}', dry_run=>false)", tuples=True)
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
    pg(f"ALTER PUBLICATION {PUB} DROP TABLE {T}")
    pg(f"ALTER PUBLICATION {PUB} DROP TABLE {P}")
    pg(f"DROP TABLE IF EXISTS {T}"); pg(f"DROP TABLE IF EXISTS {P}")
    for t in (T, P):
        subprocess.run(["nats", "--server", NATS_URL, "--creds", creds("bridge"), "kv", "del", "schemas", t, "-f"], capture_output=True)
    for db in DBS:
        rm_sqlite(db)
    print(f"CDC_PUBLIC subjects after cleanup: {len(public_subjects())} (before: {len(subs0)})")
print("PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)

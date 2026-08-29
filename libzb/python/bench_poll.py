"""Poll latency bench (NOTES §10bh): N outside UPDATEs via psql, time from the psql
return to the poll that applied the row. Same harness before/after the shared-inbox
switch; the psql spawn cost is a constant in both."""
import ctypes, json, os, statistics, subprocess, sys, time, uuid
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PSQL = ["/opt/homebrew/opt/postgresql@18/bin/psql", "-X", "-q", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-d", "postgres", "-c"]
lib = ctypes.CDLL(os.path.join(ROOT, "zig-out", "lib", "libzbcore.dylib"))
lib.zb_free.argtypes = [ctypes.c_void_p]
lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
lib.zb_client_close.argtypes = [ctypes.c_uint64]
for n, a in (("sync", []), ("poll", [ctypes.c_uint64])):
    f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a
def take(p):
    try: return json.loads(ctypes.string_at(p).decode())
    finally: lib.zb_free(p)
db = "/tmp/zb-bench-poll.sqlite3"
for f in (db, db + "-wal", db + "-shm"):
    try: os.unlink(f)
    except FileNotFoundError: pass
h = lib.zb_client_open(json.dumps({"url": "nats://127.0.0.1:4222", "credsPath": os.path.join(ROOT, "..", "scripts/native/creds/omar.creds"),
    "grammarPath": os.path.join(ROOT, "..", "grammar.json"), "dbPath": db, "principal": "omar", "clientId": "py-bench",
    "tables": ["users", "salaries", "test_types"]}).encode())
s = take(lib.zb_client_sync(h)); take(lib.zb_client_poll(h, 300))
uid = str(uuid.uuid4())
subprocess.run(PSQL + [f"INSERT INTO test_types (uid, some_text, tenant_id, inserted_at, updated_at) VALUES ('{uid}', 'bench', '{s['tenant']}', now(), now())"], check=True)
while take(lib.zb_client_poll(h, 1000)).get("applied", 0) == 0: pass

N = int(sys.argv[1]) if len(sys.argv) > 1 else 20
lat = []
for i in range(N):
    subprocess.run(PSQL + [f"UPDATE test_types SET some_text = 'bench {i}', updated_at = now() WHERE uid = '{uid}'"], check=True)
    t0 = time.monotonic(); polls = 0
    while True:
        r = take(lib.zb_client_poll(h, 2000)); polls += 1
        if r.get("applied", 0): break
    lat.append((time.monotonic() - t0) * 1000)
lat.sort()
print(f"outside UPDATE → applied, N={N}: min {lat[0]:.0f}  p50 {statistics.median(lat):.0f}  p90 {lat[int(N*0.9)-1]:.0f}  max {lat[-1]:.0f} ms")
t0 = time.monotonic(); k = 0
while time.monotonic() - t0 < 5: take(lib.zb_client_poll(h, 1000)); k += 1
print(f"idle: {k} polls of 1000 ms in 5 s")
subprocess.run(PSQL + [f"DELETE FROM test_types WHERE uid = '{uid}'"], check=True)
take(lib.zb_client_poll(h, 1000)); lib.zb_client_close(h)

"""Poll latency BENCHMARK (NOTES §10bh) — not a proof, not part of any battery.

    cd libzb && zig build && python3 python/bench_poll.py [-n N]

N outside UPDATEs via psql, timing from the psql return to the poll that applied
the row. Run the same harness before/after a change to compare; the psql spawn
cost is a constant on both sides. Every wait is bounded (10 s) so a stalled
stack exits 1 instead of hanging.
"""
import argparse, ctypes, json, statistics, subprocess, sys, time, uuid
from _env import load_lib, psql_cmd, creds, GRAMMAR, rm_sqlite

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("-n", type=int, default=20, help="number of outside UPDATEs to time (default 20)")
N = ap.parse_args().n
DEADLINE = 10.0

PSQL = psql_cmd("-c")
lib = load_lib()
lib.zb_free.argtypes = [ctypes.c_void_p]
lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
lib.zb_client_close.argtypes = [ctypes.c_uint64]
for n, a in (("sync", []), ("poll", [ctypes.c_uint64])):
    f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

def take(p):
    try: return json.loads(ctypes.string_at(p).decode())
    finally: lib.zb_free(p)

def pg(sql):
    subprocess.run(PSQL + [sql], check=True)

db = "/tmp/zb-bench-poll.sqlite3"
rm_sqlite(db)
h = lib.zb_client_open(json.dumps({"url": "nats://127.0.0.1:4222", "credsPath": creds("omar"),
    "grammarPath": GRAMMAR, "dbPath": db, "principal": "omar", "clientId": "py-bench",
    "tables": ["users", "salaries", "test_types"]}).encode())
uid = str(uuid.uuid4())
inserted = False
rc = 0
try:
    if not h:
        sys.exit("open failed (is the native stack up? nats 127.0.0.1:4222, creds for omar)")
    s = take(lib.zb_client_sync(h)); take(lib.zb_client_poll(h, 300))
    pg(f"INSERT INTO test_types (uid, some_text, tenant_id, inserted_at, updated_at) VALUES ('{uid}', 'bench', '{s['tenant']}', now(), now())")
    inserted = True
    t0 = time.monotonic()
    while take(lib.zb_client_poll(h, 1000)).get("applied", 0) == 0:
        if time.monotonic() - t0 > DEADLINE:
            print(f"✗ the seed INSERT was not applied within {DEADLINE:.0f} s"); rc = 1; sys.exit(rc)

    lat = []
    for i in range(N):
        pg(f"UPDATE test_types SET some_text = 'bench {i}', updated_at = now() WHERE uid = '{uid}'")
        t0 = time.monotonic()
        while True:
            r = take(lib.zb_client_poll(h, 2000))
            if r.get("applied", 0): break
            if time.monotonic() - t0 > DEADLINE:
                print(f"✗ UPDATE {i} was not applied within {DEADLINE:.0f} s"); rc = 1; sys.exit(rc)
        lat.append((time.monotonic() - t0) * 1000)
    lat.sort()
    t0 = time.monotonic(); k = 0
    while time.monotonic() - t0 < 5: take(lib.zb_client_poll(h, 1000)); k += 1
    print(f"bench: outside UPDATE → applied, N={N}: min {lat[0]:.0f}  p50 {statistics.median(lat):.0f}  "
          f"p90 {lat[max(int(N * 0.9) - 1, 0)]:.0f}  max {lat[-1]:.0f} ms; idle: {k} polls of 1000 ms in 5 s")
finally:
    if inserted:
        subprocess.run(PSQL + [f"DELETE FROM test_types WHERE uid = '{uid}'"], check=False)
    if h:
        take(lib.zb_client_poll(h, 1000)); lib.zb_client_close(h)
    rm_sqlite(db)
sys.exit(rc)

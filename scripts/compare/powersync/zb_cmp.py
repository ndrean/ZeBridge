"""The ZeBridge side of the PowerSync comparison (README.md here). Runs on the comparison host, next to the
temporary stack's NATS (127.0.0.1:14222, JetStream domain `cmp`), with the installed libzb (`ZB_CMP_LIB`), the
clients' creds (`ZB_CMP_CREDS`) and a file holding the database's administrator URL (`ZB_CMP_ADMIN_ENV`, for the
burst). `zebridge.sh` sets all three. Phases (each its own run):

    zb_cmp.py seed N          N clients from empty replicas, then again warm: time to the table's 10,000 rows
    zb_cmp.py roundtrip S     one client writes a marker each second for S s; another reports when it has it
    zb_cmp.py burst N         the administrator inserts N rows in one statement; one client times when it holds all
    zb_cmp.py writes P M      P clients send M inserts each as fast as they can; time to every verdict
"""
import ctypes, json, multiprocessing as mp, os, pathlib, re, statistics, subprocess, sys, time, uuid

HERE = pathlib.Path(__file__).parent
LIB = pathlib.Path(os.environ.get("ZB_CMP_LIB", "/usr/lib/x86_64-linux-gnu/libzbcore.so"))
NATS = "nats://127.0.0.1:14222"
T = "storm_items"


class Client:
    def __init__(self, i, db):
        self.lib = lib = ctypes.CDLL(str(LIB))
        lib.zb_free.argtypes = [ctypes.c_void_p]
        lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
        for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                     ("flush_outbox", [ctypes.c_uint64]), ("mutate", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p])):
            f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a
        lib.zb_client_close.argtypes = [ctypes.c_uint64]
        lib.zb_last_error.restype = ctypes.c_char_p
        p = f"p{i:03d}"
        self.h = lib.zb_client_connect(json.dumps({"natsUrl": NATS, "credsPath": str(pathlib.Path(os.environ.get("ZB_CMP_CREDS", str(HERE / "creds"))) / f"{p}.creds"), "dbPath": db,
                                                   "principal": p, "clientId": f"cmp-{i}", "tables": [T], "heartbeatMs": 0, "jsDomain": "cmp"}).encode())
        if not self.h: raise RuntimeError("connect failed: " + (lib.zb_last_error() or b"").decode())
        r = self.take(lib.zb_client_sync(self.h))
        if r.get("error"): raise RuntimeError(f"sync: {r['error']}")
        self.outcomes = {}

    def take(self, p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: self.lib.zb_free(p)

    def poll(self, ms=0):
        r = self.take(self.lib.zb_client_poll(self.h, ms))
        for o in (r.get("outcomes") or []) if isinstance(r, dict) else []:
            self.outcomes.setdefault(o.get("msgId"), time.time())
        return r

    def q(self, sql):
        r = self.take(self.lib.zb_client_query(self.h, sql.encode(), b"[]"))
        if "error" in r: raise RuntimeError(r["error"])
        return r["rows"]

    def count(self, where="TRUE"):
        try: return self.q(f"SELECT count(*) FROM {T} WHERE {where}")[0][0]
        except RuntimeError: return 0

    def mutate(self, key, values):
        return self.take(self.lib.zb_client_mutate(self.h, T.encode(), b"INSERT", json.dumps(key).encode(), json.dumps(values).encode())).get("msgId")

    def flush(self, ms):
        return self.take(self.lib.zb_client_flush_outbox(self.h, ms))

    def close(self): self.lib.zb_client_close(self.h)


def db_path(i, keep=False):
    p = f"/tmp/zbcmp-{i}.sqlite3"
    if not keep:
        for s in ("", "-wal", "-shm", ".lock"):
            try: os.unlink(p + s)
            except FileNotFoundError: pass
    return p


def seed_one(i, keep, q):
    t = time.monotonic()
    try:
        c = Client(i, db_path(i, keep))
        while c.count() < 10000 and time.monotonic() - t < 120: c.poll(100)
        q.put(time.monotonic() - t if c.count() >= 10000 else None)
        c.close()
    except Exception as e:
        q.put(f"client {i}: {e}")


def run_parallel(n, target, *args):
    q = mp.Queue()
    ps = [mp.Process(target=target, args=(i, *args, q)) for i in range(n)]
    for p in ps: p.start()
    rs = [q.get(timeout=300) for _ in ps]
    for p in ps: p.join(5)
    return rs


def admin_sql(sql):
    env = pathlib.Path(os.environ["ZB_CMP_ADMIN_ENV"]).read_text()
    u = (re.search(r"^SB_ADMIN_URL=(.*)$", env, re.M) or re.search(r"^ADMIN_DATABASE_URL=(.*)$", env, re.M)).group(1).strip().strip("'\"")
    return subprocess.run(["psql", u, "-X", "-tA", "-c", sql], capture_output=True, text=True, check=True).stdout.strip()


def writes_one(i, m, q):
    try:
        c = Client(i, db_path(i, True))
        sent = {}
        tag = f"w{i:02d}-{uuid.uuid4().hex[:6]}"
        for k in range(m):
            key = f"{tag}-{k:05d}"
            t = time.time()
            sent[c.mutate({"id": key}, {"id": key, "tenant_id": "cmp", "note": "write"})] = t
            if k % 50 == 0: c.poll(0)
        deadline = time.time() + 180
        while len([x for x in sent if x in c.outcomes]) < len(sent) and time.time() < deadline:
            c.poll(50); c.flush(50)
        lat = [(c.outcomes[x] - t) * 1000 for x, t in sent.items() if x in c.outcomes]
        q.put({"n": len(lat), "sent": len(sent), "first": min(sent.values()), "last": max(c.outcomes[x] for x in sent if x in c.outcomes) if lat else 0, "lat": lat})
        c.close()
    except Exception as e:
        q.put({"n": 0, "sent": 0, "first": float("inf"), "last": 0, "lat": [], "error": f"client {i}: {e}"})


def main():
    phase = sys.argv[1]
    if phase == "seed":
        n = int(sys.argv[2])
        for keep, label in ((False, "cold"), (True, "warm")):
            rs = run_parallel(n, seed_one, keep)
            ok = [r for r in rs if isinstance(r, float)]
            errs = [r for r in rs if isinstance(r, str)]
            print(f"zebridge seed {label}, {n} clients: synced {len(ok)}/{n}, median {statistics.median(ok):.2f} s, slowest {max(ok):.2f} s" if ok else f"seed {label}: none synced {errs[:2]}")
    elif phase == "roundtrip":
        secs = int(sys.argv[2])
        w, r = Client(0, db_path(0, True)), Client(1, db_path(1, True))
        lats = []
        tag = uuid.uuid4().hex[:6]
        for k in range(secs):
            key = f"m-{tag}-{k:03d}"
            t = time.monotonic()
            w.mutate({"id": key}, {"id": key, "tenant_id": "cmp", "note": "marker"})
            while time.monotonic() - t < 10:
                w.poll(0); r.poll(5)
                if r.count(f"id = '{key}'"): lats.append((time.monotonic() - t) * 1000); break
            time.sleep(max(0, 1 - (time.monotonic() - t)))
        lats.sort()
        print(f"zebridge round trip (client write -> PostgreSQL -> other client), {len(lats)}/{secs}: median {statistics.median(lats):.0f} ms, p95 {lats[int(0.95 * (len(lats) - 1))]:.0f} ms")
        w.close(); r.close()
    elif phase == "burst":
        n = int(sys.argv[2])
        c = Client(0, db_path(0, True))
        tag = "b" + uuid.uuid4().hex[:6]
        t0 = time.monotonic()
        admin_sql(f"INSERT INTO {T} (id, tenant_id, note) SELECT '{tag}-' || lpad(i::text, 6, '0'), 'cmp', repeat(md5(i::text), 6) FROM generate_series(1, {n}) i")
        t_commit = time.monotonic()
        while c.count(f"id LIKE '{tag}-%'") < n and time.monotonic() - t_commit < 120: c.poll(200)
        got = c.count(f"id LIKE '{tag}-%'")
        print(f"zebridge burst: insert {t_commit - t0:.2f} s; commit -> client holds {got:,}/{n:,} rows: {time.monotonic() - t_commit:.2f} s")
        c.close()
    elif phase == "writes":
        p, m = int(sys.argv[2]), int(sys.argv[3])
        rs = run_parallel(p, writes_one, m)
        for r in rs:
            if r.get("error"): print("ERROR", r["error"])
        lat = sorted(x for r in rs for x in r["lat"])
        n, first, last = sum(r["n"] for r in rs), min(r["first"] for r in rs), max(r["last"] for r in rs)
        pct = lambda f: lat[min(len(lat) - 1, int(f * len(lat)))] if lat else float("nan")
        print(f"zebridge writes, {p} clients x {m}: {n:,}/{sum(r['sent'] for r in rs):,} verdicts in {last - first:.2f} s -> {n / max(last - first, 1e-9):,.0f} writes/s; verdict median {pct(0.5):.0f} ms, p95 {pct(0.95):.0f} ms")


if __name__ == "__main__":
    main()

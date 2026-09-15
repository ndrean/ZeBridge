#!/usr/bin/env python3
"""What an index on the version column costs the writes, and what it saves the producer (NOTES §10gd).

MANUAL. A benchmark that reports, never part of a test run:

    scripts/scenarios/.venv/bin/python scripts/scenarios/version_index.py [--rows 2000000]

zebridge_enable builds a B-tree on the version column (§10gc) so a delta reads only the
rows since the last cut. The version column changes on every update, so that index also
turns every update into a non-HOT update. This measures both sides, for three variants:

  none    the table as enable left it before §10gc: primary key + (tenant_id, id)
  btree   + CREATE INDEX ON t (updated_at)          what enable builds today
  brin    + CREATE INDEX ON t USING brin (updated_at)
          PostgreSQL 16+ keeps an update HOT when only summarizing (BRIN) indexes cover
          the changed columns, so BRIN may keep HOT and still narrow the delta.

Writes, each variant on a fresh database, loaded with --rows rows:
  firehose   200 rounds of: insert 10,000 new rows, update the previous round's 10,000
  scattered  200 updates of 10,000 rows each, walking the whole table once
  repeat     200 updates of the SAME 10,000 rows (a hot set: counters, positions)
             A row updated once on a packed page cannot stay HOT whatever the indexes
             (no free space on the page); a row updated again finds the space its own
             dead versions left once pruned. The HOT cost of an index shows here.
Reported: seconds, WAL bytes, updates that stayed HOT, heap and index size after.

Reads, afterwards, as a non-owner role under the same read policy enable installs, with
`zb.tenant` set as the producer sets it:
  exists idle     the producer's "did anything change" check when nothing did
  exists changed  the same check when the last round changed rows
  delta 10k/100k  the delta query returning about 10,000 and 100,000 rows
Median of 5 runs after one warm-up; the plan's top scan node is printed.

Standalone on purpose: it starts its own PostgreSQL cluster on a scratch port in a temp
directory (§10gb lesson: WAL-heavy runs on the shared dev cluster invalidate slots), with
wal_level=logical and default buffers, then removes it.
"""
import argparse, json, os, pathlib, shutil, statistics, subprocess, sys, tempfile, time

BIN = pathlib.Path("/opt/homebrew/opt/postgresql@18/bin")
PORT = 55491
ROUND = 10_000


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


class Cluster:
    def __init__(self, tmp: pathlib.Path):
        self.data = tmp / "data"
        r = run([BIN / "initdb", "-D", self.data, "-U", "postgres", "--auth=trust", "-E", "UTF8"])
        if r.returncode: sys.exit(r.stderr)
        with open(self.data / "postgresql.conf", "a") as f:
            f.write(f"port = {PORT}\nlisten_addresses = '127.0.0.1'\nwal_level = logical\n"
                    "max_wal_size = 1GB\nshared_buffers = 128MB\nunix_socket_directories = ''\n")
        r = run([BIN / "pg_ctl", "-D", self.data, "-l", tmp / "pg.log", "-w", "start"])
        if r.returncode: sys.exit(r.stderr + (tmp / "pg.log").read_text())

    def stop(self):
        run([BIN / "pg_ctl", "-D", self.data, "-m", "fast", "-w", "stop"])


def psql(sql: str, db="postgres", user="postgres", file=False) -> str:
    cmd = [BIN / "psql", "-h", "127.0.0.1", "-p", str(PORT), "-U", user, "-d", db, "-XtAq", "-v", "ON_ERROR_STOP=1"]
    r = run(cmd + (["-f", sql] if file else ["-c", sql]))
    if r.returncode: sys.exit(f"psql failed: {r.stderr[-500:]}")
    return r.stdout.strip()


def lsn(db):
    return psql("SELECT pg_current_wal_lsn()", db)


def wal_bytes(db, a, b):
    return int(psql(f"SELECT pg_wal_lsn_diff('{b}', '{a}')", db))


def counters(db):
    time.sleep(1.5)  # a backend flushes its table counters on exit; give the stats a moment
    upd, hot, ins = psql("SELECT n_tup_upd, n_tup_hot_upd, n_tup_ins FROM pg_stat_user_tables WHERE relname='fire'", db).split("|")
    return int(upd), int(hot), int(ins)


def sizes(db):
    heap, idx = psql("SELECT pg_relation_size('fire'), pg_indexes_size('fire')", db).split("|")
    return int(heap), int(idx)


def workload(db, tmp: pathlib.Path, name: str, statements: list[str]):
    path = tmp / f"{name}.sql"
    path.write_text("\n".join(statements) + "\n")  # autocommit: one transaction per statement
    psql("CHECKPOINT", db)
    u0, h0, i0 = counters(db)
    a = lsn(db)
    t = time.monotonic()
    psql(str(path), db, file=True)
    secs = time.monotonic() - t
    b = lsn(db)
    u1, h1, i1 = counters(db)
    return {"secs": secs, "wal": wal_bytes(db, a, b), "upd": u1 - u0, "hot": h1 - h0, "ins": i1 - i0}


def explain(db, sql: str):
    times, node = [], ""
    pre = "SET ROLE zb_reader; SELECT set_config('zb.tenant', 'acme', false);"
    for i in range(6):
        out = psql(f"{pre} EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}", db)
        plan = json.loads(out[out.index("["):])[0]
        if i == 0:
            p = plan["Plan"]
            while p.get("Plans") and not p["Node Type"].endswith("Scan"):
                p = p["Plans"][0]
            if p["Node Type"] == "Bitmap Heap Scan" and p.get("Plans"):
                p = {"Node Type": "Bitmap Heap Scan", "Index Name": p["Plans"][0].get("Index Name", "?")}
            node = p["Node Type"] + (f" ({p['Index Name']})" if "Index Name" in p else "")
            rows = plan["Plan"].get("Actual Rows", 0)
            continue
        times.append(plan["Execution Time"])
    return statistics.median(times), node, rows


def variant(name: str, rows: int, tmp: pathlib.Path) -> dict:
    db = f"vi_{name}"
    psql(f"DROP DATABASE IF EXISTS {db}"); psql(f"CREATE DATABASE {db}")
    psql("""
        DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'zb_reader') THEN CREATE ROLE zb_reader; END IF; END $$;
        CREATE TABLE fire (
          id bigint PRIMARY KEY, tenant_id text NOT NULL DEFAULT 'acme', batch int NOT NULL,
          v int NOT NULL DEFAULT 0, payload text NOT NULL,
          updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz);
    """, db)
    t = time.monotonic()
    psql(f"INSERT INTO fire (id, batch, payload) SELECT g, g / {ROUND}, md5(g::text) || md5((g+1)::text) || md5((g+2)::text) "
         f"FROM generate_series(1, {rows}) g", db)
    load_s = time.monotonic() - t
    psql("""
        CREATE UNIQUE INDEX fire_zb_ri ON fire (tenant_id, id);
        GRANT SELECT ON fire TO zb_reader;
        ALTER TABLE fire ENABLE ROW LEVEL SECURITY;
        CREATE POLICY zb_reader_all ON fire FOR SELECT TO zb_reader
          USING (coalesce(current_setting('zb.tenant', true), '') = '' OR tenant_id = current_setting('zb.tenant', true) OR tenant_id = 'open');
    """, db)
    build_s = 0.0
    if name != "none":
        t = time.monotonic()
        psql(f"CREATE INDEX fire_zb_version ON fire {'USING brin ' if name == 'brin' else ''}(updated_at)", db)
        build_s = time.monotonic() - t
    psql("VACUUM ANALYZE fire", db)

    # firehose: insert a round, update the round before it
    fh = []
    for r in range(200):
        lo = rows + r * ROUND + 1
        fh.append(f"INSERT INTO fire (id, batch, payload) SELECT g, {rows // ROUND + r + 1}, md5(g::text) || md5((g+1)::text) || md5((g+2)::text) "
                  f"FROM generate_series({lo}, {lo + ROUND - 1}) g;")
        plo = lo - ROUND
        fh.append(f"UPDATE fire SET v = v + 1, updated_at = now() WHERE id BETWEEN {plo} AND {plo + ROUND - 1};")
    w_fire = workload(db, tmp, f"{name}_firehose", fh)

    total = rows + 200 * ROUND
    step = total // 200
    sc = [f"UPDATE fire SET v = v + 1, updated_at = now() WHERE id BETWEEN {1 + k * step} AND {1 + k * step + ROUND - 1};" for k in range(200)]
    w_scat = workload(db, tmp, f"{name}_scattered", sc)
    rp = [f"UPDATE fire SET v = v + 1, updated_at = now() WHERE id BETWEEN 1 AND {ROUND};" for _ in range(200)]
    w_rep = workload(db, tmp, f"{name}_repeat", rp)
    heap, idx = sizes(db)

    psql("ANALYZE fire", db)
    cut10k = psql(f"SELECT updated_at FROM fire ORDER BY updated_at DESC OFFSET {ROUND} LIMIT 1", db)
    cut100k = psql(f"SELECT updated_at FROM fire ORDER BY updated_at DESC OFFSET {10 * ROUND} LIMIT 1", db)
    reads = {
        "exists idle": explain(db, "SELECT EXISTS(SELECT 1 FROM fire WHERE updated_at > now() + interval '1 hour')"),
        "exists changed": explain(db, f"SELECT EXISTS(SELECT 1 FROM fire WHERE updated_at > '{cut10k}')"),
        "delta 10k": explain(db, f"SELECT * FROM fire WHERE updated_at > '{cut10k}'"),
        "delta 100k": explain(db, f"SELECT * FROM fire WHERE updated_at > '{cut100k}'"),
    }
    psql(f"DROP DATABASE {db}")
    return {"load_s": load_s, "build_s": build_s, "firehose": w_fire, "scattered": w_scat, "repeat": w_rep, "heap": heap, "idx": idx, "reads": reads}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--variants", default="none,btree,brin")
    ap.add_argument("--runs", type=int, default=2, help="runs per variant, interleaved, to show the noise")
    a = ap.parse_args()
    if shutil.which("caffeinate"):  # §10ge: idle sleep froze benchmark runs mid-load
        subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb-vi-"))
    cluster = Cluster(tmp)
    try:
        res = {}
        for run_i in range(a.runs):
            for v in a.variants.split(","):
                key = f"{v} #{run_i + 1}"
                print(f"== {key} ...", flush=True)
                res[key] = variant(v, a.rows, tmp)
                print(json.dumps(res[key]), flush=True)
    finally:
        cluster.stop()
        shutil.rmtree(tmp, ignore_errors=True)

    mb = lambda b: f"{b / 1048576:.0f} MB"
    print(f"\nrows loaded: {a.rows:,}; rounds of {ROUND:,}")
    print("\n| variant | index build | load | workload | seconds | WAL | updates | HOT | heap after | indexes after |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for v, r in res.items():
        for w in ("firehose", "scattered", "repeat"):
            x = r[w]
            hot = f"{100 * x['hot'] / x['upd']:.0f} %" if x["upd"] else "-"
            print(f"| {v} | {r['build_s']:.1f} s | {r['load_s']:.1f} s | {w} | {x['secs']:.1f} | {mb(x['wal'])} | {x['upd']:,} | {hot} | {mb(r['heap'])} | {mb(r['idx'])} |")
    print("\n| variant | query | median ms | rows | scan |")
    print("| --- | --- | --- | --- | --- |")
    for v, r in res.items():
        for q, (ms, node, n) in r["reads"].items():
            print(f"| {v} | {q} | {ms:.3f} | {n:,} | {node} |")


if __name__ == "__main__":
    main()

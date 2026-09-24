#!/usr/bin/env python3
"""The firehose against the chain, plain vs TLS between the bridge and NATS (NOTES §10ga).

    python scripts/scenarios/firehose_tls.py [--seconds 180] [--rate 10000]     (manual; ~10 min)

The question after §10fz: TLS leaves the burst's event rate alone, but it makes each
upload a little slower — does the generation producer still keep its chain inside the
CDC stream while the stream prunes? If a chain's cut falls below the stream's oldest
message, a client that seeds from it cannot splice and waits for the next generation.

The §10eu firehose, isolated (scratch database, scratch nats-servers, the bridge on its
own slot and port, built on burst_tls.py): every second one INSERT of `--rate` rows shaped
like test_types, then an UPDATE of the previous second's rows, so the table grows and
every delta is fat. The CDC stream's byte cap is small (256 MiB) so it prunes within the
first minute and keeps pruning: the producer's edge watch, burst trigger and fill rule
(§10eq–§10es) have to cut early for the chain to stay on the stream.

Measured per run, plain then TLS, identical otherwise:
  * once a second: the stream's oldest sequence and the newest manifest's cutoff sequence;
    margin = cutoff + 1 - oldest. Negative is a hole a client meets. Minimum, and the
    number of negative samples;
  * from the bridge log: early cuts, chains that fell off before a cut, cuts that fell
    off during their build; for each cut of the table, build and upload milliseconds;
  * what PostgreSQL absorbed, and the bridge's and nats-server's CPU.

§10gd adds two knobs, for the version index zebridge_enable builds (§10gc):
  --runs tls:off,tls:on   each run is transport:index; `off` drops fire_types_zb_version
                          before the bridge boots, `on` keeps it. Default plain:off,tls:off
                          (the §10ga comparison, as it ran before the index existed).
  --runs tls:on:defer:sync a fourth field builds every full in the build path (GENERATION_ASYNC_FULLS=false,
                          §10gf); `async` or nothing keeps the background full lane.
  --runs tls:on:nodefer   a third field turns the §10ge full deferral off for that run
                          (GENERATION_DEFER_FULLS=false); `defer` or nothing keeps it on.
  --runs tls:on:defer:async:nogen a fifth field turns the generation producer off (§10gi: the
                          CDC path's memory alone).
  ZB_VMMAP=1              every 20 s, `vmmap -summary` of the bridge into the run directory
                          (with ZB_KEEP=1 to keep it): which regions hold the resident memory.
  ZB_LEAKS=1               the bridge runs with MallocStackLogging, and once the load is over and a
                          cadence has passed, `leaks` scans it twice, a cadence apart, into the
                          run directory (§10gi). Slower bridge: never together with a measured run.
  --client                a real libzb client (C ABI, SQLite, streaming seed) opens once the load is
                          over and a cut after it exists, seeds, follows CDC until it has caught
                          up, and is compared with PostgreSQL per batch: count and sum(age) (every
                          update adds 1). The scratch nats-server then also accepts plain clients
                          (libzb takes no CA); the bridge stays on tls:// (§10gl).
  --client-at S           the same client, opened S seconds into the load: it seeds from the chain
                          live then, follows CDC through the rest of the load, and is compared
                          after it — the only check where CDC carries rows the chain never had.
  --preload N             N static rows loaded before the bridge boots and never updated:
                          every full carries them, every delta should not have to scan them.
§10gf/§10gg: `--verify` seeds the final chain in Python by the clients' plan rule and compares
every row with PostgreSQL; each run also samples the bridge's RSS and the slot's unconfirmed
WAL every second, and reports the events published and the WAL written during the load.
The machine is held awake (`caffeinate`), and a run that slept is marked INVALID (§10ge).
Each cut also reports its query phase (copy+decode+encode), and the run prints the index
names the bridge published in the table's schema (the version index must not be there).
"""
import argparse, json, os, pathlib, re, shutil, statistics, subprocess, sys, tempfile, threading, time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import burst_tls as bt  # the isolation helpers: scratch database, nats-servers, URLs, CPU

TABLE = "fire_types"
CAP_BYTES = 256 * 1024 * 1024
VERIFY = False
CLIENT = False
CLIENT_AT = None  # seconds into the load (--client-at)
TS_CLIENT_AT = None  # the same for zb-client-ts (--ts-client-at)
NODE_DIR = pathlib.Path(__file__).resolve().parents[2] / "examples" / "04-node-consumer"
LIBZB = pathlib.Path(__file__).resolve().parents[2] / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if sys.platform == "darwin" else "libzbcore.so")
CADENCE = 60


def setup_table():
    bt.psql(f"""CREATE TABLE public.{TABLE} (
        uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), batch integer NOT NULL, age integer,
        temperature double precision, price numeric(20,8), is_true boolean, some_text text,
        tags text[], matrix integer[][], metadata jsonb, last_writer varchar(255),
        inserted_at timestamptz NOT NULL, updated_at timestamptz NOT NULL, deleted_at timestamptz)""")
    bt.psql(f"CREATE INDEX ON public.{TABLE} (batch)")
    r = bt.psql(f"SELECT step, status, detail FROM zebridge_enable('public.{TABLE}'::regclass, public_reason => 'firehose benchmark', "
                f"tombstone_col => 'deleted_at', publication => '{bt.PUB}', dry_run => false) WHERE status = 'ERROR'")
    if r.stdout.strip():
        sys.exit(f"enable refused: {r.stdout}")
    # §10gl: the delete guard, which zebridge_enable installs only on a writable table (with
    # zebridge_install_write_guards, whose per-row UPDATE trigger would add a PL/pgSQL call
    # to every one of the load's updates). With it a DELETE writes deleted_at, and the
    # producer takes no count(*) per cut; a TRUNCATE is read from the relfilenode. The load
    # never deletes.
    r = bt.psql(f"CREATE TRIGGER zebridge_soft_delete_t BEFORE DELETE ON public.{TABLE} "
                f"FOR EACH ROW EXECUTE FUNCTION public.zebridge_soft_delete('deleted_at', 'updated_at')")
    if r.returncode != 0:
        sys.exit(f"delete guard: {r.stderr.strip()}")


def load_sql(path: pathlib.Path, seconds: int, rate: int):
    lines = ["\\set QUIET on", "\\o /dev/null", "SELECT clock_timestamp() AS t0 \\gset"]
    for sec in range(seconds):
        lo = sec * rate
        lines.append(
            f"INSERT INTO public.{TABLE} (batch, age, temperature, price, is_true, some_text, tags, matrix, metadata, last_writer, inserted_at, updated_at) "
            f"SELECT {sec}, i % 90, 20 + (i % 100) / 10.0, ((i % 1000) + 0.5)::numeric, i % 2 = 0, format('grow-%s', lpad(i::text, 10, '0')), "
            f"ARRAY['grow', 's-{sec}'], ARRAY[[i, 1], [2, 3]], jsonb_build_object('src', 'grow', 'i', i), 'firehose', now(), now() "
            f"FROM generate_series({lo}, {lo + rate - 1}) AS i;")
        if sec > 0:
            lines.append(f"UPDATE public.{TABLE} SET age = age + 1, updated_at = now() WHERE batch = {sec - 1};")
        lines.append(f"SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM (:'t0'::timestamptz + interval '{sec + 1} seconds') - clock_timestamp())));")
    path.write_text("\n".join(lines) + "\n")


def apply_rate(marks: list) -> int | None:
    """§10gp/§10gu: rows a client applied per second while the load ran, over the longest
    stretch in which its replica only GREW.

    ⚠️ Not the median of the per-sample rates, which is what this was: a re-seed wipes the
    table, the samples around it read as zero or negative, and the median follows them — a
    client applying 1.8M rows correctly reported 305 rows/s. A wipe splits the samples into
    runs; the longest run is the one that measures applying rather than recovering."""
    runs, cur = [], []
    for m in marks:
        if cur and m[1] < cur[-1][1]:
            runs.append(cur)
            cur = []
        cur.append(m)
    runs.append(cur)
    best = max(runs, key=lambda r: (r[-1][0] - r[0][0]) if len(r) > 1 else 0)
    if len(best) < 2 or best[-1][0] <= best[0][0]:
        return None
    return int((best[-1][1] - best[0][1]) / (best[-1][0] - best[0][0]))


def ts_client_check(url: str, run_dir: pathlib.Path, start_at: float, load_done: threading.Event, catch_up_s: int = 900) -> dict:
    """§10gn: the same measurement for zb-client-ts (Node, SQLite through
    `examples/04-node-consumer/follow-worker.ts`). The follower writes its own replica file;
    this reads it directly and compares with PostgreSQL per batch, as for libzb."""
    import sqlite3
    time.sleep(start_at)
    db = run_dir / "ts-client.sqlite3"
    log = (run_dir / "ts-client.log").open("w")
    # Node resolves "localhost" to ::1 first and the scratch server listens on 127.0.0.1
    # only: "connection refused" with the server plainly up.
    env = dict(os.environ, NATS_URL=url.replace("localhost", "127.0.0.1"), ZB_DB=str(db), ZB_TABLES=TABLE, ZB_PRINCIPAL="firehose")
    env.pop("ZB_CREDS", None)
    t0 = time.time()
    proc = subprocess.Popen(["node", "--experimental-strip-types", "follow-worker.ts"], cwd=NODE_DIR, env=env,
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=log, text=True)
    ready = proc.stdout.readline()
    try:
        first = json.loads(ready)
    except json.JSONDecodeError:
        proc.kill(); log.close()
        return {"error": f"follower did not come up: {ready.strip()[:120]!r}"}
    if not first.get("ready"):
        proc.kill(); log.close()
        return {"error": f"connect: {first.get('error')}"}
    connect_s = time.time() - t0

    def local() -> dict:
        try:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=2)
            try:
                return {int(b): (int(n), int(sa)) for b, n, sa in con.execute(f"SELECT batch, count(*), sum(age) FROM {TABLE} GROUP BY batch")}
            finally:
                con.close()
        except Exception:
            return {}

    def rows_now() -> int:
        try:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=2)
            try: return int(con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0])
            finally: con.close()
        except Exception:
            return -1

    pg: dict = {}
    end = time.time() + catch_up_s
    got: dict = {}
    marks: list = []  # §10gp: (t, rows) during the load — the apply rate to optimise against
    while time.time() < end:
        if not load_done.is_set():
            end = time.time() + catch_up_s  # the budget runs from the end of the load
            n = rows_now()
            if n >= 0:
                marks.append((time.time(), n))
            time.sleep(2)
            continue
        if not pg:
            time.sleep(15)  # the bridge publishes the load's tail
            out = subprocess.run([bt.PSQL, bt.db_url(bt.ADMIN_URL, bt.DB), "-XtA", "-F", "|", "-c",
                                  f"SELECT batch, count(*), sum(age) FROM public.{TABLE} WHERE deleted_at IS NULL GROUP BY batch"], capture_output=True, text=True).stdout
            for line in out.splitlines():
                if line:
                    b, n, sa = line.split("|")
                    pg[int(b)] = (int(n), int(sa))
        got = local()
        if got and got == pg:
            break
        time.sleep(5)
    caught = time.time() - t0
    proc.terminate()
    try: proc.wait(timeout=20)
    except subprocess.TimeoutExpired: proc.kill()
    log.close()
    text = (run_dir / "ts-client.log").read_text(errors="replace")
    diff = sorted(b for b in set(pg) | set(got) if pg.get(b) != got.get(b))
    return {"connect_s": round(connect_s, 1), "caught_up_s": round(caught, 1) if not diff else None,
            "apply_rows_s": apply_rate(marks),
            "pg_rows": sum(n for n, _ in pg.values()), "client_rows": sum(n for n, _ in got.values()),
            "batches": len(pg), "batches_wrong": len(diff), "first_wrong": [(b, pg.get(b), got.get(b)) for b in diff[:5]],
            "gaps": text.count("pruned"), "reseeds": text.count("seeded"),
            "sqlite_mib": db.stat().st_size // 2**20 if db.exists() else 0}


def client_check(cli: list, url: str, run_dir: pathlib.Path, load_end: float, wait_s: int = 150, catch_up_s: int = 900,
                 load_done: threading.Event | None = None) -> dict:
    """§10gl: what a real client gets. libzb through its C ABI (the Flutter/iOS path),
    SQLite, streaming seed; opened once a cut after the load exists, so the chain covers
    every write and CDC carries only what came after. Seed time is `sync`; then `poll`
    until every batch's count and sum(age) match PostgreSQL — the age of a row moves on
    every update, so a missed update shows as well as a missed row.

    With `load_done` (`--client-at`) the client opens during the load instead: it seeds
    from whatever chain is live, follows CDC while the load runs, and is compared once the
    load is over — the rows after its cut come only through CDC."""
    import ctypes
    if not LIBZB.exists():
        return {"error": f"{LIBZB} missing: cd libzb && zig build -Doptimize=ReleaseFast"}
    deadline = time.time() + wait_s
    while load_done is None and time.time() < deadline:
        raw = subprocess.run(cli + ["kv", "get", "generations", f"_default.{TABLE}", "--raw"], capture_output=True, text=True).stdout
        if raw.strip().startswith("{"):
            man = json.loads(raw)
            if subprocess.run(bt_psql_cutoff_after(man["cutoff_version"], load_end), capture_output=True, text=True).stdout.strip() == "t":
                break
        time.sleep(1)
    else:
        if load_done is None:
            return {"error": "no cut after the load"}

    def pg_now() -> dict:
        agg = {}
        out = subprocess.run([bt.PSQL, bt.db_url(bt.ADMIN_URL, bt.DB), "-XtA", "-F", "|", "-c",
                              f"SELECT batch, count(*), sum(age) FROM public.{TABLE} WHERE deleted_at IS NULL GROUP BY batch"], capture_output=True, text=True).stdout
        for line in out.splitlines():
            if line:
                b, n, sa = line.split("|")
                agg[int(b)] = (int(n), int(sa))
        return agg
    pg = pg_now() if load_done is None else {}
    lib = ctypes.CDLL(str(LIBZB))
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, extra in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + extra

    def take(ptr):
        try: return json.loads(ctypes.string_at(ptr).decode())
        finally: lib.zb_free(ptr)

    def local() -> dict:
        r = take(lib.zb_client_query(h, f"SELECT batch, count(*), sum(age) FROM {TABLE} GROUP BY batch".encode(), b"[]"))
        if "error" in r:
            raise RuntimeError(r["error"])
        return {int(b): (int(n), int(sa)) for b, n, sa in r["rows"]}

    # §10hf: ZB_CLIENT_ENGINE=duckdb opens the same client on libzb's DuckDB engine (a build
    # with -Dduckdb=true); the replica is then a .duckdb file, sampled with the duckdb module.
    engine = os.environ.get("ZB_CLIENT_ENGINE", "sqlite")
    db = run_dir / ("client.duckdb" if engine == "duckdb" else "client.sqlite3")
    # §10gu: the replica is sampled from OUTSIDE, on its own read-only connection. The C
    # ABI is synchronous — `zb_client_sync` holds the thread for the whole seed — so a
    # sampler living in the poll loop saw nothing at all while a client seeded through the
    # load (apply_rows_s: None, every time it mattered). The TS follower is measured this
    # way already.
    marks: list = []
    sampling = threading.Event()
    sampling.set()

    def sample_file():
        import sqlite3 as sq
        while sampling.is_set():
            try:
                if engine == "duckdb":
                    import duckdb as dk
                    con = dk.connect(str(db), read_only=True)
                else:
                    con = sq.connect(f"file:{db}?mode=ro", uri=True, timeout=1)
                try:
                    marks.append((time.time(), int(con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0])))
                finally:
                    con.close()
            except Exception:
                pass  # not created yet, or locked mid-seed: the next sample catches it
            time.sleep(2)

    threading.Thread(target=sample_file, daemon=True).start()
    t0 = time.time()
    h = lib.zb_client_connect(json.dumps({"natsUrl": url, "dbPath": str(db), "tables": [TABLE], "heartbeatMs": 0, "engine": engine,
                                       "clientId": "firehose-check", "principal": "firehose", "seedStreaming": True}).encode())
    if not h:
        return {"error": "open failed"}
    try:
        synced = take(lib.zb_client_sync(h))
        seed_s = time.time() - t0
        # A failed seed is retried by libzb at the next poll, the way an app sees it: keep
        # polling and say it happened (§10gl: a full pruned while the client read it).
        sync_error = synced.get("error")
        polls, errors, got = 0, [], {}
        end = time.time() + catch_up_s
        next_check = 0.0
        during_load_polls = 0
        while time.time() < end:
            p = take(lib.zb_client_poll(h, 1000))
            polls += 1
            if p.get("error"):
                errors.append(p["error"])
                if len(errors) > 5:
                    break
            if load_done is not None and not load_done.is_set():
                during_load_polls += 1
                end = time.time() + catch_up_s  # the budget runs from the end of the load
                continue
            if load_done is not None and not pg:
                time.sleep(15)  # the bridge publishes the load's tail
                pg = pg_now()
            if time.time() >= next_check:
                next_check = time.time() + 5
                got = local()
                if got == pg:
                    break
        caught_s = time.time() - t0
        got = got or local()
        diff = sorted(b for b in set(pg) | set(got) if pg.get(b) != got.get(b))
        return {"sync_error": sync_error, "seed_s": round(seed_s, 1), "caught_up_s": round(caught_s, 1) if not diff else None, "polls": polls,
                "polls_during_load": during_load_polls, "apply_rows_s": apply_rate(marks),
                "pg_rows": sum(n for n, _ in pg.values()), "client_rows": sum(n for n, _ in got.values()),
                "batches": len(pg), "batches_wrong": len(diff),
                "first_wrong": [(b, pg.get(b), got.get(b)) for b in diff[:5]], "poll_errors": errors[:5],
                "sqlite_mib": db.stat().st_size // 2**20 if db.exists() else 0}
    finally:
        sampling.clear()
        lib.zb_client_close(h)


def verify_chain(cli: list, tmp: pathlib.Path, load_end: float, wait_s: int = 150) -> dict:
    """§10gf: seed from the published chain the way the clients plan it
    (core.planFromManifest) and compare with PostgreSQL, row by row (uid → age, which
    every update moves). Waits first for a manifest cut after the load ended, so no
    write is outside the chain. Then every kept delta is replayed on top: nothing may
    change, and every delta's dictionary must still decode."""
    import msgpack, zstandard
    deadline = time.time() + wait_s
    man = None
    while time.time() < deadline:
        raw = subprocess.run(cli + ["kv", "get", "generations", f"_default.{TABLE}", "--raw"], capture_output=True, text=True).stdout
        if raw.strip().startswith("{"):
            man = json.loads(raw)
            built = subprocess.run(bt_psql_cutoff_after(man["cutoff_version"], load_end), capture_output=True, text=True).stdout.strip()
            if built == "t":
                break
        time.sleep(3)
    else:
        return {"error": "no chain cut after the load within the wait"}
    bucket = man["bucket"]
    cache: dict = {}

    def fetch(name: str) -> bytes:
        if name not in cache:
            path = tmp / f"obj-{name}"
            r = subprocess.run(cli + ["object", "get", bucket, name, "-O", str(path), "--force"], capture_output=True, text=True)
            if r.returncode != 0:
                raise RuntimeError(f"object {name}: {r.stderr.strip()[:200]}")
            cache[name] = path.read_bytes()
        return cache[name]

    def decode(name: str, dict_name: str | None):
        data = fetch(name)
        if data[:4] == b"\x28\xb5\x2f\xfd":
            dctx = zstandard.ZstdDecompressor(dict_data=zstandard.ZstdCompressionDict(fetch(dict_name))) if dict_name else zstandard.ZstdDecompressor()
            data = dctx.stream_reader(data).read()
        return msgpack.unpackb(data, raw=False, strict_map_key=False)

    def apply(replica: dict, doc: dict, full: bool):
        cols = doc["columns"]
        iu, ia, iv = cols.index("uid"), cols.index("age"), cols.index("updated_at")
        if full:
            replica.clear()
        for row in doc["rows"]:
            k = str(row[iu])
            old = replica.get(k)
            if old is None or str(row[iv]) >= old[1]:
                replica[k] = (str(row[ia]), str(row[iv]))

    pg = {}
    out = subprocess.run([bt.PSQL, bt.db_url(bt.ADMIN_URL, bt.DB), "-XtA", "-F", "|", "-c", f"SELECT uid::text, age::text FROM public.{TABLE}"], capture_output=True, text=True).stdout
    for line in out.splitlines():
        if line:
            u, a_ = line.split("|")
            pg[u] = a_

    def compare(replica: dict) -> dict:
        missing = sum(1 for k in pg if k not in replica)
        extra = sum(1 for k in replica if k not in pg)
        wrong = sum(1 for k, v in pg.items() if k in replica and replica[k][0] != v)
        return {"rows": len(replica), "missing": missing, "extra": extra, "wrong_age": wrong}

    result = {"manifest_gen": man["gen"], "full_gen": man["full"]["gen"], "deltas": [d["gen"] for d in man["deltas"]], "pg_rows": len(pg)}
    try:
        fresh: dict = {}
        apply(fresh, decode(man["full"]["object"], None), True)
        for d in man["deltas"]:
            if d["gen"] > man["full"]["gen"]:
                apply(fresh, decode(d["object"], d.get("dict")), False)
        result["fresh"] = compare(fresh)
        if man["deltas"]:
            # A replica already on the chain applies deltas it may partly hold: replaying
            # every kept delta, those below the full included, on the fresh replica must
            # change nothing (version-guarded, and every dictionary must still decode).
            again = dict(fresh)
            for d in man["deltas"]:
                apply(again, decode(d["object"], d.get("dict")), False)
            result["replay_all_deltas"] = compare(again)
    except Exception as e:
        result["error"] = str(e)[:300]
    return result


def bt_psql_cutoff_after(cutoff: str, load_end: float) -> list:
    return [bt.PSQL, bt.db_url(bt.ADMIN_URL, bt.DB), "-XtA", "-c", f"SELECT '{cutoff}'::timestamptz > to_timestamp({load_end})"]


class Sampler(threading.Thread):
    """Once a second: the stream's oldest sequence and the newest manifest's cutoff."""
    def __init__(self, cli, bridge_pid: int = 0, vmmap_dir: pathlib.Path | None = None):
        super().__init__(daemon=True)
        self.cli, self.stop_flag, self.samples, self.bridge_pid = cli, threading.Event(), [], bridge_pid
        self.vmmap_dir, self.vmmap_next = vmmap_dir, 0.0
        self.rss_kib: list = []
        self.rss_t: list = []  # §10gi: when each RSS sample was taken, for rss.csv
        self.slot_lag: list = []

    def run(self):
        while not self.stop_flag.is_set():
            t = time.time()
            try:
                si = json.loads(subprocess.run(self.cli + ["stream", "info", "CDC_PUBLIC", "--json"], capture_output=True, text=True, timeout=5).stdout)
                man_raw = subprocess.run(self.cli + ["kv", "get", "generations", f"_default.{TABLE}", "--raw"], capture_output=True, text=True, timeout=5).stdout
                man = json.loads(man_raw) if man_raw.strip().startswith("{") else None
                first = si["state"]["first_seq"]
                cutoff = (man or {}).get("cutoff_seq")
                self.samples.append({"t": t, "first": first, "last": si["state"]["last_seq"], "bytes": si["state"]["bytes"],
                                     "gen": (man or {}).get("gen"), "cutoff": cutoff,
                                     "margin": (cutoff + 1 - first) if cutoff is not None else None})
            except Exception as e:
                self.samples.append({"t": t, "error": str(e)[:80]})
            # §10gg: the bridge's resident memory (a full holds its rows) and the slot's lag
            # (WAL written but not yet confirmed: what PostgreSQL must keep).
            try:
                if self.bridge_pid:
                    self.rss_kib.append(int(subprocess.run(["ps", "-o", "rss=", "-p", str(self.bridge_pid)], capture_output=True, text=True, timeout=5).stdout.strip() or 0))
                    self.rss_t.append(t)
                if self.vmmap_dir and self.bridge_pid and t >= self.vmmap_next:
                    self.vmmap_next = t + 20
                    out = subprocess.run(["vmmap", "-summary", str(self.bridge_pid)], capture_output=True, text=True, timeout=15).stdout
                    (self.vmmap_dir / f"vmmap-{int(t)}.txt").write_text(out)
                lag = subprocess.run([bt.PSQL, bt.db_url(bt.ADMIN_URL, bt.DB), "-XtAc",
                                      f"SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint FROM pg_replication_slots WHERE slot_name = '{bt.SLOT}'"],
                                     capture_output=True, text=True, timeout=5).stdout.strip()
                if lag:
                    self.slot_lag.append(int(lag))
            except Exception:
                pass
            self.stop_flag.wait(max(0.0, 1.0 - (time.time() - t)))


def run(tmp: pathlib.Path, tls: bool, seconds: int, rate: int, index: bool = False, preload: int = 0, defer: bool = True, async_fulls: bool = True, generations: bool = True) -> dict:
    name = ("TLS" if tls else "plain") + (" + version index" if index else ", no version index") + ("" if defer else ", no full deferral") + ("" if async_fulls else ", fulls in the build path") + ("" if generations else ", no generations")
    bt.psql(f"TRUNCATE public.{TABLE}")
    if index:
        bt.psql(f"CREATE INDEX IF NOT EXISTS {TABLE}_zb_version ON public.{TABLE} (updated_at)")
    else:
        bt.psql(f"DROP INDEX IF EXISTS public.{TABLE}_zb_version")
    if preload:
        bt.psql(
            f"INSERT INTO public.{TABLE} (batch, age, temperature, price, is_true, some_text, tags, matrix, metadata, last_writer, inserted_at, updated_at) "
            f"SELECT -1, i % 90, 20 + (i % 100) / 10.0, ((i % 1000) + 0.5)::numeric, i % 2 = 0, format('base-%s', lpad(i::text, 10, '0')), "
            f"ARRAY['base'], ARRAY[[i, 1], [2, 3]], jsonb_build_object('src', 'base', 'i', i), 'preload', now() - interval '1 hour', now() - interval '1 hour' "
            f"FROM generate_series(1, {preload}) AS i")
    bt.psql(f"VACUUM ANALYZE public.{TABLE}")
    # The producer's bookkeeping lives in PostgreSQL: left from the previous run, it says
    # the chains are already cut, and the fresh nats-server would get none.
    bt.psql("DELETE FROM public.zebridge_generations")
    bt.psql(f"SELECT pg_drop_replication_slot('{bt.SLOT}') FROM pg_replication_slots WHERE slot_name = '{bt.SLOT}'", db=None, stop=False)
    run_dir = pathlib.Path(tempfile.mkdtemp(prefix=re.sub(r"[^a-z0-9]+", "-", name.lower()) + "-", dir=tmp))  # runs may repeat
    nats_proc, url, cli = bt.start_nats(run_dir, tls, allow_non_tls=CLIENT and tls)
    client_url = url
    if CLIENT and tls:
        url = "tls://localhost:14271"  # the bridge keeps TLS; plain is for the client only
    env = {k: v for k, v in os.environ.items() if k not in ("NATS_BRIDGE_NKEY_SEED", "NATS_CREDS", "ZB_SIGNING_SEED", "NATS_TLS_CA")}
    env.update({
        "DATABASE_READER_URL": bt.db_url(os.environ["DATABASE_READER_URL"], bt.DB),
        "DATABASE_WRITER_URL": bt.db_url(os.environ["DATABASE_WRITER_URL"], bt.DB),
        "NATS_URL": url, "LOG_LEVEL": "info", "GENERATIONS_ENABLED": "true" if generations else "false",
        "GENERATION_CADENCE_SECONDS": str(CADENCE), "CDC_MAX_BYTES": str(CAP_BYTES),
        "GENERATION_DEFER_FULLS": "true" if defer else "false",
        "GENERATION_ASYNC_FULLS": "true" if async_fulls else "false",
        # §10gt: the harness runs minutes, not hours — a checkpoint cadence in seconds, from
        # the environment, so a run can exercise the middle level at all.
        **({"GENERATION_CHECKPOINT_SECONDS": os.environ["ZB_CHECKPOINT_SECONDS"]} if os.environ.get("ZB_CHECKPOINT_SECONDS") else {}),
    })
    if tls:
        env["NATS_TLS_CA"] = str(bt.CERTS / "ca.pem")
    if os.environ.get("ZB_LEAKS"):
        env["MallocStackLogging"] = "1"  # leaks names the allocation stack of each leaked block
    log = run_dir / "bridge.log"
    bridge = subprocess.Popen([str(bt.BRIDGE), "--pub", bt.PUB, "--slot", bt.SLOT, "--port", str(bt.HTTP_PORT)],
                              env=env, stdout=log.open("w"), stderr=subprocess.STDOUT)
    sampler = Sampler(cli, bridge.pid, run_dir if os.environ.get("ZB_VMMAP") else None)
    try:
        for _ in range(300):
            if bt.published() >= 0 and "Replication started" in log.read_text(errors="replace"):
                break
            if bridge.poll() is not None:
                sys.exit(f"{name}: the bridge exited:\n{log.read_text(errors='replace')[-1500:]}")
            time.sleep(0.2)
        time.sleep(3)  # the boot tick's first cut of the empty table
        sql = tmp / "firehose.sql"
        load_sql(sql, seconds, rate)
        sampler.start()
        watch = bt.SleepWatch()
        pub0 = bt.published()
        lsn0 = bt.psql("SELECT pg_current_wal_lsn()").stdout.strip()
        b0, n0 = bt.cpu_seconds(bridge.pid), bt.cpu_seconds(nats_proc.pid)
        t0 = time.perf_counter()
        load_done = threading.Event()
        client_box: dict = {}
        client_thread = None
        ts_box: dict = {}
        ts_thread = None
        if TS_CLIENT_AT is not None:
            def ts_mid_load():
                ts_box["r"] = ts_client_check(client_url, run_dir, TS_CLIENT_AT, load_done)
            ts_thread = threading.Thread(target=ts_mid_load, daemon=True)
            ts_thread.start()
        if CLIENT_AT is not None:
            def client_mid_load():
                time.sleep(CLIENT_AT)
                client_box["r"] = client_check(cli, client_url, run_dir, 0.0, load_done=load_done)
            client_thread = threading.Thread(target=client_mid_load, daemon=True)
            client_thread.start()
        r = subprocess.run([bt.PSQL, bt.db_url(bt.ADMIN_URL, bt.DB), "-X", "-f", str(sql)], capture_output=True, text=True)
        load_done.set()
        load_s = time.perf_counter() - t0
        if r.returncode != 0:
            sys.exit(f"{name}: the load failed: {r.stderr[-600:]}")
        load_end = time.time()
        pub1 = bt.published()
        wal_written = int(bt.psql(f"SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '{lsn0}')").stdout.strip() or 0)
        time.sleep(15)  # let the last events publish and the producer take one more look
        sampler.stop_flag.set(); sampler.join(timeout=10)
        b1, n1 = bt.cpu_seconds(bridge.pid), bt.cpu_seconds(nats_proc.pid)
        rows = int(bt.psql(f"SELECT count(*) FROM public.{TABLE}").stdout.strip() or 0)
        text = log.read_text(errors="replace")
        lines = text.splitlines()
        early = sum(1 for l in lines if "cutting early" in l and f"'{TABLE}'" in l)
        deferred = sum(1 for l in lines if "deferring the depth rotation's full" in l and f"'{TABLE}'" in l)
        bg_attached = sum(1 for l in lines if "background full attached" in l and f"'{TABLE}'" in l)
        bg_discarded = sum(1 for l in lines if "background full for" in l and "discarded" in l and f"'{TABLE}'" in l)
        bg_ms = [int(m.group(1)) for m in (re.search(r"background full attached .* in (\d+) ms", l) for l in lines if f"'{TABLE}'" in l) if m]
        at_limit = sum(1 for l in lines if "the limit — building it now" in l and f"'{TABLE}'" in l)
        fell_before = sum(1 for l in lines if "fell off" in l and "cutting a delta with a fresh cut point" in l and f"'{TABLE}'" in l)
        fell_during = sum(1 for l in lines if "fell off" in l and "during the build" in l and f"'{TABLE}'" in l)
        cut_pat = re.compile(rf"g(\d+) for '_default'/'{TABLE}': ([a-z+]+) → .* in (\d+) ms")
        # §10gx: both artifacts are one streamed phase now (count, copy, encode, zstd, upload).
        ph_pat = re.compile(r"phases: full \(count\+copy\+encode\+zstd\+upload\) (\d+) ms, delta \(count\+copy\+encode\+zstd\+upload\) (\d+) ms, dictionary (\d+) ms — (\d+) full row\(s\), (\d+) delta row")
        cuts = []
        for i, l in enumerate(lines):
            m = cut_pat.search(l)
            if m and i + 1 < len(lines):
                ph = ph_pat.search(lines[i + 1])
                if ph:
                    cuts.append({"gen": int(m.group(1)), "kind": m.group(2), "build": int(m.group(3)), "upload": 0, "query": int(ph.group(2)),
                                 "full": int(ph.group(1)), "full_rows": int(ph.group(4)), "delta_rows": int(ph.group(5))})
        (run_dir / "rss.csv").write_text("unix_s,rss_kib\n" + "".join(f"{t:.1f},{k}\n" for t, k in zip(sampler.rss_t, sampler.rss_kib)))
        verified = verify_chain(cli, run_dir, load_end) if VERIFY else None
        if client_thread is not None:
            client_thread.join()
            client = client_box.get("r")
        else:
            client = client_check(cli, client_url, run_dir, load_end) if CLIENT else None
        ts_client = None
        if ts_thread is not None:
            ts_thread.join()
            ts_client = ts_box.get("r")
        leak_lines = []
        if os.environ.get("ZB_LEAKS"):
            # Idle: the last builds are done after a cadence; a second scan a cadence later
            # tells a one-off from a leak that grows.
            for i in (1, 2):
                time.sleep(CADENCE + 10)
                out = subprocess.run(["leaks", str(bridge.pid)], capture_output=True, text=True, timeout=600).stdout
                (run_dir / f"leaks-{i}.txt").write_text(out)
                leak_lines.append(next((l.strip() for l in out.splitlines() if "leaks for" in l), out[-200:].strip()))
        schema_raw = subprocess.run(cli + ["kv", "get", "schemas", TABLE, "--raw"], capture_output=True, text=True).stdout
        try:
            published_indexes = [ix["name"] for ix in json.loads(schema_raw).get("indexes", [])]
        except Exception:
            published_indexes = [f"unreadable: {schema_raw[:80]!r}"]
        good = [s for s in sampler.samples if s.get("margin") is not None]
        pruned = [s for s in good if s["first"] > 1]
        load_errors = [l for l in r.stderr.splitlines() if "ERROR" in l or "FATAL" in l]
        return {"name": name, "load_s": load_s, "events": pub1 - pub0, "wal_written": wal_written,
                "leaks": leak_lines, "run_dir": str(run_dir), "client": client, "ts_client": ts_client,
                "rss_max_mib": max(sampler.rss_kib, default=0) // 1024, "rss_med_mib": (statistics.median(sampler.rss_kib) // 1024) if sampler.rss_kib else 0,
                "slot_lag_max_mib": max(sampler.slot_lag, default=0) // 2**20, "slot_lag_med_mib": (statistics.median(sampler.slot_lag) // 2**20) if sampler.slot_lag else 0, "load_errors": load_errors, "slept_s": watch.slept_s(), "rows": rows, "bridge_cpu": b1 - b0, "nats_cpu": n1 - n0,
                "early": early, "bg_attached": bg_attached, "bg_discarded": bg_discarded, "bg_ms": bg_ms, "deferred": deferred, "at_limit": at_limit, "fell_before": fell_before, "fell_during": fell_during, "cuts": cuts,
                "samples": len(good), "pruning_samples": len(pruned),
                "min_margin": min((s["margin"] for s in pruned), default=None),
                "holes": sum(1 for s in pruned if s["margin"] < 0),
                "hole_times": [time.strftime("%H:%M:%S", time.localtime(s["t"])) for s in pruned if s["margin"] < 0],
                "errors": sum(1 for s in sampler.samples if "error" in s), "log": str(log),
                "published_indexes": published_indexes, "verified": verified}
    finally:
        sampler.stop_flag.set()
        bridge.terminate()
        try: bridge.wait(timeout=20)
        except Exception: bridge.kill()
        nats_proc.terminate()
        try: nats_proc.wait(timeout=10)
        except Exception: nats_proc.kill()
        bt.psql(f"SELECT pg_drop_replication_slot('{bt.SLOT}') FROM pg_replication_slots WHERE slot_name = '{bt.SLOT}'", db=None, stop=False)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=180)
    ap.add_argument("--rate", type=int, default=10_000)
    ap.add_argument("--cap-mib", type=int, default=256, help="CDC_MAX_BYTES in MiB: the smaller, the less time the producer has")
    ap.add_argument("--runs", default="plain:off,tls:off", help="comma-separated transport:index, e.g. tls:off,tls:on (§10gd)")
    ap.add_argument("--ts-client-at", type=float, default=None, help="the same for zb-client-ts, through examples/04-node-consumer/follow-worker.ts (§10gn)")
    ap.add_argument("--client-at", type=float, default=None, help="open the libzb client this many seconds into the load: it seeds mid-load and follows CDC (§10gm)")
    ap.add_argument("--client", action="store_true", help="after the load, a libzb client seeds and follows, compared with PostgreSQL per batch (§10gl)")
    ap.add_argument("--verify", action="store_true", help="after the load, seed from the chain in Python and compare with PostgreSQL (§10gf)")
    ap.add_argument("--preload", type=int, default=0, help="static rows in the table before the bridge boots (§10gd)")
    a = ap.parse_args()
    global CAP_BYTES, VERIFY
    VERIFY = a.verify
    global CLIENT, CLIENT_AT
    CLIENT_AT = a.client_at
    global TS_CLIENT_AT
    TS_CLIENT_AT = a.ts_client_at
    CLIENT = a.client or a.client_at is not None or a.ts_client_at is not None
    CAP_BYTES = a.cap_mib * 1024 * 1024
    bt.keep_awake()
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb_firehose_tls_"))
    try:
        bt.setup_database()
        setup_table()
        results = []
        for spec in a.runs.split(","):
            transport, ix, dfr, asy, gen = (spec.split(":") + ["", "", "", ""])[:5]
            results.append(run(tmp, transport == "tls", a.seconds, a.rate, index=(ix == "on"), preload=a.preload, defer=(dfr != "nodefer"), async_fulls=(asy != "sync"), generations=(gen != "nogen")))
            print("RESULT " + json.dumps(results[-1]), flush=True)  # kept even if a later run fails
        print(f"\nfirehose: {a.rate:,} rows/s inserted + the previous second's updated, {a.seconds} s; CDC cap {CAP_BYTES // 2**20} MiB, cadence {CADENCE} s; preload {a.preload:,} static rows")
        for r in results:
            builds = [c["build"] for c in r["cuts"] if c["delta_rows"] or c["full_rows"]]
            uploads = [c["upload"] for c in r["cuts"] if c["delta_rows"] or c["full_rows"]]
            kinds = {}
            for c in r["cuts"]:
                kinds[c["kind"]] = kinds.get(c["kind"], 0) + 1
            print(f"\n{r['name']}: load {r['load_s']:.0f} s, {r['rows']:,} rows in the table; bridge CPU {r['bridge_cpu']:.0f} s, NATS CPU {r['nats_cpu']:.0f} s")
            print(f"  events published during the load {r['events']:,} ({r['events'] / max(r['load_s'], 1):,.0f}/s); WAL written {r['wal_written'] / 2**30:.1f} GiB; "
                  f"slot lag median {r['slot_lag_med_mib']} MiB, max {r['slot_lag_max_mib']} MiB; bridge RSS median {r['rss_med_mib']} MiB, max {r['rss_max_mib']} MiB")
            if r["slept_s"] > 5:
                print(f"  ⛔ INVALID: the machine slept {r['slept_s']:.0f} s during this run")
            if r["load_errors"]:
                print(f"  ⚠️ the load logged {len(r['load_errors'])} error line(s), first: {r['load_errors'][0][:200]}")
            print(f"  fulls deferred {r['deferred']}, built at the limit {r['at_limit']}; background fulls attached {r['bg_attached']} {r['bg_ms']} ms, discarded {r['bg_discarded']}")
            print(f"  cuts of {TABLE}: {len(r['cuts'])} {kinds}; early cuts {r['early']}; fell off before a cut {r['fell_before']}; during a build {r['fell_during']}")
            if builds:
                print(f"  build ms median {statistics.median(builds):.0f}, max {max(builds)}; upload ms median {statistics.median(uploads):.0f}, max {max(uploads)}")
            for kind in sorted(kinds):
                ks = [c for c in r["cuts"] if c["kind"] == kind and (c["delta_rows"] or c["full_rows"])]
                if ks:
                    print(f"  {kind:>10}: {len(ks)} cuts; query ms median {statistics.median(c['query'] for c in ks):.0f}, max {max(c['query'] for c in ks)}; "
                          f"build ms median {statistics.median(c['build'] for c in ks):.0f}, max {max(c['build'] for c in ks)}; "
                          f"delta rows median {statistics.median(c['delta_rows'] for c in ks):.0f}")
            print(f"  indexes in the published schema: {r['published_indexes']}")
            if r.get("verified") is not None:
                print(f"  chain check: {r['verified']}")
            if r.get("client") is not None:
                print(f"  libzb client: {r['client']}")
            if r.get("ts_client") is not None:
                print(f"  zb-client-ts: {r['ts_client']}")
            print(f"  margin (cutoff + 1 - oldest), {r['pruning_samples']} of {r['samples']} samples while the stream pruned: "
                  f"min {r['min_margin']}, negative {r['holes']}; sampler errors {r['errors']}")
        return 0
    finally:
        if not os.environ.get("ZB_KEEP"):
            shutil.rmtree(tmp, ignore_errors=True)
            bt.psql(f"DROP DATABASE IF EXISTS {bt.DB}", db=None, stop=False)


if __name__ == "__main__":
    sys.exit(main())

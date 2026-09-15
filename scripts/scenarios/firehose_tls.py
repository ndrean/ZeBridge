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
"""
import argparse, json, os, pathlib, re, shutil, statistics, subprocess, sys, tempfile, threading, time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import burst_tls as bt  # the isolation helpers: scratch database, nats-servers, URLs, CPU

TABLE = "fire_types"
CAP_BYTES = 256 * 1024 * 1024
CADENCE = 60


def setup_table():
    bt.psql(f"""CREATE TABLE public.{TABLE} (
        uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), batch integer NOT NULL, age integer,
        temperature double precision, price numeric(20,8), is_true boolean, some_text text,
        tags text[], matrix integer[], metadata jsonb, last_writer varchar(255),
        inserted_at timestamptz NOT NULL, updated_at timestamptz NOT NULL)""")
    bt.psql(f"CREATE INDEX ON public.{TABLE} (batch)")
    r = bt.psql(f"SELECT step, status, detail FROM zebridge_enable('public.{TABLE}'::regclass, public_reason => 'firehose benchmark', "
                f"publication => '{bt.PUB}', dry_run => false) WHERE status = 'ERROR'")
    if r.stdout.strip():
        sys.exit(f"enable refused: {r.stdout}")


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


class Sampler(threading.Thread):
    """Once a second: the stream's oldest sequence and the newest manifest's cutoff."""
    def __init__(self, cli):
        super().__init__(daemon=True)
        self.cli, self.stop_flag, self.samples = cli, threading.Event(), []

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
            self.stop_flag.wait(max(0.0, 1.0 - (time.time() - t)))


def run(tmp: pathlib.Path, tls: bool, seconds: int, rate: int) -> dict:
    name = "TLS" if tls else "plain"
    bt.psql(f"TRUNCATE public.{TABLE}")
    # The producer's bookkeeping lives in PostgreSQL: left from the previous run, it says
    # the chains are already cut, and the fresh nats-server would get none.
    bt.psql("DELETE FROM public.zebridge_generations")
    bt.psql(f"SELECT pg_drop_replication_slot('{bt.SLOT}') FROM pg_replication_slots WHERE slot_name = '{bt.SLOT}'", db=None, stop=False)
    nats_proc, url, cli = bt.start_nats(tmp, tls)
    env = {k: v for k, v in os.environ.items() if k not in ("NATS_BRIDGE_NKEY_SEED", "NATS_CREDS", "ZB_SIGNING_SEED", "NATS_TLS_CA")}
    env.update({
        "DATABASE_READER_URL": bt.db_url(os.environ["DATABASE_READER_URL"], bt.DB),
        "DATABASE_WRITER_URL": bt.db_url(os.environ["DATABASE_WRITER_URL"], bt.DB),
        "NATS_URL": url, "LOG_LEVEL": "info", "GENERATIONS_ENABLED": "true",
        "GENERATION_CADENCE_SECONDS": str(CADENCE), "CDC_MAX_BYTES": str(CAP_BYTES),
    })
    if tls:
        env["NATS_TLS_CA"] = str(bt.CERTS / "ca.pem")
    log = tmp / f"bridge-{name}.log"
    bridge = subprocess.Popen([str(bt.BRIDGE), "--pub", bt.PUB, "--slot", bt.SLOT, "--port", str(bt.HTTP_PORT)],
                              env=env, stdout=log.open("w"), stderr=subprocess.STDOUT)
    sampler = Sampler(cli)
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
        b0, n0 = bt.cpu_seconds(bridge.pid), bt.cpu_seconds(nats_proc.pid)
        t0 = time.perf_counter()
        r = subprocess.run([bt.PSQL, bt.db_url(bt.ADMIN_URL, bt.DB), "-X", "-f", str(sql)], capture_output=True, text=True)
        load_s = time.perf_counter() - t0
        if r.returncode != 0:
            sys.exit(f"{name}: the load failed: {r.stderr[-600:]}")
        time.sleep(15)  # let the last events publish and the producer take one more look
        sampler.stop_flag.set(); sampler.join(timeout=10)
        b1, n1 = bt.cpu_seconds(bridge.pid), bt.cpu_seconds(nats_proc.pid)
        rows = int(bt.psql(f"SELECT count(*) FROM public.{TABLE}").stdout.strip() or 0)
        text = log.read_text(errors="replace")
        lines = text.splitlines()
        early = sum(1 for l in lines if "cutting early" in l and f"'{TABLE}'" in l)
        fell_before = sum(1 for l in lines if "fell off" in l and "cutting a delta with a fresh cut point" in l and f"'{TABLE}'" in l)
        fell_during = sum(1 for l in lines if "fell off" in l and "during the build" in l and f"'{TABLE}'" in l)
        cut_pat = re.compile(rf"g(\d+) for '_default'/'{TABLE}': ([a-z+]+) → .* in (\d+) ms")
        ph_pat = re.compile(r"phases: copy\+decode\+encode (\d+) ms, dictionary (\d+) ms, zstd (\d+) ms, upload (\d+) ms — (\d+) full row\(s\), (\d+) delta row")
        cuts = []
        for i, l in enumerate(lines):
            m = cut_pat.search(l)
            if m and i + 1 < len(lines):
                ph = ph_pat.search(lines[i + 1])
                if ph:
                    cuts.append({"gen": int(m.group(1)), "kind": m.group(2), "build": int(m.group(3)), "upload": int(ph.group(4)),
                                 "full_rows": int(ph.group(5)), "delta_rows": int(ph.group(6))})
        good = [s for s in sampler.samples if s.get("margin") is not None]
        pruned = [s for s in good if s["first"] > 1]
        return {"name": name, "load_s": load_s, "rows": rows, "bridge_cpu": b1 - b0, "nats_cpu": n1 - n0,
                "early": early, "fell_before": fell_before, "fell_during": fell_during, "cuts": cuts,
                "samples": len(good), "pruning_samples": len(pruned),
                "min_margin": min((s["margin"] for s in pruned), default=None),
                "holes": sum(1 for s in pruned if s["margin"] < 0),
                "errors": sum(1 for s in sampler.samples if "error" in s), "log": str(log)}
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
    a = ap.parse_args()
    global CAP_BYTES
    CAP_BYTES = a.cap_mib * 1024 * 1024
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb_firehose_tls_"))
    try:
        bt.setup_database()
        setup_table()
        results = [run(tmp, False, a.seconds, a.rate), run(tmp, True, a.seconds, a.rate)]
        print(f"\nfirehose: {a.rate:,} rows/s inserted + the previous second's updated, {a.seconds} s; CDC cap {CAP_BYTES // 2**20} MiB, cadence {CADENCE} s")
        for r in results:
            builds = [c["build"] for c in r["cuts"] if c["delta_rows"] or c["full_rows"]]
            uploads = [c["upload"] for c in r["cuts"] if c["delta_rows"] or c["full_rows"]]
            kinds = {}
            for c in r["cuts"]:
                kinds[c["kind"]] = kinds.get(c["kind"], 0) + 1
            print(f"\n{r['name']}: load {r['load_s']:.0f} s, {r['rows']:,} rows in the table; bridge CPU {r['bridge_cpu']:.0f} s, NATS CPU {r['nats_cpu']:.0f} s")
            print(f"  cuts of {TABLE}: {len(r['cuts'])} {kinds}; early cuts {r['early']}; fell off before a cut {r['fell_before']}; during a build {r['fell_during']}")
            if builds:
                print(f"  build ms median {statistics.median(builds):.0f}, max {max(builds)}; upload ms median {statistics.median(uploads):.0f}, max {max(uploads)}")
            print(f"  margin (cutoff + 1 - oldest), {r['pruning_samples']} of {r['samples']} samples while the stream pruned: "
                  f"min {r['min_margin']}, negative {r['holes']}; sampler errors {r['errors']}")
        return 0
    finally:
        if not os.environ.get("ZB_KEEP"):
            shutil.rmtree(tmp, ignore_errors=True)
            bt.psql(f"DROP DATABASE IF EXISTS {bt.DB}", db=None, stop=False)


if __name__ == "__main__":
    sys.exit(main())

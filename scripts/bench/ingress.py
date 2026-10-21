#!/usr/bin/env python3
"""Ingress benchmark: committed writes/s and verdict latency through NATS, admission and the listener.

Each run builds its own stack and removes it: a fresh PostgreSQL cluster (initdb, its own port,
so WAL-heavy runs never touch a shared cluster's slots), a private nats-server and operator,
`bridge --mutation-ingress` and one bridge with --lanes ingress lanes. The admission stream's
duplicate window is set to --dup-window-s in every run, so runs compare like for like.

    scripts/scenarios/.venv/bin/python scripts/bench/ingress.py --bridge PATH --label base \\
        --variant zb1 --rate 6000 --seconds 30 --lanes 1 --out results.jsonl

Variants: `sensors` runs examples/09-event/sensors.py unchanged (the README ramp's load);
`legacy`, `zb1`, `dup`, `reject` and `single` run scripts/bench/ingress_load.py. Committed
throughput is PostgreSQL's own row count, sampled every second, never the offered rate or the
verdict count; the ingress backlog (pending and in flight) is sampled with it, and the drain is
the time from the end of the offered load until the row count stops rising.

`cdc` is the README burst instead (2,000 × 1,000-row INSERTs, no consumer attached, LOG_LEVEL
info): the bridge's LOOP iters summed over the burst, per event, and CPU microseconds per event
(bridge_cpu_seconds_total over cdc_events_published). Iterations per event are the no-drift
signal; CPU per event catches per-iteration cost growth that iterations alone cannot see.
Not part of the scenario battery: run it on purpose, under caffeinate.
"""
import argparse, asyncio, hashlib, json, os, pathlib, re, shutil, socket, subprocess, sys, tempfile, threading, time, urllib.request

import nats
import nkeys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/scenarios"))
from rotate_client_key import client_creds as mint_client  # noqa: E402
import zb  # noqa: E402

PY = ROOT / "scripts/scenarios/.venv/bin/python"
DDL = """CREATE TABLE public.sensor_events (
    event_id uuid PRIMARY KEY, tenant_id text NOT NULL, sensor_id integer NOT NULL, kind text NOT NULL,
    value double precision NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)"""


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bridge", required=True, type=pathlib.Path)
    ap.add_argument("--label", required=True)
    ap.add_argument("--variant", choices=("sensors", "legacy", "zb1", "dup", "reject", "single", "cdc"), required=True)
    ap.add_argument("--rate", type=float, required=True, help="writes/s offered in total")
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--lanes", type=int, default=1)
    ap.add_argument("--dup-window-s", type=float, default=5)
    ap.add_argument("--per-process", type=float, default=5000, help="writes/s per load process")
    ap.add_argument("--out", type=pathlib.Path, required=True)
    ap.add_argument("--receipt-guard", action="store_true", help="switch receipt_policy.completion_guard on (receipts-locks variant B)")
    a = ap.parse_args()
    pgbin = pathlib.Path(zb._psql_binary()).parent
    procs, made = [], {}
    work = pathlib.Path(tempfile.mkdtemp(prefix="zb-ingress-bench-"))
    sock = pathlib.Path(tempfile.mkdtemp(prefix="zbb", dir="/tmp"))  # a Unix socket path must stay short
    pg_port, nats_port, http_port = free_port(), free_port(), free_port()
    db = "bench"

    def spawn(args, env, name):
        out = open(work / f"{name}.log", "w")
        p = subprocess.Popen(args, env=env, stdout=out, stderr=subprocess.STDOUT)
        procs.append(p)
        return p

    def wait_log(p, name, text, timeout=60):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if text in (work / f"{name}.log").read_text(errors="replace"):
                return
            if p.poll() is not None:
                break
            time.sleep(.1)
        raise RuntimeError(f"{name}: no '{text}'\n" + (work / f"{name}.log").read_text(errors="replace")[-2000:])

    def sql(statement, database=db):
        r = subprocess.run([str(pgbin / "psql"), "-X", "-h", "127.0.0.1", "-p", str(pg_port), "-U", "postgres", "-d", database,
                            "-v", "ON_ERROR_STOP=1", "-Atq"], input=statement, capture_output=True, text=True)
        if r.returncode:
            raise RuntimeError(r.stderr[-2000:])
        return r.stdout.strip()

    try:
        subprocess.run([str(pgbin / "initdb"), "-D", str(work / "pg"), "-U", "postgres", "-A", "trust"], capture_output=True, check=True)
        subprocess.run([str(pgbin / "pg_ctl"), "-D", str(work / "pg"), "-l", str(work / "pg.log"), "-w", "start", "-o",
                        f"-p {pg_port} -k {sock} -c listen_addresses=127.0.0.1 -c wal_level=logical -c max_wal_senders=4 "
                        f"-c max_replication_slots=4 -c max_connections=60"], capture_output=True, check=True)
        made["pg"] = True
        sql(f"CREATE DATABASE {db}", "postgres")
        env = dict(os.environ, DATABASE_READER_URL=f"postgres://bench_reader:x@127.0.0.1:{pg_port}/{db}",
                   DATABASE_WRITER_URL=f"postgres://bench_writer:x@127.0.0.1:{pg_port}/{db}", TARGET_DB=db,
                   BRIDGE_CDC_PUBLICATION="bench_pub")
        init_sql = subprocess.run([str(a.bridge), "--init-sql"], env=env, capture_output=True, text=True, check=True).stdout
        sql(init_sql)
        # The schema version each result ran on: the rendered init SQL's hash.
        schema = {"schema_sha256": hashlib.sha256(init_sql.encode()).hexdigest()[:16], "receipt_guard": a.receipt_guard}
        if a.receipt_guard:
            sql("UPDATE zebridge_private.receipt_policy SET completion_guard = true")
        sql(DDL)
        refused = sql("SELECT step || ': ' || detail FROM zebridge_enable('public.sensor_events'::regclass, tenant_col => 'tenant_id', "
                      "writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', publication => 'bench_pub', "
                      "dry_run => false) WHERE status = 'ERROR'")
        if refused:
            raise RuntimeError("zebridge_enable refused: " + refused)
        sql("INSERT INTO zebridge_user_tenants (principal, tenant_id) VALUES ('bob', 'globex')")
        if a.variant == "cdc":
            sql("CREATE TABLE public.users (id bigserial PRIMARY KEY, name text, email text, inserted_at timestamptz, updated_at timestamptz NOT NULL)")
            refused = sql("SELECT step || ': ' || detail FROM zebridge_enable('public.users'::regclass, public_reason => 'burst benchmark', "
                          "publication => 'bench_pub', dry_run => false) WHERE status = 'ERROR'")
            if refused:
                raise RuntimeError("zebridge_enable refused: " + refused)

        ident = work / "identity"
        subprocess.run([str(a.bridge), "--init-nats", "operator", "--dir", str(ident), "--js-domain", "hub"], capture_output=True, check=True)
        keys = dict(l.split("=", 1) for l in (ident / "operator.store").read_text().splitlines() if "=" in l and not l.startswith("#"))
        account = nkeys.from_seed(keys["ACCOUNT_SEED"].encode()).public_key.decode()
        conf = (ident / "nats-server.conf").read_text()
        (work / "nats.conf").write_text(conf[:conf.index("\nport:")] + f'\nhost: "127.0.0.1"\nport: {nats_port}\n'
                                        f'max_payload: 8MB\njetstream {{ domain: hub, store_dir: "{work}/js" }}\n')
        bob = work / "bob.creds"
        bob.write_text(mint_client(keys["SK_CLIENT_SEED"], account, "bob", "globex"))
        url = f"nats://127.0.0.1:{nats_port}"
        server = spawn(["nats-server", "-c", str(work / "nats.conf")], dict(os.environ), "nats")
        wait_log(server, "nats", "Server is ready")
        env = dict(env, NATS_URL=url, NATS_CREDS=str(ident / "creds/bridge.creds"), NATS_JS_DOMAIN="hub", NATS_TLS_CA="",
                   MUTATION_ADMISSION="1", GENERATIONS_ENABLED="0", ZB_INGRESS_LANES=str(a.lanes), LOG_LEVEL="info")
        gate = spawn([str(a.bridge), "--mutation-ingress"], env, "admission")
        wait_log(gate, "admission", "mutation admission ready")
        bridge = spawn([str(a.bridge), "--pub", "bench_pub", "--slot", "bench_slot", "--port", str(http_port)], env, "bridge")
        wait_log(bridge, "bridge", "Ready! Pulling mutations", timeout=90)

        async def set_window():
            nc = await nats.connect(url, user_credentials=str(ident / "creds/bridge.creds"))
            js = nc.jetstream(domain="hub")
            info = await js.stream_info("MUTATIONS")
            info.config.duplicate_window = a.dup_window_s
            await js.update_stream(config=info.config)
            window = (await js.stream_info("MUTATIONS")).config.duplicate_window
            await nc.close()
            return window
        window = asyncio.run(set_window())

        if a.variant == "cdc":
            def http(path):
                with urllib.request.urlopen(f"http://127.0.0.1:{http_port}{path}", timeout=10) as r:
                    return r.read().decode()
            events = lambda: int(re.search(r'"cdc_events_published":\s*(\d+)', http("/status")).group(1))
            cpu = lambda: float(re.search(r"^bridge_cpu_seconds_total ([\d.]+)", http("/metrics"), re.M).group(1))
            time.sleep(5)
            e0, c0, mark = events(), cpu(), len((work / "bridge.log").read_bytes())
            burst = "\n".join(f"INSERT INTO public.users (name,email,inserted_at,updated_at) SELECT 'User-{i}-'||i2, "
                              f"'u{i}-'||i2||'@example.com', now(), now() FROM generate_series(1,1000) i2;" for i in range(2000))
            t0 = time.monotonic()
            sql(burst)
            inserted = time.monotonic() - t0
            while events() - e0 < 2_000_000 and time.monotonic() - t0 < 900:
                time.sleep(.5)
            drained = time.monotonic() - t0
            c1, e1 = cpu(), events()
            # `iters` counts every loop turn, `idle` the ones that found nothing and slept:
            # the working turns (iters - idle) of every LOOP line from the burst through the
            # first line logged after the drain, so idle time either side never counts.
            done_at = len((work / "bridge.log").read_bytes())
            settle = time.monotonic() + 20
            while time.monotonic() < settle and b"LOOP iters=" not in (work / "bridge.log").read_bytes()[done_at:]:
                time.sleep(.5)
            text = (work / "bridge.log").read_bytes()[mark:].decode(errors="replace")
            loops = [tuple(map(int, m)) for m in re.findall(r"LOOP iters=(\d+) idle=(\d+)", text)]
            iters = sum(i - d for i, d in loops)
            result = {"label": a.label, **schema, "variant": "cdc", "rows": 2_000_000, "events": e1 - e0, "insert_s": round(inserted, 1),
                      "drain_s": round(drained, 1), "events_per_s": round((e1 - e0) / drained), "loop_lines": len(loops),
                      "working_iters": iters, "working_iters_per_event": round(iters / max(1, e1 - e0), 4),
                      "cpu_s": round(c1 - c0, 2), "cpu_us_per_event": round((c1 - c0) * 1e6 / max(1, e1 - e0), 2)}
            with open(a.out, "a") as f:
                f.write(json.dumps(result) + "\n")
            print(json.dumps(result, indent=1))
            return

        samples, backlog, stop = [], [], threading.Event()
        nats_cli = ["nats", "--server", url, "--creds", str(ident / "creds/bridge.creds"), "--js-domain", "hub"]
        def sample():
            while not stop.is_set():
                samples.append((time.monotonic(), int(sql("SELECT count(*) FROM sensor_events"))))
                r = subprocess.run(nats_cli + ["consumer", "info", "MUTATIONS", "bridge_mutations_worker", "--json"], capture_output=True, text=True)
                if r.returncode == 0:
                    info = json.loads(r.stdout)
                    backlog.append((time.monotonic(), info.get("num_pending", 0), info.get("num_ack_pending", 0)))
                stop.wait(1)
        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        time.sleep(1.2)
        start = time.monotonic()
        n = 1 if a.variant == "single" else max(1, round(a.rate / a.per_process))
        loads = []
        for i in range(n):
            if a.variant == "sensors":
                args = [str(PY), "-u", str(ROOT / "examples/09-event/sensors.py"), "--url", url, "--creds", str(bob),
                        "--sensors", str(int(a.rate / n / 50)), "--first-id", str(i * 1000), "--seconds", str(a.seconds)]
            else:
                args = [str(PY), "-u", str(ROOT / "scripts/bench/ingress_load.py"), "--url", url, "--creds", str(bob),
                        "--variant", a.variant, "--rate", str(a.rate / n), "--seconds", str(a.seconds),
                        "--first-id", str(i * 1000), "--dup-wait-s", str(a.dup_window_s + 3)]
            loads.append(subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True))
        outputs = [p.communicate(timeout=a.seconds * 3 + 180)[0] for p in loads]
        end = time.monotonic()
        # Drained: the row count stops rising (2 s unchanged), at most 120 s after the load.
        while time.monotonic() - end < 120:
            recent = [r for ts, r in samples if ts >= time.monotonic() - 2.5]
            if len(recent) >= 2 and len(set(recent)) == 1:
                break
            time.sleep(.5)
        stop.set()
        sampler.join()

        def rows_at(t):
            return max((r for ts, r in samples if ts <= t), default=samples[0][1])
        base = rows_at(start)
        window_rows = rows_at(start + a.seconds) - base
        parts = []
        for text in outputs:
            if a.variant == "sensors":
                done = [l for l in text.splitlines() if l.startswith("done")]
                m = re.search(r"sent ([\d,]+).*verdicts: (.*?)  ack p50 ([\d.]+) ms p99 ([\d.]+) ms  publish errors (\d+)", done[-1]) if done else None
                parts.append({"sent": int(m.group(1).replace(",", "")), "verdicts": m.group(2), "ack_p50_ms": float(m.group(3)),
                              "ack_p99_ms": float(m.group(4)), "publish_errors": int(m.group(5))} if m else {"raw": text[-500:]})
            else:
                parts.append(json.loads(text.strip().splitlines()[-1]))
        final = samples[-1][1]
        reached = min((ts for ts, r in samples if r == final), default=end)
        during = [b for b in backlog if start <= b[0] <= end + 120]
        result = {"label": a.label, **schema, "variant": a.variant, "lanes": a.lanes, "offered_per_s": a.rate, "seconds": a.seconds,
                  "load_processes": n, "dup_window_s": window,
                  "drain_s": round(max(0.0, reached - (start + a.seconds)), 1),
                  "backlog_max_pending": max((b[1] for b in during), default=None),
                  "backlog_max_in_flight": max((b[2] for b in during), default=None),
                  "committed_rows_in_window": window_rows, "committed_per_s_in_window": round(window_rows / a.seconds),
                  "committed_rows_total": samples[-1][1] - base, "load_wall_s": round(end - start, 1), "processes": parts}
        with open(a.out, "a") as f:
            f.write(json.dumps(result) + "\n")
        print(json.dumps(result, indent=1))
    finally:
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()
        if made.get("pg"):
            subprocess.run([str(pgbin / "pg_ctl"), "-D", str(work / "pg"), "-m", "fast", "stop"], capture_output=True)
        if os.environ.get("ZB_BENCH_KEEP") != "1":
            shutil.rmtree(work, ignore_errors=True)
        shutil.rmtree(sock, ignore_errors=True)


if __name__ == "__main__":
    main()

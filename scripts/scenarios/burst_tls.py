#!/usr/bin/env python3
"""The 2M-row burst, plain vs TLS between the bridge and NATS (NOTES §10fz).

    python scripts/scenarios/burst_tls.py          (manual; several minutes; stop other bridges first)

The SPEED_TEST.md load — 2,000 transactions of 1,000 small rows each — against a bridge
that is identical in both runs except its NATS URL: `nats://` to a plain nats-server, or
`tls://` to a TLS-only one (the nats.zig submodule's test certificates, verified). All of
it isolated: a scratch database rendered from the templates, its own publication and slot,
two scratch nats-servers with the KV buckets and MUTATIONS stream up.sh would create, a
bridge on its own HTTP port. Nothing of the dev stack is written to.

Per run:
  * the CDC rate: 2,000,000 over the time from the first published event to the last,
    read from the bridge's `bridge_cdc_events_published_total` every 250 ms;
  * CPU seconds of the bridge and of nats-server over the run;
  * the generation producer's full chain of the 2M rows: build time, the upload phase
    and the object size — the large-message side, where TLS costs most, and the one that
    must stay faster than events arrive or clients fall off the stream.

§10ge: `--runs tls:off,tls:on` chooses each run's transport and whether bench_users keeps
the version index zebridge_enable builds (§10gc), and each run reports the WAL the load
wrote — the index adds WAL the walsender must read even though decoding skips it.

Needs nats-server, the nats CLI, envsubst, psql, a ReleaseFast bridge, `.env.admin` and
`.env.bridge` sourced (for the role URLs and the template variables).
"""
import json, os, pathlib, re, shutil, subprocess, sys, tempfile, time, urllib.request, importlib.util

ROOT = pathlib.Path(__file__).resolve().parents[2]
BRIDGE = ROOT / "zig-out" / "bin" / "bridge"
CERTS = ROOT / "nats.zig" / "tests" / "configs" / "certs"
PSQL = shutil.which("psql") or "/opt/homebrew/opt/postgresql@18/bin/psql"
ADMIN_URL = os.environ.get("ADMIN_DATABASE_URL", "postgres://postgres@127.0.0.1:5432/postgres")
DB, PUB, SLOT, HTTP_PORT = "zb_burst_bench", "p_burst", "zb_burst_slot", 9098
STATEMENTS, PER = int(os.environ.get("ZB_BURST_STATEMENTS", "2000")), 1000
TOTAL = STATEMENTS * PER

_spec = importlib.util.spec_from_file_location("zb_derive_env", ROOT / "scripts" / "zb-derive-env.py")
_mod = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(_mod); _mod.derive_into(os.environ)


def db_url(url: str, db: str) -> str:
    return re.sub(r"/[^/?]+(\?|$)", f"/{db}\\1", url, count=1) if re.search(r"://[^/]+/", url) else url + f"/{db}"


def psql(sql: str, db: str | None = DB, stop=True) -> subprocess.CompletedProcess:
    url = ADMIN_URL if db is None else db_url(ADMIN_URL, db)
    return subprocess.run([PSQL, url, "-tA", "-q"] + (["-v", "ON_ERROR_STOP=1"] if stop else []) + ["-c", sql], capture_output=True, text=True)


def cpu_seconds(pid: int) -> float:
    t = subprocess.run(["ps", "-o", "time=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    secs = 0.0
    for part in t.replace("-", ":").split(":"):
        secs = secs * 60 + float(part or 0)
    return secs


def published() -> int:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{HTTP_PORT}/metrics", timeout=2) as r:
            m = re.search(r"^bridge_cdc_events_published_total (\d+)", r.read().decode(), re.M)
            return int(m.group(1)) if m else -1
    except Exception:
        return -1


def keep_awake():
    """§10ge: macOS idle sleep froze three benchmark runs mid-load (pmset log: sleep at
    14:54:04, wake 14:57:39, inside a firehose run). PostgreSQL's pacing follows the wall
    clock while the harness's monotonic clock stops, so a slept run looks like a short
    load that then bursts. `caffeinate -i -w <this pid>` holds the machine awake for as
    long as the harness lives. No-op where caffeinate does not exist.
    §10gj: `-i` alone did not hold it: a run started during a maintenance dark wake (the
    owner away) went back to 'Maintenance Sleep' for 271 s with the assertion held, and
    woke at the owner's keyboard. `-s` (no system sleep on AC power) is added."""
    if shutil.which("caffeinate"):
        subprocess.Popen(["caffeinate", "-i", "-s", "-w", str(os.getpid())], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


class SleepWatch:
    """Wall time against monotonic time over a run: on macOS the monotonic clock stops
    while the system sleeps, so a gap between the two is time the machine was asleep."""
    def __init__(self):
        self.wall, self.mono = time.time(), time.monotonic()

    def slept_s(self) -> float:
        return (time.time() - self.wall) - (time.monotonic() - self.mono)


def setup_database():
    psql(f"SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name = '{SLOT}'", db=None, stop=False)
    psql(f"DROP DATABASE IF EXISTS {DB}", db=None, stop=False)
    r = psql(f"CREATE DATABASE {DB}", db=None)
    if r.returncode != 0:
        sys.exit(f"cannot create {DB}: {r.stderr}")
    env = dict(os.environ, TARGET_DB=DB)
    for template in ("init.core.template.sql", "init.write.template.sql"):
        rendered = subprocess.run(["envsubst"], stdin=open(ROOT / template), capture_output=True, text=True, env=env).stdout
        r = subprocess.run([PSQL, db_url(ADMIN_URL, DB), "-v", "ON_ERROR_STOP=1", "-q"], input=rendered, capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"{template} did not apply: {r.stderr[-400:]}")
    psql(f"SELECT * FROM zebridge_create_publication('{PUB}')")
    psql("CREATE TABLE public.bench_users (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text NOT NULL, email text, "
         "inserted_at timestamptz NOT NULL, updated_at timestamptz NOT NULL)")
    r = psql(f"SELECT step, status, detail FROM zebridge_enable('public.bench_users'::regclass, public_reason => 'burst benchmark', "
             f"publication => '{PUB}', dry_run => false) WHERE status = 'ERROR'")
    if r.stdout.strip():
        sys.exit(f"enable refused: {r.stdout}")


def start_nats(tmp: pathlib.Path, tls: bool, allow_non_tls: bool = False) -> tuple[subprocess.Popen, str, list[str]]:
    """A scratch nats-server. `allow_non_tls` (with `tls`): the TLS block is configured, so
    the server advertises TLS as available, but plain clients are accepted too — what a
    server behind a TLS-terminating proxy needs (§10gb). The URL returned is then plain."""
    name = "tls" if tls else "plain"
    port = 14271 if tls else 14270
    (tmp / name).mkdir()
    conf = tmp / f"{name}.conf"
    text = f'listen: 127.0.0.1:{port}\nmax_payload: 8388608\njetstream {{ store_dir: "{tmp}/{name}", max_file_store: 20G }}\n'
    if tls:
        text += f'tls {{\n  cert_file: "{CERTS}/server-cert.pem"\n  key_file: "{CERTS}/server-key.pem"\n}}\n'
        if allow_non_tls:
            text += "allow_non_tls: true\n"
    conf.write_text(text)
    log = tmp / f"{name}.log"
    proc = subprocess.Popen(["nats-server", "-c", str(conf)], stdout=log.open("w"), stderr=subprocess.STDOUT)
    for _ in range(100):
        if "Server is ready" in log.read_text():
            break
        time.sleep(0.1)
    else:
        sys.exit(f"{name} nats-server did not start")
    secure = tls and not allow_non_tls
    url = f"{'tls' if secure else 'nats'}://localhost:{port}"
    cli = ["nats", "--server", url] + (["--tlsca", str(CERTS / "ca.pem")] if secure else [])
    for args in (["kv", "add", "schemas", "--history=10", "--replicas=1"], ["kv", "add", "tenants", "--history=1", "--replicas=1"],
                 ["kv", "add", "generations", "--history=1", "--replicas=1"], ["kv", "add", "live", "--history=1", "--ttl=90s", "--replicas=1"],
                 ["stream", "add", "MUTATIONS", "--subjects=mutation.>,mutation_error.>,mutation_ack.>", "--storage=file",
                  "--retention=work", "--max-age=2h", "--max-bytes=1G", "--replicas=1", "--discard=new", "--defaults"]):
        r = subprocess.run(cli + args, capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"{name}: {' '.join(args[:3])}: {r.stderr.strip()}")
    return proc, url, cli


def run(tmp: pathlib.Path, tls: bool, index: bool | None = None) -> dict:
    name = ("TLS" if tls else "plain") + ("" if index is None else (" + index" if index else " no index"))
    psql("TRUNCATE public.bench_users")
    if index is True:
        psql("CREATE INDEX IF NOT EXISTS bench_users_zb_version ON public.bench_users (updated_at)")
    elif index is False:
        psql("DROP INDEX IF EXISTS public.bench_users_zb_version")
    # The producer's bookkeeping lives in PostgreSQL: left from the previous run it says
    # the chains are already cut, and the fresh nats-server would get only deltas.
    psql("DELETE FROM public.zebridge_generations")
    psql(f"SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name = '{SLOT}'", db=None, stop=False)
    run_dir = pathlib.Path(tempfile.mkdtemp(prefix=re.sub(r"[^a-z0-9]+", "-", name.lower()) + "-", dir=tmp))
    nats_proc, url, _ = start_nats(run_dir, tls)
    env = {k: v for k, v in os.environ.items() if k not in ("NATS_BRIDGE_NKEY_SEED", "NATS_CREDS", "ZB_SIGNING_SEED", "NATS_TLS_CA")}
    env.update({
        "DATABASE_READER_URL": db_url(os.environ["DATABASE_READER_URL"], DB),
        "DATABASE_WRITER_URL": db_url(os.environ["DATABASE_WRITER_URL"], DB),
        "NATS_URL": url, "LOG_LEVEL": "info",
        "GENERATIONS_ENABLED": "true", "GENERATION_CADENCE_SECONDS": "20",
    })
    if tls:
        env["NATS_TLS_CA"] = str(CERTS / "ca.pem")
    log = run_dir / "bridge.log"
    bridge = subprocess.Popen([str(BRIDGE), "--pub", PUB, "--slot", SLOT, "--port", str(HTTP_PORT)],
                              env=env, stdout=log.open("w"), stderr=subprocess.STDOUT)
    try:
        for _ in range(300):
            if published() >= 0 and "Replication started" in log.read_text(errors="replace"):
                break
            if bridge.poll() is not None:
                sys.exit(f"{name}: the bridge exited:\n{log.read_text(errors='replace')[-1500:]}")
            time.sleep(0.2)
        else:
            sys.exit(f"{name}: the bridge never became ready:\n{log.read_text(errors='replace')[-1500:]}")
        base = published()
        load = tmp / "load.sql"
        load.write_text("\n".join(
            f"INSERT INTO public.bench_users (name,email,inserted_at,updated_at) "
            f"SELECT 'User-{i}-'||i2, 'u{i}-'||i2||'@example.com', now(), now() FROM generate_series(1,{PER}) i2;"
            for i in range(STATEMENTS)))
        watch = SleepWatch()
        b0, n0 = cpu_seconds(bridge.pid), cpu_seconds(nats_proc.pid)
        lsn0 = psql("SELECT pg_current_wal_lsn()").stdout.strip()
        t_load = time.perf_counter()
        loader = subprocess.Popen([PSQL, db_url(ADMIN_URL, DB), "-q", "-f", str(load)], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        t_first = t_last = None
        deadline = time.perf_counter() + 900
        while time.perf_counter() < deadline:
            n = published() - base
            now = time.perf_counter()
            if n > 0 and t_first is None:
                t_first = now
            if n >= TOTAL:
                t_last = now
                break
            time.sleep(0.25)
        loader.wait()
        load_s = time.perf_counter() - t_load
        wal = int(psql(f"SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '{lsn0}')").stdout.strip())
        b1, n1 = cpu_seconds(bridge.pid), cpu_seconds(nats_proc.pid)
        if t_last is None:
            sys.exit(f"{name}: only {published() - base} of {TOTAL} events published within 15 min")
        # the full chain of all the rows: wait for the producer's line
        full = None
        deadline = time.perf_counter() + float(os.environ.get("ZB_CHAIN_WAIT", "300"))
        pat = re.compile(r"g(\d+) for '[^']*'/'bench_users': [a-z+]*full .* in (\d+) ms")
        phases = re.compile(r"phases: copy\+decode\+encode (\d+) ms, dictionary (\d+) ms, zstd (\d+) ms, upload (\d+) ms — (\d+) full row")
        sizes = re.compile(r"'bench_users': g\d+ [a-z+]*full (\d+) -> (\d+) bytes")
        while time.perf_counter() < deadline and full is None:
            lines = log.read_text(errors="replace").splitlines()
            for i, line in enumerate(lines):
                m = pat.search(line)
                if not m:
                    continue
                for nxt in lines[i + 1:i + 4]:
                    ph = phases.search(nxt)
                    if ph and int(ph.group(5)) >= TOTAL:
                        size = None
                        for prev in reversed(lines[max(0, i - 4):i + 1]):
                            sm = sizes.search(prev)
                            if sm:
                                size = int(sm.group(2))
                                break
                        full = {"build_ms": int(m.group(2)), "encode_ms": int(ph.group(1)), "zstd_ms": int(ph.group(3)),
                                "upload_ms": int(ph.group(4)), "rows": int(ph.group(5)), "bytes": size}
            time.sleep(1)
        loop = [l for l in log.read_text(errors="replace").splitlines() if "LOOP " in l][-3:]
        return {"name": name, "rate": TOTAL / (t_last - t_first), "drain_s": t_last - t_first, "load_s": load_s,
                "bridge_cpu": b1 - b0, "nats_cpu": n1 - n0, "full": full, "loop": loop, "log": str(log), "wal": wal, "slept_s": watch.slept_s()}
    finally:
        bridge.terminate()
        try: bridge.wait(timeout=20)
        except Exception: bridge.kill()
        nats_proc.terminate()
        try: nats_proc.wait(timeout=10)
        except Exception: nats_proc.kill()
        psql(f"SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name = '{SLOT}'", db=None, stop=False)


def main() -> int:
    for tool in ("nats-server", "nats", "envsubst"):
        if shutil.which(tool) is None:
            sys.exit(f"{tool} not on PATH")
    if not BRIDGE.exists():
        sys.exit("build the bridge first: zig build -Doptimize=ReleaseFast")
    keep_awake()
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb_burst_tls_"))
    keep = os.environ.get("ZB_KEEP")
    try:
        setup_database()
        runs = next((a.split("=", 1)[1] for a in sys.argv[1:] if a.startswith("--runs=")), None)
        if runs:
            results = []
            for spec in runs.split(","):
                transport, _, ix = spec.partition(":")
                results.append(run(tmp, tls=(transport == "tls"), index=(ix == "on") if ix else None))
                print("RESULT " + json.dumps(results[-1]), flush=True)
        else:
            results = [run(tmp, tls=False), run(tmp, tls=True)]
        print(f"\n{TOTAL:,} rows, {STATEMENTS} transactions of {PER}")
        print(f"{'':16} {'events/s':>10} {'drain':>8} {'bridge CPU':>11} {'NATS CPU':>9} {'WAL':>9} | {'full chain':>10} {'upload':>9} {'object':>9}")
        for r in results:
            f = r["full"] or {}
            obj = f"{f['bytes'] / 1e6:.1f} MB" if f.get("bytes") else "?"
            print(f"{r['name']:16} {r['rate']:>10,.0f} {r['drain_s']:>7.1f}s {r['bridge_cpu']:>10.1f}s {r['nats_cpu']:>8.1f}s {r['wal'] / 2**20:>6.0f} MB | "
                  f"{(str(f.get('build_ms')) + ' ms') if f else 'not seen':>10} {(str(f.get('upload_ms')) + ' ms') if f else '':>9} {obj:>9}")
        p, t = results[0], results[-1]
        if not runs: print(f"TLS/plain: rate x{t['rate'] / p['rate']:.2f}, bridge CPU x{t['bridge_cpu'] / max(p['bridge_cpu'], 0.01):.2f}, "
              f"NATS CPU x{t['nats_cpu'] / max(p['nats_cpu'], 0.01):.2f}"
              + (f", chain upload x{t['full']['upload_ms'] / max(p['full']['upload_ms'], 1):.2f}" if p["full"] and t["full"] else ""))
        for r in results:
            print(f"{r['name']} LOOP tail: " + " || ".join(l.split('LOOP ', 1)[-1] for l in r["loop"]))
        return 0
    finally:
        if not keep:
            shutil.rmtree(tmp, ignore_errors=True)
            psql(f"DROP DATABASE IF EXISTS {DB}", db=None, stop=False)


if __name__ == "__main__":
    sys.exit(main())

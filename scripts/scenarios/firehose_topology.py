#!/usr/bin/env python3
"""Two edge topologies under the firehose, with clients attached (NOTES §10gb).

    python scripts/scenarios/firehose_topology.py [--clients 20] [--seconds 180] [--cap-mib 64]
    (manual; ~10 min; nothing else writing heavy WAL, no other bridge stopped meanwhile)

§10ga measured the chain with no consumer connected. With clients, encrypting every
delivery to every phone is the dominant TLS work, and where it runs is the difference
between the two topologies:

  topology 2  nats-server's client port is TLS; the bridge dials tls://; the consumers
              dial nats-server over TLS: nats-server encrypts the fan-out itself.
  topology 1  nats-server listens plain on localhost; the bridge dials nats://; HAProxy
              terminates the consumers' TLS (TCP mode, TLS-first handshake) and forwards
              plain: HAProxy encrypts the fan-out, nats-server writes plain bytes.

Same machine, same load (the §10eu firehose on a scratch database), same consumers
(`nats bench js ordered`, N clients tailing CDC_PUBLIC), same cap. Measured per run: the
chain margin every second (cutoff + 1 − oldest; negative is a hole), early cuts, fallen
cuts, build and upload times, and the CPU of nats-server, HAProxy, the bridge and the
consumer processes.
"""
import argparse, os, pathlib, shutil, statistics, subprocess, sys, tempfile, time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import burst_tls as bt
import firehose_tls as fh

HAPROXY_PORT = 14272


def start_haproxy(tmp: pathlib.Path, backend_port: int) -> subprocess.Popen:
    pem = tmp / "haproxy-server.pem"
    pem.write_text((bt.CERTS / "server-cert.pem").read_text() + (bt.CERTS / "server-key.pem").read_text())
    cfg = tmp / "haproxy.cfg"
    cfg.write_text(f"""global
    maxconn 10000
defaults
    mode tcp
    timeout connect 5s
    timeout client 1h
    timeout server 1h
frontend nats_tls
    bind 127.0.0.1:{HAPROXY_PORT} ssl crt {pem}
    default_backend nats_plain
backend nats_plain
    server nats 127.0.0.1:{backend_port}
""")
    r = subprocess.run(["haproxy", "-c", "-f", str(cfg)], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"haproxy config: {r.stderr[-400:]}")
    return subprocess.Popen(["haproxy", "-f", str(cfg)], stdout=(tmp / "haproxy.log").open("w"), stderr=subprocess.STDOUT)


def run(tmp: pathlib.Path, topology: int, clients: int, seconds: int, rate: int) -> dict:
    name = f"topology {topology}"
    bridge_tls = topology == 2
    bt.psql(f"TRUNCATE public.{fh.TABLE}")
    bt.psql("DELETE FROM public.zebridge_generations")
    bt.psql(f"SELECT pg_drop_replication_slot('{bt.SLOT}') FROM pg_replication_slots WHERE slot_name = '{bt.SLOT}'", db=None, stop=False)
    for sub in ("plain", "tls"):
        shutil.rmtree(tmp / sub, ignore_errors=True)
        (tmp / f"{sub}.log").unlink(missing_ok=True)
    # Topology 1: nats-server keeps a TLS block with allow_non_tls, so it ADVERTISES TLS
    # while the bridge and HAProxy talk to it plain. Without the block, a client that
    # asked for TLS through HAProxy refuses the server's plain INFO ("secure connection
    # not available") — found by the first run of this harness.
    nats_proc, url, cli = bt.start_nats(tmp, tls=True, allow_non_tls=(topology == 1))
    haproxy = start_haproxy(tmp, 14271) if topology == 1 else None
    env = {k: v for k, v in os.environ.items() if k not in ("NATS_BRIDGE_NKEY_SEED", "NATS_CREDS", "ZB_SIGNING_SEED", "NATS_TLS_CA")}
    env.update({
        "DATABASE_READER_URL": bt.db_url(os.environ["DATABASE_READER_URL"], bt.DB),
        "DATABASE_WRITER_URL": bt.db_url(os.environ["DATABASE_WRITER_URL"], bt.DB),
        "NATS_URL": url, "LOG_LEVEL": "info", "GENERATIONS_ENABLED": "true",
        "GENERATION_CADENCE_SECONDS": str(fh.CADENCE), "CDC_MAX_BYTES": str(fh.CAP_BYTES),
    })
    if bridge_tls:
        env["NATS_TLS_CA"] = str(bt.CERTS / "ca.pem")
    log = tmp / f"bridge-t{topology}.log"
    bridge = subprocess.Popen([str(bt.BRIDGE), "--pub", bt.PUB, "--slot", bt.SLOT, "--port", str(bt.HTTP_PORT)],
                              env=env, stdout=log.open("w"), stderr=subprocess.STDOUT)
    consumers = None
    sampler = fh.Sampler(cli)
    try:
        for _ in range(300):
            if bt.published() >= 0 and "Replication started" in log.read_text(errors="replace"):
                break
            if bridge.poll() is not None:
                sys.exit(f"{name}: the bridge exited:\n{log.read_text(errors='replace')[-1500:]}")
            time.sleep(0.2)
        time.sleep(3)
        # the consumers: TLS to nats-server (topology 2) or TLS-first to HAProxy (topology 1)
        cons_url = url if topology == 2 else f"tls://localhost:{HAPROXY_PORT}"
        cons_cmd = ["nats", "--server", cons_url, "--tlsca", str(bt.CERTS / "ca.pem")] + (["--tlsfirst"] if topology == 1 else []) + \
                   ["bench", "js", "ordered", "--stream", "CDC_PUBLIC", "--clients", str(clients), "--msgs", "1000000000", "--no-progress"]
        consumers = subprocess.Popen(cons_cmd, stdout=(tmp / f"consumers-t{topology}.log").open("w"), stderr=subprocess.STDOUT)
        time.sleep(3)
        if consumers.poll() is not None:
            sys.exit(f"{name}: consumers exited: {(tmp / f'consumers-t{topology}.log').read_text()[-800:]}")
        sql = tmp / "firehose.sql"
        fh.load_sql(sql, seconds, rate)
        sampler.start()
        pids = {"nats-server": nats_proc.pid, "bridge": bridge.pid, "consumers": consumers.pid}
        if haproxy:
            pids["haproxy"] = haproxy.pid
        cpu0 = {k: bt.cpu_seconds(p) for k, p in pids.items()}
        t0 = time.perf_counter()
        r = subprocess.run([bt.PSQL, bt.db_url(bt.ADMIN_URL, bt.DB), "-X", "-f", str(sql)], capture_output=True, text=True)
        load_s = time.perf_counter() - t0
        if r.returncode != 0:
            sys.exit(f"{name}: the load failed: {r.stderr[-600:]}")
        time.sleep(15)
        sampler.stop_flag.set(); sampler.join(timeout=10)
        cpu = {k: bt.cpu_seconds(p) - cpu0[k] for k, p in pids.items()}
        consumers_alive = consumers.poll() is None
        lines = log.read_text(errors="replace").splitlines()
        early = sum(1 for l in lines if "cutting early" in l and f"'{fh.TABLE}'" in l)
        fell = sum(1 for l in lines if "fell off" in l)
        import re
        cut_pat = re.compile(rf"g(\d+) for '_default'/'{fh.TABLE}': ([a-z+]+) → .* in (\d+) ms")
        ph_pat = re.compile(r"upload (\d+) ms — (\d+) full row\(s\), (\d+) delta row")
        builds, uploads = [], []
        for i, l in enumerate(lines):
            m = cut_pat.search(l)
            if m and i + 1 < len(lines):
                ph = ph_pat.search(lines[i + 1])
                if ph and (int(ph.group(2)) or int(ph.group(3))):
                    builds.append(int(m.group(3))); uploads.append(int(ph.group(1)))
        pruned = [s for s in sampler.samples if s.get("margin") is not None and s["first"] > 1]
        return {"name": name, "load_s": load_s, "cpu": cpu, "consumers_alive": consumers_alive,
                "cuts": len(builds), "early": early, "fell": fell,
                "build_med": statistics.median(builds) if builds else None, "build_max": max(builds) if builds else None,
                "upload_med": statistics.median(uploads) if uploads else None, "upload_max": max(uploads) if uploads else None,
                "samples": len(pruned), "min_margin": min((s["margin"] for s in pruned), default=None),
                "holes": sum(1 for s in pruned if s["margin"] < 0)}
    finally:
        sampler.stop_flag.set()
        for p in (consumers, bridge, haproxy, nats_proc):
            if p is None:
                continue
            p.terminate()
            try: p.wait(timeout=15)
            except Exception: p.kill()
        bt.psql(f"SELECT pg_drop_replication_slot('{bt.SLOT}') FROM pg_replication_slots WHERE slot_name = '{bt.SLOT}'", db=None, stop=False)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", type=int, default=20)
    ap.add_argument("--seconds", type=int, default=180)
    ap.add_argument("--rate", type=int, default=10_000)
    ap.add_argument("--cap-mib", type=int, default=64)
    a = ap.parse_args()
    fh.CAP_BYTES = a.cap_mib * 1024 * 1024
    for tool in ("haproxy", "nats", "nats-server"):
        if shutil.which(tool) is None:
            sys.exit(f"{tool} not on PATH")
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb_firehose_topo_"))
    try:
        bt.setup_database()
        fh.setup_table()
        results = [run(tmp, 2, a.clients, a.seconds, a.rate), run(tmp, 1, a.clients, a.seconds, a.rate)]
        print(f"\nfirehose {a.rate:,} rows/s for {a.seconds} s, {a.clients} TLS consumers tailing CDC_PUBLIC, cap {a.cap_mib} MiB, cadence {fh.CADENCE} s")
        for r in results:
            c = r["cpu"]
            print(f"\n{r['name']}: consumers alive at the end {r['consumers_alive']}")
            print("  CPU s: " + ", ".join(f"{k} {v:.1f}" for k, v in c.items()))
            print(f"  cuts {r['cuts']} (early {r['early']}), fell off {r['fell']}; build ms median {r['build_med']}, max {r['build_max']}; upload ms median {r['upload_med']}, max {r['upload_max']}")
            print(f"  margin over {r['samples']} samples while pruning: min {r['min_margin']}, negative {r['holes']}")
        return 0
    finally:
        if not os.environ.get("ZB_KEEP"):
            shutil.rmtree(tmp, ignore_errors=True)
            bt.psql(f"DROP DATABASE IF EXISTS {bt.DB}", db=None, stop=False)


if __name__ == "__main__":
    sys.exit(main())

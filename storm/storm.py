#!/usr/bin/env python3
"""A reconnect storm against a ZeBridge stack: N real libzb clients (the published `zebridge`
package), started at once, timed.

  storm.py enroll CODES IDS --bridge URL [--parallel 8]
      One identity per invite code (one code per line in CODES), into IDS/<n>.identity. Run
      where the bridge is reachable without a proxy's rate limit (on the server, inside its
      network): each client then keeps its identity for every run.

  storm.py run IDS N --bridge URL [--warm] [--markers SECONDS] [--dbs DIR]
      N clients from IDS, all started together (one thread each, in this process). Cold by
      default: each seeds the table from nothing; --warm keeps the replicas of the last run,
      as after a server restart. Then, for --markers seconds, client 0 writes one row a
      second, and every client records when each arrives: the spread of one write to N
      clients, and each client's longest gap (restart NATS during that time, and the gap is
      the time it took to come back).

The table is storm_items (setup.sql), tenant `storm`.
"""
import argparse, json, os, pathlib, resource, statistics, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from zebridge import ZeBridge

TABLE = "storm_items"


def pct(values, p):
    if not values:
        return None
    v = sorted(values)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]


def summary(name, values, unit="s"):
    if not values:
        return f"{name}: none"
    return (f"{name}: median {statistics.median(values):.3f} {unit}, p95 {pct(values, 95):.3f} {unit}, "
            f"slowest {max(values):.3f} {unit} (n={len(values)})")


def enroll(args):
    codes = [c.strip() for c in pathlib.Path(args.codes).read_text().splitlines() if c.strip()]
    ids = pathlib.Path(args.ids)
    ids.mkdir(parents=True, exist_ok=True)
    scratch = pathlib.Path("/tmp/storm-enroll")
    scratch.mkdir(exist_ok=True)

    def one(i_code):
        i, code = i_code
        ident = ids / f"{i:04d}.identity"
        if ident.exists():
            return "kept"
        # The bridge redeems a few invites at a time and answers 503 "busy — retry shortly"
        # to the rest; a brand-new tenant's stream also appears a moment after its first
        # enrollment (StreamNotFound); a lookup can fail once under load (unreachable). All
        # "not yet": retried here.
        for attempt in range(30):
            try:
                zb = ZeBridge(bridge_url=args.bridge, invite=code, tables=[TABLE],
                              db_path=str(scratch / f"{i:04d}.db"), identity_path=str(ident))
                zb.close()
                return "enrolled"
            except Exception as e:
                if ident.exists():      # enrolled, then the first sync failed: the identity is kept
                    return "enrolled"
                if not any(w in str(e) for w in ("503", "StreamNotFound", "unreachable")):
                    raise
                time.sleep(0.5 + attempt * 0.2)
        raise RuntimeError(f"invite {i}: still busy after 30 tries")

    t0 = time.monotonic()
    with ThreadPoolExecutor(args.parallel) as ex:
        results = list(ex.map(one, enumerate(codes)))
    print(f"{results.count('enrolled')} enrolled, {results.count('kept')} kept, in {time.monotonic() - t0:.1f} s → {ids}")


PER_PROCESS = 60   # libzb holds at most 64 clients per process (HandleTableFull)


def worker(args):
    """One process's share of the storm: clients [offset, offset+count), all started at
    start_at; from markers_at, the writer's client 0 writes one marker a second. Wall-clock
    times throughout, so the processes' numbers compare. Raw results to --out."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, 65536) if hard != resource.RLIM_INFINITY else 65536, hard))
    idents = sorted(pathlib.Path(args.ids).glob("*.identity"))[args.offset: args.offset + args.count]
    dbs = pathlib.Path(args.dbs)
    n = len(idents)
    clients, connect_s, errors = [None] * n, [None] * n, []
    events = [[] for _ in range(n)]

    def start_one(i):
        def on_change(report, i=i):
            if report.get("applied") or report.get("outcomes"):
                events[i].append(time.time())
        time.sleep(max(0.0, args.start_at - time.time()))
        t = time.time()
        try:
            clients[i] = ZeBridge(bridge_url=args.bridge, tables=[TABLE], identity_path=str(idents[i]),
                                  db_path=str(dbs / f"{args.offset + i:04d}.db"), on_change=on_change)
            connect_s[i] = time.time() - t
        except Exception as e:
            errors.append(f"client {args.offset + i}: {e}")

    threads = [threading.Thread(target=start_one, args=(i,), daemon=True) for i in range(n)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    writes = []
    if args.markers:
        time.sleep(max(0.0, args.markers_at - time.time()))
        if args.writer and clients[0]:
            tag = datetime.now(timezone.utc).strftime("%H%M%S")
            k = 0
            while time.time() < args.markers_at + args.markers:
                k += 1
                sent = time.time()   # before the call: a slow mutate() must not shorten the latency
                try:
                    clients[0].mutate(TABLE, "INSERT", {"id": f"m-{tag}-{k:04d}"},
                                      {"id": f"m-{tag}-{k:04d}", "tenant_id": "storm", "note": "marker",
                                       "updated_at": datetime.now(timezone.utc).isoformat()})
                    writes.append(sent)
                except Exception as e:
                    errors.append(f"marker {k}: {e}")
                time.sleep(max(0.0, args.markers_at + k - time.time()))
        else:
            time.sleep(max(0.0, args.markers_at + args.markers - time.time()))
        time.sleep(5)   # the last markers arrive

    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_mb = rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024
    pathlib.Path(args.out).write_text(json.dumps({
        "offset": args.offset, "n": n, "connect_s": connect_s, "errors": errors,
        "events": [[t for t in ev if t >= args.markers_at] for ev in events] if args.markers else [],
        "writes": writes, "writer": args.writer, "peak_rss_mb": rss_mb}))
    for c in clients:
        if c:
            try:
                c.close()
            except Exception:
                pass


def run(args):
    """The storm: N clients over ceil(N / 60) worker processes, started together."""
    import subprocess, tempfile
    idents = sorted(pathlib.Path(args.ids).glob("*.identity"))
    if len(idents) < args.n:
        sys.exit(f"only {len(idents)} identities in {args.ids}, {args.n} asked")
    dbs = pathlib.Path(args.dbs)
    dbs.mkdir(parents=True, exist_ok=True)
    if not args.warm:
        for f in dbs.glob("*"):
            f.unlink()
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="storm-"))
    start_at = time.time() + 3 + args.n / 100        # time for the workers to load
    markers_at = start_at + args.settle
    procs, outs = [], []
    for k, off in enumerate(range(0, args.n, PER_PROCESS)):
        out = tmp / f"w{k}.json"
        outs.append(out)
        cmd = [sys.executable, __file__, "worker", args.ids, "--offset", str(off),
               "--count", str(min(PER_PROCESS, args.n - off)), "--bridge", args.bridge, "--dbs", str(dbs),
               "--start-at", str(start_at), "--markers", str(args.markers), "--markers-at", str(markers_at),
               "--out", str(out)] + (["--writer"] if k == 0 else [])
        procs.append(subprocess.Popen(cmd, stderr=subprocess.DEVNULL))
    if args.markers:
        print(f"  markers from {datetime.fromtimestamp(markers_at).strftime('%H:%M:%S')} for {args.markers} s "
              f"(restart NATS during that time to time a reconnect)", flush=True)
    for pr in procs:
        pr.wait()
    parts = [json.loads(o.read_text()) for o in outs if o.exists()]
    report_parts(parts, args)


def report_parts(parts, args):
    connect = [c for p in parts for c in p["connect_s"] if c is not None]
    errors = [e for p in parts for e in p["errors"]]
    n = sum(p["n"] for p in parts)
    print(f"▶ {n} clients in {len(parts)} process(es), {'warm' if args.warm else 'cold'} start: "
          f"{len(connect)} synced, {n - len(connect)} failed")
    print("  " + summary("time to synced", connect))
    for e in errors[:5]:
        print("  ✗", e)
    writes = next((p["writes"] for p in parts if p["writer"]), [])
    if args.markers and writes:
        lat, gaps, short = [], [], 0
        for p in parts:
            for j, ev in enumerate(p["events"]):
                if p["writer"] and j == 0:
                    continue
                for w in writes:
                    arrived = next((t for t in ev if t >= w), None)
                    if arrived is not None:
                        lat.append(arrived - w)
                seq = [writes[0]] + ev + [writes[-1]]
                gaps.append(max(b - a for a, b in zip(seq, seq[1:])))
                short += len([w for w in writes if any(t >= w for t in ev)]) < len(writes) * 0.5
        print("  " + summary("marker latency", lat))
        print("  " + summary("longest gap per client", gaps))
        print(f"  {len(writes)} markers written; {short} client(s) received fewer than half")
    rss = [p["peak_rss_mb"] / max(1, p["n"]) for p in parts]
    print(f"  memory here: {statistics.mean(rss):.1f} MB per client")
    if args.json:
        pathlib.Path(args.json).write_text(json.dumps({"n": n, "warm": args.warm, "parts": parts}))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("enroll")
    e.add_argument("codes"); e.add_argument("ids")
    e.add_argument("--bridge", required=True); e.add_argument("--parallel", type=int, default=4)
    r = sub.add_parser("run")
    r.add_argument("ids"); r.add_argument("n", type=int)
    r.add_argument("--bridge", required=True)
    r.add_argument("--warm", action="store_true")
    r.add_argument("--markers", type=int, default=0)
    r.add_argument("--settle", type=float, default=20, help="seconds from the start to the first marker")
    r.add_argument("--dbs", default=str(pathlib.Path(__file__).parent / "run"))
    r.add_argument("--json")
    w = sub.add_parser("worker")
    w.add_argument("ids"); w.add_argument("--offset", type=int, required=True); w.add_argument("--count", type=int, required=True)
    w.add_argument("--bridge", required=True); w.add_argument("--dbs", required=True)
    w.add_argument("--start-at", type=float, required=True); w.add_argument("--markers", type=int, default=0)
    w.add_argument("--markers-at", type=float, default=0); w.add_argument("--writer", action="store_true")
    w.add_argument("--out", required=True)
    args = ap.parse_args()
    {"enroll": enroll, "run": run, "worker": worker}[args.cmd](args)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Counted ids against the chain: do clients that come and go converge, and how fast?

    python scripts/scenarios/gen_follow.py [--seconds 200] [--rate 200] [--noise 500]     (manual; ~6 min)

An emitter inserts ids 1, 2, 3… into a tenant-scoped table, `gen_ids` (tenant `acme`, plus
ONE `_default` row: the shared rows a tenant table's clients follow on CDC_PUBLIC), while a
public table, `gen_noise`, fills CDC_PUBLIC. The CDC streams keep 60 s of events and the
producer cuts every 20 s, so pruning starts within the first minute.

The oracle is the count: at any moment the truth is "ids 1..N". A replica is right when
count(*) = count(DISTINCT id) = max - min + 1 and the set equals PostgreSQL's.

Four libzb clients, each its own replica and process (its log stays its own), all mapped
to `acme`, on a schedule relative to the emitter's start:
  fresh   opens mid-load and stays
  short   offline 30 s — inside the stream's window: resumes from the tail
  long    offline 120 s — past the window: a real gap, healed from a chain
  alice   online the longest, then reconnects 5 s after going away, once CDC_PUBLIC has
          pruned past her position on it (the shared cut covers it: GENERATION.md)

Per client and per (re)connect: how long `connect`+`sync` took, how long until its max(id)
reached PostgreSQL's (a catch-up over 10 s is flagged), and from libzb's own log the seeds,
the refusals (`predates replica`), the gaps healed and the prunes met. At the end every
client must equal PostgreSQL.

Isolated like burst_tls.py: a scratch database (rendered from the templates), a scratch
nats-server without auth, the bridge on its own slot and port. Nothing of the dev stack is
written to, but the scratch slot's WAL is the dev cluster's: keep a disk guard on.

Needs nats-server, the nats CLI, envsubst, psql, a ReleaseFast bridge and libzb, and
`.env.admin` and `.env.bridge` sourced (the role URLs and the template variables).
"""
import argparse, ctypes, json, os, pathlib, subprocess, sys, tempfile, time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

ROOT = pathlib.Path(__file__).resolve().parents[2]
LIBZB = ROOT / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if sys.platform == "darwin" else "libzbcore.so")
TABLE, NOISE, TENANT = "gen_ids", "gen_noise", "acme"
CADENCE_S, CDC_AGE_S, CDC_MSGS = 20, 60, 20_000
SLOW_S = 10.0
# name → (client, [(open, close)]) in seconds from the emitter's start; close None = stay to
# the end. The zb-client-ts ones join at 75 s, once CDC_PUBLIC has pruned (60 s window): a
# browser joining a long-lived hub meets a stream whose first message is far past 1.
SCHEDULE = {
    "fresh": ("libzb", [(60, None)]),
    "short": ("libzb", [(15, 60), (90, None)]),
    "long": ("libzb", [(15, 40), (160, None)]),
    "alice": ("libzb", [(5, 150), (155, None)]),
    "ts_bob": ("ts", [(75, 120), (150, None)]),
    "ts_alice": ("ts", [(75, 170), (175, None)]),
}
NODE_DIR = ROOT / "examples" / "04-node-consumer"
# libzb's own words, counted per client (libzb/src/client.zig)
MARKS = {
    # zb-client-ts (examples/04-node-consumer, the library's log on stderr)
    "gap detected": "Gap detected!",
    "refused (predates this replica)": "predates this replica",
    "waiting for a chain": "waiting up to",
    # libzb
    "seeded": "seeded ",
    "refused (predates replica)": "chain predates replica",
    "chain behind the stream": "predates the stream",
    "gap healed": "gap healed",
    "pruned under the drain": "pruned under",
}


# ── the worker: one client, one process ────────────────────────────────────────
def ts_worker(a) -> int:
    """One zb-client-ts client (Node) through its schedule: a process per leg on the same
    replica file — going offline is the process ending. `ready` is printed once connect()
    returns, so the time to it is the connect a browser waits on."""
    import sqlite3
    import burst_tls as bt

    def say(**kv):
        print(json.dumps({"t": round(time.time() - a.t0, 1), **kv}), flush=True)

    def pg_max() -> int:
        return int(bt.psql(f"SELECT coalesce(max(id), 0) FROM public.{TABLE} WHERE tenant_id = '{TENANT}'").stdout.strip() or 0)

    def local() -> dict:
        try:
            con = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True, timeout=2)
            try:
                n, d, lo, hi = con.execute(f"SELECT count(*), count(DISTINCT id), coalesce(min(id), 0), coalesce(max(id), 0) FROM {TABLE} WHERE tenant_id = '{TENANT}'").fetchone()
                return {"count": n, "distinct": d, "min": lo, "max": hi}
            finally:
                con.close()
        except Exception as e:
            return {"error": str(e)}

    # Node resolves "localhost" to ::1 first; the scratch server listens on 127.0.0.1 only.
    env = dict(os.environ, NATS_URL=a.url.replace("localhost", "127.0.0.1"), ZB_DB=a.db, ZB_TABLES=TABLE, ZB_PRINCIPAL=f"gen_{a.name}")
    env.pop("ZB_CREDS", None)
    for i, (t_open, t_close) in enumerate(json.loads(a.schedule)):
        while time.time() < a.t0 + t_open:
            time.sleep(0.2)
        target = pg_max()
        t_c = time.time()
        proc = subprocess.Popen(["node", "--experimental-strip-types", "follow-worker.ts"], cwd=NODE_DIR, env=env,
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=sys.stderr, text=True)
        first = proc.stdout.readline()
        connect_s = time.time() - t_c
        try:
            ready = json.loads(first).get("ready")
        except json.JSONDecodeError:
            ready = False
        if not ready:
            say(event="open_failed", leg=i, said=first.strip()[:160]); proc.kill(); return 1
        caught = None
        end = (a.t0 + t_close) if t_close is not None else None
        while True:
            now = time.time()
            if caught is None:
                if local().get("max", -1) >= target:
                    caught = now - t_c
                    say(event="caught_up", leg=i, target=target, connect_s=round(connect_s, 2), catchup_s=round(caught, 2))
                elif now - t_c > 300:
                    say(event="never_caught_up", leg=i, target=target, local=local()); break
            if end is not None and now >= end:
                break
            if end is None and pathlib.Path(a.done_file).exists() and caught is not None:
                break
            time.sleep(0.25)
        if t_close is not None:
            say(event="offline", leg=i, poll_errors=0)
            proc.terminate()
            try: proc.wait(timeout=20)
            except subprocess.TimeoutExpired: proc.kill()
            continue
        while not pathlib.Path(a.done_file).exists():
            time.sleep(0.5)
        final_pg = int(pathlib.Path(a.done_file).read_text())
        t_f = time.time()
        while time.time() - t_f < 180:
            l = local()
            if l.get("count") == final_pg and l.get("distinct") == final_pg and l.get("min") == 1 and l.get("max") == final_pg:
                break
            time.sleep(0.5)
        say(event="final", local=local(), pg=final_pg, converge_s=round(time.time() - t_f, 1), poll_errors=0)
        proc.terminate()
        try: proc.wait(timeout=20)
        except subprocess.TimeoutExpired: proc.kill()
    return 0


def worker(a) -> int:
    """One libzb client through its schedule; JSON lines on stdout, libzb's log on stderr."""
    if a.kind == "ts":
        return ts_worker(a)
    import burst_tls as bt
    lib = ctypes.CDLL(str(LIBZB))
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, extra in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + extra

    def take(ptr):
        try: return json.loads(ctypes.string_at(ptr).decode())
        finally: lib.zb_free(ptr)

    def say(**kv):
        print(json.dumps({"t": round(time.time() - a.t0, 1), **kv}), flush=True)

    def pg_max() -> int:
        out = bt.psql(f"SELECT coalesce(max(id), 0) FROM public.{TABLE} WHERE tenant_id = '{TENANT}'").stdout.strip()
        return int(out or 0)

    def local(h) -> dict:
        r = take(lib.zb_client_query(h, f"SELECT count(*), count(DISTINCT id), coalesce(min(id), 0), coalesce(max(id), 0) FROM {TABLE} WHERE tenant_id = '{TENANT}'".encode(), b"[]"))
        if "error" in r:
            return {"error": r["error"]}
        n, d, lo, hi = (int(x) for x in r["rows"][0])
        return {"count": n, "distinct": d, "min": lo, "max": hi}

    sched = json.loads(a.schedule)
    for i, (t_open, t_close) in enumerate(sched):
        while time.time() < a.t0 + t_open:
            time.sleep(0.2)
        target = pg_max()
        t_c = time.time()
        h = lib.zb_client_connect(json.dumps({"natsUrl": a.url, "dbPath": a.db, "tables": [TABLE], "heartbeatMs": 0,
                                              "clientId": f"gen-{a.name}", "principal": f"gen_{a.name}"}).encode())
        if not h:
            say(event="open_failed", leg=i); return 1
        synced = take(lib.zb_client_sync(h))
        connect_s = time.time() - t_c
        caught = None
        poll_errors = 0
        end = (a.t0 + t_close) if t_close is not None else None
        while True:
            p = take(lib.zb_client_poll(h, 500))
            if p.get("error"):
                poll_errors += 1
            now = time.time()
            if caught is None:
                l = local(h)
                if l.get("max", -1) >= target:
                    caught = now - t_c
                    say(event="caught_up", leg=i, target=target, connect_s=round(connect_s, 2), catchup_s=round(caught, 2),
                        sync_error=synced.get("error"))
                elif now - t_c > 300:
                    say(event="never_caught_up", leg=i, target=target, local=l); break
            if end is not None and now >= end:
                break
            if end is None and a.done_file and pathlib.Path(a.done_file).exists() and caught is not None:
                break
        if t_close is not None:
            say(event="offline", leg=i, poll_errors=poll_errors)
            lib.zb_client_close(h)
            continue
        # The last leg: once the emitter is done, poll until this replica equals PostgreSQL.
        while not pathlib.Path(a.done_file).exists():
            take(lib.zb_client_poll(h, 500))
        final_pg = int(pathlib.Path(a.done_file).read_text())
        t_f = time.time()
        while time.time() - t_f < 180:
            take(lib.zb_client_poll(h, 500))
            l = local(h)
            if l.get("count") == final_pg and l.get("distinct") == final_pg and l.get("min") == 1 and l.get("max") == final_pg:
                break
        say(event="final", local=local(h), pg=final_pg, converge_s=round(time.time() - t_f, 1), poll_errors=poll_errors)
        lib.zb_client_close(h)
    return 0


# ── the harness ────────────────────────────────────────────────────────────────
def setup_tables(bt) -> None:
    open_tenant = os.environ.get("OPEN_TENANT", "_default")
    bt.psql(f"""CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, tenant_id text NOT NULL, note text,
        updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)""")
    r = bt.psql(f"SELECT step, status, detail FROM zebridge_enable('public.{TABLE}'::regclass, tenant_col => 'tenant_id', "
                f"tombstone_col => 'deleted_at', publication => '{bt.PUB}', dry_run => false) WHERE status = 'ERROR'")
    if r.stdout.strip() or r.returncode != 0:
        sys.exit(f"enable {TABLE} refused: {r.stdout}{r.stderr}")
    bt.psql(f"CREATE TABLE public.{NOISE} (id bigint PRIMARY KEY, payload text, updated_at timestamptz NOT NULL DEFAULT now())")
    r = bt.psql(f"SELECT step, status, detail FROM zebridge_enable('public.{NOISE}'::regclass, public_reason => 'gen_follow noise', "
                f"publication => '{bt.PUB}', dry_run => false) WHERE status = 'ERROR'")
    if r.stdout.strip() or r.returncode != 0:
        sys.exit(f"enable {NOISE} refused: {r.stdout}{r.stderr}")
    # the clients' principals, all in acme (the scratch NATS has no auth: the mapping is the identity)
    values = ", ".join(f"('gen_{n}', '{TENANT}')" for n in SCHEDULE)
    bt.psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) VALUES {values}")
    # the one shared row, before anything else
    bt.psql(f"INSERT INTO public.{TABLE} (id, tenant_id, note) VALUES (0, '{open_tenant}', 'shared')")


def emitter_sql(path: pathlib.Path, seconds: int, rate: int, noise: int) -> None:
    """One psql session: each second `rate` new ids in acme and `noise` public rows, paced on
    the wall clock from the session's own start."""
    lines = ["SELECT set_config('zb.gen_t0', extract(epoch from clock_timestamp())::text, false);"]
    for s in range(seconds):
        lines.append(f"INSERT INTO public.{TABLE} (id, tenant_id, note) SELECT g, '{TENANT}', 'n' FROM generate_series({s * rate + 1}, {(s + 1) * rate}) g;")
        if noise:
            lines.append(f"INSERT INTO public.{NOISE} (id, payload) SELECT g, 'x' FROM generate_series({s * noise + 1}, {(s + 1) * noise}) g;")
        lines.append(f"SELECT pg_sleep(greatest(0, current_setting('zb.gen_t0')::float8 + {s + 1} - extract(epoch from clock_timestamp())));")
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seconds", type=int, default=200)
    ap.add_argument("--rate", type=int, default=200, help="new ids per second in acme")
    ap.add_argument("--noise", type=int, default=500, help="public rows per second (prunes CDC_PUBLIC)")
    ap.add_argument("--worker", help=argparse.SUPPRESS)
    ap.add_argument("--t0", type=float, help=argparse.SUPPRESS)
    ap.add_argument("--url", help=argparse.SUPPRESS)
    ap.add_argument("--db", help=argparse.SUPPRESS)
    ap.add_argument("--name", help=argparse.SUPPRESS)
    ap.add_argument("--kind", default="libzb", help=argparse.SUPPRESS)
    ap.add_argument("--schedule", help=argparse.SUPPRESS)
    ap.add_argument("--done-file", help=argparse.SUPPRESS)
    a = ap.parse_args()
    if a.worker:
        return worker(a)

    import burst_tls as bt
    if not LIBZB.exists():
        sys.exit(f"{LIBZB} missing: cd libzb && zig build -Doptimize=ReleaseFast")
    bt.keep_awake()
    watch = bt.SleepWatch()
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="gen-follow-"))
    bt.setup_database()
    setup_tables(bt)
    nats_proc, url, cli = bt.start_nats(tmp, tls=False)
    env = {k: v for k, v in os.environ.items() if k not in ("NATS_BRIDGE_NKEY_SEED", "NATS_CREDS", "ZB_SIGNING_SEED", "NATS_TLS_CA")}
    env.update({
        "DATABASE_READER_URL": bt.db_url(os.environ["DATABASE_READER_URL"], bt.DB),
        "DATABASE_WRITER_URL": bt.db_url(os.environ["DATABASE_WRITER_URL"], bt.DB),
        "NATS_URL": url, "LOG_LEVEL": "info",
        "GENERATIONS_ENABLED": "true", "GENERATION_CADENCE_SECONDS": str(CADENCE_S),
        "CDC_MAX_AGE_SECONDS": str(CDC_AGE_S), "CDC_MAX_MSGS": str(CDC_MSGS),
    })
    blog = tmp / "bridge.log"
    bridge = subprocess.Popen([str(bt.BRIDGE), "--pub", bt.PUB, "--slot", bt.SLOT, "--port", str(bt.HTTP_PORT)],
                              env=env, stdout=blog.open("w"), stderr=subprocess.STDOUT)
    workers: dict = {}
    try:
        for _ in range(300):
            if "Replication started" in blog.read_text(errors="replace"):
                break
            if bridge.poll() is not None:
                sys.exit(f"the bridge exited:\n{blog.read_text(errors='replace')[-1500:]}")
            time.sleep(0.2)
        else:
            sys.exit(f"the bridge never became ready:\n{blog.read_text(errors='replace')[-1500:]}")
        time.sleep(CADENCE_S + 5)  # a first chain of every pair, the shared row in it
        load = tmp / "emit.sql"
        emitter_sql(load, a.seconds, a.rate, a.noise)
        done_file = tmp / "done"
        t0 = time.time()
        emitter = subprocess.Popen([bt.PSQL, bt.db_url(bt.ADMIN_URL, bt.DB), "-q", "-f", str(load)],
                                   stdout=subprocess.DEVNULL, stderr=(tmp / "emit.err").open("w"))
        for name, (kind, sched) in SCHEDULE.items():
            wdir = tmp / name
            wdir.mkdir()
            workers[name] = (subprocess.Popen(
                [sys.executable, __file__, "--worker", "1", "--t0", str(t0), "--url", url, "--db", str(wdir / "replica.sqlite3"),
                 "--name", name, "--kind", kind, "--schedule", json.dumps(sched), "--done-file", str(done_file)],
                stdout=(wdir / "events.jsonl").open("w"), stderr=(wdir / "libzb.log").open("w"), env=os.environ.copy()), wdir)
        emitter.wait()
        final = int(bt.psql(f"SELECT count(*) FROM public.{TABLE} WHERE tenant_id = '{TENANT}'").stdout.strip())
        done_file.write_text(str(final))
        print(f"emitter done: {final} ids in {TENANT} over {time.time() - t0:.0f} s ({a.rate}/s, noise {a.noise}/s)")
        for name, (p, _) in workers.items():
            try:
                p.wait(timeout=600)
            except subprocess.TimeoutExpired:
                p.kill()

        # ── the report ──
        failed = 0
        for name, (_, wdir) in workers.items():
            events = [json.loads(l) for l in (wdir / "events.jsonl").read_text().splitlines() if l.strip()]
            logtext = (wdir / "libzb.log").read_text(errors="replace")
            counts = {k: logtext.count(v) for k, v in MARKS.items()}
            print(f"\n{name} ({SCHEDULE[name][0]})  schedule {SCHEDULE[name][1]}")
            for e in events:
                if e["event"] == "caught_up":
                    flag = "  ⚠️ slow" if e["catchup_s"] > SLOW_S else ""
                    print(f"  t={e['t']:>6}s leg {e['leg']}: connect{'' if SCHEDULE[name][0] == 'ts' else '+sync'} {e['connect_s']} s{'  ⚠️ slow connect' if e['connect_s'] > SLOW_S else ''}, caught up with max(id) {e['target']} after {e['catchup_s']} s{flag}")
                elif e["event"] == "final":
                    l = e["local"]
                    ok = l.get("count") == l.get("distinct") == e["pg"] and l.get("min") == 1 and l.get("max") == e["pg"]
                    failed += 0 if ok else 1
                    print(f"  final: {'✓' if ok else '✗'} count {l.get('count')} distinct {l.get('distinct')} min {l.get('min')} max {l.get('max')} "
                          f"(PostgreSQL {e['pg']}), converged in {e['converge_s']} s, poll errors {e['poll_errors']}")
                elif e["event"] in ("never_caught_up", "open_failed"):
                    failed += 1
                    print(f"  ✗ {e}")
            print("  log: " + ", ".join(f"{k} {v}" for k, v in counts.items() if v or SCHEDULE[name][0] == "libzb" and k in ("seeded", "gap healed")))
        slept = watch.slept_s()
        if slept > 5:
            print(f"\n⚠️ INVALID: the machine slept {slept:.0f} s during the run")
        print(f"\nrun directory: {tmp}")
        return 1 if failed else 0
    finally:
        for p, _ in workers.values():
            if p.poll() is None:
                p.kill()
        bridge.terminate()
        try:
            bridge.wait(timeout=20)
        except subprocess.TimeoutExpired:
            bridge.kill()
        nats_proc.terminate()
        bt.drop_scratch()


if __name__ == "__main__":
    sys.exit(main())

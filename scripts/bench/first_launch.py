#!/usr/bin/env python3
"""First launch on a phone-like link: fresh install → all rows queryable and caught up.

One libzb client at a time, an empty SQLite replica, a table of ROWS rows of about 200
bytes, through scripts/scenarios/netem.py at each link profile. A benchmark, not a
scenario: never part of run.py (it owns the bridge, takes minutes, and its numbers are
the output).

Measured, per run:
  sync_s          zb_client_connect + sync: connect (an existing identity, no enrollment),
                  chain read, the base fetched, inflated, decoded and applied (libzb's
                  phase split below), then the stream drained to its tail — sync returns
                  caught up, so the replay of the backlog is inside it
  after_sync_s    after sync, until the replica's checksum equals PostgreSQL's: what sync
                  left to apply (near 0 when it drained the tail)
  total_s         the two: what the user waits for
  fetch/inflate/decode/apply_ms   libzb's log line for the seed
  cpu_s           this process's user+sys CPU over the run (libzb runs in-process): an
                  energy proxy, not a battery measurement
  db_bytes        the SQLite files after the run (main + WAL)
Once, server side: the base's build time and compressed size (the producer's log line).

The base is built before the clients run (a cache hit); its build cost is reported apart.
The backlog stays on the stream because the second bridge cuts no generation (cadence
3600 s): checked before and after the runs.

    caffeinate -i scripts/scenarios/.venv/bin/python scripts/bench/first_launch.py
Env: FL_ROWS (50000), FL_BACKLOG (1000), FL_RUNS (3). Needs the dev stack, no bridge
running, NATS_CREDS = bridge.creds (the manifest reads), BRIDGE_CDC_PUBLICATION.
"""
import hashlib, json, os, pathlib, re, resource, signal, socket, statistics, subprocess, sys, tempfile, time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scenarios"))
import zb  # noqa: E402
from clients import Lib, fresh_sqlite  # noqa: E402

T = "fl_rows"
ROWS = int(os.environ.get("FL_ROWS", "50000"))
BACKLOG = int(os.environ.get("FL_BACKLOG", "1000"))
RUNS = int(os.environ.get("FL_RUNS", "3"))
# FL_PHONE=1: no Mac runs; build the base, write the backlog, mint an invite and hold the bridge
# (bound to the LAN) for examples/06-large-table/react-native on a real phone.
PHONE = os.environ.get("FL_PHONE") == "1"
LOG = "/tmp/zb_first_launch_bridge.log"
# (name, one-way delay ms, Mbit/s per direction; 0 = unlimited)
PROFILES = [("local", 0, 0), ("4g-50ms-10mbit", 25, 10), ("slow-4g-150ms-1.6mbit", 75, 1.6)]
ENABLE = (f"tenant_col => 'tenant_id', version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"generations => true, publication => '{zb.publication()}', dry_run => false")
SEEDED = re.compile(rf"{T}: seeded (\d+) row\(s\) from chain g\d+.*?fetch (\d+) ms, inflate (\d+) ms, decode (\d+) ms, apply (\d+) ms")
BUILT = r"🧬 g(\d+) for '{tenant}'/'" + T + r"': (\w+), (\d+) row\(s\), (\d+) B → \S+ in (\d+) ms"  # B: the stored, compressed object


def until(pred, budget, step=0.2):
    end = time.monotonic() + budget
    while time.monotonic() < end:
        if pred(): return True
        time.sleep(step)
    return False


class Relay:
    """netem.py with a delay and a rate, on a free port."""
    def __init__(self, delay_ms, mbit):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0)); self.port = s.getsockname()[1]
        self.log = tempfile.NamedTemporaryFile("w+", prefix="zb_fl_relay_", delete=False)
        self.p = subprocess.Popen([sys.executable, str(zb.ROOT / "scripts" / "scenarios" / "netem.py"), "--listen", str(self.port),
                                   "--to", zb.nats_server().split("://")[-1], "--delay-ms", str(delay_ms), "--rate-mbit", str(mbit)],
                                  stdout=self.log, stderr=subprocess.STDOUT)
        if not until(lambda: "listening" in open(self.log.name).read(), 10): raise RuntimeError("relay did not start")
        self.url = f"nats://127.0.0.1:{self.port}"
    def close(self):
        self.p.send_signal(signal.SIGTERM)
        try: self.p.wait(timeout=5)
        except subprocess.TimeoutExpired: self.p.kill()
        os.unlink(self.log.name)


def pg_sum():
    return zb.psql(f"SELECT md5(string_agg(uid::text || note, ',' ORDER BY uid)) FROM public.{T} WHERE deleted_at IS NULL")


def replica_sum(c):
    rows = c.q(f"SELECT uid, note FROM {T} WHERE deleted_at IS NULL ORDER BY uid")
    return hashlib.md5(",".join(f"{u}{n}" for u, n in rows).encode()).hexdigest() if rows else None


def manifest(tenant):
    m = zb.kv_get("generations", f"{tenant}.{T}")
    return json.loads(m) if m else {}


def teardown():
    zb.forget_table(T)
    for sql in (f"DROP TABLE IF EXISTS public.{T}", f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'",
                f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'"):
        zb.psql(sql, quiet=True)


def one_run(tenant, url, want):
    """One fresh client; libzb's stderr is captured for its seed line."""
    db = f"/tmp/zb-fl-{os.getpid()}.sqlite3"; fresh_sqlite(db)
    err = tempfile.NamedTemporaryFile("w+", prefix="zb_fl_libzb_", delete=False)
    sys.stderr.flush(); saved = os.dup(2); os.dup2(err.fileno(), 2)
    c = None
    try:
        r0 = resource.getrusage(resource.RUSAGE_SELF); t0 = time.monotonic()
        c = Lib(db, [T], "py-first-launch", nats_url=url)
        t1 = time.monotonic()
        while replica_sum(c) != want:
            if time.monotonic() - t1 > 300: raise RuntimeError("never caught up")
            c.poll(200)
        t2 = time.monotonic(); r1 = resource.getrusage(resource.RUSAGE_SELF)
        rows = c.q(f"SELECT count(*) FROM {T}")[0][0]
    finally:
        if c: c.close()
        sys.stderr.flush(); os.dup2(saved, 2); os.close(saved)
    text = open(err.name, errors="replace").read(); os.unlink(err.name)
    m = SEEDED.search(text)
    size = sum(os.path.getsize(db + s) for s in ("", "-wal") if os.path.exists(db + s))
    fresh_sqlite(db)
    if os.path.exists(db + ".lock"): os.remove(db + ".lock")  # libzb's lock file, which fresh_sqlite leaves
    return {"sync_s": round(t1 - t0, 3), "after_sync_s": round(t2 - t1, 3), "total_s": round(t2 - t0, 3),
            "seeded_rows": int(m.group(1)) if m else None, "rows": rows,
            # libzb's own account of the run (its lines carry no timestamps): kept as evidence.
            "libzb": [l for l in text.splitlines() if T in l or "CDC_" in l][:12],
            **({k: int(v) for k, v in zip(("fetch_ms", "inflate_ms", "decode_ms", "apply_ms"), m.groups()[1:])} if m else {}),
            "cpu_s": round((r1.ru_utime + r1.ru_stime) - (r0.ru_utime + r0.ru_stime), 3), "db_bytes": size}


def main():
    if zb.another_bridge_running(): sys.exit("a bridge is running — this benchmark starts its own")
    tenant = zb.tenant_of("omar")
    teardown()
    out = {"rows": ROWS, "backlog": BACKLOG, "runs": RUNS, "tenant": tenant, "profiles": {}}
    try:
        zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY, note text NOT NULL, tenant_id varchar(255) NOT NULL, "
                "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz"
                + (", age int NOT NULL DEFAULT 1" if PHONE else "") + ")")  # the phone app checks sum(age)
        # ~200 bytes a row: a 16-byte uuid, a 160-byte note, the tenant and two timestamps.
        zb.psql(f"INSERT INTO public.{T} (uid, note, tenant_id) SELECT gen_random_uuid(), "
                f"rpad('row ' || g, 160, '.'), '{tenant}' FROM generate_series(1, {ROWS}) g")
        enabled = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE})")
        if "error" in enabled.lower(): sys.exit(f"zebridge_enable: {enabled}")

        # 1. the base, built once (server-side cost, reported apart)
        with zb.Bridge(LOG + ".build", GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as b:
            if not b.wait_for_log("Generation producer started", 30): sys.exit("bridge did not start")
            built = re.compile(BUILT.format(tenant=re.escape(tenant)))
            if not until(lambda: bool(built.search(b.text())) and (manifest(tenant).get("full") or {}).get("gen") == manifest(tenant).get("gen"), 180, 1):
                sys.exit("no base was built")
            g, kind, rows, stored, ms = built.search(b.text()).groups()
            out["base"] = {"gen": int(g), "kind": kind, "rows": int(rows), "compressed_bytes": int(stored), "build_ms": int(ms)}
            print(json.dumps({"base": out["base"]}), flush=True)

        # 2. a bridge that cuts no generation: the backlog stays on the stream
        with zb.Bridge(LOG + ".serve", GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="86400" if PHONE else "3600",  # a phone build can take most of an hour
                       **({"BRIDGE_BIND": "0.0.0.0"} if PHONE else {})) as b:
            if not b.wait_for_log("Generation producer started", 30): sys.exit("bridge did not restart")
            time.sleep(3)
            gen0 = manifest(tenant).get("gen")
            stream = zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + tenant

            def measure(label):
                """Every profile, RUNS fresh clients each, against the current PostgreSQL state."""
                want = pg_sum()
                for name, delay, mbit in PROFILES:
                    runs = []
                    for i in range(RUNS):
                        relay = Relay(delay, mbit) if (delay or mbit) else None
                        try: r = one_run(tenant, relay.url if relay else None, want)
                        finally:
                            if relay: relay.close()
                        print(json.dumps({"case": label, "profile": name, "run": i + 1, **r}), flush=True)
                        runs.append(r)
                    out["profiles"][f"{label} / {name}"] = {"delay_ms_one_way": delay, "mbit": mbit, "runs": runs,
                        "median": {k: statistics.median(r[k] for r in runs) for k in runs[0] if isinstance(runs[0][k], (int, float))}}

            if not PHONE: measure("base only")
            seq0 = json.loads(zb.nats_cli("stream", "info", stream, "-j").stdout)["state"]["last_seq"]
            # One transaction per row, as edits arrive: the bridge packs a transaction's
            # changes into one message, so one big UPDATE would be one message to replay.
            zb.psql(f"DO $$ DECLARE r record; BEGIN FOR r IN SELECT uid FROM public.{T} ORDER BY uid LIMIT {BACKLOG} LOOP "
                    f"UPDATE public.{T} SET note = rpad('changed ' || r.uid, 160, '-'), updated_at = now() WHERE uid = r.uid; "
                    "COMMIT; END LOOP; END $$")
            last = lambda: json.loads(zb.nats_cli("stream", "info", stream, "-j").stdout)["state"]["last_seq"]
            seen = [seq0]
            def settled():
                now = last(); stable = now == seen[-1] and now > seq0; seen.append(now); return stable
            if not until(settled, 120, 3): sys.exit("the backlog never reached the stream")
            out["backlog_messages"] = seen[-1] - seq0
            if PHONE: return phone(tenant, gen0)
            measure(f"+{BACKLOG} edits")
            out["backlog_on_stream"] = manifest(tenant).get("gen") == gen0
    finally:
        teardown()
    print(f"\nmedians of {RUNS}; backlog {BACKLOG} edits in {out.get('backlog_messages')} stream messages")
    print("case / profile                                total   sync      after     fetch  inflate decode apply  cpu    db")
    for name, p in out["profiles"].items():
        m = p["median"]
        print(f"{name:42s} {m['total_s']:6.2f}s {m['sync_s']:8.2f}s {m['after_sync_s']:9.2f}s {m.get('fetch_ms', 0):6.0f} "
              f"{m.get('inflate_ms', 0):6.0f} {m.get('decode_ms', 0):6.0f} {m.get('apply_ms', 0):5.0f} {m['cpu_s']:5.2f}s {m['db_bytes'] / 1e6:5.1f} MB")
    print(json.dumps(out))
    if not out.get("backlog_on_stream"): print("⚠️ a generation was cut during the runs: catch-up may have used deltas, not the stream")


def phone(tenant, gen0):
    """Hold the bridge for a real phone: base + backlog on the stream, as the Mac runs saw it."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as u:  # the default route's address; UDP connect sends nothing
        u.connect(("192.0.2.1", 9)); lan = u.getsockname()[0]
    code = zb.psql("SELECT zebridge_invite('omar', '" + tenant + "')").strip()
    port = zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--port") + 1]
    print(f"\nPHONE READY: {ROWS} rows, {BACKLOG} edits on the stream after the base's cut. In examples/06-large-table/react-native:\n"
          f"  EXPO_PUBLIC_ZB_TABLE={T} EXPO_PUBLIC_ZB_INVITE={code} EXPO_PUBLIC_ZB_BRIDGE_URL=http://{lan}:{port} "
          f"EXPO_PUBLIC_ZB_NATS_URL=nats://{lan}:4222\n"
          f"  expected: count {ROWS}, sum(age) {ROWS}. Enter ends it (the table is dropped).", flush=True)
    try: input()
    except (EOFError, KeyboardInterrupt): pass
    if manifest(tenant).get("gen") != gen0: print("⚠️ a generation was cut meanwhile: the phone may have used deltas")


if __name__ == "__main__":
    main()

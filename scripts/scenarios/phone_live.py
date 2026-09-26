"""§10jc: scenario B with a PHONE as the client — a live replica hit by a burst, then a
sustained load — on the dev stack, which is the only NATS a phone on the LAN can reach.

The phone's app is not instrumented: its CONSUMER is. Once a second the harness asks
nats-server for every consumer on the tenant's CDC stream (one `$JS.API.CONSUMER.LIST`
request) and reads the ones filtered to the table: their ack floor is what the phone has
applied and committed (both clients ack after COMMIT). Against the stream's own timeline
(its last sequence, sampled at the same instants) that gives the phone's lag in seconds;
a stream whose first sequence passed the phone's position is a fall-off; a consumer name
never seen before is a re-seed or a reopen. Convergence is the app's count/sum against
PostgreSQL's, printed here.

The table is `fire_types` on tenant globex (bob's), created here and dropped by
`teardown` — the bridge prunes its stream messages, chain objects and KV keys. The
fixture `test_types` is never written.

    set -a; . ./.env.admin; . ./.env.bridge; set +a
    scripts/scenarios/.venv/bin/python scripts/scenarios/phone_live.py setup --preload 1000000
    # open the app on the phone, following fire_types, and let it seed
    scripts/scenarios/.venv/bin/python scripts/scenarios/phone_live.py run --rate 5000 --seconds 90 --burst-seconds 5 --label libzb-5k
    scripts/scenarios/.venv/bin/python scripts/scenarios/phone_live.py teardown

⚠️ The dev bridge takes the burst: run it with a ring sized for it (RING_BUFFER_COUNT; a
full ring back-pressures the WAL reader — delayed, never dropped — and the run reports
any "Ring buffer full" line). Keep the dev bridge running throughout: an inactive slot on
this cluster is invalidated by a WAL-heavy load (NOTES §10bm).
"""
import argparse, json, os, pathlib, statistics, subprocess, sys, threading, time

ROOT = pathlib.Path(__file__).resolve().parents[2]
PSQL = "/opt/homebrew/opt/postgresql@18/bin/psql"
ADMIN_URL = os.environ.get("ADMIN_DATABASE_URL", "postgres://postgres@127.0.0.1:5432/postgres")
TABLE, TENANT, STREAM = "fire_types", "globex", "CDC_globex"
NATS = ["nats", "--creds", str(ROOT / "scripts/native/creds/bridge.creds"), "--inbox-prefix", "_INBOX.bridge", "-s", "nats://127.0.0.1:4222"]
BRIDGE_LOG = ROOT / "scripts/native/bridge.log"
OUT = pathlib.Path(os.environ.get("ZB_PHONE_OUT", "/tmp/zb-phone"))


def psql(sql: str) -> str:
    r = subprocess.run([PSQL, ADMIN_URL, "-XtAq", "-v", "ON_ERROR_STOP=1", "-c", sql], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"psql: {r.stderr.strip()[:400]}")
    return r.stdout.strip()


def api(subject: str, body: str = "") -> dict:
    r = subprocess.run(NATS + ["req", subject, body, "--raw"], capture_output=True, text=True, timeout=10)
    return json.loads(r.stdout)


def pg_facts() -> tuple[int, int]:
    n, s = psql(f"SELECT count(*), COALESCE(sum(age), 0) FROM public.{TABLE} WHERE tenant_id = '{TENANT}'").split("|")
    return int(n), int(s)


def setup(preload: int):
    psql(f"DROP TABLE IF EXISTS public.{TABLE}")
    time.sleep(3)  # the bridge's drop prune runs before the table comes back
    psql(f"""CREATE TABLE public.{TABLE} (
        uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id text NOT NULL DEFAULT '{TENANT}',
        batch integer NOT NULL, age integer, temperature double precision, price numeric(20,8), is_true boolean,
        some_text text, tags text[], matrix integer[][], metadata jsonb, last_writer varchar(255),
        inserted_at timestamptz NOT NULL, updated_at timestamptz NOT NULL, deleted_at timestamptz)""")
    psql(f"CREATE INDEX ON public.{TABLE} (batch)")
    psql(f"CREATE UNIQUE INDEX {TABLE}_zb_ri ON public.{TABLE} (tenant_id, uid)")
    psql(f"ALTER TABLE public.{TABLE} REPLICA IDENTITY USING INDEX {TABLE}_zb_ri")
    if preload:
        psql(f"INSERT INTO public.{TABLE} (batch, age, temperature, price, is_true, some_text, tags, matrix, metadata, last_writer, inserted_at, updated_at) "
             f"SELECT -1, i % 90, 20 + (i % 100) / 10.0, ((i % 1000) + 0.5)::numeric, i % 2 = 0, format('base-%s', lpad(i::text, 10, '0')), "
             f"ARRAY['base'], ARRAY[[i, 1], [2, 3]], jsonb_build_object('src', 'base', 'i', i), 'preload', now(), now() "
             f"FROM generate_series(1, {preload}) AS i")
    psql(f"VACUUM ANALYZE public.{TABLE}")
    mark = BRIDGE_LOG.stat().st_size
    out = psql(f"SELECT step || ' ' || status FROM zebridge_enable('public.{TABLE}'::regclass, tenant_col => 'tenant_id', "
               f"version_col => 'updated_at', tombstone_col => 'deleted_at', publication => 'my_pub', dry_run => false) WHERE status = 'ERROR'")
    if out:
        sys.exit(f"zebridge_enable refused: {out}")
    print(f"{TABLE}: {preload:,} rows on {TENANT}, enabled; waiting for its first chain…", flush=True)
    for _ in range(600):
        with BRIDGE_LOG.open(errors="replace") as f:
            f.seek(mark)
            if f"for '{TENANT}'/'{TABLE}': full" in f.read():
                break
        time.sleep(1)
    else:
        sys.exit("no chain for the table after 10 minutes — is the dev bridge running?")
    # The re-creation's DDL events can still move the table's seed epoch after that first
    # chain (a client then drops its watermark and waits for a full at the new epoch):
    # ready means a chain AT the descriptor's epoch, the pair unchanged for 15 s.
    stable, prev = 0, None
    for _ in range(900):
        try:
            d = json.loads(subprocess.run(NATS + ["kv", "get", "schemas", TABLE, "--raw"], capture_output=True, text=True, timeout=10).stdout)
            m = json.loads(subprocess.run(NATS + ["kv", "get", "generations", f"{TENANT}.{TABLE}", "--raw"], capture_output=True, text=True, timeout=10).stdout)
            pair = (d.get("seed_epoch", 0), m.get("seed_epoch", 0), m.get("gen"))
        except Exception:
            pair = None
        ok = pair is not None and pair[0] == pair[1]
        stable = stable + 1 if ok and pair == prev else 0
        prev = pair
        if stable >= 15:
            break
        time.sleep(1)
    else:
        sys.exit(f"the table's seed epoch never settled with a chain at it (last: {prev})")
    n, s = pg_facts()
    print(f"chain g{prev[2]} at seed epoch {prev[0]} is out. PostgreSQL: {n:,} rows, sum(age) {s:,}. Open the app following {TABLE} and let it seed.")


def teardown():
    psql(f"DROP TABLE IF EXISTS public.{TABLE}")
    print(f"{TABLE} dropped; the bridge prunes its stream messages, chain objects and KV keys.")


def mine(infos: list) -> dict:
    """The consumers filtered to the table (a phone's drain and tail consumers)."""
    out = {}
    for c in infos:
        cfg = c.get("config", {})
        subs = [cfg.get("filter_subject") or ""] + list(cfg.get("filter_subjects") or [])
        if any(TABLE in s for s in subs):
            out[c["name"]] = {"ack": c.get("ack_floor", {}).get("stream_seq", 0), "pending": c.get("num_pending", 0),
                              "ack_pending": c.get("num_ack_pending", 0), "delivered": c.get("delivered", {}).get("stream_seq", 0)}
    return out


def sample() -> dict:
    si = api(f"$JS.API.STREAM.INFO.{STREAM}")["state"]
    cl = api(f"$JS.API.CONSUMER.LIST.{STREAM}", "{}").get("consumers") or []
    return {"t": time.time(), "first": si["first_seq"], "last": si["last_seq"], "cons": mine(cl)}


def caught_up(s: dict) -> bool:
    return bool(s["cons"]) and all(c["pending"] == 0 and c["ack_pending"] == 0 for c in s["cons"].values())


def run(rate: int, seconds: int, burst_s: float, label: str):
    OUT.mkdir(parents=True, exist_ok=True)
    print("waiting for the phone: a consumer on the table, caught up, three samples in a row…", flush=True)
    ok = 0
    while ok < 3:
        s = sample()
        ok = ok + 1 if caught_up(s) else 0
        time.sleep(1)
    print(f"phone live on {list(s['cons'])}; position {max(c['ack'] for c in s['cons'].values())}", flush=True)
    base = int(psql(f"SELECT COALESCE(max(batch), 0) + 1 FROM public.{TABLE}"))
    log_mark = BRIDGE_LOG.stat().st_size
    samples, stop = [], threading.Event()

    def sampler():
        while not stop.is_set():
            t = time.time()
            try:
                samples.append(sample())
            except Exception as e:
                samples.append({"t": t, "error": str(e)[:80]})
            stop.wait(max(0.0, 1.0 - (time.time() - t)))
    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    # The burst: 1,000-row transactions, as fast as PostgreSQL goes, for burst_s seconds —
    # never more than eight statements ahead of the commits psql has echoed.
    t_burst = time.time()
    proc = subprocess.Popen([PSQL, ADMIN_URL, "-X", "-q"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
    done = {"k": 0}
    threading.Thread(target=lambda: [done.__setitem__("k", done["k"] + 1) for l in proc.stdout if l.startswith("B")], daemon=True).start()
    k = 0
    while time.time() - t_burst < burst_s:
        while k - done["k"] > 8:
            time.sleep(0.002)
        proc.stdin.write(
            f"INSERT INTO public.{TABLE} (batch, age, temperature, price, is_true, some_text, tags, matrix, metadata, last_writer, inserted_at, updated_at) "
            f"SELECT {base}, i % 90, 20 + (i % 100) / 10.0, ((i % 1000) + 0.5)::numeric, i % 2 = 0, format('burst-%s', lpad(i::text, 10, '0')), "
            f"ARRAY['burst'], ARRAY[[i, 1], [2, 3]], jsonb_build_object('src', 'burst', 'i', i), 'phone', now(), now() "
            f"FROM generate_series({k * 1000}, {k * 1000 + 999}) AS i;\n\\echo B\n")
        proc.stdin.flush()
        k += 1
    proc.stdin.close(); proc.wait()
    burst_end = time.time()
    burst_rows = done["k"] * 1000
    print(f"burst: {burst_rows:,} rows in {burst_end - t_burst:.1f} s ({burst_rows / (burst_end - t_burst):,.0f} rows/s)", flush=True)

    # The sustained load: each second `rate` inserts, then the previous second's rows updated.
    lines = ["\\set QUIET on", "\\o /dev/null", "SELECT clock_timestamp() AS t0 \\gset"]
    for sec in range(seconds):
        b = base + 1 + sec
        lines.append(f"INSERT INTO public.{TABLE} (batch, age, temperature, price, is_true, some_text, tags, matrix, metadata, last_writer, inserted_at, updated_at) "
                     f"SELECT {b}, i % 90, 20 + (i % 100) / 10.0, ((i % 1000) + 0.5)::numeric, i % 2 = 0, format('grow-%s', lpad(i::text, 10, '0')), "
                     f"ARRAY['grow'], ARRAY[[i, 1], [2, 3]], jsonb_build_object('src', 'grow', 'i', i), 'phone', now(), now() "
                     f"FROM generate_series(0, {rate - 1}) AS i;")
        if sec > 0:
            lines.append(f"UPDATE public.{TABLE} SET age = age + 1, updated_at = now() WHERE batch = {b - 1};")
        lines.append(f"SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM (:'t0'::timestamptz + interval '{sec + 1} seconds') - clock_timestamp())));")
    sql = OUT / f"{label}.sql"
    sql.write_text("\n".join(lines) + "\n")
    r = subprocess.run([PSQL, ADMIN_URL, "-X", "-v", "ON_ERROR_STOP=1", "-f", str(sql)], capture_output=True, text=True)
    load_end = time.time()
    if r.returncode != 0:
        print(f"⚠️ the load failed: {r.stderr[-300:]}")
    print(f"load: {seconds} s at {rate:,} rows/s ({2 * rate:,} events/s) took {load_end - burst_end:.0f} s; waiting for the phone to catch up…", flush=True)

    # Caught up: every table consumer drained, three samples in a row (15 minutes at most).
    deadline = load_end + 900
    while time.time() < deadline:
        time.sleep(1)
        tail = [x for x in samples[-3:] if "cons" in x]
        if len(tail) == 3 and all(caught_up(x) for x in tail):
            break
    converged_s = round(samples[-3]["t"] - load_end, 1) if time.time() < deadline else None
    stop.set(); th.join(timeout=5)

    good = [x for x in samples if "cons" in x]
    seen, new_consumers = set(samples[0].get("cons", {}) if samples else []), 0
    lags, falls = [], 0
    for i, x in enumerate(good):
        new_consumers += sum(1 for n in x["cons"] if n not in seen)
        seen |= set(x["cons"])
        pos = max((c["ack"] for c in x["cons"].values()), default=0)
        if pos and x["first"] > pos + 1:
            falls += 1
        # Lag: how long ago the first event the phone has NOT applied was published — the
        # first sample at which the stream's last sequence had passed the phone's position.
        if caught_up(x) or x["last"] <= pos:
            lag = 0.0
        else:
            published = next((y["t"] for y in good[: i + 1] if y["last"] > pos), x["t"])
            lag = x["t"] - published
        lags.append((x["t"], lag))
    live = lambda l: l <= 3.0
    back = None
    after = [(t, l) for t, l in lags if burst_end <= t <= load_end]
    for (t, l), (_, l2) in zip(after, after[1:]):
        if live(l) and live(l2):
            back = round(t - burst_end, 1)
            break
    sustained = sorted(l for t, l in lags if burst_end + 10 <= t <= load_end)
    q = lambda f: round(sustained[min(len(sustained) - 1, int(f * len(sustained)))], 1) if sustained else None
    with BRIDGE_LOG.open(errors="replace") as f:
        f.seek(log_mark)
        ring_full = sum(1 for l in f if "Ring buffer full" in l)
    n, s = pg_facts()
    res = {"label": label, "rate_rows_s": rate, "events_s": 2 * rate, "seconds": seconds,
           "burst_rows": burst_rows, "burst_rows_s": int(burst_rows / max(burst_end - t_burst, 0.001)),
           "max_lag_s": round(max((l for _, l in lags), default=0), 1), "back_live_s": back,
           "sustained_lag_s_p50": q(0.5), "sustained_lag_s_p90": q(0.9),
           "live_samples": sum(1 for t, l in lags if burst_end <= t <= load_end and live(l)),
           "samples": sum(1 for t, _ in lags if burst_end <= t <= load_end),
           "fell_off_samples": falls, "new_consumers": new_consumers, "converged_s_after_load": converged_s,
           "ring_buffer_full_lines": ring_full, "pg_rows": n, "pg_sum_age": s,
           "series": [(round(t - t_burst, 1), round(l, 1)) for t, l in lags]}
    (OUT / f"{label}.json").write_text(json.dumps(res))
    shown = {k: v for k, v in res.items() if k != "series"}
    print("RESULT " + json.dumps(shown), flush=True)
    print(f"\n→ press 'check' in the app: it should read {n:,} rows, sum(age) {s:,}")


def main():
    ap = argparse.ArgumentParser()
    sp = ap.add_subparsers(dest="cmd", required=True)
    p = sp.add_parser("setup"); p.add_argument("--preload", type=int, default=1_000_000)
    p = sp.add_parser("run"); p.add_argument("--rate", type=int, required=True); p.add_argument("--seconds", type=int, default=90)
    p.add_argument("--burst-seconds", type=float, default=5.0); p.add_argument("--label", required=True)
    sp.add_parser("teardown")
    a = ap.parse_args()
    if a.cmd == "setup":
        setup(a.preload)
    elif a.cmd == "run":
        run(a.rate, a.seconds, a.burst_seconds, a.label)
    else:
        teardown()


if __name__ == "__main__":
    main()

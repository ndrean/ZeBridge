#!/usr/bin/env python3
"""The wall (NOTES §10eg): what a client meets when its position fell off a pruned
CDC stream — and what the bridge says about the window before that happens.

The CDC streams keep `CDC_MAX_AGE_SECONDS` of events (three cadences by default),
capped by `CDC_MAX_BYTES` and `CDC_MAX_MSGS`. A client further behind than a stream
holds takes the gap rule (PROTOCOL §7): it re-seeds from the chain and resumes at
the newest manifest's cutoff sequence. That works while the cutoff is still in the
stream, i.e. while the window covers two cadences. Three phases, each on its own
bridge (this scenario runs the bridge on the LIVE slot, so stop yours first):

  1. the count valve ends the window, the chain is inside it — the watcher (TS)
     and the emitter (libzb) each park past the window and come back: gap, re-seed,
     the three sides agree; writes the watcher made while parked go out after;
  2. the age is set under the floor (30 s against a 60 s cadence) — the doctor and
     boot say so, the fleet monitor marks the window short, and both clients park
     past it: they must NOT resume past the hole; agreement within three cadences
     is the verdict, the clients' logs are the evidence;
  3. the bridge is down longer than the age — nothing was published meanwhile, so
     no position falls off; both agree once the first generation after the restart
     is cut and the mutations queued in the stream are drained.

Emitter bob (libzb), watcher mary (Node); a second libzb client (bob, another
replica) keeps the sting going while the emitter is parked. Rate ~5 writes/s.
"""
import argparse, json, os, re, subprocess, sys, time, urllib.request
import zb
from clients import Lib, Node, fresh_sqlite
import drip

T = drip.T
EM_DB, EM2_DB, WA_DB = "/tmp/zb-wall-emitter.sqlite3", "/tmp/zb-wall-emitter2.sqlite3", "/tmp/zb-wall-watcher.sqlite3"
EM_ERR, WA_LOG = "/tmp/zb_wall_emitter.log", "/tmp/zb_wall_watcher.log"
BRIDGE_LOG = "/tmp/zb_wall_bridge.log"
CADENCE = 60
failed = 0


def check(label, cond):
    global failed
    (zb.ok if cond else zb.bad)(f"{time.strftime('%H:%M:%S')} {label}")
    if not cond: failed += 1
    sys.stdout.flush()
    return cond


def health(port, timeout=60):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as r:
                if r.status == 200: return True
        except Exception: pass
        time.sleep(0.5)
    return False


def window_metrics(port, stream):
    """`bridge_cdc_window_seconds` and `_short` for one stream, from the fleet monitor."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3) as r:
            text = r.read().decode()
    except Exception:
        return None, None
    w = re.search(rf'bridge_cdc_window_seconds\{{stream="{stream}"\}} (-?\d+)', text)
    s = re.search(rf'bridge_cdc_window_short\{{stream="{stream}"\}} (\d+)', text)
    return (int(w.group(1)) if w else None), (int(s.group(1)) if s else None)


def stream_state(name):
    """first_seq / last_seq / messages of one stream, from NATS's monitor port."""
    with urllib.request.urlopen("http://127.0.0.1:8222/jsz?streams=true", timeout=3) as r:
        j = json.load(r)
    for acc in j.get("account_details", []):
        for s in acc.get("stream_detail", []):
            if s["name"] == name: return s["state"]
    return {}


def sting(em, seconds, interval_ms, live, counter):
    """The wasp's trio — INSERT, UPDATE the previous, DELETE the one before — for `seconds`."""
    t_end = time.monotonic() + seconds
    while time.monotonic() < t_end:
        tick_at = time.monotonic()
        i = counter[0]; counter[0] += 1
        uid, row = drip.payload(i, em.tenant)
        em.mutate(T, "INSERT", {"uid": uid}, row); live.append(uid)
        if len(live) >= 2:
            em.mutate(T, "UPDATE", {"uid": live[-2]}, {"some_text": f"drip-{i-1:08d}", "temperature": 30 + (i % 50) / 10, "tags": ["drip", "touched"], "metadata": {"source": "wall", "touched": i}})
        if len(live) >= 3:
            em.mutate(T, "DELETE", {"uid": live.pop(0)})
        em.poll(0); em.flush(0)
        left = interval_ms / 1000 - (time.monotonic() - tick_at)
        if left > 0: time.sleep(left)


def sting_until(em, pred, max_seconds, interval_ms, live, counter, every=5):
    """Sting until `pred()` holds (checked every `every` s) or `max_seconds` pass; seconds
    stung, or None. A CDC message is a published BATCH, not a write, so the messages a
    sting puts on the stream per second is not knowable up front — the wall is reached
    when NATS says so, not when a timer does."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < max_seconds:
        sting(em, every, interval_ms, live, counter)
        try:
            if pred(): return round(time.monotonic() - t0)
        except Exception:
            pass
    return None


def agree(clients, budget):
    """Poll the libzb clients and compare the three sides until they agree; seconds or None."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        for c in clients:
            if isinstance(c, Lib): c.poll(50); c.flush(0)
        try:
            pg = drip.pg_texts()
            if all(drip.replica_texts(c) == pg for c in clients): return round(time.monotonic() - t0, 1)
        except Exception:
            pass
        time.sleep(0.5)
    return None


def log_lines(path, pattern, since=0):
    try:
        text = open(path, errors="replace").read()
    except FileNotFoundError:
        return []
    return [l.strip()[:160] for l in text[since:].splitlines() if re.search(pattern, l)]


def file_len(path):
    try: return os.path.getsize(path)
    except FileNotFoundError: return 0


_bridges = [0]


def bridge(port, **env):
    """One log per bridge (`/tmp/zb_wall_bridge.<n>.log`): the wind-down bridge used to
    overwrite the phase's log, and the producer's repair lines with it."""
    base = dict(RING_BUFFER_COUNT=os.environ.get("RING_BUFFER_COUNT", "8192"), GENERATION_CADENCE_SECONDS=str(CADENCE),
                GENERATIONS_ENABLED="1", FLEET_POLL_SECONDS="10", BRIDGE_PORT=str(port))
    base.update({k: str(v) for k, v in env.items()})
    _bridges[0] += 1
    return zb.Bridge(BRIDGE_LOG.replace(".log", f".{_bridges[0]}.log"), **base)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--emitter", default="bob")
    ap.add_argument("--watcher", default="mary")
    ap.add_argument("--slot", default=os.environ.get("BRIDGE_CDC_SLOT", "my_slot"))
    ap.add_argument("--pub", default=os.environ.get("BRIDGE_CDC_PUBLICATION") or "my_pub")
    ap.add_argument("--interval-ms", type=int, default=600, help="tick interval: three writes per tick (default ~5 writes/s)")
    ap.add_argument("--phase", default="1,2,3")
    a = ap.parse_args()
    phases = {int(x) for x in a.phase.split(",")}
    port = int(os.environ.get("BRIDGE_PORT", "27434"))
    os.environ.setdefault("BRIDGE_CDC_PUBLICATION", a.pub)
    zb.BRIDGE_ARGS[:] = ["--slot", a.slot, "--pub", a.pub, "--port", str(port)]
    # By the port, not the process table: a bridge started with arguments escapes the
    # harness's anchored pgrep (the wasp's lesson).
    if health(port, timeout=2):
        sys.exit(f"a bridge answers on :{port} — this scenario runs its own on the live slot, with its own retention; stop yours first")

    err = open(EM_ERR, "w"); os.dup2(err.fileno(), 2)   # libzb's prints, for the evidence grep
    for f in (EM_DB, EM2_DB, WA_DB): fresh_sqlite(f)
    counter, live = [0], []
    tables = [T]
    stream = None
    em = wa = em2 = None

    def open_clients():
        nonlocal em, wa, em2, stream
        em = Lib(EM_DB, tables, "py-wall", principal=a.emitter)
        em2 = Lib(EM2_DB, tables, "py-wall-2", principal=a.emitter)
        wa = Node(WA_DB, WA_LOG, principal=a.watcher)
        if not em.tenant or wa.tenant != em.tenant:
            sys.exit(f"emitter and watcher must share a tenant ({a.emitter}={em.tenant!r}, {a.watcher}={wa.tenant!r})")
        stream = f"CDC_{em.tenant}"
        t0 = time.monotonic()
        while time.monotonic() - t0 < 180:
            em.poll(100); em2.poll(0)
            try:
                if em.q(f"SELECT count(*) FROM {T}") and em2.q(f"SELECT count(*) FROM {T}") and wa.q(f"SELECT count(*) FROM {T}"): break
            except Exception: pass
            time.sleep(0.5)

    def close_clients():
        for c in (em, em2):
            if c: c.close()
        if wa: wa.close()

    # ── phase 1: the count valve, the chain inside the window ─────────────────
    if 1 in phases:
        print(f"── phase 1: CDC_MAX_MSGS=1000 at ~{3000 // a.interval_ms} writes/s — a window of ~{a.interval_ms // 3} s against a floor of {2 * CADENCE} s")
        with bridge(port, CDC_MAX_AGE_SECONDS=10 * CADENCE, CDC_MAX_MSGS=1000) as b:
            check("bridge up on the live slot with the phase-1 retention", health(port))
            open_clients()
            check(f"emitter {a.emitter} ×2 (libzb) and watcher {a.watcher} (Node) up on {em.tenant}", True)
            sting(em, 45, a.interval_ms, live, counter)
            check(f"warm-up: the three sides agree", agree([em, em2, wa], 60) is not None)

            # the watcher parks, writes three rows meanwhile, the sting goes on past the window
            wa.disconnect()
            parked_at = stream_state(stream)["last_seq"]
            wa_since, em_hole_since = file_len(WA_LOG), file_len(EM_ERR)
            queued = []
            for k in range(3):
                uid = drip.payload(90000 + k, em.tenant)[0]
                wa.mutate(T, "INSERT", {"uid": uid}, drip.payload(90000 + k, em.tenant)[1] | {"uid": uid, "some_text": f"drip-queued-{k}"}); queued.append(uid)
            secs = sting_until(em, lambda: stream_state(stream)["first_seq"] > parked_at + 1, 900, a.interval_ms, live, counter)
            st = stream_state(stream)
            check(f"the stream pruned past the watcher's position after {secs}s: first_seq {st['first_seq']} > parked at {parked_at} + 1 ({st['messages']} msgs kept)", secs is not None)
            w, short = window_metrics(port, stream)
            check(f"fleet monitor: {stream} holds {w}s, floor {2 * CADENCE}s, short={short} — the count valve ends the window above the floor", w is not None and w >= 2 * CADENCE and short == 0)

            wa.connect()
            secs = agree([em, em2, wa], 120)
            ev = log_lines(WA_LOG, r"Seeding|stream first|predates|Giving up", wa_since)
            check(f"watcher back: gap taken and re-seeded, three sides agree in {secs}s — {ev[:2]}", secs is not None and any("Seeding" in l or "stream first" in l for l in ev))
            # the SECOND emitter was never parked — only not polled while the sting ran —
            # and fell off the stream all the same: the live gap rule (§10ei)
            hole = log_lines(EM_ERR, r"pruned under the live consumer", em_hole_since)
            check(f"the idle emitter took the gap on its next poll, live: {hole[:1]}", bool(hole))
            t_end = time.monotonic() + 60
            ob = drip.count_where(wa, "%outbox%")
            while ob and time.monotonic() < t_end:
                time.sleep(1); ob = drip.count_where(wa, "%outbox%")
            in_pg = zb.psql(f"SELECT count(*) FROM {T} WHERE some_text LIKE 'drip-queued-%' AND deleted_at IS NULL", quiet=True).strip()
            check(f"the watcher's three queued writes went out after the re-seed: outbox {ob}, in PostgreSQL {in_pg}", ob == 0 and in_pg == "3")
            for uid in queued: em.mutate(T, "DELETE", {"uid": uid})

            # the emitter parks (libzb: closed, reopened on the same replica) while the second emitter stings
            em.close(); em = None
            parked_at = stream_state(stream)["last_seq"]
            em_since = file_len(EM_ERR)
            secs = sting_until(em2, lambda: stream_state(stream)["first_seq"] > parked_at + 1, 900, a.interval_ms, live, counter)
            st = stream_state(stream)
            check(f"the stream pruned past the emitter's position after {secs}s: first_seq {st['first_seq']} > parked at {parked_at} + 1", secs is not None)
            em = Lib(EM_DB, tables, "py-wall", principal=a.emitter)
            secs = agree([em, em2, wa], 120)
            ev = log_lines(EM_ERR, r"seed|gap|predates|chain|position", em_since)
            check(f"emitter back: gap taken and re-seeded, three sides agree in {secs}s — {ev[:2]}", secs is not None)
            close_clients(); em = em2 = wa = None
        time.sleep(2)

    # ── phase 2: the age under the floor ─────────────────────────────────────
    if 2 in phases:
        print(f"── phase 2: CDC_MAX_AGE_SECONDS=30 against a {CADENCE} s cadence — under the floor of {2 * CADENCE} s")
        doc = subprocess.run([str(zb.BRIDGE), "--diagnose", *zb.BRIDGE_ARGS], capture_output=True, text=True,
                             env=zb.bridge_env(CDC_MAX_AGE_SECONDS="30", GENERATION_CADENCE_SECONDS=str(CADENCE)))
        check("the doctor reports the age under 2 × cadence as a finding (exit 1)", doc.returncode == 1 and "BELOW 2 × GENERATION_CADENCE_SECONDS" in doc.stderr)
        with bridge(port, CDC_MAX_AGE_SECONDS=30) as b:
            check("bridge up with the phase-2 retention", health(port))
            check("boot warns: CDC_MAX_AGE_SECONDS below 2 × cadence", b.wait_for_log("BELOW 2 × cadence", 20))
            open_clients()
            sting(em, 45, a.interval_ms, live, counter)
            check("warm-up: the three sides agree", agree([em, em2, wa], 60) is not None)
            bridge_since = file_len(b.log_path)   # the boot tick may repair the previous bridge's chain; what counts is after
            # both park; the sting goes on for 90 s through the second emitter
            wa.disconnect(); em.close(); em = None
            parked_at = stream_state(stream)["last_seq"]
            wa_since, em_since = file_len(WA_LOG), file_len(EM_ERR)
            secs = sting_until(em2, lambda: stream_state(stream)["first_seq"] > parked_at + 1, 300, a.interval_ms, live, counter)
            sting(em2, 45, a.interval_ms, live, counter)   # well past it: the chain's cutoff must fall off too
            st = stream_state(stream)
            check(f"the stream pruned past both positions after {secs}s: first_seq {st['first_seq']} > parked at {parked_at} + 1 ({st['messages']} msgs, 30 s)", secs is not None)
            w, short = window_metrics(port, stream)
            check(f"fleet monitor: {stream} holds {w}s, floor {2 * CADENCE}s, short={short} — the window is marked short", short == 1)
            warned = b.wait_for_log("under the 2 × cadence floor", 15)
            check("the bridge log carries the short-window warning", warned)
            # return: the honest outcomes are a re-seed that lands, or a wait for the next
            # generation; a silent resume past the hole shows as a disagreement
            wa.connect()
            em = Lib(EM_DB, tables, "py-wall", principal=a.emitter)
            secs = agree([em, em2, wa], 3 * CADENCE + 30)
            ev_w = log_lines(WA_LOG, r"Seeding|stream first|predates|waiting|Giving up", wa_since)
            ev_e = log_lines(EM_ERR, r"seed|gap|predates|waiting|chain|position|hole", em_since)
            check(f"both back under the floor: three sides agree within 3 cadences ({secs}s)", secs is not None)
            # Under the floor the chain's newest cutoff is inside the 30 s window about
            # half the time (a generation every 60 s while the table moves): then the
            # seed splices at once; otherwise both say `predates the stream` and wait
            # for the next one. Either is honest — what may never happen is a resume
            # past the hole, and the agreement above is the proof of that. Evidence:
            waited = [l for l in ev_w + ev_e if "predates the stream" in l]
            print(f"  · {'waited for the next generation: ' + waited[0] if waited else 'the newest cutoff was inside the window: spliced at once'}")
            # §10eq: the producer's edge watch re-cuts a pair before its cut falls off the
            # stream, so under the floor the log must show early cuts and no fallen chain
            # for the stung table.
            early = log_lines(b.log_path, r"cutting early", bridge_since)
            fell = [l for l in log_lines(b.log_path, r"chain g[0-9]+ fell off", bridge_since) if "test_types" in l and "globex" in l]
            check(f"the edge watch cut early ({len(early)} time(s)) and the stung table's chain never fell off ({len(fell)})", bool(early) and not fell)
            print(f"  · watcher log: {ev_w[:4]}")
            print(f"  · emitter log: {ev_e[:4]}")
            close_clients(); em = em2 = wa = None
        time.sleep(2)

    # ── phase 3: the bridge down longer than the age ─────────────────────────
    if 3 in phases:
        print(f"── phase 3: the bridge down for 60 s with a 30 s age — nothing published meanwhile, nothing falls off")
        with bridge(port, CDC_MAX_AGE_SECONDS=30):
            check("bridge up", health(port))
            open_clients()
            sting(em, 45, a.interval_ms, live, counter)
            check("warm-up: the three sides agree", agree([em, em2, wa], 60) is not None)
            heads = stream_state(stream)["last_seq"]
        # down: ten writes from the emitter wait in the MUTATIONS stream, no bridge drains them
        t_down = time.monotonic()
        for k in range(10):
            uid, row = drip.payload(counter[0], em.tenant); counter[0] += 1
            em.mutate(T, "INSERT", {"uid": uid}, row); live.append(uid); em.poll(0); em.flush(0)
        while time.monotonic() - t_down < 60:
            em.poll(200); em.flush(0)
        st = stream_state(stream)
        check(f"streams untouched while the bridge was down: last_seq {st['last_seq']} == {heads}, {st['messages']} msgs left after the age", st["last_seq"] == heads)
        with bridge(port, CDC_MAX_AGE_SECONDS=30) as b:
            check("bridge back", health(port))
            secs = agree([em, em2, wa], 3 * CADENCE)
            check(f"after the outage: the ten queued writes drained, the three sides agree in {secs}s", secs is not None)
            ob = drip.count_where(em, "%outbox%")
            check(f"emitter outbox empty: {ob}", ob == 0)
            close_clients(); em = em2 = wa = None

    # ── wind down: tombstone what is still live ───────────────────────────────
    if live:
        with bridge(port):
            health(port)
            c = Lib(EM_DB, tables, "py-wall", principal=a.emitter)
            for uid in live: c.mutate(T, "DELETE", {"uid": uid})
            for _ in range(60):
                c.poll(50); c.flush(20)
                if drip.count_where(c, "%outbox%") == 0: break
            c.close()
    print(f"  · done: {counter[0]} ticks; {'FAILED ' + str(failed) + ' check(s)' if failed else 'clean'} — restart your bridge: RING_BUFFER_COUNT=8192 ./zig-out/bin/bridge --slot {a.slot} --pub {a.pub}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main() or 0)

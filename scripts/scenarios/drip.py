"""The wasp: a slow, steady sting for hours (NOTES §10dx).

    scripts/scenarios/run.py live -k drip            # 60 minutes by default
    scripts/scenarios/drip.py --minutes 240 --interval-ms 200

Every tick, against the RUNNING stack (this scenario owns nothing): one INSERT with the
awkward columns of `test_types` (arrays, a nested array, jsonb, numeric, a float), one
UPDATE of the previous row, one DELETE (a tombstone) of the one before — through libzb
as the emitter (bob), with a passive Node replica (mary) following the same tenant.
Everything the battery runs is fast and short; hours are where leaks, drift, retention
roll-over, the producer under a steady stream and the sweeper under live traffic live.

Once a minute, invariants — and the run stops on the first one that breaks:
  · PostgreSQL, the emitter's replica and the watcher's replica agree on the live drip
    rows (count and the set of texts);
  · the emitter's outbox is empty, nothing is held on the watcher;
  · no verdict other than `accepted` was seen by either client (a single writer);
  · resident memory of the emitter (this process), the Node worker, the bridge and the
    NATS server is reported, and flagged when it doubles from its first sample;
  · tombstones stay bounded: the sweeper runs `--once` every --sweep-every seconds, and
    the watermark must move when it does.
"""
import argparse, json, os, re, signal, subprocess, sys, time, uuid
import zb
from clients import Lib, Node, fresh_sqlite

T = "test_types"
EMITTER_DB, WATCHER_DB = "/tmp/zb-drip-emitter.sqlite3", "/tmp/zb-drip-watcher.sqlite3"
EMITTER_ERR, WATCHER_LOG = "/tmp/zb_drip_emitter.log", "/tmp/zb_drip_watcher.log"
BAD_VERDICT = re.compile(r"rejected|row_deleted|\bstale\b|edit LOST|refused|failed after", re.I)


def rss_kb(pid):
    try: return int(subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True).stdout.strip() or 0)
    except Exception: return 0


def pid_of(pattern):
    r = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True)
    return int(r.stdout.split()[0]) if r.stdout.split() else 0


def pg_texts():
    out = zb.psql(f"SELECT string_agg(some_text, ',' ORDER BY some_text) FROM {T} WHERE deleted_at IS NULL AND some_text LIKE 'drip-%'", quiet=True).strip()
    return out.split(",") if out else []


def replica_texts(c):
    return sorted(r[0] for r in c.q(f"SELECT some_text FROM {T} WHERE deleted_at IS NULL AND some_text LIKE 'drip-%'"))


def count_where(c, table_like):
    names = [r[0] for r in c.q("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ?", [table_like])]
    return sum(c.q(f"SELECT count(*) FROM {n}")[0][0] for n in names) if names else 0


def wire_now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def payload(i, tenant):
    # inserted_at is NOT NULL with no database default (Ecto timestamps), so the row
    # carries it; updated_at is set from the write's version by the ingress.
    uid = str(uuid.uuid4())
    return uid, {
        "inserted_at": wire_now(),
        "uid": uid, "some_text": f"drip-{i:08d}", "age": i % 90, "is_true": i % 2 == 0,
        "tags": ["drip", f"tick-{i}", "with,comma"], "matrix": [[i, i * 2], [1, 2]],
        "metadata": {"source": "drip", "i": i, "nested": {"ok": True}},
        "price": f"{(i % 1000) + 0.5:.8f}", "temperature": 20 + (i % 100) / 10,
        "tenant_id": tenant,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=60)
    ap.add_argument("--interval-ms", type=int, default=200)
    ap.add_argument("--sweep-every", type=int, default=3600, help="seconds between `bridge_sweeper --once` runs; 0 = never")
    ap.add_argument("--emitter", default="bob")
    ap.add_argument("--watcher", default="mary")
    a = ap.parse_args()
    # The LIVE stack: ask its health endpoint (BRIDGE_PORT), not the process table — a
    # bridge started with arguments does not match the harness's anchored pgrep.
    import urllib.request
    try:
        with urllib.request.urlopen(f"{zb.http_base()}/health", timeout=3) as r:
            if r.status != 200: raise RuntimeError(r.status)
    except Exception as e:
        sys.exit(f"no bridge answers at {zb.http_base()}/health ({e}) — the wasp stings a LIVE stack; start it first")
    err = open(EMITTER_ERR, "w"); os.dup2(err.fileno(), 2)   # libzb's prints, for the verdict grep
    fresh_sqlite(EMITTER_DB); fresh_sqlite(WATCHER_DB)
    em = Lib(EMITTER_DB, [T], "py-drip", principal=a.emitter)
    wa = Node(WATCHER_DB, WATCHER_LOG, principal=a.watcher)
    tenant = em.tenant
    if not tenant or wa.tenant != tenant:
        sys.exit(f"emitter and watcher must share a tenant ({a.emitter}={em.tenant!r}, {a.watcher}={wa.tenant!r})")
    t0 = time.monotonic()
    while time.monotonic() - t0 < 120:
        em.poll(100)
        try:
            if em.q(f"SELECT count(*) FROM {T}") and wa.q(f"SELECT count(*) FROM {T}"): break
        except Exception: pass
        time.sleep(0.5)
    zb.ok(f"emitter {a.emitter} (libzb) and watcher {a.watcher} (Node) up on tenant {tenant}; stinging {T} every {a.interval_ms} ms for {a.minutes} min")

    stop = {"now": False}
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("now", True))
    pids = {"emitter": os.getpid(), "node": wa.p.pid, "bridge": pid_of("zig-out/bin/bridge"), "nats": pid_of("nats-server")}
    rss0 = {k: rss_kb(v) for k, v in pids.items()}
    live, i, failed, last_flush = [], 0, 0, {}
    ops = {"insert": 0, "update": 0, "delete": 0}
    deadline = time.monotonic() + a.minutes * 60
    next_check = time.monotonic() + 60
    next_sweep = time.monotonic() + a.sweep_every if a.sweep_every else float("inf")
    last_watermark = zb.psql("SELECT watermark FROM zebridge_gc_watermark", quiet=True).strip()

    def check(label, cond):
        nonlocal failed
        (zb.ok if cond else zb.bad)(time.strftime("%H:%M:%S ") + label)
        if not cond: failed += 1
        sys.stdout.flush()

    while not stop["now"] and time.monotonic() < deadline and failed == 0:
        tick_at = time.monotonic()
        i += 1
        uid, row = payload(i, tenant)
        em.mutate(T, "INSERT", {"uid": uid}, row)
        ops["insert"] += 1
        live.append(uid)
        if len(live) >= 2:
            em.mutate(T, "UPDATE", {"uid": live[-2]}, {"some_text": f"drip-{i-1:08d}", "temperature": 30 + (i % 50) / 10, "tags": ["drip", "touched"], "metadata": {"source": "drip", "touched": i}})
            ops["update"] += 1
        if len(live) >= 3:
            em.mutate(T, "DELETE", {"uid": live.pop(0)})
            ops["delete"] += 1
        em.poll(0); last_flush = em.flush(20)

        if time.monotonic() >= next_sweep:
            next_sweep = time.monotonic() + a.sweep_every
            r = subprocess.run([str(zb.SWEEPER), "--once"], capture_output=True, text=True, env={**os.environ, "GC_THRESHOLD_MS": os.environ.get("GC_THRESHOLD_MS", "3600000")}, timeout=600)
            reaped = [l for l in (r.stdout + r.stderr).splitlines() if "reaped" in l]
            wm = zb.psql("SELECT watermark FROM zebridge_gc_watermark", quiet=True).strip()
            check(f"sweeper --once ran: {'; '.join(reaped) or 'nothing to reap'}; watermark {last_watermark} → {wm}", r.returncode == 0 and wm != last_watermark)
            last_watermark = wm

        if time.monotonic() >= next_check:
            next_check = time.monotonic() + 60
            # let the last trio land, then compare the three sides
            settle = time.monotonic() + 10
            pg = pg_texts()
            while time.monotonic() < settle:
                em.poll(50); em.flush(20)
                pg = pg_texts()
                try:
                    if replica_texts(em) == pg == replica_texts(wa): break
                except Exception: pass
                time.sleep(0.3)
            e_t, w_t = replica_texts(em), replica_texts(wa)
            minutes = round((time.monotonic() - t0) / 60, 1)
            check(f"[{minutes} min, {i} ticks, {sum(ops.values())} writes] live drip rows agree: PG {len(pg)}, emitter {len(e_t)}, watcher {len(w_t)}", e_t == pg == w_t)
            ob, held = count_where(em, "%outbox%"), count_where(wa, "_zebridge_inbox")
            check(f"nothing pending: emitter outbox {ob}, watcher held {held}", ob == 0 and held == 0)
            vc = (last_flush or {}).get("verdicts", {})
            refused = sum(vc.get(k, 0) for k in ("stale", "rejected", "row_deleted", "failed", "other"))
            bad_w = sum(1 for l in open(WATCHER_LOG) if BAD_VERDICT.search(l) and "VERDICT" in l)
            check(f"no refused verdict: emitter accepted {vc.get('accepted', 0)}, refused {refused} {({k: v for k, v in vc.items() if v and k != 'accepted'}) or ''}; watcher {bad_w}", refused == 0 and bad_w == 0)
            tombs = zb.psql(f"SELECT count(*) FROM {T} WHERE deleted_at IS NOT NULL AND some_text LIKE 'drip-%'", quiet=True).strip()
            rss = {k: rss_kb(v) for k, v in pids.items()}
            grown = [k for k in rss if rss0[k] and rss[k] > 2 * rss0[k] and minutes >= 10]
            print(f"  · tombstones {tombs} · rss MB: " + ", ".join(f"{k} {rss[k]//1024} (from {rss0[k]//1024})" for k in rss) + (f" · DOUBLED: {grown}" if grown else ""))
            check("resident memory has not doubled", not grown)
            sys.stdout.flush()

        left = a.interval_ms / 1000 - (time.monotonic() - tick_at)
        if left > 0: time.sleep(left)

    # ── wind down: tombstone what is still live, leave the tombstones to the sweeper ──
    for uid in live:
        em.mutate(T, "DELETE", {"uid": uid})
    for _ in range(30):
        em.poll(50); em.flush(20)
        if count_where(em, "%outbox%") == 0: break
    print(f"  · done: {i} ticks, {ops}; {'STOPPED by an invariant' if failed else 'clean'}")
    em.close(); wa.close()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

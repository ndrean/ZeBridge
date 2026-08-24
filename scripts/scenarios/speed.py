#!/usr/bin/env python3
"""The README's burst-throughput benchmark, automated: rate *and* memory, not a log to eyeball.

`burst.py` drives the load and tells you to watch the bridge's own log. That is fine for
"is it still healthy" but useless for "did this change the numbers" — every run before this
one required a human to poll `/metrics` by hand and do the arithmetic in their head (see the
README's "An example of a measured throughput", which documents exactly that manual dance).
This script does the dance itself: it starts its own bridge sized for what `users` actually
needs (not the README's literal `BASE_BUF=14` — see `BRIDGE_ENV` below), drives the same
burst, polls `/metrics` for the end-to-end rate, and samples the bridge process's own RSS
throughout — so a run after a config/allocation change (a smaller `ColumnView`, an
auto-detected `MAX_COLUMNS`, a different `BASE_BUF`) reports a real number next to the
boot-time one the bridge computes for itself, instead of asking you to trust the
arithmetic.

Two things worth reading side by side in the report:

  1. **Rate.** Comparable only via `iters` at a fixed event count — see the README's own
     caveat: absolute throughput moves with machine, build mode and PostgreSQL version, so
     a rerun that differs is not automatically a regression. This exists to catch a
     *collapse* (the O(n^2) WAL-reader bug this project has already hit once), not to chase
     a faster number.
  2. **Memory.** Settled RSS before the load starts vs. peak RSS during/after it, next to
     the bridge's own "Event ring: N MB" boot log line. The delta between settled-RSS and
     the boot-computed ring size is everything else resident (libpq, the NATS client, the
     snapshot/encode arenas) — worth knowing is roughly stable across runs, since a growing
     gap would mean something *outside* the ring is the thing actually using more memory.

⚠️ **The slot is shared with every other `zb.Bridge`-based scenario**, and an inactive
replication slot is not a no-op: PostgreSQL retains WAL from its `restart_lsn` forward —
that retention is the whole point of a slot, it's what makes CDC durable across a bridge
restart — so a slot left behind by an earlier scenario run sits there holding real WAL
open, unpruned, until something attaches and confirms past it. Timing a run against that
backlog still present measures "drain today's load *plus* redecode however much debris
piled up since the last scenario," not the load alone — inflated `recv_ms`/`proc_ms`/CPU
that has nothing to do with whatever you actually changed. `skip_wal()` (same helper
`wide.py` uses, same reason) fast-forwards the slot past everything pending *before* the
timed bridge starts, so the run measures only what it just inserted.

Usage:  python scripts/scenarios/speed.py [statements] [rows_per_statement]

Reuses `users` — the table topology.json's `public_tables` already names — creating it
only if genuinely absent; an existing table is left structurally alone and only
`TRUNCATE`d. Starts its own bridge on the shared probe slot/port and refuses to run if
another bridge process is already up. Needs DATABASE_URL.
"""

import os
import pathlib
import re
import subprocess
import sys
import threading
import time
import urllib.request

import zb

TABLE = "users"
PUB = os.environ.get("BRIDGE_CDC_PUBLICATION", "my_pub")
SLOT = "zb_probe"  # matches ZB_BRIDGE_ARGS' default; skip_wal() advances it by name
SCRATCH = pathlib.Path(os.environ.get("TMPDIR", "/tmp"))
METRICS_PORT = 9096  # ZB_BRIDGE_ARGS' default --port

# ⚠️ NOT the README's literal BASE_BUF=14 (16 KB/event). That figure sizes for a row this
# fixture never produces — `users` here packs to a couple hundred bytes (two short text
# columns, two timestamps) — so 16 KB/slot × 65536 slots is 1 GB of data slab reserved to
# hold rows an order of magnitude smaller. 11 (2 KB) still leaves ~10x headroom over a
# realistic row while cutting the data slab to 128 MB. Override via env if you want a
# literal side-by-side against the README's own numbers instead of a right-sized run.
BRIDGE_ENV = {
    "BASE_BUF": os.environ.get("SPEED_BASE_BUF", "11"),
    "RING_BUFFER_COUNT": os.environ.get("SPEED_RING_BUFFER_COUNT", "65536"),
    # ⚠️ FORCED to info, not defaulted — `bridge_env`'s `setdefault` respects an
    # inherited LOG_LEVEL, and `.env.bridge`/dev shells commonly carry
    # LOG_LEVEL=debug. At debug the bridge logs PER EVENT (a `writev` per line from
    # the hot path), which cost a measured **4x CPU per event** (8.5s → 35s for 2M)
    # and reported ~55k ev/s on a machine whose real number is ~160k — three days
    # spent looking regression-shaped, entirely the harness's own logging. A
    # benchmark at debug measures the logger, not the bridge (NOTES.md §4.6).
    "LOG_LEVEL": os.environ.get("SPEED_LOG_LEVEL", "info"),
}


def skip_wal():
    """Move the slot past everything already pending, so the timed run starts clean.

    Requires the slot to be INACTIVE — must run before the timed bridge starts, never
    while one is attached.
    """
    zb.psql(f"SELECT pg_replication_slot_advance('{SLOT}', pg_current_wal_lsn())", quiet=True)


def rss_kb(pid: int) -> int:
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
    return int(out.stdout.strip()) if out.stdout.strip() else 0


class RssSampler(threading.Thread):
    """Peak and trough RSS of one pid while something else runs."""

    def __init__(self, pid: int):
        super().__init__(daemon=True)
        self.pid, self.peak, self.trough, self.stop = pid, 0, 10**9, False

    def run(self):
        while not self.stop:
            r = rss_kb(self.pid)
            if r:
                self.peak = max(self.peak, r)
                self.trough = min(self.trough, r)
            time.sleep(0.2)


def settled_baseline(pid: int) -> int:
    """RSS once the boot allocation (the ring slab paging in) has stopped moving.

    ⚠️ Sampled too early this measures the ring buffer being touched for the first time,
    not the load — see wide.py's identical helper, which found this the hard way.
    """
    time.sleep(6)
    samples = []
    for _ in range(10):
        samples.append(rss_kb(pid))
        time.sleep(0.2)
    return min(samples)


def seed_table():
    """Get `users` ready for a burst, WITHOUT assuming this script owns it.

    ⚠️ Never `DROP`/`CREATE` here. `users` is topology.json's own `public_tables` entry —
    a real migrated table in whatever project this runs against, not a throwaway fixture
    this script invented (unlike `wide.py`'s `wide_rows_*` or `decode_integrity.py`'s
    `decode_fixture`, which really are ephemeral and safe to drop). Dropping someone's
    actual schema to satisfy a benchmark script is exactly the mistake this comment exists
    to prevent a repeat of. If it's missing, create the minimal shape the README's method
    describes; if it's already there, leave its structure alone and only `TRUNCATE` for a
    clean row count.
    """
    exists = zb.psql(
        f"SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='{TABLE}'"
    ).strip()
    if not exists:
        zb.psql(
            f"CREATE TABLE public.{TABLE} ("
            "  id bigserial PRIMARY KEY, name text NOT NULL, email text NOT NULL, "
            "  inserted_at timestamptz NOT NULL DEFAULT now(), "
            "  updated_at timestamptz NOT NULL DEFAULT now());",
            quiet=True,
        )
    else:
        zb.psql(f"TRUNCATE public.{TABLE};", quiet=True)

    in_pub = zb.psql(
        f"SELECT 1 FROM pg_publication_tables WHERE pubname='{PUB}' AND tablename='{TABLE}'"
    ).strip()
    if not in_pub:
        zb.psql(
            "INSERT INTO public.zebridge_public_tables (tbl, reason) VALUES "
            f"('public.{TABLE}'::regclass, 'speed.py fixture') ON CONFLICT DO NOTHING; "
            f"ALTER PUBLICATION {PUB} ADD TABLE public.{TABLE};",
            quiet=True,
        )


def burst_sql(statements: int, per: int) -> str:
    # Separate statements, each its own transaction — see burst.py, which this mirrors:
    # the shape that builds a backlog rather than the WAL only becoming visible once at
    # the very end.
    return "\n".join(
        f"INSERT INTO public.{TABLE} (name,email,inserted_at,updated_at) "
        f"SELECT 'User-{i}-'||i2, 'u{i}-'||i2||'@example.com', now(), now() "
        f"FROM generate_series(1,{per}) i2;"
        for i in range(statements)
    )


def metric(port: int, name: str) -> float | None:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as r:
            body = r.read().decode()
    except OSError:
        return None
    m = re.search(rf"^{re.escape(name)} (\S+)$", body, re.MULTILINE)
    return float(m.group(1)) if m else None


def boot_facts(log_text: str) -> dict:
    facts = {}
    for pattern, key in (
        (r"MAX_COLUMNS=\d+ \([^\n]+\)", "max_columns"),
        (r"Event ring: [^\n]+", "event_ring"),
        (r"Columns slab: [^\n]+", "columns_slab"),
        (r"Metadata: [^\n]+", "metadata"),
    ):
        m = re.search(pattern, log_text)
        if m:
            facts[key] = m.group(0)
    return facts


def main():
    statements = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    per = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    total = statements * per

    running = subprocess.run(["pgrep", "-f", "zig-out/bin/bridge"], capture_output=True, text=True)
    if running.stdout.strip():
        sys.exit(
            "another bridge is already running — its /metrics would be indistinguishable "
            "from this one's.\n  pkill -f zig-out/bin/bridge"
        )

    # The slot is created by the bridge on first start, and `skip_wal` needs it to exist
    # before it can advance it — same bootstrap wide.py uses, same reason.
    with zb.Bridge(SCRATCH / "speed_init.log", **BRIDGE_ENV) as br:
        if not br.wait_for_log("Replication started successfully", timeout=60):
            sys.exit(f"could not start a bridge — see {SCRATCH / 'speed_init.log'}")

    print(f"seeding public.{TABLE} ({BRIDGE_ENV})")
    seed_table()
    # The DROP/CREATE/ALTER PUBLICATION above is itself WAL the next bridge would
    # otherwise have to decode through before reaching the burst — skip it too.
    skip_wal()

    log_path = SCRATCH / "speed.log"
    with zb.Bridge(log_path, **BRIDGE_ENV) as br:
        if not br.wait_for_log("Replication started successfully", timeout=60):
            sys.exit(f"bridge did not come up — see {log_path}")

        facts = boot_facts(br.text())
        for line in facts.values():
            print(f"  {line}")

        base = settled_baseline(br.proc.pid)
        print(f"  settled RSS before load: {base:,} KB")

        # ⚠️ Not the LOOP log's own `cpu=%` — the README's own method warns that field is
        # a per-interval average, and to compute CPU from `bridge_cpu_seconds_total`
        # deltas instead. Sampled here (immediately before the load) and again once the
        # drain completes, both against the exact same `load_started`/`end_to_end_elapsed`
        # wall-clock window, so the ratio is a real CPU-seconds-per-wall-second figure.
        cpu_before = metric(METRICS_PORT, "bridge_cpu_seconds_total")

        sampler = RssSampler(br.proc.pid)
        sampler.start()

        sql = burst_sql(statements, per)
        print(f"\ninserting {total:,} rows as {statements} transactions of {per}")
        # ⚠️ `load_started` is the ONE clock the headline rate is measured against — the
        # README defines end-to-end as `total / (t_at_2M - t_at_start)`, `t_at_start`
        # being before the first INSERT, not after PostgreSQL finishes writing. A drain
        # timer that starts only once `psql` returns silently drops PostgreSQL's own
        # write time from the denominator and reports a rate that is too high, not too
        # low — the CDC events already published *during* that write make the mistake
        # invisible unless you compare against the documented method line by line.
        load_started = time.time()
        res = subprocess.run(zb.PSQL.split() + ["-q"], input=sql, capture_output=True, text=True)
        if res.returncode != 0:
            sampler.stop = True
            sys.exit(res.stderr.strip())
        pg_elapsed = time.time() - load_started
        print(f"  PostgreSQL absorbed them in {pg_elapsed:.1f}s")

        print("draining...")
        deadline = load_started + max(60.0, total / 20_000)  # generous: never the bottleneck
        last = None
        while time.time() < deadline:
            n = metric(METRICS_PORT, "bridge_cdc_events_published_total")
            if n is not None:
                last = n
                if n >= total:
                    break
            time.sleep(0.25)
        end_to_end_elapsed = time.time() - load_started
        drain_only_elapsed = end_to_end_elapsed - pg_elapsed
        cpu_after = metric(METRICS_PORT, "bridge_cpu_seconds_total")

        sampler.stop = True
        sampler.join(timeout=2)
        # A few more ticks after the drain settles — the slab was fully touched by now,
        # this catches any post-drain release the sampler's last tick missed.
        peak = max(sampler.peak, rss_kb(br.proc.pid))

    if last is None or last < total:
        zb.bad(f"only {last or 0:,.0f}/{total:,} events published before the deadline — "
               "see the log for a stuck drain (recv_ms climbing toward the loop interval)")
        return 1

    rate = total / end_to_end_elapsed if end_to_end_elapsed > 0 else float("inf")
    print(f"\n{total:,} events in {end_to_end_elapsed:.1f}s end-to-end "
          f"({pg_elapsed:.1f}s PostgreSQL write + {drain_only_elapsed:.1f}s drain after) "
          f"→ {rate:,.0f} events/s")
    print(f"RSS: {base:,} KB settled before load → {peak:,} KB peak (+{peak - base:,} KB)")
    if cpu_before is not None and cpu_after is not None and end_to_end_elapsed > 0:
        cpu_frac = (cpu_after - cpu_before) / end_to_end_elapsed
        print(f"CPU: {cpu_after - cpu_before:.2f}s of process time over {end_to_end_elapsed:.1f}s wall "
              f"→ {cpu_frac * 100:.0f}% of one core, averaged (bridge_cpu_seconds_total, not the LOOP log's cpu=%)")
    for line in facts.values():
        print(f"  {line}")
    print(
        "\nNot a pass/fail number by itself — see the README's 'An example of a measured "
        "throughput' for what iters/idle/recv_ms mean and how to read a regression out of "
        f"{log_path}."
    )
    zb.ok(f"{total:,} events, {rate:,.0f} events/s end-to-end, peak +{peak - base:,} KB over settled baseline")
    return 0


sys.exit(main())

#!/usr/bin/env python3
"""Snapshot memory is bounded by the message budget, not by the table.

A snapshot's working set used to be a function of **table width discovered at runtime**:
`PQgetCopyData` was drained in a loop that exits only at end-of-COPY, so the whole result
was resident before any byte budget was consulted. A 500 MB table meant 500 MB of bridge.

Three claims, none of them checkable on a narrow table — on `test_types`, 10,000 rows and
3 rows per chunk both fit the budget, so the bug is invisible there:

  1. **residency is flat.** Ten times the data must not mean ten times the memory. Chunks
     are sized from the pre-scan's average row, and a running sum bounds what any single
     chunk transfers.
  2. **an unpublishable row is refused before a byte moves** — from one `max()` query,
     not after transferring the row. A row over the message budget cannot be sent by any
     path: a snapshot cannot split a row and CDC cannot carry it either.
  3. **an aborted snapshot leaves nothing behind.** `INIT` is discard-old, so orphans from
     a failing table evict *another* table's good snapshot once the stream fills.

⚠️ **`PQgetCopyData` returning -1 does not mean the COPY succeeded.** A server-side error
stops the data identically, and the error surfaces only from a later `PQgetResult`. Without
that check an interrupted snapshot ends mid-table and is published as *complete* — which is
what act 3 is really testing.

⚠️ **This scenario owns the only bridge.** The snapshot consumer is a fixed durable
(`bridge_snapshot_worker`), so a second bridge competes for the same requests and either
could serve them. It starts its own on its own slot and port, and refuses to run beside
another.

Usage:  python scripts/scenarios/wide.py

Seeds and drops its own `wide_rows` fixture. Needs DATABASE_URL (it starts a bridge).
"""

import asyncio
import os
import pathlib
import re
import subprocess
import sys
import threading
import time

import zb

SMALL_T, BIG_T = "wide_rows_s", "wide_rows_b"
TABLE = BIG_T  # acts 2 and 3 work on the larger fixture
ROW_BYTES = 262144          # 256 KiB — comfortably under the budget, so these rows are legal
SMALL, BIG = 200, 2000      # 10x the data is what makes act 1 a bound rather than a number
MAX_PAYLOAD = 1_048_576
PUB = os.environ.get("BRIDGE_CDC_PUBLICATION", "my_pub")
SLOT = "zb_probe"           # matches ZB_BRIDGE_ARGS' default; act 2 advances it by name
SCRATCH = pathlib.Path(os.environ.get("TMPDIR", "/tmp"))

# The fixture's rows must fit CDC's per-event buffer or the table is refused before the
# snapshot path is ever reached, and acts 1 and 3 would measure nothing. 19 → 512 KB.
# ⚠️ Not 20: the bridge refuses BASE_BUF=20 against a 1 MiB max_payload, because the
# envelope needs room on top of the row.
# ⚠️ The smallest ring buffer the bridge accepts (the floor is 1024). It is CDC's, not the
# snapshot's, but it is pre-allocated at boot — at 2048 slots it is a 1 GB slab whose pages
# are still being shed while the snapshot runs, and it swamped the measurement completely:
# the 200-row run reported +105 MB and the 2000-row run +10 MB, i.e. the *smaller* table
# costing ten times more.
#
# ⚠️ Do not lower it past the floor "to be safe": an out-of-range value falls back to the
# **default** (65536), not to the nearest valid one, so asking for 64 gets 65536 — a 32 GB
# slab that the sizing guard then refuses outright.
#
# What actually keeps the slab quiet is `skip_wal()`: pages are resident only once touched,
# and with nothing to replay CDC never touches them.
BRIDGE_ENV = {"BASE_BUF": "19", "RING_BUFFER_COUNT": "1024"}


def skip_wal():
    """Move the slot past everything written so far.

    ⚠️ Otherwise the bridge replays the seed INSERTs through CDC *while* the snapshot runs
    — for the 2000-row fixture that is 522 MB of unrelated traffic through the ring buffer,
    and the RSS being attributed to the snapshot would be mostly that.
    """
    zb.psql(f"SELECT pg_replication_slot_advance('{SLOT}', pg_current_wal_lsn())", quiet=True)


def seed(table: str, n: int):
    # Incompressible on purpose. `repeat('x', …)` is squashed by TOAST to a few hundred KB
    # for the whole table, so the fixture would look wide while transferring almost
    # nothing — and `octet_length` (what the pre-scan uses) would still report 256 KiB,
    # hiding the difference.
    zb.psql(
        f"DROP TABLE IF EXISTS public.{table}; "
        f"CREATE TABLE public.{table} (id bigint PRIMARY KEY, blob text NOT NULL, doc jsonb); "
        f"INSERT INTO public.{table} SELECT i, "
        f"(SELECT string_agg(md5(random()::text), '') FROM generate_series(1, {ROW_BYTES // 32})), "
        f"'{{\"k\":\"v\"}}'::jsonb FROM generate_series(1,{n}) i; "
        "INSERT INTO public.zebridge_public_tables (tbl, reason) VALUES "
        f"('public.{table}'::regclass, 'wide.py fixture') ON CONFLICT DO NOTHING; "
        f"ALTER PUBLICATION {PUB} ADD TABLE public.{table}",
        quiet=True,
    )


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
            time.sleep(0.15)


def settled_baseline(pid: int) -> int:
    """RSS once the boot allocation has stopped moving.

    ⚠️ Sampling immediately after startup measures the ring-buffer slab being shed, not the
    snapshot. Taken naively, the baseline came out *above* the later peak and the delta
    clamped to zero — which read as "perfectly bounded" while measuring nothing at all.
    """
    time.sleep(6)
    return min(rss_kb(pid) for _ in _tick(10, 0.2))


def _tick(n: int, gap: float):
    for _ in range(n):
        yield None
        time.sleep(gap)


def run_snapshot(table: str = TABLE) -> tuple[int, int, str]:
    """(chunks, largest_chunk_bytes, stdout) — snapshot.py owns the replay."""
    res = subprocess.run(
        [sys.executable, str(zb.ROOT / "scripts/scenarios/snapshot.py"), table],
        capture_output=True, text=True, timeout=500, env=os.environ,
    )
    m = re.search(r"(\d+) chunk\(s\)", res.stdout)
    # ⚠️ Parse the `chunk bytes:` line only. A loose scan for long numbers picks up the
    # LSN, a 10-digit value that looks exactly like an oversized chunk — it once made this
    # report a 3 GB chunk that did not exist.
    line = next((l for l in res.stdout.splitlines() if "chunk bytes:" in l), "")
    body = line.split("chunk bytes:", 1)[1].split("(max_payload")[0] if line else ""
    sizes = [int(x.replace(",", "")) for x in re.findall(r"\d[\d,]*", body)]
    return (int(m.group(1)) if m else 0, max(sizes) if sizes else 0, res.stdout)


def purge(stream: str):
    zb.nats_cli("stream", "purge", stream, "-f")


def kv_raw(bucket: str, key: str) -> bytes | None:
    """A KV value as bytes, or None when the key is absent.

    ⚠️ Not `zb.nats_cli`, which decodes as text: the `snapshots` descriptor is msgpack and
    raises `UnicodeDecodeError` on the way back. Absence is a returncode, not empty output.
    """
    seed_file = SCRATCH / "zb_seed.nk"
    seed_file.write_text(os.environ.get("NATS_NKEY_SEED", ""))
    scheme, _, rest = zb.NATS_URL.partition("://")
    server = f"{scheme}://{rest.rsplit('@', 1)[-1]}"
    res = subprocess.run(
        ["nats", "--server", server, "--nkey", str(seed_file), "kv", "get", bucket, key, "--raw"],
        capture_output=True,
    )
    return res.stdout if res.returncode == 0 else None


def measure(br, table: str) -> tuple[int, int, int]:
    """One snapshot inside an already-warm bridge. Returns (chunks, largest, peak_rss_kb)."""
    purge("REQUESTS")
    sampler = RssSampler(br.proc.pid)
    sampler.start()
    chunks, largest, _ = run_snapshot(table)
    sampler.stop = True
    sampler.join(timeout=2)
    return chunks, largest, sampler.peak


async def main():
    failed = 0

    running = subprocess.run(["pgrep", "-f", "zig-out/bin/bridge"], capture_output=True, text=True)
    if running.stdout.strip():
        sys.exit(
            "another bridge is already running.\n"
            "  The snapshot consumer is a fixed durable (`bridge_snapshot_worker`), so two\n"
            "  bridges compete for the same requests and either could serve them — which\n"
            "  would make every measurement below meaningless rather than merely noisy.\n"
            "    pkill -f zig-out/bin/bridge"
        )

    try:
        # The slot is created by the bridge on first start, and `skip_wal` needs it to
        # exist before the first seed. One short-lived bridge makes it.
        with zb.Bridge(SCRATCH / "wide_init.log", **BRIDGE_ENV) as br:
            if not br.wait_for_log("Replication started successfully", timeout=60):
                sys.exit(f"could not start a bridge — see {SCRATCH / 'wide_init.log'}")

        # ══ 1. the bound ═══════════════════════════════════════════════════════
        #
        # ⚠️ **Both sizes are measured inside ONE bridge process**, and that is the whole
        # design of this act rather than a convenience. Measured across two fresh bridges
        # the numbers were not merely noisy, they were *inverted* — 200 rows reporting
        # +321 MB against 2000 rows at +94 MB — because what `ps` sees is the pre-allocated
        # ring-buffer slab paging in on its own schedule, which has nothing to do with the
        # snapshot and dwarfs it.
        #
        # In one warm process the slab is identical for both, so the difference between the
        # two peaks is attributable to the only thing that changed: ten times the table.
        print(f"1. residency against table size ({ROW_BYTES // 1024} KiB rows)")
        seed(SMALL_T, SMALL)
        seed(BIG_T, BIG)
        # Slot inactive because no bridge is running — `pg_replication_slot_advance`
        # requires that, which is also why this cannot be done between the two snapshots.
        skip_wal()

        results = {}
        with zb.Bridge(SCRATCH / "wide_bound.log", **BRIDGE_ENV) as br:
            if not br.wait_for_log("Replication started successfully", timeout=60):
                sys.exit(f"the bridge did not come up — see {SCRATCH / 'wide_bound.log'}")
            base = settled_baseline(br.proc.pid)

            for table, n in ((SMALL_T, SMALL), (BIG_T, BIG)):
                on_disk = zb.psql(
                    f"SELECT pg_size_pretty(pg_total_relation_size('public.{table}'))"
                ).strip()
                chunks, largest, peak = measure(br, table)
                results[n] = (chunks, largest, peak)
                if chunks == 0:
                    zb.bad(f"{n} rows: no chunks replayed — the snapshot did not complete")
                    failed += 1
                    continue
                print(f"   {n:>5} rows / {on_disk:>7} → {chunks:>4} chunks, "
                      f"{n / chunks:.1f} rows/chunk, peak {peak:,} KB (base {base:,})")

                # The signature of byte-sized chunks: budget x fill-factor / row size is
                # ~3 here. Loose on purpose — this is a *shape* test, and row-count
                # pagination would put every row in one chunk.
                if n / chunks > 20:
                    zb.bad(f"{n / chunks:.0f} rows per chunk — chunks sized by row count, not bytes")
                    failed += 1
                if largest > MAX_PAYLOAD:
                    zb.bad(f"a chunk reached {largest:,} bytes, over max_payload")
                    failed += 1

        if len(results) == 2 and all(c for c, _, _ in results.values()):
            zb.ok(f"chunks sized by bytes ({SMALL / results[SMALL][0]:.1f} rows/chunk), "
                  f"largest {max(r[1] for r in results.values()):,} B inside the budget")

            small_d = max(results[SMALL][2] - base, 1)
            big_d = max(results[BIG][2] - base, 0)
            if big_d > small_d * 3:
                zb.bad(
                    f"10x the data cost {big_d / small_d:.1f}x the memory "
                    f"(+{small_d:,} → +{big_d:,} KB over a {base:,} KB base) — "
                    "residency is tracking the table"
                )
                failed += 1
            else:
                zb.ok(f"10x the data cost {big_d / small_d:.1f}x the memory "
                      f"(+{small_d:,} → +{big_d:,} KB) — bounded by the budget, not the table")

        # ══ 2. a row nothing can carry — the PRE-SCAN, isolated ════════════════
        #
        # ⚠️ Isolation is the whole difficulty. Any row over the snapshot budget also
        # exceeds the CDC event buffer (BASE_BUF=20 is refused against a 1 MiB
        # max_payload), so with a bridge running CDC *always* refuses the table first and
        # the snapshot short-circuits on the registry without the pre-scan ever running.
        #
        # So: insert while no bridge is up, then advance the replication slot past it.
        # CDC starts after the row and never sees it, leaving the pre-scan as the only
        # thing that can refuse — deterministically, rather than by winning a race.
        print("\n2. one row larger than any message")
        zb.psql(
            f"INSERT INTO public.{TABLE} (id, blob, doc) VALUES (999999, "
            "(SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 65536)), '{}'::jsonb)",
            quiet=True,
        )
        zb.psql(f"SELECT pg_replication_slot_advance('{SLOT}', pg_current_wal_lsn())", quiet=True)

        with zb.Bridge(SCRATCH / "wide_prescan.log", **BRIDGE_ENV) as br:
            if not br.wait_for_log("Replication started successfully", timeout=60):
                sys.exit("the bridge did not come up for the pre-scan check")
            time.sleep(3)
            purge("REQUESTS")
            run_snapshot()
            time.sleep(3)
            log = br.text()

            if "widest row is" in log:
                zb.ok("the pre-scan refused the table from one max() query")
            else:
                zb.bad("the pre-scan never ran — nothing reported a widest row")
                failed += 1

            # The isolation itself, asserted rather than assumed: if CDC had seen the row
            # this message would be here and the refusal above would prove nothing.
            if "does not fit in the" in log:
                zb.bad(
                    "CDC also refused the table, so the slot advance did not isolate the "
                    "pre-scan and act 2 proves nothing about it"
                )
                failed += 1
            else:
                zb.ok("and CDC never saw the row, so that refusal was the pre-scan's alone")

            doc = (kv_raw("schemas", TABLE) or b"").decode("utf-8", errors="replace")
            if '"suspended":true' in doc and "row_too_large" in doc:
                zb.ok("clients were told: suspended, reason row_too_large")
            else:
                zb.bad(f"no row_too_large suspension published (KV said {doc[:80]})")
                failed += 1

        # ══ 3. an abort leaves nothing behind ══════════════════════════════════
        print("\n3. a snapshot killed in flight")
        zb.psql(f"DELETE FROM public.{TABLE} WHERE id = 999999", quiet=True)

        with zb.Bridge(SCRATCH / "wide_abort.log", **BRIDGE_ENV) as br:
            if not br.wait_for_log("Replication started successfully", timeout=60):
                sys.exit("the bridge did not come up for the abort check")
            time.sleep(3)
            purge("REQUESTS")
            purge("INIT")

            # ⚠️ Act 1 ran two *successful* snapshots, and each legitimately published a
            # descriptor. Asserting "no descriptor exists" would therefore fail on a stale
            # one that proves nothing about this abort. Clear it, so anything present
            # afterwards was published by the run we are about to kill.
            zb.nats_cli("kv", "del", "snapshots", TABLE, "-f")

            snap = subprocess.Popen(
                [sys.executable, str(zb.ROOT / "scripts/scenarios/snapshot.py"), TABLE],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=os.environ,
            )
            killed = False
            for _ in range(80):
                n = zb.psql(
                    "SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'COPY %'"
                ).strip()
                if n and n != "0":
                    zb.psql(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE query LIKE 'COPY %'",
                        quiet=True,
                    )
                    killed = True
                    break
                time.sleep(0.4)
            snap.wait(timeout=200)
            time.sleep(6)

            if not killed:
                zb.bad("never caught a COPY in flight — the snapshot finished too fast to interrupt")
                failed += 1
            else:
                log = br.text()
                # Without the PQgetResult check this reads as a clean end-of-table and the
                # partial snapshot is published as complete.
                if "CopyFailed" in log:
                    zb.ok("the killed COPY was detected as a failure, not as end-of-table")
                else:
                    zb.bad(
                        "the aborted COPY was not reported as failed — a truncated snapshot "
                        "would be published as complete"
                    )
                    failed += 1

                if "purged orphaned snapshot data" in log:
                    zb.ok("its orphan chunks were purged rather than left to age out")
                else:
                    zb.bad(
                        "orphan chunks were not purged: INIT is discard-old, so a repeatedly "
                        "failing table evicts other tables' good snapshots"
                    )
                    failed += 1

                # The client-safety argument, checked rather than asserted in prose: the
                # descriptor is published only after COMMIT, and a client keys entirely off
                # `snapshot_id`. No descriptor means no client can name those chunks.
                desc = kv_raw("snapshots", TABLE)
                if desc:
                    zb.bad("a descriptor was published for an aborted snapshot — clients could read it")
                    failed += 1
                else:
                    zb.ok("no descriptor published, so no client can reach the partial chunks")

    finally:
        for t in (SMALL_T, BIG_T):
            zb.psql(f"DROP TABLE IF EXISTS public.{t}", quiet=True)
        zb.psql(
            "DELETE FROM public.zebridge_public_tables WHERE reason = 'wide.py fixture'",
            quiet=True,
        )

    return 1 if failed else 0


zb.run(main)

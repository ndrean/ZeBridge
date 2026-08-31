#!/usr/bin/env python3
"""The ring is pre-allocated at startup, so a bad size is an OOM kill, not slow degradation.

`BASE_BUF` and `RING_BUFFER_COUNT` decide how much memory the bridge claims **before it
does any work**, and both failure modes are invisible until they are fatal:

  * a slab that does not fit is not a gradual slowdown — the allocation succeeds, the
    pages are touched under load, and the kernel kills the process;
  * a row larger than `max_payload` packs successfully and is then **refused at publish
    time**, forever, with nothing in the data path saying why.

So the bridge refuses to start rather than warn. This asserts that it actually does, and
that the numbers it reports are the numbers it means.

⚠️ **The ring is two allocations**, and the check counted only one of them until
2026-08-19. Every event carries a fixed `[max_columns]ColumnView` array whose size does
**not** shrink with `BASE_BUF`, so the smaller the event buffer the more the metadata
dominates — at `BASE_BUF=11` it is comparable to the data itself. Under-reporting it meant
the guard waved through configurations the machine could not run. §4 below is the
regression test for that: two independent computations of the same total must agree.

⚠️ An out-of-range value is **clamped**, not defaulted. `RING_BUFFER_COUNT=64` means "less
memory"; substituting the default gave 65536 — a thousand times *more* — which at
`BASE_BUF=19` is a 32 GB ring the sizing check then refused, so a request for less produced
a fatal error about more. Unparseable input still takes the default, having no intent to
preserve.

Usage:  python scripts/scenarios/sizing.py

Starts short-lived bridges of its own (own slot and port, via `ZB_BRIDGE_ARGS`). Needs
DATABASE_READER_URL. Drops the probe slot and its `zebridge_limits` row at the end.
"""

import asyncio
import pathlib
import os
import re
import sys

import zb

SCRATCH = pathlib.Path(os.environ.get("TMPDIR", "/tmp"))
SLOT = zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"

# Big enough to be refused on any machine: 512 KB events x 1,048,576 slots is ~550 GB once
# the per-event metadata is counted. Hardcoding a "too big" number that depends on the host
# would make this pass or fail by accident.
HUGE_RING = str(1024 * 1024)


def boot(name: str, **env) -> tuple[int | None, str]:
    """Start a bridge, wait for it to settle or die. Returns (exit_code_or_None, log)."""
    with zb.Bridge(SCRATCH / f"sizing_{name}.log", **env) as br:
        # Either it refuses (exits) or it gets far enough to allocate and report.
        br.wait_for_log("Event ring:", timeout=25)
        code = br.proc.poll()
        if code is None:
            # ⚠️ Generous, and it has to be. "Event ring:" is logged BEFORE the
            # max_payload refusal, and between them the bridge registers its row-width
            # budget — which re-bakes the width guard of every published table (DDL per
            # table) when the budget changed. One second was enough by hand and not
            # under a loaded battery: the run reported "exit None, nothing fired" for a
            # bridge that refused correctly a moment later (measured 2026-08-31).
            code = br.wait_for_exit(timeout=20)
        return code, br.text()


def cleanup():
    """Drop the probe slot and its registration. An inactive slot pins WAL, and the
    probes that DID start registered a BASE_BUF budget in `zebridge_limits` — the width
    guard takes MIN over instances that still exist, so a lingering probe row would hold
    every writer to the probe's budget (see speed.py's identical note)."""
    zb.psql(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_replication_slots "
        f"WHERE slot_name='{SLOT}' AND NOT active) "
        f"THEN PERFORM pg_drop_replication_slot('{SLOT}'); END IF; END $$",
        quiet=True,
    )
    zb.psql(f"DELETE FROM public.zebridge_limits WHERE slot = '{SLOT}'", quiet=True)


async def main():
    try:
        return await checks()
    finally:
        cleanup()


async def checks():
    failed = 0

    # ── 1. a row that cannot fit a NATS message ────────────────────────────────
    print("1. BASE_BUF above what the server accepts")
    # ⚠️ With the DEFAULT ring this config is 64 GB and the memory guard fires first — a
    # correct refusal for the wrong reason, which would have let the payload check rot
    # untested. The minimum ring keeps it inside memory so the payload check is the only
    # thing that can refuse it.
    code, log = boot("payload", BASE_BUF="20", RING_BUFFER_COUNT="1024")
    if code not in (None, 0) and "EventBufferExceedsMaxPayload" in log:
        zb.ok(f"BASE_BUF=20 refused against a 1 MiB max_payload (exit {code})")
    else:
        which = "SlabExceedsMemory" if "SlabExceedsMemory" in log else "nothing"
        zb.bad(
            f"BASE_BUF=20 was not refused by the payload check (exit {code}, {which} fired) "
            "— rows of that size pack and are then rejected at publish time, with nothing "
            "in the data path explaining it"
        )
        failed += 1

    # ── 2. a ring that cannot fit in memory ────────────────────────────────────
    print("\n2. a ring larger than the machine")
    code, log = boot("ram", BASE_BUF="19", RING_BUFFER_COUNT=HUGE_RING)
    if code not in (None, 0) and "SlabExceedsMemory" in log:
        zb.ok(f"a {HUGE_RING}-slot ring at BASE_BUF=19 was refused (exit {code})")
    else:
        zb.bad(f"an unfittable ring was accepted (exit {code}) — this is an OOM kill under load")
        failed += 1

    # ⚠️ The message must name **both** terms, or an operator whose problem is metadata
    # reads a data-slab number, halves RING_BUFFER_COUNT, and is puzzled when it barely
    # helps.
    if "of data" in log and "metadata" in log:
        zb.ok("and the refusal breaks out the data and metadata terms")
    else:
        zb.bad("the refusal does not separate data from metadata — the operator cannot tell which to change")
        failed += 1

    # ── 3. out of range clamps; unparseable defaults ───────────────────────────
    print("\n3. values outside the range")
    _, log = boot("clamp_low", RING_BUFFER_COUNT="64")
    if "clamped to 1024" in log:
        zb.ok("RING_BUFFER_COUNT=64 clamped up to the 1024 floor, and the bridge started")
    else:
        zb.bad(
            "RING_BUFFER_COUNT=64 was not clamped. If it fell back to the default it "
            "silently became 65536 — a thousand times MORE than was asked for"
        )
        failed += 1

    _, log = boot("clamp_high", RING_BUFFER_COUNT="99999999")
    if "clamped to 1048576" in log:
        zb.ok("RING_BUFFER_COUNT=99999999 clamped down to the ceiling")
    else:
        zb.bad("an over-large RING_BUFFER_COUNT was not clamped to the ceiling")
        failed += 1

    # Unparseable is a different signal: it carries no direction, so the default is the
    # only sensible answer and clamping would be inventing intent.
    _, log = boot("garbage", RING_BUFFER_COUNT="banana")
    if "using default" in log and "banana" in log:
        zb.ok("an unparseable value still falls back to the default, not a clamp")
    else:
        zb.bad("an unparseable RING_BUFFER_COUNT did not report a fallback to the default")
        failed += 1

    # ── 4. the guard and the allocator agree ───────────────────────────────────
    #
    # ⚠️ The regression test that matters. These are two independent computations of the
    # same quantity — the startup check in bridge.zig and the allocator's own report in
    # batch_publisher.zig — and they disagreed by 40% at the defaults for as long as the
    # check ignored per-event metadata. A guard that under-reports is worse than none: it
    # licenses a configuration the machine cannot run.
    print("\n4. the sizing check and the allocator report the same number")
    _, log = boot("agree")
    guard = re.search(r"Event ring: (\d+) MB", log)
    alloc = re.search(r"Total: (\d+) MB", log)
    if not guard or not alloc:
        zb.bad(f"could not read both totals (guard={bool(guard)}, allocator={bool(alloc)})")
        failed += 1
    elif guard.group(1) == alloc.group(1):
        zb.ok(f"both report {guard.group(1)} MB")
    else:
        zb.bad(
            f"the sizing check says {guard.group(1)} MB but the allocator takes "
            f"{alloc.group(1)} MB — the check is not measuring what is allocated"
        )
        failed += 1

    return 1 if failed else 0


zb.run(main)

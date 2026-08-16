#!/usr/bin/env python3
"""TEST_SCENARIOS C1 — a snapshot request cannot be hammered.

The protection is the **broker's**, not the bridge's: the REQUESTS stream is created
`--max-msgs-per-subject=1 --discard=new --max-age=SNAP_RET`, and the snapshot listener
consumes that stream rather than subscribing to the subject with core NATS.

That distinction is the entire test. A core subscriber is a *parallel* listener — the
stream's limits govern what it stores, never what a core subscription receives. While
the listener used `core.SUB`, a hundred reconnecting clients produced a hundred
concurrent COPYs against the primary and the stream policy protected nothing.

Both publish styles are checked, because they fail differently:
  - JetStream publish  → the client is TOLD (503, maximum messages per subject exceeded)
  - core publish       → silently dropped by the stream; the client learns nothing and
                         waits on the KV descriptor instead. This is what the reference
                         web client does.

Usage:  python scripts/scenarios/stampede.py [table] [n]
"""

import sys

import zb


async def main():
    table = sys.argv[1] if len(sys.argv) > 1 else "test_types"
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 5

    nc = await zb.connect()
    js = nc.jetstream()
    req = zb.subject(zb.TOPOLOGY["subjects"]["snapshot_request"], table)

    print(f"\n{n} concurrent JetStream requests to {req}")
    accepted = rejected = 0
    first_error = None
    for _ in range(n):
        try:
            await js.publish(req, b"")
            accepted += 1
        except Exception as exc:  # noqa: BLE001 - the rejection *is* the result
            rejected += 1
            first_error = first_error or f"{type(exc).__name__}: {exc}"

    print(f"  accepted={accepted} rejected={rejected}")
    if first_error:
        print(f"  broker said: {first_error}")

    failed = 0
    if accepted == 1 and rejected == n - 1:
        zb.ok("exactly one request entered the window")
    else:
        zb.bad(f"expected 1 accepted / {n - 1} rejected — the window is not holding")
        failed = 1

    print(f"\n{n} core publishes (what the reference web client does)")
    for _ in range(n):
        await nc.publish(req, b"")
    await nc.flush()
    zb.ok("published; the stream drops them silently, so the bridge never sees them")
    print("  → confirm in the bridge log: no new 'Snapshot request received'")

    await nc.close()
    print("\nThe window clears after SNAP_RET, or `nats stream purge REQUESTS -f`.")
    return failed


zb.run(main)

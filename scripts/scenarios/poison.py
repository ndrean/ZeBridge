#!/usr/bin/env python3
"""Ingress poison-pill guard — one bad message must not pin the worker.

Before the guard, a payload that could not decode was NAK'd forever: **24 redeliveries
in 15 seconds**, `Outstanding Acks: 1`, and the queue never advanced past it. One
malformed message from one client stalled every other client's writes.

Two halves stop it now:
  - `isPermanent(err)` — a payload that cannot decode will fail identically forever, so
    it is dead-lettered to `mutation_error.<table>` and **ACKed** (handled, badly but
    finally). Transient failures still NAK and retry.
  - `max_deliver = 5` on the consumer — the server-side bound that catches anything the
    bridge misclassifies as retryable.

Usage:  python scripts/scenarios/poison.py [table]
"""

import sys

import zb


async def main():
    table = sys.argv[1] if len(sys.argv) > 1 else "test_types"
    nc = await zb.connect()
    js = nc.jetstream()
    prefix = zb.TOPOLOGY["subjects"]["mutations_prefix"]

    cases = [
        ("not msgpack at all", zb.subject(prefix, "probe", table, "insert"), b"this is not msgpack"),
        ("too few subject tokens", zb.subject(prefix, "probe", table), b"\x80"),
        ("unknown verb", zb.subject(prefix, "probe", table, "truncate"), b"\x80"),
        ("the bridge's own table", zb.subject(prefix, "probe", "zebridge_ddl_events", "insert"), b"\x80"),
    ]

    print()
    for label, subj, body in cases:
        try:
            await js.publish(subj, body)
            print(f"  published: {label:24} → {subj}")
        except Exception as exc:  # noqa: BLE001
            print(f"  publish refused ({label}): {exc}")

    await nc.close()
    print(
        "\nExpect in the bridge log, once each and never repeating:\n"
        "  🔴 Unprocessable mutation (…): dead-lettering, not retrying\n"
        "  ↩️  dead-letter → mutation_error.<table>\n"
        "  🔴 '<principal>' refused writes to 'zebridge_ddl_events' …\n"
        "\nThen confirm the queue is not stuck:\n"
        "  nats consumer info MUTATIONS bridge_mutations_worker\n"
        "  → Outstanding Acks: 0 · Redelivered: 0 · Unprocessed: 0"
    )
    return 0


zb.run(main)

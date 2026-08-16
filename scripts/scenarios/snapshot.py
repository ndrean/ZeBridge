#!/usr/bin/env python3
"""TEST_SCENARIOS B/C — request a snapshot, replay it, check it against PostgreSQL.

The decisive assertion is not the row *count* — it is that the emitted key sequence is
**identical to PostgreSQL's own `ORDER BY`**. That compares against the server's
collation rather than the client's idea of ordering, which matters: under a non-C
collation `User-*` sorts after `n-*`, so a byte-order check would report a false failure.

This is what proves composite keyset pagination. A chunk boundary must be able to fall
*inside* a run of equal leading-column values — paging on the first column alone would
silently skip the rest of that run, and every row count would still look plausible.

That only bites once there is more than one chunk, and the chunk size is compile-time
(`config.zig: Snapshot.chunk_size`, 10_000). Below that the run is still worth having —
it checks the descriptor, the framing and the ordering — but it is not exercising the
boundary. Load more than 10k rows to make it do so.

Usage:  python scripts/scenarios/snapshot.py [table]
"""

import asyncio
import sys

import zb
from nats.js.api import ConsumerConfig, DeliverPolicy


async def main():
    table = sys.argv[1] if len(sys.argv) > 1 else "test_types"

    pk = zb.psql(
        "SELECT a.attname FROM pg_index i "
        "JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true "
        "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum "
        f"WHERE i.indrelid='public.{table}'::regclass AND i.indisprimary ORDER BY k.ord"
    ).splitlines()
    if not pk:
        sys.exit(f"'{table}' has no primary key — the bridge refuses it (PROTOCOL §9)")
    print(f"\nprimary key: ({', '.join(pk)})")

    nc = await zb.connect()
    js = nc.jetstream()
    kv_snap = await js.key_value(zb.TOPOLOGY["kv"]["snapshots"])

    # Remember what is already there. The KV keeps the *previous* descriptor until the
    # new snapshot finishes, so "read the KV after publishing" races and will happily
    # replay a stale snapshot's chunks against the current table.
    async def descriptor():
        try:
            return zb.decode((await kv_snap.get(table)).value)
        except Exception:  # noqa: BLE001
            return None

    before = await descriptor()
    stale_id = before["snapshot_id"] if before else None

    req = zb.subject(zb.TOPOLOGY["subjects"]["snapshot_request"], table)
    try:
        await js.publish(req, b"")
        print(f"requested {req}")
    except Exception as exc:  # noqa: BLE001
        # Not a failure: the window is occupied, so a snapshot is already in flight (C1).
        # Then the descriptor we want *is* the existing one — stop waiting for a new id.
        print(f"request refused ({exc}) — reading the existing descriptor")
        stale_id = None

    desc = None
    for _ in range(30):
        desc = await descriptor()
        if desc and desc["snapshot_id"] != stale_id:
            break
        desc = None
        await asyncio.sleep(1)
    if not desc:
        sys.exit("no fresh snapshot descriptor appeared — is the bridge running?")
    print(f"descriptor: snapshot_id={desc['snapshot_id']} lsn={desc['lsn']}")

    # Chunks are arrays of row-arrays, positional against the schema's column order.
    filt = f"init.snap.{table}.{desc['snapshot_id']}.>"
    sub = await js.pull_subscribe(
        filt, durable=None, stream=zb.TOPOLOGY["streams"]["init"],
        config=ConsumerConfig(deliver_policy=DeliverPolicy.ALL, filter_subject=filt),
    )
    columns = zb.psql(
        f"SELECT attname FROM pg_attribute WHERE attrelid='public.{table}'::regclass "
        "AND attnum>0 AND NOT attisdropped ORDER BY attnum"
    ).splitlines()
    pk_idx = [columns.index(c) for c in pk]

    rows, chunks = [], 0
    while True:
        try:
            batch = await sub.fetch(100, timeout=2)
        except Exception:  # noqa: BLE001
            break
        if not batch:
            break
        for msg in batch:
            chunks += 1
            decoded = zb.decode(msg.data)
            if isinstance(decoded, list):
                rows.extend(decoded)
            await msg.ack()
    await nc.close()

    keys = [tuple(str(r[i]) for i in pk_idx) for r in rows]
    expected = [
        tuple(line.split("|"))
        for line in zb.psql(
            f"SELECT {', '.join(pk)} FROM public.{table} ORDER BY {', '.join(pk)}"
        ).splitlines()
        if line
    ]

    print(f"\n{chunks} chunk(s), {len(rows)} rows, {len(set(keys))} distinct keys")
    failed = 0
    if len(set(keys)) != len(keys):
        zb.bad("duplicate keys — pagination replayed a boundary")
        failed = 1
    else:
        zb.ok("no duplicate keys")

    if expected and keys != expected:
        zb.bad(f"sequence differs from PostgreSQL's ORDER BY ({len(expected)} rows there)")
        for i, (a, b) in enumerate(zip(keys, expected)):
            if a != b:
                print(f"    first divergence at {i}: snapshot={a} postgres={b}")
                break
        failed = 1
    elif expected:
        zb.ok("key sequence identical to PostgreSQL's own ORDER BY")
    return failed


zb.run(main)

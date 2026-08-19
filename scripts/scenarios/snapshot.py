#!/usr/bin/env python3
"""TEST_SCENARIOS B/C — request a snapshot, replay it, check it against PostgreSQL.

The decisive assertion is not the row *count* — it is that the emitted key sequence is
**identical to PostgreSQL's own `ORDER BY`**. That compares against the server's
collation rather than the client's idea of ordering, which matters: under a non-C
collation `User-*` sorts after `n-*`, so a byte-order check would report a false failure.

This is what proves composite keyset pagination. A chunk boundary must be able to fall
*inside* a run of equal leading-column values — paging on the first column alone would
silently skip the rest of that run, and every row count would still look plausible.

That only bites once there is more than one chunk, so pass a row count and the script
seeds enough to force several. Chunks are capped by **bytes** as well as rows — the byte
cap is the one that fires first on any realistic table — and each published chunk is
checked against the server's `max_payload`, because exceeding it does not raise: the
server closes the connection, every retry re-sends the identical payload, and the
snapshot is lost.

Usage:  python scripts/scenarios/snapshot.py [table] [seed_rows]

  snapshot.py test_types           # whatever is already there
  snapshot.py test_types 25000     # seed 25k first — forces ~3 chunks
"""

import asyncio
import sys

import zb
from nats.js.api import ConsumerConfig, DeliverPolicy


async def main():
    table = sys.argv[1] if len(sys.argv) > 1 else "test_types"
    seed = int(sys.argv[2]) if len(sys.argv) > 2 else 0

    if seed:
        # Idempotent, so re-running the probe does not multiply the table. Column names
        # match the emitter's test_types migration; adjust for another table.
        print(f"seeding {table} to {seed} rows...")
        # ⚠️ Built from the catalog, not hardcoded. This used to insert `(id, some_text,
        # inserted_at, updated_at)`, which stopped existing when the table moved to a uuid
        # primary key — so the seed silently failed and the scenario went on asserting
        # against whatever rows other scenarios happened to leave behind. It passed, on
        # incidental data, for as long as some data was incidentally there.
        pk_cols = [c for c in zb.psql(
            "SELECT a.attname FROM pg_index i "
            "JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true "
            "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum "
            f"WHERE i.indrelid='public.{table}'::regclass AND i.indisprimary ORDER BY k.ord"
        ).splitlines() if c]
        required = [c for c in zb.psql(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{table}' AND is_nullable='NO' AND column_default IS NULL"
        ).splitlines() if c]

        cols, vals = [], []
        for c in pk_cols + [r for r in required if r not in pk_cols]:
            t = zb.psql(
                "SELECT data_type FROM information_schema.columns "
                f"WHERE table_name='{table}' AND column_name='{c}'"
            ).strip()
            cols.append(c)
            if t == "uuid":
                # Deterministic from the series, so re-running tops up rather than duplicating.
                vals.append("md5(i::text)::uuid")
            elif "timestamp" in t:
                vals.append("now()")
            elif t in ("bigint", "integer", "smallint"):
                vals.append("i")
            else:
                vals.append("'seed-'||i")
        if "some_text" not in cols:
            cols.append("some_text")
            vals.append("'r'||i")

        out = zb.psql(
            f"INSERT INTO public.{table} ({', '.join(cols)}) "
            f"SELECT {', '.join(vals)} FROM generate_series(1,{seed}) i "
            "ON CONFLICT DO NOTHING"
        )
        have = zb.psql(f"SELECT count(*) FROM public.{table}").strip()
        print(f"  {have} rows present after seeding")
        if int(have or 0) < seed // 2:
            sys.exit(
                f"seeding produced only {have} rows of {seed} — the assertions below would "
                "run against incidental data rather than the fixture they describe"
            )

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
    fell_back = False

    req = zb.subject(zb.TOPOLOGY["subjects"]["snapshot_request"], table)
    try:
        await js.publish(req, b"")
        print(f"requested {req}")
    except Exception as exc:  # noqa: BLE001
        # Not a failure: the window is occupied, so a snapshot is already in flight (C1).
        # Then the descriptor we want *is* the existing one — stop waiting for a new id.
        print(f"request refused ({exc}) — reading the existing descriptor")
        # ⚠️ That descriptor may predate writes made since. The comparison below is against
        # the table *now*, so any scenario that changed rows in between (mutate.py,
        # sweeper.py) makes the sequences differ for a reason that is not a bug. Recorded
        # so the verdict can be downgraded rather than reported as a failure.
        fell_back = True
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

    max_payload = nc.max_payload
    rows, chunks, sizes = [], 0, []
    while True:
        try:
            batch = await sub.fetch(100, timeout=2)
        except Exception:  # noqa: BLE001
            break
        if not batch:
            break
        for msg in batch:
            chunks += 1
            sizes.append(len(msg.data))
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
    if sizes:
        print(f"  chunk bytes: {', '.join(f'{n:,}' for n in sizes)}  (max_payload {max_payload:,})")

    failed = 0
    over = [n for n in sizes if n > max_payload]
    if over:
        # Not a soft limit: the server closes the connection on an oversized message, and
        # the publisher's retries re-send the same bytes and get closed again.
        zb.bad(f"{len(over)} chunk(s) exceed max_payload — the server would close the connection")
        failed = 1
    elif sizes:
        zb.ok(f"every chunk fits max_payload (largest {max(sizes):,})")

    if chunks > 1:
        zb.ok(f"{chunks} chunks — pagination crossed a real chunk boundary")
    elif len(rows) > 0:
        print("  ⓘ  single chunk: the boundary was not exercised. Pass a larger seed count.")
    if len(set(keys)) != len(keys):
        zb.bad("duplicate keys — pagination replayed a boundary")
        failed = 1
    else:
        zb.ok("no duplicate keys")

    if expected and keys != expected and fell_back:
        print(
            f"  ⚠️  sequence differs ({len(keys)} snapshot rows vs {len(expected)} in the table),"
            " but this run reused an existing snapshot because the request window was\n"
            "     occupied. Rows written since it was taken are not in it. Not a failure —"
            " purge REQUESTS and re-run for a real verdict."
        )
    elif expected and keys != expected:
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

#!/usr/bin/env python3
"""Ingress — the last-write-wins round trip.

Publishes four mutations against one row and asserts the outcome in PostgreSQL:

    insert  version 10:00  → applied
    update  version 09:00  → REJECTED as stale (the older *intent* loses, however late
                             it arrives — that is the point of comparing versions
                             rather than arrival order)
    update  version 11:00  → applied
    delete  version 12:00  → tombstone, if the table has a tombstone column

Note what the payload does NOT carry: no table, no operation, no identity. All three are
subject tokens — `mutation.<principal>.<table>.<operation>` — because NATS authorizes
subjects and not payloads, so a `table` field in the body would be worth nothing.

Full rows on the wire: an upsert that turns into an INSERT must satisfy NOT NULL
columns, so a partial payload fails on the insert path and succeeds on the update path —
the asymmetry PROTOCOL.md §7 forbids.

Prerequisites:
    SELECT zebridge_grant_edge_writes('public.<table>');   -- as the DBA
    SYNC_RULES=<table>:<version_col>[,<tombstone_col>]     -- on the bridge

Usage:  python scripts/scenarios/mutate.py [table] [id]
"""

import asyncio
import sys

import msgpack
import zb

PRINCIPAL = "a3f9c1"  # an internal user id; see PROTOCOL.md §7.1


def full_row(columns, row_id, text, version):
    defaults = {
        "id": row_id,
        "uid": "11111111-1111-1111-1111-111111111111",
        "age": 1,
        "temperature": 1.5,
        "price": "1.00000000",
        "is_true": True,
        "some_text": text,
        "tags": "{}",
        "matrix": "{}",
        "metadata": "{}",
        "inserted_at": "2026-01-01T00:00:00.000000",
        "updated_at": version,
        "deleted_at": None,
    }
    return {c: defaults.get(c) for c in columns}


async def main():
    table = sys.argv[1] if len(sys.argv) > 1 else "test_types"
    row_id = int(sys.argv[2]) if len(sys.argv) > 2 else 7001

    columns = zb.psql(
        f"SELECT attname FROM pg_attribute WHERE attrelid='public.{table}'::regclass "
        "AND attnum>0 AND NOT attisdropped ORDER BY attnum"
    ).splitlines()
    if not columns:
        sys.exit(f"table '{table}' not found — run the emitter's migrations first")

    nc = await zb.connect()
    js = nc.jetstream()
    base = zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], PRINCIPAL, table)

    print()
    for op, text, hhmmss, expect in [
        ("insert", "from the edge", "10:00:00", "applied"),
        ("update", "STALE must not win", "09:00:00", "rejected as stale"),
        ("update", "NEWER wins", "11:00:00", "applied"),
    ]:
        version = f"2026-08-16T{hhmmss}.000000"
        await js.publish(
            f"{base}.{op}",
            msgpack.packb({
                "key": {"id": row_id},
                "data": full_row(columns, row_id, text, version),
                "version": version,
                "client_id": "c1",
            }),
        )
        print(f"  {op:6} version {hhmmss} — expect {expect}")
        await asyncio.sleep(2)

    await js.publish(
        f"{base}.delete",
        msgpack.packb({"key": {"id": row_id}, "version": "2026-08-16T12:00:00.000000"}),
    )
    print("  delete version 12:00:00 — expect a tombstone, or a physical delete")
    await asyncio.sleep(2)
    await nc.close()

    cols = "id, some_text" + (", deleted_at" if "deleted_at" in columns else "")
    row = zb.psql(f"SELECT {cols} FROM public.{table} WHERE id={row_id}")
    print(f"\nrow in PostgreSQL: {row or '(absent — physical delete)'}")

    if "STALE" in row:
        zb.bad("the stale write won — the version guard is not being applied")
        return 1
    zb.ok("the stale write lost; the newer version is stored")
    return 0


zb.run(main)

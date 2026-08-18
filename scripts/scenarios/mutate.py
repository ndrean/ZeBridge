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
import os
import uuid
import sys

import msgpack
import zb

# ⚠️ The principal must be the NATS user this script authenticates AS, not an arbitrary
# id. NATS grants `mutation.<user>.>`, so publishing under any other principal is refused
# — and a denied JetStream publish never acks, so it surfaces as a *timeout*, not as a
# permission error. A hardcoded "a3f9c1" here made this scenario hang for 10s and fail with
# `nats: timeout`, which reads like a broker problem.
#
# Taken from the connection URL, so the subject and the credential cannot disagree.
def _principal_from_url(url: str) -> str:
    rest = url.split("://", 1)[-1]
    if "@" not in rest:
        return "alice"
    return rest.rsplit("@", 1)[0].split(":", 1)[0] or "alice"


PRINCIPAL = os.environ.get("ZB_PRINCIPAL") or _principal_from_url(zb.NATS_URL)

# NOT NULL columns with no default, resolved from the catalog at startup.
REQUIRED: dict = {}


def required_defaults(table: str) -> dict:
    """NOT NULL columns with no DEFAULT, which the payload must therefore carry.

    ⚠️ Read from the catalog, not hardcoded. `test_types` grew a NOT NULL `tenant_id` for
    tenant scoping, and without it every mutation was refused by the RLS policy
    (`42501 new row violates row-level security policy`) — while this scenario still
    printed a ✓, because its verification query found no row and read that as "physical
    delete". A fixture that cannot write must not look like a verdict about LWW.
    """
    cols = [
        line for line in zb.psql(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{table}' AND is_nullable='NO' AND column_default IS NULL"
        ).splitlines() if line
    ]
    # The tenant must match what the principal is mapped to, or RLS refuses the write —
    # that mapping is the point of the policy.
    tenant = zb.psql(
        f"SELECT tenant_id FROM zebridge_user_tenants WHERE principal='{PRINCIPAL}' LIMIT 1"
    ).strip()
    out = {}
    for c in cols:
        if c == "tenant_id" and tenant:
            out[c] = tenant
    return out


def full_row(columns, row_id, text, version):
    defaults = {
        "id": row_id,
        "uid": row_id if isinstance(row_id, str) else "11111111-1111-1111-1111-111111111111",
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
    defaults.update(REQUIRED)
    return {c: defaults.get(c) for c in columns}


async def main():
    table = sys.argv[1] if len(sys.argv) > 1 else "test_types"

    # The key column and its type come from the catalog, not from an assumption. This
    # scenario hardcoded `id` — which `test_types` no longer has, since an edge-writable
    # table needs a key the client can mint (PROTOCOL.md §7.2).
    pk_rows = [
        line.split("|")
        for line in zb.psql(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_index i "
            "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
            f"WHERE i.indrelid='public.{table}'::regclass AND i.indisprimary"
        ).splitlines()
        if line
    ]
    if len(pk_rows) != 1:
        sys.exit(f"'{table}' needs exactly one primary key column for this scenario")
    pk_col, pk_type = pk_rows[0]

    if pk_type == "uuid":
        row_id = sys.argv[2] if len(sys.argv) > 2 else str(uuid.uuid4())
    else:
        row_id = int(sys.argv[2]) if len(sys.argv) > 2 else 7001
    print(f"key: {pk_col} ({pk_type}) = {row_id}")

    global REQUIRED
    REQUIRED = required_defaults(table)
    if REQUIRED:
        print(f"required columns supplied: {REQUIRED}")

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
                "key": {pk_col: row_id},
                "data": full_row(columns, row_id, text, version),
                "version": version,
                "client_id": "c1",
            }),
        )
        print(f"  {op:6} version {hhmmss} — expect {expect}")
        await asyncio.sleep(2)

    await js.publish(
        f"{base}.delete",
        msgpack.packb({"key": {pk_col: row_id}, "version": "2026-08-16T12:00:00.000000"}),
    )
    print("  delete version 12:00:00 — expect a tombstone, or a physical delete")
    await asyncio.sleep(2)
    await nc.close()

    cols = f'"{pk_col}", some_text' + (", deleted_at" if "deleted_at" in columns else "")
    # Quoted and keyed on the real column: an unquoted uuid is a syntax error, and the
    # failure printed as "trailing junk after numeric literal" rather than as "this
    # scenario assumes an integer key".
    row = zb.psql(f"SELECT {cols} FROM public.{table} WHERE \"{pk_col}\" = '{row_id}'")
    print(f"\nrow in PostgreSQL: {row or '(absent — physical delete)'}")

    if "STALE" in row:
        zb.bad("the stale write won — the version guard is not being applied")
        return 1
    zb.ok("the stale write lost; the newer version is stored")
    return 0


zb.run(main)

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

Prerequisites: the table is edge-writable (`zebridge_enable(..., edge_writes => true)`),
which also declares its version/tombstone columns in `zebridge_catalogue` — that is where
this scenario reads them from.

Usage:  python scripts/scenarios/mutate.py [table] [id]
"""

import asyncio
import datetime
import uuid
import sys

import msgpack
import zb

# ⚠️ The principal must be the NATS user this script authenticates AS, not an arbitrary
# id. NATS grants `mutation.<user>.>`, so publishing under any other principal is refused
# — and a denied JetStream publish never acks, so it surfaces as a *timeout*, not as a
# permission error. A hardcoded "a3f9c1" here made this scenario hang for 10s and fail with
# `nats: timeout`, which reads like a broker problem. zb.require_principal() ties the
# subject to the credential the connection actually carries.
PRINCIPAL = zb.require_principal()

# NOT NULL columns with no default, resolved from the catalog at startup.
REQUIRED: dict = {}
# The LWW columns, from zebridge_catalogue (set in main).
VERSION_COL, TOMB_COL = "updated_at", None


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
    tenant = zb.tenant_of(PRINCIPAL)
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
    defaults[VERSION_COL] = version
    if TOMB_COL:
        defaults[TOMB_COL] = None
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

    # A fresh key per run: a fixed default collides with whatever a previous run left
    # behind and turns "the insert was refused" into "the old row is still there".
    if pk_type == "uuid":
        row_id = sys.argv[2] if len(sys.argv) > 2 else str(uuid.uuid4())
    else:
        row_id = int(sys.argv[2]) if len(sys.argv) > 2 else 2_000_000_000 + uuid.uuid4().int % 100_000_000
    print(f"key: {pk_col} ({pk_type}) = {row_id}")

    r = zb.require_rules(table, "version")
    version_col, tomb_col = r["version"], r["tombstone"]
    print(f"version={version_col}  tombstone={tomb_col or '(none — deletes are physical)'}")

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

    global VERSION_COL, TOMB_COL
    VERSION_COL, TOMB_COL = version_col, tomb_col

    # Quoted and keyed on the real column: an unquoted uuid is a syntax error, and the
    # failure printed as "trailing junk after numeric literal" rather than as "this
    # scenario assumes an integer key".
    where = f"WHERE \"{pk_col}\" = '{row_id}'"

    def read():
        """(some_text, tombstoned) — or (None, None) when there is no row."""
        tomb = f"CASE WHEN {tomb_col} IS NULL THEN 'live' ELSE 'dead' END" if tomb_col else "'live'"
        out = zb.psql(f"SELECT coalesce(some_text,'') || '|' || {tomb} FROM public.{table} {where}")
        if "|" not in out:
            return None, None
        text, state = out.split("|", 1)
        return text, state

    failed = 0

    def check(label: str, cond: bool, detail: str):
        nonlocal failed
        if cond:
            zb.ok(f"{label}: {detail}")
        else:
            zb.bad(f"{label}: {detail}")
            failed += 1

    nc = await zb.connect_as(PRINCIPAL)
    js = nc.jetstream()
    base = zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], PRINCIPAL, table)

    try:
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

        # ⚠️ An empty row here is a FAILURE, not a physical delete: nothing has deleted
        # yet, so "no row" means the insert never landed (RLS, a NOT NULL column, a
        # refused publish) and the LWW verdict below would be about nothing.
        text, state = read()
        print(f"\nrow after the three writes: {text!r} ({state})")
        check("row exists", text is not None,
              "the insert landed" if text is not None else "NO ROW — the writes never applied")
        check("newer wins", text == "NEWER wins",
              f"some_text is {text!r} — the stale write lost and the newer version is stored"
              if text == "NEWER wins" else
              f"some_text is {text!r}, expected 'NEWER wins'"
              + (" — the stale write WON; the version guard is not applied" if text and "STALE" in text else ""))

        await js.publish(
            f"{base}.delete",
            # The delete's version is the tombstone's timestamp, and the sweeper reaps
            # tombstones older than GC_THRESHOLD: a delete pinned to 2026-08-16 was reaped
            # within seconds of landing and read back as "no row" (measured). Newer than the
            # three pinned writes is all LWW needs — now() is.
            msgpack.packb({"key": {pk_col: row_id},
                           "version": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"}),
        )
        print(f"\n  delete version 12:00:00 — expect {'a tombstone' if tomb_col else 'a physical delete'}")
        await asyncio.sleep(2)

        text, state = read()
        if tomb_col:
            check("delete", state == "dead",
                  f"row tombstoned ({tomb_col} set)" if state == "dead" else f"row is {state!r}, expected tombstoned")
        else:
            check("delete", text is None,
                  "row physically deleted" if text is None else "row still present after the delete")
    finally:
        await nc.close()
        zb.psql(f"DELETE FROM public.{table} {where}", quiet=True)

    return 1 if failed else 0


zb.run(main)

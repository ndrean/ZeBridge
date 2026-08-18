#!/usr/bin/env python3
"""Equal versions — broken deterministically, or refused, but never resolved by luck.

Last-write-wins compares `stored.version < incoming.version`. That `<` **rejects a tie**,
and a rejection is not a resolution: two replicas that each wrote the same version to the
same row will each refuse the other's write and stay divergent forever, with no error
anywhere. PROTOCOL.md §7.3 therefore specifies the comparison as `(version, client_id)`.

⚠️ Ties are not exotic. They are the **normal** case for an integer version column — two
clients that read the same value both send `stored + 1` — and happen with timestamps
whenever two writes land in the same microsecond.

The rule (higher `client_id` wins) is arbitrary. What is not arbitrary is that it must be
**total and order-independent**: two replicas given the same pair of writes must reach the
same row regardless of arrival order. A rule that depends on who arrived last is a race
wearing a rule's clothes, so this scenario sends each pair twice, in both orders.

Three things checked:

  1. with a tiebreak column, an equal-version write is **resolved**, and the same writer
     wins whichever order the two arrive in;
  2. the winner's id is **stored**, because the next comparison reads it off the row;
  3. `client_id` in `data` is **ignored** — the bridge stamps it from the envelope, or a
     client could claim any identity at the moment a tie is decided.

Usage:  python scripts/scenarios/tiebreak.py [table]

Needs the table's `SYNC_RULES` entry to carry a third column, and the bridge running with
it:

    SYNC_RULES=test_types:updated_at,deleted_at,last_writer
"""

import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone

import msgpack
import zb

TABLE = sys.argv[1] if len(sys.argv) > 1 else "test_types"
LOW, HIGH = "c-aaa", "c-zzz"


def principal() -> str:
    rest = zb.NATS_URL.split("://", 1)[-1]
    return rest.rsplit("@", 1)[0].split(":", 1)[0] if "@" in rest else "alice"


def version_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


async def main():
    failed = 0
    who = principal()

    # ── the configuration this scenario is about ───────────────────────────────
    rules = os.environ.get("SYNC_RULES", "")
    entry = next((r for r in rules.split(";") if r.strip().startswith(f"{TABLE}:")), None)
    cols = entry.split(":", 1)[1].split(",") if entry else []
    if len(cols) < 3:
        sys.exit(
            f"'{TABLE}' has no tiebreak column in SYNC_RULES, so equal versions are refused\n"
            f"  rather than resolved — which is the safe default, not a bug.\n"
            f"    SYNC_RULES={TABLE}:updated_at,deleted_at,last_writer\n"
            "  Add the column, restart the bridge with that value, and re-run."
        )
    version_col, client_col = cols[0].strip(), cols[2].strip()
    print(f"version column: {version_col}   tiebreak column: {client_col}\n")

    have = zb.psql(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name='{TABLE}' AND column_name='{client_col}'"
    ).strip()
    if not have:
        sys.exit(f"SYNC_RULES names '{client_col}' but '{TABLE}' has no such column")

    # Every NOT NULL column with no default must be supplied, or the write is refused for
    # a reason that has nothing to do with ties. Read from the catalog: a hardcoded list
    # silently stopped inserting when this table grew a NOT NULL tenant_id.
    required = [
        c for c in zb.psql(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{TABLE}' AND is_nullable='NO' AND column_default IS NULL"
        ).splitlines() if c
    ]
    tenant = zb.psql(
        f"SELECT tenant_id FROM zebridge_user_tenants WHERE principal='{who}' LIMIT 1"
    ).strip()

    nc = await zb.connect()
    js = nc.jetstream()
    subject = zb.subject(
        zb.TOPOLOGY["subjects"]["mutations_prefix"], who, TABLE, "insert"
    )

    async def write(uid: str, version: str, client_id: str, text: str, forged: str | None = None):
        data = {"uid": uid, "some_text": text, version_col: version, "inserted_at": version}
        for c in required:
            if c == "tenant_id" and tenant:
                data[c] = tenant
            elif c not in data:
                data[c] = version
        # The forgery case: `client_id` set inside `data`, which the bridge must ignore.
        if forged is not None:
            data[client_col] = forged
        await js.publish(
            subject,
            msgpack.packb({"key": {"uid": uid}, "data": data, "version": version, "client_id": client_id}),
            headers={"Nats-Msg-Id": f"tie-{client_id}-{uid}"},
        )
        await asyncio.sleep(2.5)

    async def winner(uid: str):
        row = zb.psql(
            f"SELECT some_text || '|' || coalesce({client_col},'(none)') "
            f"FROM public.{TABLE} WHERE uid = '{uid}'"
        ).strip()
        return row.split("|") if "|" in row else (row, "")

    # ── 1 + 2. the same pair, both orders ──────────────────────────────────────
    for label, first, second in (
        ("low then high", LOW, HIGH),
        ("high then low", HIGH, LOW),
    ):
        uid = str(uuid.uuid4())
        v = version_now()  # ONE version for both writes: this is the tie
        await write(uid, v, first, f"{label}: from {first}")
        await write(uid, v, second, f"{label}: from {second}")

        text, stored = await winner(uid)
        if not text:
            zb.bad(f"{label}: no row at all — the write was refused for another reason")
            failed += 1
            continue

        if stored == HIGH:
            zb.ok(f"{label}: '{HIGH}' won, and is stored as the last writer")
        else:
            zb.bad(f"{label}: '{stored}' won — order changed the outcome, so this is a race, not a rule")
            failed += 1

        zb.psql(f"DELETE FROM public.{TABLE} WHERE uid = '{uid}'", quiet=True)

    # ── 3. a client cannot claim an identity in `data` ─────────────────────────
    #
    # If `data.<client_col>` were honoured, the loser of a tie could simply declare itself
    # the higher id and win — the same forgery the version column is protected from.
    uid = str(uuid.uuid4())
    v = version_now()
    await write(uid, v, LOW, "forged: claims to be the high id in data", forged=HIGH)
    _, stored = await winner(uid)
    if stored == LOW:
        zb.ok(f"`{client_col}` in `data` ignored — stamped from the envelope ('{LOW}')")
    elif stored == HIGH:
        zb.bad(f"`data.{client_col}` was honoured: a client can claim any identity when a tie is decided")
        failed += 1
    else:
        zb.bad(f"unexpected stored writer: '{stored}'")
        failed += 1
    zb.psql(f"DELETE FROM public.{TABLE} WHERE uid = '{uid}'", quiet=True)

    await nc.close()
    return 1 if failed else 0


zb.run(main)

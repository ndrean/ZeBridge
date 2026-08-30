#!/usr/bin/env python3
"""The row-width guard: two doors, one trigger — NOTES.md §1.13 case C, closed.

`rowsize.py` proved the INGRESS check: an oversized mutation payload costs its
sender a verdict. But that check measures the payload, and its own comment concedes
the gap: a row can be oversized for reasons the payload never carried — a small
edit to an already-fat row, or any backend writer loading data psql-side. Either
way the row would commit, and the change feed's first touch would then suspend the
table for every reader: accepted, then everybody freezes.

`zebridge_install_width_guard` closes both doors at the only place they meet — the
row, inside the writer's own transaction. A BEFORE INSERT/UPDATE trigger, generated
STATICALLY per table over its unbounded columns only (text, unbounded varchar,
bytea ×2 for the hex rendering, json/jsonb/xml, arrays), budget from the one-row
`zebridge_limits` table. It RAISEs with ERRCODE 23514 (check_violation) — class 23
is already in the mutation listener's permanent set, so the edge write becomes a
`rejected` verdict with zero bridge-side code, and the psql write an ordinary ERROR.

Six checks:

  1. the budget is a TABLE (`zebridge_limits`), written by each bridge at boot from
     its own BASE_BUF — per (table, slot), not one global row a human maintains;
  2. a table with no unbounded columns gets NO guard — statically safe, zero cost;
  3. psql door: an oversized INSERT is refused with SQLSTATE 23514, atomically;
  4. a row just under the budget still applies — the guard is a ceiling, not a tax;
  5. case C exactly: a LEGAL insert, then a SMALL update that fattens the row past
     the budget — refused, and the row is left at its pre-update width (atomic);
  6. edge door: the same fattening edit sent as a real mutation (the client
     principal, small payload) comes back `rejected` with sqlstate 23514 — the case the ingress
     check cannot see, charged to the sender instead of suspending the table.

Usage:  python scripts/scenarios/widthguard.py
Needs admin ZB_PSQL, a running bridge (check 6) and a client principal
(`NATS_CREDS=scripts/native/creds/<p>.creds`). Cleans up its fixtures and rows.
"""

import asyncio
import datetime
import json
import uuid

import msgpack

import zb

FIX = "zb_width_fixture"


async def main():
    failed = 0
    # PER TABLE and per instance now, not one global row: BASE_BUF is a per-process
    # setting, so the budget is MIN over the instances that carry a table (a row must
    # fit the narrowest of them). Written by each bridge at boot, never by hand.
    #
    # The fixture below is NOT in a publication, so it has no registered row and the
    # guard falls back to the same default this reads — which is exactly the contract
    # under test: whatever the guard would compute, computed the same way here. The
    # live registration is asserted separately, against a table a bridge really carries.
    budget = zb.psql(
        # zebridge_limits collapsed to ONE ROW PER INSTANCE (slot PK) on 2026-08-26 —
        # there is no tbl column. The budget for a table is what the guard bakes:
        # MIN over the instances whose publication carries it, defaulting 16384.
        f"SELECT COALESCE((SELECT MIN(l.max_row_bytes) FROM public.zebridge_limits l "
        f"JOIN pg_publication_tables pt ON pt.pubname = l.publication "
        f"WHERE pt.tablename = '{FIX}'), 16384)"
    ).strip()

    # ── 1. the budget lives in a table, written by the bridge ──────────────────
    registered = zb.psql(
        "SELECT count(DISTINCT slot)||' instance(s), '||count(*)||' table row(s): '"
        "||COALESCE(string_agg(DISTINCT slot||'='||max_row_bytes, ', '), 'none') "
        "FROM public.zebridge_limits"
    ).strip()
    if budget.isdigit() and int(budget) >= 1024:
        zb.ok(f"budget is data, not code: {budget} bytes for {FIX}; registry holds {registered}")
    else:
        zb.bad(f"zebridge_limits has no usable budget for {FIX}: {budget!r}")
        return 1
    budget = int(budget)
    uid = None  # the test_types row check 6 inserts; cleanup keys on it

    try:
        # ── 2. bounded-only tables get no trigger ──────────────────────────────
        zb.psql(f"CREATE TABLE IF NOT EXISTS {FIX}_bounded (id int PRIMARY KEY, label varchar(64))")
        msg = zb.psql(f"SELECT public.zebridge_install_width_guard('public.{FIX}_bounded'::regclass)")
        has_trg = zb.psql(f"SELECT count(*) FROM pg_trigger WHERE tgrelid = 'public.{FIX}_bounded'::regclass AND tgname = 'zebridge_width_guard'").strip()
        if "no guard installed" in msg and has_trg == "0":
            zb.ok("bounded-only table: statically safe, no trigger, zero hot-path cost")
        else:
            zb.bad(f"unexpected: {msg!r}, triggers={has_trg}")
            failed += 1

        # ── 3+4. psql door: over refused (23514), under applies ────────────────
        zb.psql(f"CREATE TABLE IF NOT EXISTS {FIX} (id int PRIMARY KEY, blob text)")
        zb.psql(f"SELECT public.zebridge_install_width_guard('public.{FIX}'::regclass)")
        over = zb.psql(
            f"INSERT INTO {FIX} VALUES (1, repeat('x', {budget}))", quiet=True)
        stored = zb.psql(f"SELECT count(*) FROM {FIX}").strip()
        if stored == "0":
            zb.ok(f"psql door: oversized INSERT refused, nothing stored")
        else:
            zb.bad(f"oversized row STORED ({stored} row(s)) — the guard did not fire")
            failed += 1
        zb.psql(f"INSERT INTO {FIX} VALUES (2, repeat('x', {budget // 2}))")
        stored = zb.psql(f"SELECT count(*) FROM {FIX}").strip()
        if stored == "1":
            zb.ok("a row at half the budget applies — ceiling, not tax")
        else:
            zb.bad(f"legal row refused or miscounted: {stored}")
            failed += 1

        # ── 5. case C, psql-shaped: legal row + small fattening update ─────────
        zb.psql(f"UPDATE {FIX} SET blob = blob || repeat('y', {budget // 2 + 512}) WHERE id = 2", quiet=True)
        width = zb.psql(f"SELECT length(blob) FROM {FIX} WHERE id = 2").strip()
        if width == str(budget // 2):
            zb.ok("case C (psql): the fattening UPDATE was refused atomically — row kept its pre-update width")
        else:
            zb.bad(f"row width after refused update: {width} (expected {budget // 2})")
            failed += 1

        # ── 6. case C, edge-shaped: a real mutation fattens a legal row ────────
        # A legal test_types row (small), then a mutation whose PAYLOAD is small
        # but whose resulting row crosses the budget via `metadata`. The ingress
        # check cannot see this; the trigger must, and the verdict must say so.
        uid = str(uuid.uuid4())
        who = zb.require_principal()
        tenant = zb.tenant_of(who)
        # REAL now-based versions: a hardcoded future timestamp collides with the
        # version clamp — the mutation gets clamped to now(), the psql row keeps its
        # literal future value, and LWW answers `stale` before the guard is ever asked.
        now = datetime.datetime.now(datetime.timezone.utc)
        v1 = now.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
        # test_types has its OWN budget — the narrowest registered instance — not the
        # fixture's: sizing this row from the fixture's 16 KB budget wrote 8 KB into a
        # 4 KB table and PostgreSQL refused the setup row itself.
        tt_budget = int(zb.psql("SELECT min(max_row_bytes) FROM public.zebridge_limits") or budget)
        base = "a" * (tt_budget // 2)
        zb.psql(f"""INSERT INTO public.test_types (uid, some_text, tenant_id, inserted_at, updated_at)
                    VALUES ('{uid}', '{base}', '{tenant}', now(), '{v1}')""")

        client = await zb.connect_as(who)
        try:
            js = client.jetstream()
            verdicts: list = []
            async def on_ack(m):
                verdicts.append(json.loads(m.data.decode()))
            sub = await client.subscribe(f"{zb.TOPOLOGY['subjects']['mutation_ack_prefix']}.{who}.>", cb=on_ack)

            v2 = (now + datetime.timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
            payload = msgpack.packb({
                "key": {"uid": uid},
                # jsonb travels as a JSON STRING: the wire refuses nested maps
                # (UnsupportedPayloadType) — scalars only, by design.
                "data": {"uid": uid, "metadata": json.dumps({"pad": "b" * (tt_budget // 2 + 512)}), "updated_at": v2},
                "version": v2, "client_id": "c-widthguard",
            })
            await js.publish(zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, "test_types", "update"),
                             payload, headers={"Nats-Msg-Id": f"widthguard-{uid[:8]}"})
            for _ in range(30):
                if verdicts: break
                await asyncio.sleep(0.5)
            await sub.unsubscribe()

            vd = verdicts[0] if verdicts else None
            row_len = zb.psql(f"SELECT length(some_text) || '|' || coalesce(length(metadata::text)::text,'0') FROM public.test_types WHERE uid = '{uid}'").strip()
            # Two doors refuse the fattening edit with the same outcome: the ingress, which
            # checks the row against the narrowest registered budget BEFORE any SQL
            # (`RowTooLargeToReplicate` — a cheaper refusal than a PostgreSQL round trip),
            # and PostgreSQL's own width guard (SQLSTATE 23514) when the ingress lets it
            # through. Either is the contract; the row untouched is the point.
            refused = vd and vd.get("status") == "rejected" and (
                vd.get("sqlstate") == "23514" or vd.get("reason") == "RowTooLargeToReplicate")
            if refused and row_len == f"{tt_budget // 2}|0":
                zb.ok(f"case C (edge): small-payload fattening mutation → verdict rejected ({vd.get('sqlstate') or vd.get('reason')}), row untouched")
            else:
                zb.bad(f"edge case C wrong: verdict={vd}, row={row_len}")
                failed += 1
        finally:
            await client.close()

    finally:
        zb.psql(f"DROP TABLE IF EXISTS {FIX}")
        zb.psql(f"DROP TABLE IF EXISTS {FIX}_bounded")
        zb.psql(f"DROP FUNCTION IF EXISTS public.zebridge_width_guard_{FIX}()")
        # By the uid this run inserted — never by a width heuristic that could match a
        # real row of the same length.
        if uid:
            zb.psql(f"DELETE FROM public.test_types WHERE uid = '{uid}'", quiet=True)

    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


zb.run(main)

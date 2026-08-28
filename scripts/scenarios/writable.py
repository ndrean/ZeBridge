#!/usr/bin/env python3
"""Edge-writability — does the schema honour what the GRANT declares?

Two independent statements decide whether a table can be written from the edge, and
nothing used to compare them:

  **intent**     `GRANT INSERT ... TO bridge_writer` — explicit, per-table, made by a
                 DBA. Unlike a config file it cannot drift from what is enforced,
                 because it *is* what PostgreSQL enforces.
  **capability** the schema: a usable version column (PROTOCOL.md §7.3), and a primary
                 key a client can mint (§7.2).

`SYNC_RULES` is neither. It says *how* to write a table — which column is the version,
which is the tombstone — never *whether* the table may be written. So a table can be
configured for writes it will never perform, or granted writes its schema cannot honour,
and both look fine until a mutation is already in flight.

⚠️ The failure this exists to catch is **silent**. `permission denied` is a SQL error,
and the bridge classifies every SQL error as transient — it cannot distinguish a refused
grant from a dropped connection until the reply channel exists. So the mutation is NAK'd
and retried to `max_deliver`, and then JetStream simply stops delivering it. Nothing
publishes a dead letter. A client sees no error, no row, and no CDC echo: the write
evaporates. The only trace is one line in the bridge's log.

Usage:  python scripts/scenarios/writable.py [writable_table] [readonly_table]

  writable.py                       # test_types (writable) vs users (outbound-only)
  writable.py notes users           # a different pair
"""

import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone

import json

import msgpack
import zb

# One row per published table: what the grant says, and what the schema can do.
INTENT_VS_CAPABILITY = """
SELECT DISTINCT pt.tablename,
       CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bridge_writer')
            THEN has_table_privilege('bridge_writer', cl.oid, 'INSERT') ELSE false END,
       coalesce((
         SELECT bool_or(pg_get_expr(ad.adbin, ad.adrelid) LIKE 'nextval%'
                        OR pk.attidentity <> '')
         FROM pg_index i
         JOIN pg_attribute pk ON pk.attrelid = i.indrelid AND pk.attnum = ANY(i.indkey)
         LEFT JOIN pg_attrdef ad ON ad.adrelid = pk.attrelid AND ad.adnum = pk.attnum
         WHERE i.indrelid = cl.oid AND i.indisprimary
       ), false),
       EXISTS (SELECT 1 FROM pg_attribute va
               WHERE va.attrelid = cl.oid AND va.attname = 'updated_at'
                 AND va.attnum > 0 AND NOT va.attisdropped),
       EXISTS (SELECT 1 FROM pg_attribute ta
               WHERE ta.attrelid = cl.oid AND ta.attname = 'deleted_at'
                 AND ta.attnum > 0 AND NOT ta.attisdropped)
FROM pg_publication_tables pt
JOIN pg_namespace ns ON ns.nspname = pt.schemaname
JOIN pg_class cl ON cl.relname = pt.tablename AND cl.relnamespace = ns.oid
WHERE pt.pubname = '{pub}' AND pt.tablename NOT LIKE 'zebridge%'
ORDER BY pt.tablename
"""


def version_now() -> str:
    """The shape CDC echoes back: ISO with `T`, microseconds, no zone suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")


async def main():
    writable = sys.argv[1] if len(sys.argv) > 1 else "test_types"
    readonly = sys.argv[2] if len(sys.argv) > 2 else "users"
    pub = zb.publication()

    # ── 1. the matrix ────────────────────────────────────────────────────────────
    print("published table    grant  key-mintable  version  tombstone   verdict")
    print("─" * 76)
    facts = {}
    failed = 0
    for line in zb.psql(INTENT_VS_CAPABILITY.format(pub=pub)).splitlines():
        if not line:
            continue
        name, granted, db_key, has_version, has_tombstone = line.split("|")
        g, dbk = granted == "t", db_key == "t"
        v, t = has_version == "t", has_tombstone == "t"
        facts[name] = {"granted": g, "mintable": not dbk, "version": v, "tombstone": t}

        # The mismatches. Each one fails at runtime; the point of the report is that
        # they are all visible here, before a single mutation is sent.
        if g and dbk:
            verdict, bad = "🔴 granted, key not mintable", True
        elif g and not v:
            verdict, bad = "🔴 granted, no version column", True
        elif g:
            verdict, bad = "✍️  writable", False
        else:
            verdict, bad = "outbound-only", False
        failed += bad
        print(
            f"{name:<18} {'yes' if g else 'no ':<6} {'yes' if not dbk else 'NO ':<13} "
            f"{'yes' if v else 'no ':<8} {'yes' if t else 'no ':<11} {verdict}"
        )

    for name in (writable, readonly):
        if name not in facts:
            sys.exit(f"\n'{name}' is not in publication '{pub}'")

    print()
    if failed:
        zb.bad(f"{failed} table(s) granted writes their schema cannot honour")
    else:
        zb.ok("every granted table can honour a write")

    if facts[readonly]["granted"]:
        zb.bad(f"'{readonly}' was meant to be the outbound-only fixture but has INSERT")
        failed += 1

    # ── 2. does the published schema agree with the matrix? (NOTES.md §1.11) ─────
    #
    # The bridge now computes this same verdict independently, in Zig, at boot
    # (`preflight.reportVersionColumns`) and on a `CREATE TABLE` DDL event
    # (`preflight.reportTable`), and publishes it as `writable` in the schema payload
    # (`$KV.schemas.<table>`). This checks the two never drift, the same way the matrix
    # above checks grant vs. schema — same shape of test, one layer further out.
    #
    # ⚠️ `expected` here is the matrix's own simplification (a hardcoded `updated_at`
    # presence-and-type check), not the bridge's full `classifyVersionColumn` — it
    # cannot tell a genuinely unusable column (`.creation_column`, a nullable/naive
    # timestamp) from an absent one. Fine for these fixtures, which don't exercise that
    # distinction; a mismatch here on a table that does would need a closer look before
    # trusting either side.
    print()
    for name, f in facts.items():
        res = zb.nats_cli("kv", "get", "schemas", name, "--raw")
        if res.returncode != 0 or not res.stdout.strip():
            zb.bad(f"'{name}': could not read schema from KV")
            failed += 1
            continue
        try:
            published = json.loads(res.stdout).get("writable")
        except json.JSONDecodeError:
            zb.bad(f"'{name}': schema is not valid JSON")
            failed += 1
            continue

        expected = f["granted"] and f["mintable"] and f["version"]
        if published is None:
            zb.bad(f"'{name}': schema publishes \"writable\": null — expected {expected}")
            failed += 1
        elif published != expected:
            zb.bad(
                f"'{name}': schema publishes \"writable\": {published}, but "
                f"grant+schema say {expected}"
            )
            failed += 1
        else:
            zb.ok(f"'{name}': schema publishes \"writable\": {published}, agrees")

    # ── 3. what a client actually observes on a refused table ────────────────────
    #
    # The decisive assertion, and the reason this is a scenario rather than a query:
    # the refusal is real (PostgreSQL says so) but *invisible on the wire*. Anything
    # that later makes it visible — the reply channel — should turn this assertion
    # around, and this is where that will be noticed.
    nc = await zb.connect()
    js = nc.jetstream()

    # `mutation_ack.<principal>.>`, not `mutation_error.>`: the dead letter is keyed by
    # table and readable by every principal, so clients are no longer granted it. A
    # verdict is keyed by the client's own msg_id and scoped to the principal, which is
    # what makes it matchable to a queued write and safe to grant.
    verdicts = []
    sub = await nc.subscribe(f"{zb.TOPOLOGY['subjects']['mutation_ack_prefix']}.alice.>")

    async def collect():
        async for msg in sub.messages:
            verdicts.append(json.loads(msg.data.decode()))

    task = asyncio.create_task(collect())

    async def send(table, key):
        v = version_now()
        msg_id = f"scenario-{table}-{list(key.values())[0]}"
        payload = {
            "key": key,
            "data": {**key, "updated_at": v},
            "version": v,
            "client_id": "scenario-writable",
        }
        subject = zb.subject(
            zb.TOPOLOGY["subjects"]["mutations_prefix"], "alice", table, "insert"
        )
        # The msg_id is the correlation token *and* becomes a subject token, so it must
        # avoid `.`, `*`, `>` and whitespace — a value carrying a dot silently changes the
        # subject's token count. Without it the bridge has nothing to address a verdict
        # to, and skips publishing one.
        await js.publish(
            subject,
            msgpack.packb(payload, use_bin_type=True),
            headers={"Nats-Msg-Id": msg_id},
        )
        return v

    print(f"\nsending one INSERT to '{readonly}' (expected: refused by PostgreSQL)")
    ro_key = {"id": 2_000_000_000 + int(uuid.uuid4().int % 1_000_000)}
    await send(readonly, ro_key)

    # Long enough for several redeliveries. `max_deliver` is 5 at ~1s apart, so this
    # window covers the whole retry sequence rather than catching it mid-flight.
    await asyncio.sleep(8)

    landed = zb.psql(
        f"SELECT count(*) FROM public.{readonly} WHERE id = {ro_key['id']}"
    ).strip()
    print(f"  row in PostgreSQL : {landed}")
    print(f"  verdicts received : {len(verdicts) or 'none'}")

    if landed != "0":
        zb.bad(f"'{readonly}' accepted a write it was not granted")
        failed += 1
    elif not verdicts:
        # This *was* the documented behaviour, and the assertion used to pass here: the
        # write evaporated with no row, no error and no echo. It is now a regression —
        # the verdict is published on the bridge's last delivery attempt, so its absence
        # means either the bridge never gave up or it could not address the verdict
        # (a missing Nats-Msg-Id makes one unaddressable).
        zb.bad(
            f"'{readonly}' refused the write and said nothing — no verdict within the "
            "retry budget. This is the silent failure verdicts exist to remove."
        )
        failed += 1
    else:
        v = verdicts[0]
        zb.ok(
            f"'{readonly}' refused the write and said so: status={v.get('status')} "
            f"sqlstate={v.get('sqlstate')} — {v.get('detail','').strip()}"
        )
        # Asserting on the *reason* rather than on "some verdict" keeps this honest: a
        # verdict naming the wrong cause would otherwise pass.
        #
        # ⚠️ Two refusals are correct here, and which one you get depends on the table's
        # key. A missing grant surfaces as PostgreSQL's `42501 insufficient_privilege`,
        # but a **sequence-backed primary key is refused first**, before any SQL runs,
        # because granting INSERT would not fix it — the client still could not mint a
        # key. So `DbAllocatedKey` superseding `42501` is the bridge reporting the deeper
        # problem, not losing the shallower one; `scripts/scenarios/keys.py` covers it.
        seq_key = zb.psql(
            "SELECT coalesce(pg_get_serial_sequence('public.%s', a.attname), '') "
            "FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
            "WHERE i.indrelid='public.%s'::regclass AND i.indisprimary LIMIT 1" % (readonly, readonly)
        ).strip()
        if seq_key:
            if v.get("reason") == "DbAllocatedKey":
                zb.ok(f"and refused on the key shape first ('{readonly}' is backed by {seq_key})")
            else:
                zb.bad(
                    f"'{readonly}' has a sequence-backed key, so the write should be refused as "
                    f"DbAllocatedKey before the privilege check; got {v.get('reason')!r}"
                )
                failed += 1
        elif v.get("sqlstate") != "42501":
            zb.bad(f"expected SQLSTATE 42501 (insufficient_privilege), got {v.get('sqlstate')!r}")
            failed += 1

    task.cancel()
    await nc.close()
    return 1 if failed else 0


zb.run(main)

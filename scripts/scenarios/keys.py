#!/usr/bin/env python3
"""A database-allocated primary key on an edge-writable table corrupts quietly.

PROTOCOL.md §7.2: a mutation carries its own `key`, so the client mints the primary key.
On a `serial` / `bigserial` / `identity` column that is a landmine, and — uniquely among
the bridge's findings — **the writes succeed**:

  * an explicit `id` does not advance `nextval`, so every edge-written key is a value the
    application's own next `INSERT` will eventually collide with, months later, for no
    reason visible in the application's history;
  * the same collision arriving through the bridge does not even error — the upsert's
    `ON CONFLICT DO UPDATE` silently overwrites whichever row was there first, and
    last-write-wins decides between two rows that were never related;
  * two offline clients creating rows collide by construction, because both mint from the
    same small integers.

⚠️ **The bridge refuses these writes** — it does not merely warn. Silent cross-row data
loss is not a tuning choice, and unlike every other refusal the damage is invisible at the
moment it is done.

⚠️ **The refusal is scoped to the write path, not the table** — *blocked*, not *suspended*,
and those mean opposite things to a reader. A suspension tells every client to drop its
local copy; a write refusal must leave them untouched. Such a table replicates outbound
perfectly well and its readers are not at risk, so refusing it in the shared registry would
blind every consumer for a hazard that exists only on ingress.

⚠️ **A refused client is not a blocked client.** It gets one "no thank you" for one
message and keeps everything else, including CDC **for the very table it wrote to** — its
write was wrong, its subscription was not. So the checks below run on the **same
connection** that was just refused, rather than on a fresh reader: "some reader still gets
CDC" is a weaker claim and would pass even if the refused client had been cut off.

Four ways the scoping could be wrong, none visible from the write side:

  1. the refusal reaches the shared registry → CDC dropped for everyone;
  2. a `suspended` schema is published → clients drop their local copy;
  3. the refusal cascades to other tables on the same listener;
  4. the refused client loses CDC for the table it wrote to.

⚠️ The escape is a migration, not a flag: a uuid key, or **drop the column `DEFAULT`** and
assign clients disjoint ranges. That second option keeps §7.2's integer strategy available
— dropping the default is what turns the sequence off, and the sequence is what this
detects.

⚠️ The check only fires when the table is **both** sequence-backed **and** granted INSERT.
A read-only table with a `bigserial` key is entirely normal and says nothing — which is
why this scenario has to grant the privilege to see it, and revoke it afterwards.

Usage:  python scripts/scenarios/keys.py [table]

  keys.py            # users — bigserial PK, outbound-only in the sample schema
"""

import asyncio
import os
import pathlib
import sys
from urllib.parse import unquote

import msgpack
from nats.js.api import ConsumerConfig, DeliverPolicy

import zb

TABLE = sys.argv[1] if len(sys.argv) > 1 else "users"
SCRATCH = pathlib.Path(os.environ.get("TMPDIR", "/tmp"))


# ⚠️ Same rule as `mutate.py` and `rowsize.py`: the principal is the NATS user this
# script authenticates AS (`zb.require_principal()`: ZB_PRINCIPAL, else the creds file's
# stem). A guessed name publishes on a subject the connection is not allowed to use,
# and the bridge dead-letters it with **no addressable verdict** — every assertion
# below then fails at once and looks exactly like a write-path leak.


def writer_role() -> str:
    """The writer role's name, out of the URL the bridge dials it with."""
    url = os.environ.get("DATABASE_WRITER_URL")
    if not url:
        sys.exit("DATABASE_WRITER_URL is not set — set -a && . ./.env.bridge && set +a")
    rest = url.split("://", 1)[-1]
    creds = rest.rsplit("@", 1)[0] if "@" in rest else ""
    return unquote(creds.partition(":")[0])


def metric(name: str) -> float | None:
    import urllib.request
    try:
        # The long-running bridge's: the probe has exited by the time this is read.
        with urllib.request.urlopen(zb.http_base() + "/metrics", timeout=4) as r:
            for line in r.read().decode().splitlines():
                if line.startswith(f"{name} "):
                    return float(line.split()[1])
    except Exception:  # noqa: BLE001
        return None
    return None


async def main():
    failed = 0
    writer = writer_role()
    who0 = zb.require_principal()

    pk_seq = zb.psql(
        "SELECT coalesce(pg_get_serial_sequence('public.%s', a.attname), '') "
        "FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
        "WHERE i.indrelid='public.%s'::regclass AND i.indisprimary LIMIT 1" % (TABLE, TABLE)
    ).strip()
    if not pk_seq:
        sys.exit(
            f"'{TABLE}' has no sequence-backed primary key, so there is nothing to detect.\n"
            "  This scenario needs a table whose PK is serial/bigserial/identity."
        )
    print(f"{TABLE}: primary key is backed by {pk_seq}\n")

    # ⚠️ Granting INSERT is what makes the finding reachable. Reverted in `finally`,
    # because leaving it would open a table the sample schema deliberately keeps
    # outbound-only.
    zb.psql(f"GRANT INSERT ON public.{TABLE} TO {writer}", quiet=True)
    try:
        log = SCRATCH / "keys_preflight.log"
        with zb.Bridge(log) as br:
            br.wait_for_log("Edge writes:", timeout=45)
            text = br.text()

        if "database-allocated" not in text:
            zb.bad(
                f"preflight did not flag '{TABLE}' despite a sequence-backed key and an "
                "INSERT grant — every edge write would plant a collision unannounced"
            )
            failed += 1
        else:
            zb.ok("preflight flags a database-allocated key on an edge-writable table")

        # ⚠️ The warning must say the writes are REFUSED. It once said they would succeed
        # and corrupt quietly, which was true when this was only a warning — an operator
        # reading the old text now would expect damage that no longer happens, and might
        # "fix" it by removing the grant they actually wanted.
        if "REFUSES every mutation" in text:
            zb.ok("and states that mutations are refused, matching what the write path does")
        else:
            zb.bad(
                "the preflight warning does not say mutations are refused — it disagrees "
                "with the write path, so the operator cannot predict what happens"
            )
            failed += 1

        # ⚠️ The decisive assertion: a mutation must be REFUSED and no row written.
        # Preflight warning without enforcement would leave the corruption available to
        # anyone who did not read the log.
        # ⚠️ ONE client for everything below. The question is not "does some reader still
        # get CDC" — it is whether the client that was just refused still does. A refused
        # write must cost that client one message, not its subscription.
        # ⚠️ Baseline, not zero: the registry legitimately holds tables refused at boot
        # (no PK, no CDC route). The claim is that *this write* adds nothing to it.
        refused_before = metric("bridge_refused_tables")

        # As the CLIENT principal, confined exactly as a real client is: connected as
        # the bridge this would pass while proving nothing. A client holds no core
        # `cdc.>` subscription — public tables are read through a consumer on
        # CDC_PUBLIC, delivered on its inbox — so that is how CDC is watched here.
        nc = await zb.connect_as(who0)
        js = nc.jetstream()
        cdc_seen = []
        cdc_sub = await js.subscribe(
            f"{zb.TOPOLOGY['subjects']['cdc_prefix']}.{TABLE}.>",
            stream=zb.TOPOLOGY["cdc_streams"]["public"],
            config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW),
        )

        async def watch_cdc():
            async for m in cdc_sub.messages:
                cdc_seen.append(m.subject)

        cdc_task = asyncio.create_task(watch_cdc())
        verdicts = {}
        # ⚠️ `mutation_ack.>` is denied — a client is granted only its own subtree.
        sub = await nc.subscribe(f"{zb.TOPOLOGY['subjects']['mutation_ack_prefix']}.{who0}.*")

        async def collect():
            async for m in sub.messages:
                verdicts[m.subject.rsplit(".", 1)[-1]] = zb.decode(m.data)

        task = asyncio.create_task(collect())
        await asyncio.sleep(1)

        probe_id = 987654321
        msg_id = f"key-{os.urandom(6).hex()}"
        v = zb.psql("SELECT to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US') || 'Z'").strip()
        who = who0
        await js.publish(
            zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, TABLE, "insert"),
            msgpack.packb({
                "key": {"id": probe_id},
                "data": {"id": probe_id, "name": "seq key probe", "inserted_at": v, "updated_at": v},
                "version": v, "client_id": "c-keys",
            }),
            headers={"Nats-Msg-Id": msg_id},
        )
        await asyncio.sleep(4)
        task.cancel()

        verdict = verdicts.get(msg_id)
        landed = zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE id = {probe_id}").strip()
        if landed != "0":
            zb.bad(f"the write LANDED on a sequence-backed key — it will collide with the application's own inserts")
            failed += 1
            zb.psql(f"DELETE FROM public.{TABLE} WHERE id = {probe_id}", quiet=True)
        elif verdict and verdict.get("reason") == "DbAllocatedKey":
            zb.ok("a mutation is refused with `DbAllocatedKey`, and no row is written")
        elif verdict:
            zb.bad(f"refused, but for the wrong reason: {verdict}")
            failed += 1
        else:
            zb.bad("no verdict — the client cannot tell a refused write from a lost one")
            failed += 1

        # ══ a refused writer must not blind the readers ════════════════════════
        #
        # ⚠️ The claim the scoping rests on: refusing WRITES is not suspending the TABLE.
        # Three ways it could be wrong, none visible from the write path alone — the
        # refusal could land in the shared registry (CDC dropped for everyone), publish a
        # `suspended` schema (clients drop their local copy), or cascade to other tables
        # on the same listener. A reader is the only thing that can tell.
        print("\nand the refused client is not blocked:")
        js2 = js
        mark = len(cdc_seen)
        await asyncio.sleep(1)

        # A change made the ordinary way — the application writing directly. This is what
        # every reader of this table depends on continuing to arrive.
        marker = os.urandom(4).hex()
        zb.psql(
            f"INSERT INTO public.{TABLE} (name, inserted_at, updated_at) "
            f"VALUES ('reader probe {marker}', now(), now())",
            quiet=True,
        )

        # And a write to a DIFFERENT table, with a mintable key, which must be unaffected.
        other = "test_types"
        # The tenant this principal is mapped to, or RLS refuses the write (same lookup
        # as `mutate.py`); an unmapped principal falls back to the open tenant.
        tenant = zb.tenant_of(who0) or zb.TOPOLOGY["open_tenant"]
        other_uid = __import__("uuid").uuid4()
        ov = zb.psql("SELECT to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US') || 'Z'").strip()
        await js2.publish(
            zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who0, other, "insert"),
            msgpack.packb({
                "key": {"uid": str(other_uid)},
                "data": {"uid": str(other_uid), "some_text": "unaffected", "inserted_at": ov,
                         "updated_at": ov, "tenant_id": tenant},
                "version": ov, "client_id": "c-keys",
            }),
            headers={"Nats-Msg-Id": f"other-{marker}"},
        )
        await asyncio.sleep(5)
        cdc_task.cancel()
        try:
            await cdc_sub.unsubscribe()   # the ephemeral consumer goes with it
        except Exception:  # noqa: BLE001
            pass
        await nc.close()

        # ⚠️ Same client, same connection, *after* its write was refused.
        if any(f"{TABLE}." in x for x in cdc_seen[mark:]):
            zb.ok(f"the refused client itself still receives `cdc.{TABLE}.*`")
        else:
            zb.bad(
                f"no CDC for '{TABLE}' reached the client after its write was refused — a "
                "rejected mutation has cost it the whole table, not one message"
            )
            failed += 1

        doc = zb.psql(f"SELECT count(*) FROM public.{other} WHERE uid = '{other_uid}'").strip()
        if doc == "1":
            zb.ok(f"and a write to '{other}' still applies — the refusal did not cascade")
        else:
            zb.bad(f"a write to '{other}' did not apply; the refusal leaked across tables")
            failed += 1
        zb.psql(f"DELETE FROM public.{other} WHERE uid = '{other_uid}'", quiet=True)
        zb.psql(f"DELETE FROM public.{TABLE} WHERE name LIKE 'reader probe %'", quiet=True)

        # ⚠️ "Blocked, not suspended" — the two states a client cannot tell apart from the
        # write side, and which mean opposite things on the read side. A suspension tells
        # every client to DROP its local copy of the table; a write refusal must leave
        # them alone.
        doc = zb.kv_get("schemas", TABLE).replace(" ", "")
        if '"suspended":true' in doc:
            zb.bad(
                f"'{TABLE}' was published as SUSPENDED — every client drops its local copy "
                "of a table that is only unwritable, not unreadable"
            )
            failed += 1
        elif '"pg"' in doc:
            zb.ok("the table is still published as a live schema, not a suspension")
        else:
            zb.bad(f"no usable schema in KV for '{TABLE}' (got {doc[:60]!r})")
            failed += 1

        # The registry drives CDC dropping. A write refusal must never reach it — and the
        # mutation listener holds no reference to it, which this is the end-to-end proof of.
        refused_now = metric("bridge_refused_tables")
        if refused_now is None or refused_before is None:
            print("  ⓘ  /metrics unreachable — skipping the registry check")
        elif refused_now == refused_before:
            zb.ok(
                f"and the refused registry did not move ({refused_now:.0f} before and after) — "
                "the write path never touches it"
            )
        else:
            zb.bad(
                f"bridge_refused_tables went {refused_before:.0f} → {refused_now:.0f}: a "
                "write-path refusal reached the shared registry, which drops CDC for every reader"
            )
            failed += 1

    finally:
        # ⚠️ The probe bridge above created a replication slot, and an inactive slot pins
        # WAL for the whole cluster until `max_slot_wal_keep_size` invalidates it — which
        # is how the LIVE bridge's own slot got invalidated during a long suite run.
        # `check.py` flags the orphan; leaving it for a human to drop makes every later
        # full-battery run report a finding this scenario caused (2026-08-31).
        slot = zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"
        zb.psql(f"SELECT pg_drop_replication_slot('{slot}') FROM pg_replication_slots "
                f"WHERE slot_name = '{slot}' AND NOT active", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_limits WHERE slot = '{slot}'", quiet=True)
        zb.psql(f"REVOKE INSERT ON public.{TABLE} FROM {writer}", quiet=True)
        still = zb.psql(
            f"SELECT count(*) FROM information_schema.role_table_grants "
            f"WHERE table_name='{TABLE}' AND grantee='{writer}' AND privilege_type='INSERT'"
        ).strip()
        if still != "0":
            print(f"  ⚠️  INSERT on {TABLE} is STILL granted to {writer} — revoke it by hand")

    return 1 if failed else 0


zb.run(main)

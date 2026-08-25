#!/usr/bin/env python3
"""Caches that must notice a schema change — or the bridge serves yesterday's catalog.

Three caches sit between a `ALTER TABLE` and a client, each invalidated by a different
signal, and **none of them by a timer**. A cache that misses its signal does not fail: it
keeps answering, correctly-looking, from a schema that no longer exists.

  1. **the schema in KV** — invalidated by a DDL event trigger writing to
     `zebridge_ddl_events`, which travels the WAL like any other row. This is the only
     path: the bridge never polls the catalog for shape.
  2. **the refusal registry** — a table with no primary key is refused and its events
     dropped. The refusal lifts on the DDL event carrying a fixed schema, and on a DROP
     (⚠️ different code paths — a drop carries no schema to be fixed by, so it clears
     separately, or the registry keeps announcing a table that no longer exists).
  3. **the mutation listener's catalog cache** — column names and the primary key, read
     once per table because a mutation is already one synchronous round trip.
  4. **the replication thread's relation cache** — the column list from the last
     `RELATION` message, which WAL tuples are decoded against **positionally**. ⚠️ The
     only one of the four whose staleness produces *wrong data* instead of a refusal: a
     shifted list does not error, it moves every value one column out of place. Tested
     with plain SQL rather than a mutation, so the CDC path is exercised alone.

The rule these caches serve is PROTOCOL.md §0.

Every act runs against a **live bridge**, started before the migration: nothing here polls
the catalog, so a value that changes can only have arrived through the path under test.

⚠️ These are tested **through the real chain**, not by calling the caches: every unit test
for the registry passes today, and the interesting failures are all in the wiring between
the trigger, the WAL, the ring buffer and the KV — which no unit test crosses.

Usage:  python scripts/scenarios/invalidate.py

Needs the bridge running, a live NATS, and admin psql. Restores every schema it changes,
including on failure.

⚠️ Act 3's `SCRATCH` fixture stays suspended after its no-PK refusal lifts — for a
*different* reason (`no_cdc_subject`), not a bug. It was never declared in
`grammar.json`'s `public_tables`, so its CDC subject has no stream to reach it, and the
bridge now refuses that at the source instead of hanging on the first publish attempt
(`src/refused_tables.zig`, found live via this exact scenario). Making it fully resume
would need `grammar.json` changed and the bridge restarted, which defeats the "no
restart needed" point this act is making about the no-PK refusal specifically.
"""

import asyncio
import json
import sys
import urllib.request
import uuid
from datetime import datetime, timezone

import msgpack
import zb

TABLE = "test_types"
PROBE = "zb_probe"          # the column added and dropped on the live table
SCRATCH = "zb_invalidate"   # created without a key, fixed, then dropped
PUB = "my_pub"
METRICS = "http://127.0.0.1:9090/metrics"

SETTLE = 3.0  # DDL → trigger → WAL → ring buffer → KV


def principal() -> str:
    rest = zb.NATS_URL.split("://", 1)[-1]
    return rest.rsplit("@", 1)[0].split(":", 1)[0] if "@" in rest else "alice"


def kv_get(key: str) -> dict | None:
    """The schemas bucket, read the way a client reads it."""
    res = zb.nats_cli("kv", "get", "schemas", key, "--raw")
    if res.returncode != 0 or not res.stdout.strip():
        return None
    try:
        return json.loads(res.stdout)
    except json.JSONDecodeError:
        return None


def kv_columns(key: str) -> list[str]:
    doc = kv_get(key)
    if not doc or "pg" not in doc:
        return []
    return [c["name"] for c in doc["pg"]["columns"]]


def metric(name: str) -> float | None:
    try:
        with urllib.request.urlopen(METRICS, timeout=4) as r:
            for line in r.read().decode().splitlines():
                if line.startswith(f"{name} "):
                    return float(line.split()[1])
    except Exception:  # noqa: BLE001
        return None
    return None


def version_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


async def main():
    failed = 0
    who = principal()
    nc = await zb.connect()
    js = nc.jetstream()

    required = [
        c for c in zb.psql(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{TABLE}' AND is_nullable='NO' AND column_default IS NULL"
        ).splitlines() if c
    ]
    tenant = zb.psql(
        f"SELECT tenant_id FROM zebridge_user_tenants WHERE principal='{who}' LIMIT 1"
    ).strip()

    # Verdicts, collected for the whole run: the write path answers on
    # `mutation_ack.<principal>.<msg_id>`, and "refused" is as much a result as "ok".
    verdicts: dict[str, dict] = {}
    ack_pat = zb.TOPOLOGY["subjects"]["mutation_ack_prefix"]
    ack_sub = await nc.subscribe(f"{ack_pat}.{who}.*")

    async def collect_acks():
        async for m in ack_sub.messages:
            try:
                verdicts[m.subject.rsplit(".", 1)[-1]] = zb.decode(m.data)
            except Exception:  # noqa: BLE001
                pass

    cdc_seen: list[str] = []
    cdc_events: list[dict] = []
    cdc_sub = await nc.subscribe("cdc.>")

    async def collect_cdc():
        async for m in cdc_sub.messages:
            cdc_seen.append(m.subject)
            try:
                cdc_events.append(zb.decode(m.data))
            except Exception:  # noqa: BLE001
                pass

    t1 = asyncio.create_task(collect_acks())
    t2 = asyncio.create_task(collect_cdc())
    await asyncio.sleep(0.5)

    async def write(table: str, data: dict, note: str) -> dict | None:
        """One mutation, returning its verdict (None if none arrived)."""
        msg_id = f"inv-{uuid.uuid4().hex[:12]}"
        v = version_now()
        payload = {"key": {"uid": data["uid"]}, "data": data, "version": v, "client_id": "c-inv"}
        await js.publish(
            zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, table, "insert"),
            msgpack.packb(payload),
            headers={"Nats-Msg-Id": msg_id},
        )
        await asyncio.sleep(3.0)
        return verdicts.get(msg_id)

    def base_row(text: str) -> dict:
        uid = str(uuid.uuid4())
        v = version_now()
        d = {"uid": uid, "some_text": text, "inserted_at": v, "updated_at": v}
        for c in required:
            if c == "tenant_id" and tenant:
                d[c] = tenant
            elif c not in d:
                d[c] = v
        return d

    # `SCRATCH` is created dynamically here, not declared in `grammar.json:public_tables`
    # and not tenant-scoped — so `cdc.<SCRATCH>.*` matches no stream's subject filter, and
    # never can without editing `grammar.json` and restarting the bridge (its own config
    # is read once at boot, never re-read live — editing `CDC_PUBLIC`'s live subjects
    # alone does not make the bridge believe the table is routable, and rightly so: an
    # operator manually widening a stream is not the same as *declaring* a table public).
    #
    # ⚠️ Found live: before the bridge refused this at the source, publishing to an
    # unrouted subject did not fail fast — it blocked for the full publish timeout, then
    # reconnected, and retried the identical dead end until the retry budget exhausted
    # and the bridge self-terminated (`FATAL: stopping bridge to prevent WAL overflow`).
    # That was the empirical version of the risk SECURITY.md §1.4's migration checklist
    # warns about. The bridge now refuses such a table immediately
    # (`no_cdc_subject`, `src/refused_tables.zig`) instead of ever attempting the
    # publish — which is why act 3 below does not expect `SCRATCH` to resume CDC after
    # gaining a primary key: it correctly stays suspended, for a different reason.

    try:
        # ══ 0. warm the write path's cache ══════════════════════════════════════
        #
        # ⚠️ Not optional, and not setup. The write path reads a table's catalog facts on
        # first use, so against a freshly restarted bridge the cache is EMPTY and the
        # first mutation after a migration fills it with the new shape — passing act 1
        # without ever invalidating anything. The bug this scenario exists for needs a
        # cache that was populated *before* the ALTER, which is also the only state a
        # long-running bridge is ever in.
        warm = base_row("invalidate: warming the catalog cache")
        await write(TABLE, warm, "warm")
        if zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE uid='{warm['uid']}'").strip() != "1":
            sys.exit(
                "could not seed a row before the migration, so the cache is not warm and "
                "act 1 would pass without testing anything"
            )
        zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{warm['uid']}'", quiet=True)
        print("cache warmed with the pre-migration shape\n")

        # ══ 1. a column appears ═════════════════════════════════════════════════
        print(f"1. ALTER TABLE {TABLE} ADD COLUMN {PROBE}")
        zb.psql(f"ALTER TABLE public.{TABLE} ADD COLUMN {PROBE} text", quiet=True)
        await asyncio.sleep(SETTLE)

        if PROBE in kv_columns(TABLE):
            zb.ok(f"the schema in KV grew '{PROBE}' — trigger → WAL → KV intact")
        else:
            zb.bad(
                f"'{PROBE}' never reached the KV schema: clients keep migrating to the "
                "old shape, and nothing retries"
            )
            failed += 1

        # The write path holds its own catalog cache, filled on first use and never
        # re-read on a clock. If it does not notice, a column the client can SEE in the
        # schema is one it can never WRITE.
        row = base_row("invalidate: writing the new column")
        row[PROBE] = "hello"
        verdict = await write(TABLE, row, "new column")
        stored = zb.psql(
            f"SELECT coalesce({PROBE},'(null)') FROM public.{TABLE} WHERE uid='{row['uid']}'"
        ).strip()
        if stored == "hello":
            zb.ok(f"a mutation writing '{PROBE}' was applied — the catalog cache noticed")
        else:
            reason = (verdict or {}).get("reason", "no verdict")
            zb.bad(
                f"the new column is unwritable: verdict={reason!r}, stored={stored!r}. "
                "The catalog cache is stale and nothing drops it."
            )
            failed += 1
        zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{row['uid']}'", quiet=True)

        # ⚠️ The third cache, and the only one whose staleness produces *wrong data*
        # rather than a refusal: the replication thread decodes WAL tuples positionally
        # against the column list from the last RELATION message. A stale list does not
        # error — it shifts every value one column left of where it belongs.
        #
        # Written with plain SQL, not a mutation, so this is the CDC path alone.
        uid = str(uuid.uuid4())
        mark = len(cdc_events)
        zb.psql(
            f"INSERT INTO public.{TABLE} (uid, some_text, {PROBE}, inserted_at, updated_at, tenant_id) "
            f"VALUES ('{uid}', 'cdc after alter', 'probe-value', now(), now(), '{tenant}')",
            quiet=True,
        )
        await asyncio.sleep(SETTLE)
        ev = next(
            (e for e in cdc_events[mark:]
             if e.get("table") == TABLE and e.get("data", {}).get("uid") == uid),
            None,
        )
        if ev is None:
            zb.bad(f"no CDC event at all for the row inserted after ADD COLUMN")
            failed += 1
        elif ev["data"].get(PROBE) != "probe-value":
            zb.bad(
                f"the CDC event does not carry '{PROBE}' correctly "
                f"(got {ev['data'].get(PROBE)!r}, some_text={ev['data'].get('some_text')!r}): "
                "the relation cache is stale and values are decoded against the old shape"
            )
            failed += 1
        elif ev["data"].get("some_text") != "cdc after alter":
            zb.bad(f"columns are misaligned: some_text={ev['data'].get('some_text')!r}")
            failed += 1
        else:
            zb.ok(f"CDC carries '{PROBE}' with every other column still aligned")
        zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'", quiet=True)

        # ══ 2. the column goes away ═════════════════════════════════════════════
        print(f"\n2. ALTER TABLE {TABLE} DROP COLUMN {PROBE}")
        zb.psql(f"ALTER TABLE public.{TABLE} DROP COLUMN {PROBE}", quiet=True)
        await asyncio.sleep(SETTLE)

        if PROBE not in kv_columns(TABLE):
            zb.ok(f"the schema in KV lost '{PROBE}'")
        else:
            zb.bad(f"'{PROBE}' is still in the KV schema after DROP COLUMN")
            failed += 1

        # A write naming the dropped column must be refused, not quietly dropped: the
        # client is asserting a value it believes is stored.
        row = base_row("invalidate: writing a dropped column")
        row[PROBE] = "should not apply"
        verdict = await write(TABLE, row, "dropped column")
        if verdict and verdict.get("status") != "ok":
            zb.ok(f"a write naming the dropped column was refused ({verdict.get('reason','')[:40]})")
        elif verdict:
            zb.bad(f"the write was accepted although '{PROBE}' no longer exists")
            failed += 1
        else:
            zb.bad("no verdict at all for a write against a dropped column")
            failed += 1
        zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{row['uid']}'", quiet=True)

        # ⚠️ The decisive one. A stale-catalog failure must not WEDGE the write path: the
        # next well-formed mutation has to succeed, or one client's bad write stops
        # everyone's until a restart.
        row = base_row("invalidate: the next normal write")
        await write(TABLE, row, "recovery")
        alive = zb.psql(
            f"SELECT count(*) FROM public.{TABLE} WHERE uid='{row['uid']}'"
        ).strip()
        if alive == "1":
            zb.ok("the next ordinary write still applied — the write path is not wedged")
        else:
            zb.bad("the write path stopped applying after a schema change: restart-only recovery")
            failed += 1
        zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{row['uid']}'", quiet=True)

        uid = str(uuid.uuid4())
        mark = len(cdc_events)
        zb.psql(
            f"INSERT INTO public.{TABLE} (uid, some_text, inserted_at, updated_at, tenant_id) "
            f"VALUES ('{uid}', 'cdc after drop', now(), now(), '{tenant}')",
            quiet=True,
        )
        await asyncio.sleep(SETTLE)
        ev = next(
            (e for e in cdc_events[mark:]
             if e.get("table") == TABLE and e.get("data", {}).get("uid") == uid),
            None,
        )
        if ev is None:
            zb.bad("no CDC event for the row inserted after DROP COLUMN")
            failed += 1
        elif PROBE in ev["data"]:
            zb.bad(f"CDC still emits '{PROBE}' after it was dropped — stale relation cache")
            failed += 1
        elif ev["data"].get("some_text") != "cdc after drop":
            zb.bad(f"columns are misaligned after DROP COLUMN: some_text={ev['data'].get('some_text')!r}")
            failed += 1
        else:
            zb.ok(f"CDC dropped '{PROBE}' and the remaining columns stayed aligned")
        zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'", quiet=True)

        # ══ 3. refused, then fixed, without a restart ═══════════════════════════
        print(f"\n3. a table with no primary key, then one with")
        before_refused = metric("bridge_refused_tables") or 0
        zb.psql(f"DROP TABLE IF EXISTS public.{SCRATCH}", quiet=True)
        zb.psql(
            f"CREATE TABLE public.{SCRATCH} (id bigint NOT NULL, name text)", quiet=True
        )
        # The publication guard refuses an unscoped table; this one is a fixture, and
        # saying so in `zebridge_public_tables` is the documented way to say it.
        zb.psql(
            "INSERT INTO public.zebridge_catalogue (tbl, public_reason) VALUES "
            f"('{SCRATCH}', 'invalidate.py fixture') "
            "ON CONFLICT DO NOTHING",
            quiet=True,
        )
        zb.psql(f"ALTER PUBLICATION {PUB} ADD TABLE public.{SCRATCH}", quiet=True)
        await asyncio.sleep(SETTLE)

        doc = kv_get(SCRATCH)
        if doc and doc.get("suspended") and doc.get("reason") == "no_primary_key":
            zb.ok(f"'{SCRATCH}' was refused for no_primary_key, and clients were told")
        else:
            zb.bad(
                f"no suspension published for a keyless table (KV said {doc}). "
                "Clients would build a table whose DELETEs cannot be expressed."
            )
            failed += 1

        mark = len(cdc_seen)
        zb.psql(f"INSERT INTO public.{SCRATCH} (id, name) VALUES (1, 'while refused')", quiet=True)
        await asyncio.sleep(SETTLE)
        if any(SCRATCH in s for s in cdc_seen[mark:]):
            zb.bad(f"CDC for refused '{SCRATCH}' reached the wire — the refusal drops nothing")
            failed += 1
        else:
            zb.ok("its events were dropped while refused")

        # The migration that fixes it. No restart, no signal: the DDL event is the signal.
        #
        # ⚠️ `SCRATCH`'s catalogue row was written after the bridge booted — deliberately,
        # it is a throwaway fixture, not a real table anyone should add to the static
        # config for. So gaining a primary key lifts the `no_primary_key` refusal, but the
        # table correctly *stays* suspended, now for `no_cdc_subject` (its CDC subject
        # still has no stream to reach — see the note above `try:`). Expecting it to fully
        # resume here would mean expecting the bridge to trust a table it was never told
        # about, which is the exact hole `no_cdc_subject` exists to close.
        zb.psql(f"ALTER TABLE public.{SCRATCH} ADD PRIMARY KEY (id)", quiet=True)
        await asyncio.sleep(SETTLE)

        doc = kv_get(SCRATCH)
        if doc and doc.get("suspended") and doc.get("reason") == "no_cdc_subject":
            zb.ok(
                "the no_primary_key refusal lifted on the fixing migration — no restart "
                "needed — and it now correctly stays suspended for a different reason: "
                "declared in the catalogue only after this bridge booted (boot-level derivation)"
            )
        elif doc and doc.get("suspended") and doc.get("reason") == "no_primary_key":
            zb.bad(f"'{SCRATCH}' is still suspended for no_primary_key after gaining one: the fix did not take")
            failed += 1
        else:
            zb.bad(
                f"'{SCRATCH}' is not suspended for no_cdc_subject as expected (KV said "
                f"{str(doc)[:80]}) — an undeclared table's CDC subject still has nowhere "
                "to reach, and should be refused for it"
            )
            failed += 1

        # ══ 4. the table disappears ═════════════════════════════════════════════
        print(f"\n4. DROP TABLE {SCRATCH}")
        zb.psql(f"DROP TABLE public.{SCRATCH}", quiet=True)
        await asyncio.sleep(SETTLE)

        doc = kv_get(SCRATCH)
        if doc and doc.get("dropped"):
            zb.ok("a drop tombstone reached the KV — clients can drop their local copy")
        else:
            zb.bad(
                f"no drop tombstone for '{SCRATCH}' (KV said {str(doc)[:60]}): clients keep "
                "a table that no longer exists upstream"
            )
            failed += 1

        after_refused = metric("bridge_refused_tables")
        if after_refused is not None and after_refused <= before_refused:
            zb.ok(f"the refusal registry is back to its baseline ({after_refused:.0f})")
        else:
            zb.bad(
                f"bridge_refused_tables is {after_refused} vs {before_refused} at the start: "
                "a dropped table is still being announced as refused"
            )
            failed += 1

    finally:
        # Every schema this touched, restored — including on an assertion failure, or the
        # next scenario in the suite inherits a half-migrated table.
        zb.psql(f"ALTER TABLE public.{TABLE} DROP COLUMN IF EXISTS {PROBE}", quiet=True)
        zb.psql(f"DROP TABLE IF EXISTS public.{SCRATCH}", quiet=True)
        zb.psql(
            "DELETE FROM public.zebridge_catalogue WHERE public_reason = 'invalidate.py fixture'",
            quiet=True,
        )
        t1.cancel()
        t2.cancel()
        await nc.close()

    return 1 if failed else 0


zb.run(main)

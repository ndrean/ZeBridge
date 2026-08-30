#!/usr/bin/env python3
"""A table born in front of the system — zero-restart onboarding, end to end.

The live-birth exercise (2026-08-25), recorded, now in the catalogue era with a LIVE
catalogue: a table that did not exist when the bridge booted is created and
`zebridge_enable`d mid-flight, and everything cascades with ZERO restarts. The
catalogue row `zebridge_enable` writes travels the WAL like any other row, and the
bridge reloads at its commit (NOTES §10bj): CDC_PUBLIC's subject filter is
reconciled, the newborn's `no_cdc_subject` refusal — the refusal every undeclared
table gets — is lifted, and its schema is published, all from inside the one
migration. Nothing is pre-declared; the probe boots knowing nothing of the fixture.

Checks:

  1. `zebridge_enable` on the newborn reports the full cascade in one migration:
     grants, guards, width guard, catalogue, publication LAST, and `T3 bridge:LIVE`
     — the function's own statement that no restart is owed;
  2. live reconciliation: `cdc.zb_livebirth.>` appears in CDC_PUBLIC's subject filter
     after the migration, put there by a bridge that booted without the row;
  3. the schema reaches `$KV.schemas` LIVE and unsuspended — the refusal lifted and
     the DDL pipeline carried a table the boot preflight never saw;
  4. a row written now reaches the wire on the newborn's subject — routing is real;
  5. the chain manifest appears within a cadence, derived (no GENERATION_RULES for
     it anywhere), and every cutoff in it is CANONICAL UTC (`+00`) — the regression
     check for the manifest timezone mix the exercise found;
  6. the width guard born from the migration refuses an oversized psql UPDATE
     atomically (SQLSTATE 23514) — `updated_at` as the version column, so the
     WHOLE onboarding needed no env rules and no restart;
  7. teardown: DROP TABLE publishes the schema tombstone live, same pipeline; and
     deleting the catalogue row is reloaded live too — the fixture's subject leaves
     CDC_PUBLIC with the row, no boot needed to forget it.

⚠️ Owns the only bridge: a production bridge would reload on the same catalogue rows
and race the probe's stream edits. Stop yours first; restart it after.

Usage:  python scripts/scenarios/livebirth.py   (admin ZB_PSQL + probe-bridge env)
"""

import asyncio
import json
import sys
import time

import zb

FIX = "zb_livebirth"
LOG = "/tmp/zb_livebirth_bridge.log"
CDC = zb.TOPOLOGY["subjects"]["cdc_prefix"]
CDC_PUBLIC = zb.TOPOLOGY["cdc_streams"]["public"]
OPEN = zb.TOPOLOGY["open_tenant"]
REASON = "livebirth scenario fixture — declared while the probe runs"


def cdc_public_subjects() -> list[str]:
    info = zb.nats_cli("stream", "info", CDC_PUBLIC, "--json")
    return json.loads(info.stdout)["config"]["subjects"] if info.returncode == 0 else []


async def wait_for(pred, seconds=30, step=1.0):
    """⚠️ async, and it must stay so: a `time.sleep` here blocks the event loop, and the
    NATS subscription that fills `cdc_seen` never gets to run — the rows were routed
    and the check still said "unrouted" (measured 2026-08-29)."""
    for _ in range(int(seconds / step)):
        if pred():
            return True
        await asyncio.sleep(step)
    return False


async def main():
    failed = 0

    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")

    zb.psql(f"DROP TABLE IF EXISTS public.{FIX}", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{FIX}'", quiet=True)

    nc = await zb.connect()
    cdc_seen: list[str] = []
    cdc_sub = await nc.subscribe(f"{CDC}.{FIX}.>")

    async def watch():
        async for m in cdc_sub.messages:
            cdc_seen.append(m.subject)

    watcher = asyncio.create_task(watch())

    try:
        # GENERATIONS_ENABLED, explicitly: the probe inherits the runner's shell, not
        # .env.bridge — without the master switch (and with no GENERATION_RULES, since
        # derivation is the thing under test) the producer never starts. Measured.
        with zb.Bridge(LOG,
                       GENERATIONS_ENABLED="1",
                       GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("probe bridge never started its producer")
                print(bridge.text()[-1500:])
                return 1

            # The premise: the probe booted knowing nothing of the fixture.
            if f"{CDC}.{FIX}.>" in cdc_public_subjects():
                zb.bad(f"{CDC}.{FIX}.> already in {CDC_PUBLIC} at boot — a stale declaration; clean up first")
                return 1

            # ── 1. the birth: one migration, the full cascade ──────────────────
            out = zb.psql(f"""
                CREATE TABLE public.{FIX} (uid uuid PRIMARY KEY, txt text, updated_at timestamptz NOT NULL);
                SELECT step || ':' || status FROM zebridge_enable('public.{FIX}',
                    writable => true, version_col => 'updated_at',
                    -- the fixture has no tombstone column ON PURPOSE (it tests the birth
                    -- cascade, not delete semantics) — the gate added 2026-08-27 refuses
                    -- writable-without-tombstone unless the acceptance is explicit:
                    allow_physical_deletes => true,
                    public_reason => '{REASON}',
                    -- named explicitly: zebridge_enable has no default publication
                    -- (NOTES §10ad), and this scenario runs on the same feed the
                    -- probe bridge is booted against.
                    publication => '{zb.publication()}', dry_run => false);
            """)
            # splitlines, NOT split(): 'width guard:done' contains a space and whitespace
            # splitting can never match it — the first run failed on exactly that.
            got = {}
            for l in out.splitlines():
                step, _, status = l.strip().rpartition(":")
                if step:
                    got[step] = status
            # A step is satisfied when it was done now OR already held: a re-run on a
            # half-cleaned fixture reports 'already'/'skipped' and that is not a failure.
            fine = {"done", "ok", "already", "skipped"}
            need = ["grants", "guards", "width guard", "catalogue", "publication"]
            short = [s for s in need if got.get(s) not in fine]
            if not short and got.get("T3 bridge") == "LIVE":
                zb.ok("one migration, full cascade: grants, guards, width guard, catalogue, "
                      "publication — and T3 bridge:LIVE, no restart owed")
            else:
                zb.bad(f"cascade incomplete: {short or ''} T3={got.get('T3 bridge')!r} (got {got})")
                failed += 1

            # ── 2. live reconciliation of the stream filter ────────────────────
            if await wait_for(lambda: f"{CDC}.{FIX}.>" in cdc_public_subjects(), 20):
                zb.ok(f"{CDC}.{FIX}.> bound in {CDC_PUBLIC} by a bridge that booted without the row — reloaded live")
            else:
                zb.bad(f"{CDC}.{FIX}.> not in {CDC_PUBLIC} after the migration (bound: {cdc_public_subjects()})")
                failed += 1

            # ── 3. schema reaches clients LIVE, unsuspended ────────────────────
            def live_schema():
                raw = zb.kv_get("schemas", FIX)
                try:
                    doc = json.loads(raw) if raw else None
                except json.JSONDecodeError:
                    return None
                return doc if doc and "pg" in doc and not doc.get("suspended") else None

            if await wait_for(lambda: live_schema() is not None, 30):
                zb.ok("schema published live and unsuspended: the refusal lifted, the DDL pipeline "
                      "carried a table the boot never saw")
            else:
                raw = zb.kv_get("schemas", FIX)
                zb.bad(f"no live $KV.schemas key for the newborn within 30s (KV said {raw[:100]!r})")
                failed += 1

            # ── 4. rows reach the wire — routing is real ───────────────────────
            zb.psql(f"""INSERT INTO public.{FIX} (uid, txt, updated_at) VALUES
                        (gen_random_uuid(), 'born live', now()),
                        (gen_random_uuid(), 'second row', now())""")
            if await wait_for(lambda: any(s.startswith(f"{CDC}.{FIX}.") for s in cdc_seen), 15):
                zb.ok(f"rows written after the birth arrived on {CDC}.{FIX}.* — routed, live")
            else:
                zb.bad(f"no CDC for '{FIX}' — declared, keyed, published, yet unrouted")
                failed += 1

            # ── 5. derived chain, canonical UTC bounds ─────────────────────────
            man = None
            for _ in range(30):
                r = zb.nats_cli("kv", "get", zb.TOPOLOGY["generations"]["kv"], f"{OPEN}.{FIX}", "--raw")
                if r.returncode == 0 and r.stdout.strip():
                    man = json.loads(r.stdout.strip().splitlines()[0])
                    break
                await asyncio.sleep(1)
            if man is None:
                zb.bad("no chain manifest within 30s — derivation did not pick up the newborn")
                failed += 1
            else:
                cutoffs = [man["cutoff_version"], man["full"]["cutoff"]] + \
                          [c for d in man.get("deltas", []) for c in (d["prev_cutoff"], d["cutoff"])]
                if all(c.endswith("+00") for c in cutoffs):
                    zb.ok(f"chain derived (g{man['gen']}), every cutoff canonical UTC — the manifest-timezone regression stays dead")
                else:
                    zb.bad(f"non-canonical cutoff in manifest: {cutoffs}")
                    failed += 1

            # ── 6. the migration-born width guard, psql door ───────────────────
            # Per (table, slot) since 2026-08-26 — the bridge registers its own
            # BASE_BUF at boot; MIN because a row must fit the narrowest carrier.
            # zebridge_limits is ONE ROW PER INSTANCE (slot PK) — no tbl column. The
            # budget for a table is what the guard bakes: MIN over the instances
            # whose publication carries it, defaulting 16384.
            budget = int(zb.psql(
                f"SELECT COALESCE((SELECT MIN(l.max_row_bytes) FROM public.zebridge_limits l "
                f"JOIN pg_publication_tables pt ON pt.pubname = l.publication "
                f"WHERE pt.tablename = '{FIX}'), 16384)"
            ).strip())
            zb.psql(f"UPDATE public.{FIX} SET txt = repeat('x', {budget}), updated_at = now() WHERE txt = 'born live'", quiet=True)
            width = zb.psql(f"SELECT length(txt) FROM public.{FIX} WHERE length(txt) >= {budget}").strip()
            if width == "":
                zb.ok("width guard armed by the same migration: oversized psql UPDATE refused atomically")
            else:
                zb.bad(f"oversized row stored: {width} bytes")
                failed += 1

            # ── 7. death is live too ───────────────────────────────────────────
            zb.psql(f"DROP TABLE public.{FIX}")

            def tombstoned():
                raw = zb.kv_get("schemas", FIX)
                return not raw or '"dropped":true' in raw.replace(" ", "")

            if await wait_for(tombstoned, 30):
                zb.ok("DROP TABLE published the tombstone live — same pipeline, both directions")
            else:
                zb.bad("schema key never tombstoned after DROP")
                failed += 1

            # The catalogue row's removal is a catalogue commit like any other: the
            # bridge reloads and CDC_PUBLIC forgets the subject — no boot needed.
            zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{FIX}'", quiet=True)
            if await wait_for(lambda: f"{CDC}.{FIX}.>" not in cdc_public_subjects(), 20):
                zb.ok(f"the catalogue DELETE was reloaded live too — {CDC}.{FIX}.> left {CDC_PUBLIC}")
            else:
                zb.bad(f"{CDC}.{FIX}.> still in {CDC_PUBLIC} after its catalogue row was deleted")
                failed += 1

    finally:
        watcher.cancel()
        await nc.close()
        zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{FIX}'", quiet=True)
        zb.psql(f"DROP TABLE IF EXISTS public.{FIX}", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_generations WHERE tbl = '{FIX}'", quiet=True)
        zb.nats_cli("kv", "del", zb.TOPOLOGY["generations"]["kv"], f"{OPEN}.{FIX}", "-f")

    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


zb.run(main)

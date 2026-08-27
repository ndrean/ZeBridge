#!/usr/bin/env python3
"""A table born in front of the system — zero-restart onboarding, end to end.

The live-birth exercise (2026-08-25), recorded, now in the catalogue era: a table
that did not exist when the bridge booted is created and `zebridge_enable`d
mid-flight, and everything must cascade with ZERO restarts — the DDL pipeline
publishes its schema live, the generation producer derives it on the next tick
(the publication IS the list), and the width guard is armed from the same
migration. The one residue is deliberate and visible: the bridge derives the
public set and CDC_PUBLIC's subject filter from `zebridge_catalogue` at BOOT, so
the fixture's catalogue row is written BEFORE the probe bridge starts — one
INSERT, no temp topology, no `nats stream edit` by hand (both of which this
scenario needed before the bridge learned to reconcile its own streams).

Checks:

  0. boot reconciliation: the probe bridge itself put `cdc.zb_livebirth.>` into
     CDC_PUBLIC's subject filter, from the pre-declared catalogue row;
  1. `zebridge_enable` on the newborn reports the full cascade in one migration:
     grants, guards, width guard, catalogue, publication LAST;
  2. the schema reaches `$KV.schemas` LIVE — no bridge restart, the DDL pipeline
     carries a table the boot preflight never saw;
  3. the chain manifest appears within a cadence, derived (no GENERATION_RULES for
     it anywhere), and every cutoff in it is CANONICAL UTC (`+00`) — the regression
     check for the manifest timezone mix the exercise found;
  4. the width guard born from the migration refuses an oversized psql UPDATE
     atomically (SQLSTATE 23514) — `updated_at` as the version column, so the
     WHOLE onboarding needed no env rules and no restart;
  5. teardown: DROP TABLE publishes the schema tombstone live, same pipeline.

⚠️ Owns the only bridge (its probe reconciles CDC_PUBLIC to the catalogue as IT
sees it; a production bridge booted without the fixture row would refuse the
fixture as not-routable). Stop yours first; restart it after — its own boot
reconciliation removes the fixture subject again.

Usage:  python scripts/scenarios/livebirth.py   (admin ZB_PSQL + probe-bridge env)
"""

import json
import subprocess
import sys
import time

import zb

FIX = "zb_livebirth"
LOG = "/tmp/zb_livebirth_bridge.log"


def main_sync():
    failed = 0

    running = subprocess.run(["pgrep", "-f", "zig-out/bin/bridge"], capture_output=True, text=True)
    if running.stdout.strip():
        sys.exit("another bridge is already running — this scenario owns the only bridge")

    zb.psql(f"DROP TABLE IF EXISTS public.{FIX}", quiet=True)

    # The pre-declaration: one catalogue row, BEFORE the probe boots. This is what
    # the temp-topology + stream-edit machinery used to fake — the bridge now reads
    # the same fact from the table and edits the stream itself.
    zb.psql(
        "INSERT INTO public.zebridge_catalogue (tbl, public_reason) VALUES "
        f"('{FIX}', 'livebirth scenario fixture — pre-declared before probe boot') "
        "ON CONFLICT (tbl) DO NOTHING", quiet=True)

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

            # ── 0. the probe's own boot put the fixture into CDC_PUBLIC ────────
            info = zb.nats_cli("stream", "info", "CDC_PUBLIC", "--json")
            bound = json.loads(info.stdout)["config"]["subjects"] if info.returncode == 0 else []
            if f"cdc.{FIX}.>" in bound:
                zb.ok("boot reconciliation bound cdc.zb_livebirth.> in CDC_PUBLIC — no stream edit by hand")
            else:
                zb.bad(f"cdc.{FIX}.> not in CDC_PUBLIC after boot (bound: {bound})")
                failed += 1

            # ── 1. the birth: one migration, the full cascade ──────────────────
            out = zb.psql(f"""
                CREATE TABLE public.{FIX} (uid uuid PRIMARY KEY, txt text, updated_at timestamptz NOT NULL);
                SELECT step || ':' || status FROM zebridge_enable('public.{FIX}',
                    writable => true, version_col => 'updated_at',
                    -- the fixture has no tombstone column ON PURPOSE (it tests the birth
                    -- cascade, not delete semantics) — the gate added 2026-08-27 refuses
                    -- writable-without-tombstone unless the acceptance is explicit:
                    allow_physical_deletes => true,
                    public_reason => 'livebirth scenario fixture — pre-declared before probe boot', dry_run => false);
            """)
            need = {"grants:done", "guards:done", "width guard:done", "catalogue:done", "publication:done"}
            # splitlines, NOT split(): 'width guard:done' contains a space and whitespace
            # splitting can never match it — the first run failed on exactly that.
            got = set(l.strip() for l in out.splitlines())
            if need <= got:
                zb.ok("one migration, full cascade: grants, guards, width guard, catalogue, publication")
            else:
                zb.bad(f"cascade incomplete: {sorted(need - got)}")
                failed += 1

            # ── 2. schema reaches clients LIVE (no restart) ────────────────────
            key_seen = False
            for _ in range(30):
                r = zb.nats_cli("kv", "get", zb.TOPOLOGY["kv"]["schemas"], FIX, "--raw")
                if r.returncode == 0 and '"sqlite"' in r.stdout:
                    key_seen = True
                    break
                time.sleep(1)
            if key_seen:
                zb.ok("schema published live: the DDL pipeline carried a table the boot never saw")
            else:
                zb.bad("no $KV.schemas key for the newborn within 30s")
                failed += 1

            # rows, so the chain has content
            zb.psql(f"""INSERT INTO public.{FIX} (uid, txt, updated_at) VALUES
                        (gen_random_uuid(), 'born live', now()),
                        (gen_random_uuid(), 'second row', now())""")

            # ── 3. derived chain, canonical UTC bounds ─────────────────────────
            man = None
            for _ in range(30):
                r = zb.nats_cli("kv", "get", zb.TOPOLOGY["generations"]["kv"], f"_default.{FIX}", "--raw")
                if r.returncode == 0 and r.stdout.strip():
                    man = json.loads(r.stdout.strip().splitlines()[0])
                    break
                time.sleep(1)
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

            # ── 4. the migration-born width guard, psql door ───────────────────
            # Per (table, slot) since 2026-08-26 — the bridge registers its own
            # BASE_BUF at boot; MIN because a row must fit the narrowest carrier.
            # zebridge_limits collapsed to ONE ROW PER INSTANCE (slot PK) on
            # 2026-08-26 — there is no tbl column. The budget for a table is what
            # the guard bakes: MIN over the instances whose publication carries
            # it, defaulting 16384.
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

            # ── 5. death is live too ───────────────────────────────────────────
            zb.psql(f"DROP TABLE public.{FIX}")
            gone = False
            for _ in range(30):
                r = zb.nats_cli("kv", "get", zb.TOPOLOGY["kv"]["schemas"], FIX, "--raw")
                if r.returncode != 0 or '"dropped":true' in r.stdout.replace(" ", ""):
                    gone = True
                    break
                time.sleep(1)
            if gone:
                zb.ok("DROP TABLE published the tombstone live — same pipeline, both directions")
            else:
                zb.bad("schema key never tombstoned after DROP")
                failed += 1

    finally:
        # The catalogue row goes; CDC_PUBLIC keeps the fixture subject until the NEXT
        # bridge boot reconciles it away — restart your bridge after this scenario.
        zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{FIX}'", quiet=True)
        zb.psql(f"DROP TABLE IF EXISTS public.{FIX}", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_generations WHERE tbl = '{FIX}'", quiet=True)
        zb.nats_cli("kv", "del", zb.TOPOLOGY["generations"]["kv"], f"_default.{FIX}", "-f")

    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


async def main():
    return main_sync()


zb.run(main)

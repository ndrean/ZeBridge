#!/usr/bin/env python3
"""The legacy oversized row — every guard that fires AFTER prevention failed.

`widthguard.py` proves the trigger stops NEW oversized rows at both doors. This
scenario is about the rows the trigger never saw: data loaded before the guard
existed (SECURITY.md §1.8 calls them bait). Three checks in the matrix had no test
until this file — the producer's detector, the decode-time quarantine, and the boot
preflight — plus the de-quarantine recipe, proven mechanical.

The story, in acts:

  1. **plant the bait** — width trigger disabled, one 20 KB row inserted, trigger
     re-armed. Exactly what a pre-guard deployment looks like.
  2. **the producer detects** (fix 1, the measureWidestRow retirement survivor): the
     first generation build warns loudly — "widest row ... at or over the ... CDC
     event buffer" — chains carry the row, CDC is on notice.
  3. **a touch quarantines** (the containment of last resort): a small legacy-path
     UPDATE reaches CDC decode, the row does not fit the slot, and the table is
     SUSPENDED — log line and `$KV.schemas` key (`"suspended":true`) both asserted.
  4. **boot re-proves the refusal**: a fresh bridge's preflight flags the stored row
     ("a row already stored is N bytes...") — quarantine is not a flag someone can
     forget to re-check, it is re-derived from the data every boot.
  5. **de-quarantine is mechanical**: hard-remove the bait (user triggers disabled —
     the soft-delete guard would otherwise just tombstone it, still stored, still
     too wide), boot again: preflight passes ("Stored rows and column defaults
     fit"), the boot schema republish overwrites the suspended key, clients thaw.

⚠️ **This scenario owns the only bridge** (wide.py's rule): it starts three probe
bridges in sequence, and a concurrently running production bridge would decode the
same WAL, quarantine `test_types` for real clients, and stay quarantined until an
operator restart. Stop the main bridge first; restart it after.

Usage:  python scripts/scenarios/legacybait.py   (admin ZB_PSQL + probe-bridge env)
"""

import subprocess
import sys
import time
import uuid

import zb

LOG_A = "/tmp/zb_legacybait_a.log"
LOG_B = "/tmp/zb_legacybait_b.log"
LOG_C = "/tmp/zb_legacybait_c.log"


def main_sync():
    failed = 0

    running = subprocess.run(["pgrep", "-f", "zig-out/bin/bridge"], capture_output=True, text=True)
    if running.stdout.strip():
        sys.exit("another bridge is already running — this scenario owns the only bridge "
                 "(it quarantines test_types on purpose; a live bridge would carry that "
                 "to real clients and keep it until an operator restart)")

    uid = str(uuid.uuid4())
    try:
        # ── 1. plant the bait ──────────────────────────────────────────────────
        zb.psql("ALTER TABLE public.test_types DISABLE TRIGGER zebridge_width_guard")
        zb.psql(f"""INSERT INTO public.test_types (uid, some_text, tenant_id, inserted_at, updated_at)
                    VALUES ('{uid}', repeat('x', 20000), 'acme', now(), now())""")
        zb.psql("ALTER TABLE public.test_types ENABLE TRIGGER zebridge_width_guard")
        stored = zb.psql(f"SELECT length(some_text) FROM public.test_types WHERE uid = '{uid}'").strip()
        if stored == "20000":
            zb.ok("bait planted: a 20 KB row is stored, the guard re-armed behind it")
        else:
            zb.bad(f"bait not stored: {stored!r}")
            return 1

        # ── 2. the producer detects ────────────────────────────────────────────
        with zb.Bridge(LOG_A,
                       GENERATION_RULES="test_types:acme",
                       GENERATION_CADENCE_SECONDS="5") as a:
            if a.wait_for_log("widest row", timeout=40):
                zb.ok("producer detector: first build warned — widest row at or over the CDC event buffer")
            else:
                zb.bad("producer never warned about the legacy row")
                failed += 1

            # ── 3. a touch quarantines ─────────────────────────────────────────
            # The touch must take the legacy path: the re-armed trigger refuses ANY
            # update to this row (it re-measures the whole row), which is itself
            # correct — so disable, touch, re-arm, exactly like an old backend would
            # have written before the guard existed.
            zb.psql("ALTER TABLE public.test_types DISABLE TRIGGER zebridge_width_guard")
            zb.psql(f"UPDATE public.test_types SET age = 99, updated_at = now() WHERE uid = '{uid}'")
            zb.psql("ALTER TABLE public.test_types ENABLE TRIGGER zebridge_width_guard")
            if a.wait_for_log("SUSPENDING 'test_types'", timeout=30):
                zb.ok("decode-time quarantine: the touch suspended the table (containment of last resort)")
            else:
                zb.bad("no suspension after touching the legacy row")
                failed += 1
            time.sleep(2)  # let the suspension reach $KV.schemas

        kv = zb.nats_cli("kv", "get", zb.TOPOLOGY["kv"]["schemas"], "test_types", "--raw")
        if '"suspended":true' in kv.stdout.replace(" ", ""):
            zb.ok('clients are told: $KV.schemas carries "suspended": true')
        else:
            zb.bad(f"schema key not suspended: {kv.stdout[:120]!r}")
            failed += 1

        # ── 4. boot re-proves the refusal ──────────────────────────────────────
        with zb.Bridge(LOG_B) as b:
            if b.wait_for_log("a row already stored is", timeout=40):
                zb.ok("boot preflight: quarantine re-derived from the data, not remembered from a flag")
            else:
                zb.bad("fresh boot did not flag the stored oversized row")
                failed += 1

        # ── 5. de-quarantine is mechanical ─────────────────────────────────────
        # Hard-remove: the soft-delete guard would tombstone the row — still stored,
        # still too wide — so every user trigger steps aside for the repair.
        zb.psql("ALTER TABLE public.test_types DISABLE TRIGGER USER")
        zb.psql(f"DELETE FROM public.test_types WHERE uid = '{uid}'")
        zb.psql("ALTER TABLE public.test_types ENABLE TRIGGER USER")

        with zb.Bridge(LOG_C) as cbr:
            if cbr.wait_for_log("Stored rows and column defaults fit", timeout=40):
                zb.ok("repair + reboot: preflight passes — the same check that refused now readmits")
            else:
                zb.bad("preflight did not pass after the bait was removed")
                failed += 1
            time.sleep(2)  # let the boot schema republish land

        kv = zb.nats_cli("kv", "get", zb.TOPOLOGY["kv"]["schemas"], "test_types", "--raw")
        if '"suspended":true' not in kv.stdout.replace(" ", ""):
            zb.ok("the healthy boot schema overwrote the suspended key — clients thaw")
        else:
            zb.bad("schema key still suspended after recovery boot")
            failed += 1

    finally:
        zb.psql("ALTER TABLE public.test_types ENABLE TRIGGER USER", quiet=True)
        zb.psql("ALTER TABLE public.test_types DISABLE TRIGGER USER", quiet=True)
        zb.psql(f"DELETE FROM public.test_types WHERE uid = '{uid}'", quiet=True)
        zb.psql("ALTER TABLE public.test_types ENABLE TRIGGER USER", quiet=True)

    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


async def main():
    return main_sync()


zb.run(main)

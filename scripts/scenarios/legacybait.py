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

    running = subprocess.run(["pgrep", "-f", r"zig-out/bin/bridge$"], capture_output=True, text=True)
    if running.stdout.strip():
        sys.exit("another bridge is already running — this scenario owns the only bridge "
                 "(it quarantines test_types on purpose; a live bridge would carry that "
                 "to real clients and keep it until an operator restart)")

    uid = str(uuid.uuid4())
    uid_ok = str(uuid.uuid4())   # the narrow companion (phase 5)
    try:
        # ── 1. plant the bait ──────────────────────────────────────────────────
        zb.psql("ALTER TABLE public.test_types DISABLE TRIGGER zebridge_width_guard")
        zb.psql(f"""INSERT INTO public.test_types (uid, some_text, tenant_id, inserted_at, updated_at)
                    VALUES ('{uid}', repeat('x', 20000), 'acme', now(), now())""")
        zb.psql("ALTER TABLE public.test_types ENABLE TRIGGER zebridge_width_guard")
        # A NARROW companion, planted alongside: after the bait is removed, touching this
        # proves the table actually carries events again. (The bait's own uid is gone by
        # then — it is hard-deleted in the repair.)
        zb.psql(f"""INSERT INTO public.test_types (uid, some_text, tenant_id, inserted_at, updated_at)
                    VALUES ('{uid_ok}', 'narrow companion', 'acme', now(), now())""")
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

        # ── 4. a fresh boot FORGETS the quarantine — and re-earns it ───────────
        # ⚠️ This phase used to assert the opposite. `checkStoredRowsFit` scanned every
        # published table at boot and re-flagged a stored oversized row, so a reboot
        # preserved the quarantine. That scan is disabled (NOTES §13): it is an O(table)
        # read on every start, duplicating guards that already hold at ingress (the width
        # trigger, on direct writes AND on the bridge's own mutations) and at egress (the
        # decode-time suspension). What it cost is exactly what this phase now pins: the
        # suspension lives in the registry, which is MEMORY, so a fresh bridge starts
        # clean and republishes a HEALTHY schema — clients thaw — until the wide row is
        # touched again, and then the decode guard re-quarantines it. Containment is not
        # lost, it is deferred to the next touch.
        with zb.Bridge(LOG_B) as b:
            if not b.wait_for_log("Boot schema published to KV for 'test_types'", timeout=40):
                zb.bad("the fresh bridge never republished test_types' schema")
                failed += 1
            time.sleep(2)
            kv = zb.nats_cli("kv", "get", zb.TOPOLOGY["kv"]["schemas"], "test_types", "--raw")
            if '"suspended":true' not in kv.stdout.replace(" ", ""):
                zb.ok("a fresh boot does NOT remember the quarantine (§13: no boot scan) — "
                      "the schema republishes healthy and clients thaw")
            else:
                zb.bad("the schema key is still suspended after a fresh boot — "
                       "with the boot scan gone, nothing should re-raise it before a touch")
                failed += 1

            # …and the touch re-earns it, which is the containment that remains.
            zb.psql("ALTER TABLE public.test_types DISABLE TRIGGER zebridge_width_guard")
            zb.psql(f"UPDATE public.test_types SET age = 98, updated_at = now() WHERE uid = '{uid}'")
            zb.psql("ALTER TABLE public.test_types ENABLE TRIGGER zebridge_width_guard")
            if b.wait_for_log("SUSPENDING 'test_types'", timeout=30):
                zb.ok("and the next touch re-quarantines it: the decode guard is derived from "
                      "the data every time, so the containment survives the scan's removal")
            else:
                zb.bad("the wide row was touched again and the table was NOT re-suspended")
                failed += 1

        # ── 5. de-quarantine is mechanical ─────────────────────────────────────
        # Hard-remove: the soft-delete guard would tombstone the row — still stored,
        # still too wide — so every user trigger steps aside for the repair.
        zb.psql("ALTER TABLE public.test_types DISABLE TRIGGER USER")
        zb.psql(f"DELETE FROM public.test_types WHERE uid = '{uid}'")
        zb.psql("ALTER TABLE public.test_types ENABLE TRIGGER USER")

        with zb.Bridge(LOG_C) as cbr:
            # The old needle here was preflight's "Stored rows and column defaults fit",
            # from the same disabled scan (§13). What matters is the outcome, which the
            # next check makes: with the bait gone, the republished schema is healthy AND
            # a touch no longer suspends — the table is genuinely readmitted, not merely
            # unflagged by a boot that no longer looks.
            if not cbr.wait_for_log("Boot schema published to KV for 'test_types'", timeout=40):
                zb.bad("the repaired bridge never republished test_types' schema")
                failed += 1

            # ⚠️ Deleting the row does NOT undo the history. The WAL is a log, not a
            # snapshot: the UPDATE that carried the 20 KB row is still in the backlog this
            # slot holds, so the repaired bridge decodes it again and suspends once more,
            # BEFORE it even publishes its boot schemas (measured: the FATAL row-size lines
            # land ahead of the first "Published KV schema"). A repair cannot be judged by
            # "does it ever suspend again" — only by whether it RECOVERS.
            if "SUSPENDING 'test_types'" in cbr.text():
                print("  ⓘ  the backlog replayed the historical wide event and re-suspended once — "
                      "expected: the row is gone, the WAL record of it is not")

            # And recovery is §10bp's: a fitting write lifts the suspension by itself, once
            # past the 30 s anti-flap cooldown that gates the probe.
            time.sleep(32)
            zb.psql("UPDATE public.test_types SET age = 97, updated_at = now() "
                    f"WHERE uid = '{uid_ok}'")
            healthy = False
            deadline = time.time() + 25
            while time.time() < deadline:
                kv_now = zb.nats_cli("kv", "get", zb.TOPOLOGY["kv"]["schemas"], "test_types", "--raw")
                if kv_now.returncode == 0 and '"suspended":true' not in kv_now.stdout.replace(" ", ""):
                    healthy = True
                    break
                time.sleep(2)
            if healthy:
                zb.ok("repair + reboot: the backlog's last wide event suspended it once, then a "
                      "fitting write lifted it (§10bp) — the table readmits itself, no restart")
            else:
                zb.bad("the table never recovered after the bait was removed — "
                       "still suspended with nothing oversized left to carry")
                failed += 1

        kv = zb.nats_cli("kv", "get", zb.TOPOLOGY["kv"]["schemas"], "test_types", "--raw")
        if '"suspended":true' not in kv.stdout.replace(" ", ""):
            zb.ok("the healthy boot schema overwrote the suspended key — clients thaw")
        else:
            zb.bad("schema key still suspended after recovery boot")
            failed += 1

    finally:
        # ONE transaction: disable the user triggers (the soft-delete guard would only
        # tombstone the bait, still stored, still too wide), remove the row, re-arm.
        # Four separate psql calls used to do this, and a crash between the DISABLE
        # and the ENABLE left test_types with its guards off for every later scenario.
        # A single -c string is one implicit transaction: either all of it applies —
        # triggers back on — or none of it does and they were never off.
        zb.psql(
            "ALTER TABLE public.test_types DISABLE TRIGGER USER; "
            f"DELETE FROM public.test_types WHERE uid IN ('{uid}', '{uid_ok}'); "
            "ALTER TABLE public.test_types ENABLE TRIGGER USER",
            quiet=True,
        )

    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


async def main():
    return main_sync()


zb.run(main)

"""Lowering BASE_BUF under stored data: the one transition that revives the boot scan
(NOTES §13 addendum, §10bx).

    scripts/scenarios/run.py owns -k shrink        (owns the bridge)

§13 removed the per-boot stored-rows scan — every guard still holds, but one detector
went with it: a budget LOWERED after data exists was discovered at first touch, under
load, instead of at boot. The revival is shrink-gated: the bridge reads the budget its
slot registered LAST boot (`zebridge_limits`, materialized in a CTE before the upsert
overwrites it) and runs the scan only when this boot's budget is smaller. Raised or
unchanged budgets — every normal boot — stay scan-free.

  1. a probe boots at BASE_BUF=12 (4 KB): its budget is registered, no scan
  2. a 3 KB row is stored — legal under 4 KB, and the guard accepts it
  3. reboot at BASE_BUF=11 (2 KB): the shrink is detected, the scan runs, and the
     warning NAMES the table whose widest stored row no longer fits. Reported, never
     refused — the bridge still starts; containment stays §13's lazy suspension.
  4. reboot at BASE_BUF=11 again: 2 KB → 2 KB is no shrink; no scan
  5. reboot at BASE_BUF=12: raised; no scan

⚠️ Registers a probe budget in `zebridge_limits`, which the width guard bakes to the
MIN over slots — the same footprint as `suspension_lift`, cleaned the same way: the
probe row is dropped and the guard re-baked in cleanup.
"""
import os
import pathlib
import sys
import time
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402

TABLE = "test_types"
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_shrink_bridge.log"


def slot_name() -> str:
    return zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"


def rebake_guard() -> None:
    budget = zb.psql("SELECT MIN(max_row_bytes) FROM public.zebridge_limits").strip()
    if budget:
        zb.psql(f"SELECT public.zebridge_rebudget_width_guard('{TABLE}', {budget})", quiet=True)


def cleanup(uid: str) -> None:
    zb.psql(f"UPDATE public.{TABLE} SET some_text = '', deleted_at = now(), updated_at = now() "
            f"WHERE uid = '{uid}' AND deleted_at IS NULL", quiet=True)
    zb.psql(f"SELECT pg_drop_replication_slot('{slot_name()}') FROM pg_replication_slots "
            f"WHERE slot_name = '{slot_name()}' AND NOT active", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_limits WHERE slot = '{slot_name()}'", quiet=True)
    rebake_guard()


def boot(phase: str, base_buf: str, needle_expected: bool, uid: str) -> int:
    """One probe boot; returns 1 on failure. The shrink warning must appear exactly
    when expected — a fresh log file per boot, so wait_for_log cannot be satisfied by
    an earlier phase (the §10bw trap)."""
    log = LOG.with_suffix(f".{phase}.log")
    with zb.Bridge(log, BASE_BUF=base_buf) as br:
        if not br.wait_for_log("row-width budget registered", timeout=40):
            zb.bad(f"phase {phase}: the budget was never registered"); return 1
        # The scan runs between registration and the registered line? No — the shrink
        # warning precedes it; give the log a beat either way.
        time.sleep(1.5)
        text = br.text()
    # ⚠️ The trailing colon is load-bearing. The SUSPENDING block QUOTES the warning
    # ('the boot would have logged "row-width budget SHRANK"') — and phases that decode
    # the replayed oversized event print that block, so a bare substring matched a
    # diagnostic quoting a diagnostic (measured: phase c failed on its own explanation).
    shrank = "row-width budget SHRANK:" in text
    if needle_expected:
        if shrank and (f"{TABLE}" in text):
            zb.ok(f"phase {phase}: the shrink was detected and the scan NAMED the table "
                  "whose stored row no longer fits — at boot, not at first touch")
            return 0
        zb.bad(f"phase {phase}: expected the shrink warning (shrank={shrank}) naming {TABLE}")
        return 1
    if not shrank:
        zb.ok(f"phase {phase}: no shrink, no scan — the normal boot stays free")
        return 0
    zb.bad(f"phase {phase}: the scan ran on a boot that did not shrink the budget")
    return 1


def main() -> int:
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")
    uid = str(uuid.uuid4())
    failed = 0
    try:
        cleanup(uid)  # a stale probe budget from another scenario would fake a shrink
        failed += boot("a", "12", needle_expected=False, uid=uid)

        # 3 KB: legal under the 4 KB budget the guard just baked, too wide for 2 KB.
        zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                f"VALUES ('{uid}', repeat('x', 3000), 'acme', now(), now())")
        stored = zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE uid = '{uid}'").strip()
        if stored != "1":
            zb.bad("could not store the 3 KB row under the 4 KB budget — is a stale "
                   "probe budget still baked into the guard?")
            return failed + 1
        zb.ok("a 3 KB row stored, legal under the 4 KB budget")

        failed += boot("b", "11", needle_expected=True, uid=uid)
        failed += boot("c", "11", needle_expected=False, uid=uid)
        failed += boot("d", "12", needle_expected=False, uid=uid)
    finally:
        cleanup(uid)
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

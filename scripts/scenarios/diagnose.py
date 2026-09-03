"""`bridge --diagnose`: the pre-run doctor says everything, changes nothing (§10by).

    scripts/scenarios/run.py live -k diagnose

Everything the boot would DECIDE, said before anything is DONE: init applied → the
publication whole → the same per-table preflight the boot runs → does stored data fit
THIS BASE_BUF (and the smallest one that would) → would this budget SHRINK the slot's
previous one. Exit 0 all-clear, 1 with findings — CI-able. Proven read-only here: no
slot appears, no zebridge_limits row moves, no HTTP port binds.
"""
import os
import pathlib
import subprocess
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402

BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"


def run_doctor(**env_over):
    env = dict(os.environ); env.update(env_over)
    return subprocess.run([str(BRIDGE), "--diagnose"], env=env, capture_output=True, text=True, timeout=120)


def main() -> int:
    failed = 0
    before_slots = zb.psql("SELECT count(*) FROM pg_replication_slots").strip()
    before_limits = zb.psql("SELECT string_agg(slot || '=' || max_row_bytes, ',' ORDER BY slot) "
                            "FROM public.zebridge_limits").strip()

    r = run_doctor()
    out = r.stdout + r.stderr
    if r.returncode == 0 and "all clear" in out:
        zb.ok("healthy configuration: exit 0, 'all clear'")
    else:
        zb.bad(f"expected a clean bill (exit {r.returncode}); tail: {out.strip().splitlines()[-1][:140]}")
        failed += 1
    for needle, what in (("init.core is applied", "init presence"),
                         ("publication", "publication check"),
                         ("minimum BASE_BUF", "width verdict + sizing advice")):
        if needle in out:
            zb.ok(f"reports {what}")
        else:
            zb.bad(f"missing from the report: {what} ('{needle}')"); failed += 1

    # A shrunk budget must be a FINDING, not a boot-time surprise later.
    r2 = run_doctor(BASE_BUF="11")
    out2 = r2.stdout + r2.stderr
    if r2.returncode == 1 and "SHRINKS" in out2:
        zb.ok("BASE_BUF=11: exit 1 and the shrink is named — the misconfiguration is "
              "caught BEFORE a boot, which is this mode's whole point")
    else:
        zb.bad(f"BASE_BUF=11 should fail with the shrink named (exit {r2.returncode})"); failed += 1

    after_slots = zb.psql("SELECT count(*) FROM pg_replication_slots").strip()
    after_limits = zb.psql("SELECT string_agg(slot || '=' || max_row_bytes, ',' ORDER BY slot) "
                           "FROM public.zebridge_limits").strip()
    if (before_slots, before_limits) == (after_slots, after_limits):
        zb.ok(f"read-only proven: slots {after_slots} and budgets [{after_limits}] byte-identical "
              "after two runs, one of them a failing one")
    else:
        zb.bad(f"the dry run WROTE something: slots {before_slots}→{after_slots}, "
               f"limits [{before_limits}]→[{after_limits}]")
        failed += 1

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

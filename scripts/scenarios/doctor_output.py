#!/usr/bin/env python3
"""`bridge --diagnose` on a dropped table: a note, never an error.

A catalogue row outlives its table on purpose (MIGRATIONS.md: a table re-created under the
name syncs again with the same rules). The doctor and the boot's preflight must say so
calmly, once, and not as a fault:

  A  the dropped table is listed in the doctor's one "Dropped, its sync rules kept" block,
     at info level;
  B  no warning or error line names it (the old doctor printed an error per table, then a
     missing schema and a missing chain per tenant);
  C  the preflight counts it in its one "catalogue row(s) without a table" line, and
     prints no "Tenant check skipped";
  D  the open tenant (_default) is not reported out of the sweeper's reach: the write
     policy admits it to every principal, the sweeper included.

Needs the dev database and the bridge's environment (.env.bridge); the bridge itself may
be running or not.

Usage:  scripts/scenarios/.venv/bin/python scripts/scenarios/doctor_output.py
"""
import os, re, subprocess, sys

import zb

TABLE = "zz_doctor_dropped"
BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
PUB = os.environ.get("BRIDGE_CDC_PUBLICATION", "my_pub")


def main() -> int:
    failed = 0

    def check(good, label):
        nonlocal failed
        (zb.ok if good else zb.bad)(label)
        failed += 0 if good else 1

    zb.psql(f"DROP TABLE IF EXISTS public.{TABLE}", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{TABLE}'", quiet=True)
    try:
        zb.psql(f"CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text, updated_at timestamptz NOT NULL DEFAULT now())")
        zb.psql(f"SELECT step FROM public.zebridge_enable('public.{TABLE}'::regclass, public_reason => 'doctor scenario', "
                f"publication => '{PUB}', dry_run => false)", quiet=True)
        zb.psql(f"DROP TABLE public.{TABLE}")
        kept = zb.psql(f"SELECT count(*) FROM public.zebridge_catalogue WHERE tbl = '{TABLE}'", quiet=True).strip()
        if kept != "1":
            zb.bad(f"setup: the catalogue row is not there after the drop ({kept})")
            return 1

        r = subprocess.run([str(BRIDGE), "--diagnose", "--pub", PUB], capture_output=True, text=True, timeout=120)
        out = r.stdout + r.stderr
        lines = out.splitlines()

        block = [i for i, l in enumerate(lines) if "(diagnose)" in l and "Dropped, its sync rules kept" in l]
        listed = bool(block) and lines[block[0]].startswith("info(") and TABLE in (lines[block[0] + 1] if block[0] + 1 < len(lines) else "")
        check(listed, f"A the dropped table is in the one info block 'Dropped, its sync rules kept'")

        loud = [l for l in lines if TABLE in l and re.match(r"^(warning|error)\(", l)]
        # a block's table list is an indented continuation line: attribute it to its header
        for i, l in enumerate(lines):
            if l.startswith("    ") and TABLE in l and i > 0 and re.match(r"^(warning|error)\(", lines[i - 1]):
                loud.append(lines[i - 1] + " / " + l.strip())
        check(not loud, f"B no warning or error names it ({len(loud)} line(s)){': ' + loud[0][:160] if loud else ''}")

        # D: the open tenant is never "out of the sweeper's reach": the write policy admits
        # it to every principal, the sweeper included, with no mapping row. The audit used
        # to count it, and the doctor warned about a tenant the sweeper does reap.
        # The block: a warning header, then one indented "table: tenant, tenant" line per table.
        open_named = []
        for i, l in enumerate(lines):
            if "sweeper" in l.lower() and re.match(r"^(warning|error)\(", l):
                j = i + 1
                while j < len(lines) and lines[j].startswith("    "):
                    if re.search(r"[:,]\s*_default(,|$)", lines[j].strip()):
                        open_named.append(lines[j].strip())
                    j += 1
        check(not open_named, f"D the open tenant is not reported out of the sweeper's reach{': ' + open_named[0][:120] if open_named else ''}")

        pre = [l for l in lines if "catalogue row(s) without a table" in l]
        skipped = [l for l in lines if "Tenant check skipped" in l]
        check(len(pre) == 1 and pre[0].startswith("info(") and not skipped,
              f"C the preflight: one info line ({len(pre)}), no 'Tenant check skipped' ({len(skipped)})")
    finally:
        zb.psql(f"DROP TABLE IF EXISTS public.{TABLE}", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{TABLE}'", quiet=True)
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

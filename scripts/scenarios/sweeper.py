#!/usr/bin/env python3
"""Tombstone GC — does the sweeper reap past the threshold and only past it?

A soft delete leaves the row with its tombstone set, so a late edit from an offline client
is overruled rather than resurrecting the row. Tombstones must eventually be reaped, and
`GC_THRESHOLD_MS` is therefore **the maximum offline window this deployment supports**: a
client offline longer than that can resurrect a row, because the tombstone that would have
overruled it is gone.

So the sweeper has two ways to be wrong, and only one of them is visible:

  * reaping **too little** — the table grows. Annoying, obvious.
  * reaping **too early** — a client still inside its allowed offline window loses the
    tombstone that protects it. Nothing errors; a deleted row comes back weeks later.

This seeds tombstones either side of the threshold and asserts exactly which survive.

⚠️ It also asserts the sweeper's *sources*: the set of tables comes from `SYNC_RULES`, the
same variable the bridge reads, so the two cannot disagree about which column is the
tombstone. An earlier version read `GC_TABLES` and deleted `WHERE _deleted = true AND
_hlc < $1` — columns no table has — so it matched nothing and the guarantee above was not
enforced at all, silently, for as long as it existed.

Usage:  python scripts/scenarios/sweeper.py [table]

Needs a table with a tombstone column, and `DATABASE_WRITER_URL` set:

    set -a && . ./.env.bridge && set +a
"""

import os
import re
import subprocess
import sys

import zb

TABLE = sys.argv[1] if len(sys.argv) > 1 else "test_types"
TOMBSTONE = "deleted_at"
VERSION = "updated_at"
THRESHOLD_MS = 3_600_000  # 1 hour
MARK = "sweeper-scenario"

SWEEPER = zb.ROOT / "zig-out" / "bin" / "bridge_sweeper"


async def main():
    if not SWEEPER.exists():
        sys.exit(f"{SWEEPER} not built — run `zig build`")
    if not os.environ.get("DATABASE_WRITER_URL"):
        sys.exit("DATABASE_WRITER_URL is not set.\n  set -a && . ./.env.bridge && set +a")

    cols = zb.psql(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name='{TABLE}' AND column_name IN ('{TOMBSTONE}','{VERSION}')"
    ).split()
    if TOMBSTONE not in cols:
        sys.exit(f"'{TABLE}' has no '{TOMBSTONE}' column — nothing to sweep")

    zb.psql(f"DELETE FROM public.{TABLE} WHERE some_text LIKE '{MARK}%'", quiet=True)

    # Ages chosen around the boundary rather than far from it: a sweeper that compares the
    # wrong column, or the right column against the wrong clock, still passes a test whose
    # rows are days apart.
    seeds = [
        ("well past", "3 hours", False),
        ("just past", "61 minutes", False),
        ("just inside", "59 minutes", True),
        ("live row", None, True),
    ]
    # Every NOT NULL column without a DEFAULT has to be supplied, and the set is read from
    # the catalog rather than hardcoded: `test_types` grew a NOT NULL `tenant_id` for the
    # RLS work, and a hardcoded column list silently inserted *nothing* — every row was
    # rejected, the sweep found an empty table, and the scenario reported "reaped TOO
    # EARLY" for rows that had never existed. A seeding failure must not look like a
    # verdict about the thing under test.
    required = [
        line for line in zb.psql(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{TABLE}' AND is_nullable='NO' AND column_default IS NULL "
            f"AND column_name NOT IN ('uid','inserted_at','{VERSION}')"
        ).splitlines() if line
    ]
    # ⚠️ A tenant the SWEEPER is mapped to, not a literal.
    #
    # The sweeper is a principal like any other and is bounded by `zebridge_user_tenants`
    # — it has no blanket rights. Seeding rows under a tenant nobody granted it produced a
    # correct refusal that read as a bug: rows inserted fine (as admin), the sweep found
    # nothing it was allowed to touch, and the scenario reported "reaped too little".
    #
    # That refusal is the design working. To test the *reaping*, the fixture has to sit
    # inside the sweeper's reach — which is also a check that the mapping exists at all.
    sweeper_tenant = zb.psql(
        "SELECT tenant_id FROM zebridge_user_tenants WHERE principal='zb_sweeper' LIMIT 1"
    ).strip()
    if required and not sweeper_tenant:
        sys.exit(
            "zb_sweeper is not mapped to any tenant, so it can reap nothing.\n"
            "  INSERT INTO zebridge_user_tenants (principal, tenant_id) VALUES ('zb_sweeper', '<tenant>');\n"
            "  SELECT * FROM zebridge_audit_sweeper();   -- lists tenants with no mapping"
        )

    extra_cols = "".join(f", {c}" for c in required)
    extra_vals = "".join(f", '{sweeper_tenant}'" for _ in required)

    for label, age, _ in seeds:
        ts = "NULL" if age is None else f"now() - interval '{age}'"
        out = zb.psql(
            f"INSERT INTO public.{TABLE} (uid, some_text, inserted_at, {VERSION}, {TOMBSTONE}{extra_cols}) "
            f"VALUES (gen_random_uuid(), '{MARK}: {label}', now(), now(), {ts}{extra_vals})",
            quiet=True,
        )
        _ = out

    seeded = zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE some_text LIKE '{MARK}%'").strip()
    if seeded != str(len(seeds)):
        sys.exit(
            f"seeding failed: {seeded} of {len(seeds)} rows inserted into '{TABLE}'.\n"
            f"  Required columns detected: {required or '(none)'}\n"
            "  Fix the fixture before reading anything below as a sweeper verdict."
        )

    print(f"seeded 4 rows in '{TABLE}' (threshold {THRESHOLD_MS // 60000} min)\n")

    env = dict(os.environ)
    env["SYNC_RULES"] = f"{TABLE}:{VERSION},{TOMBSTONE}"
    env["GC_THRESHOLD_MS"] = str(THRESHOLD_MS)
    env["GC_INTERVAL_MS"] = "999999999"  # one pass, then it sleeps
    # The sweeper is a daemon: one pass, then it sleeps for GC_INTERVAL_MS. Timing out is
    # the expected end of a single-pass run, not a failure — the work is already done and
    # the output is on the pipe.
    try:
        run = subprocess.run([str(SWEEPER)], env=env, capture_output=True, text=True, timeout=15)
        out = run.stdout + run.stderr
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or b"").decode() + (e.stderr or b"").decode()

    failed = 0
    if f"sweeping {TABLE} on tombstone column '{TOMBSTONE}'" not in out:
        zb.bad("the sweeper did not derive the table/column from SYNC_RULES")
        print("   ", out.strip().splitlines()[:3])
        failed += 1
    else:
        zb.ok("table and tombstone column derived from SYNC_RULES")

    if "permission denied" in out:
        zb.bad("permission denied — bridge_writer needs DELETE (zebridge_grant_edge_writes)")
        failed += 1

    survivors = [
        line for line in zb.psql(
            f"SELECT some_text FROM public.{TABLE} WHERE some_text LIKE '{MARK}%' ORDER BY some_text"
        ).splitlines() if line
    ]
    expected = sorted(f"{MARK}: {label}" for label, _, keep in seeds if keep)

    print("  survivors:")
    for s in survivors:
        print(f"    {s}")

    if sorted(survivors) == expected:
        zb.ok("reaped exactly the tombstones past the threshold; kept the rest")
    else:
        missing = set(expected) - set(survivors)
        extra = set(survivors) - set(expected)
        if missing:
            # The dangerous direction: a client still inside its offline window just lost
            # the tombstone that would have overruled its queued edit.
            zb.bad(f"reaped TOO EARLY — these should have survived: {sorted(missing)}")
        if extra:
            zb.bad(f"reaped too little — these should have gone: {sorted(extra)}")
        failed += 1

    reaped = re.search(r"reaped (\d+) tombstone", out)
    print(f"\n  sweeper reported: {reaped.group(0) if reaped else '(nothing reaped)'}")

    zb.psql(f"DELETE FROM public.{TABLE} WHERE some_text LIKE '{MARK}%'", quiet=True)
    return 1 if failed else 0


zb.run(main)

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

⚠️ It also asserts the sweeper's *sources*: the table and its tombstone column come from
`zebridge_catalogue` — written by `zebridge_enable` in the same transaction as the
soft-delete trigger, and read by the bridge and the sweeper alike, so the two cannot
disagree about which column is the tombstone. An earlier version read `GC_TABLES` and
deleted `WHERE _deleted = true AND _hlc < $1` — columns no table has — so it matched
nothing and the guarantee above was not enforced at all, silently, for as long as it
existed.

⚠️ A sweeper pass reaps EVERY catalogued table's tombstones, and there is no undo. This
scenario therefore runs the sweeper SCOPED: `SWEEP_ONLY_TABLES` (a filter the sidecar
honours, see src/bridge_sweeper.zig) is set to a throwaway fixture this scenario creates,
registers through `zebridge_enable`, and drops — so a test pass never touches a real
table's tombstones, however old.

Usage:  python scripts/scenarios/sweeper.py

Needs `DATABASE_WRITER_URL` (the sweeper's own connection) and admin psql:

    set -a && . ./.env.bridge && set +a
"""

import os
import re
import subprocess
import sys

import zb

FIX = "zb_sweeper_probe"      # a table this scenario owns: created, enabled, dropped
THRESHOLD_MS = 3_600_000      # 1 hour
MARK = "sweeper-scenario"


def setup_fixture():
    zb.psql(f"DROP TABLE IF EXISTS public.{FIX} CASCADE", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{FIX}'", quiet=True)
    out = zb.psql(f"""
        CREATE TABLE public.{FIX} (uid uuid PRIMARY KEY, some_text text,
            inserted_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL, deleted_at timestamptz);
        SELECT step || ':' || status FROM zebridge_enable('public.{FIX}',
            writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at',
            public_reason => 'sweeper scenario fixture',
            publication => '{zb.publication()}', dry_run => false);
    """)
    if "catalogue:done" not in out:
        sys.exit(f"zebridge_enable did not register the fixture:\n{out}")


def teardown_fixture():
    zb.psql(f"DROP TABLE IF EXISTS public.{FIX} CASCADE", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{FIX}'", quiet=True)


async def main():
    if not zb.SWEEPER.exists():
        sys.exit(f"{zb.SWEEPER} not built — run `zig build`")
    if not os.environ.get("DATABASE_WRITER_URL"):
        sys.exit("DATABASE_WRITER_URL is not set.\n  set -a && . ./.env.bridge && set +a")

    setup_fixture()
    try:
        return await run()
    finally:
        teardown_fixture()


async def run():
    # The columns the sweeper will use, from the same place it reads them.
    r = zb.require_rules(FIX, "version", "tombstone")
    version, tombstone = r["version"], r["tombstone"]

    # Ages chosen around the boundary rather than far from it: a sweeper that compares the
    # wrong column, or the right column against the wrong clock, still passes a test whose
    # rows are days apart.
    seeds = [
        ("well past", "3 hours", False),
        ("just past", "61 minutes", False),
        ("just inside", "59 minutes", True),
        ("live row", None, True),
    ]
    for label, age, _ in seeds:
        ts = "NULL" if age is None else f"now() - interval '{age}'"
        zb.psql(
            f"INSERT INTO public.{FIX} (uid, some_text, inserted_at, {version}, {tombstone}) "
            f"VALUES (gen_random_uuid(), '{MARK}: {label}', now(), now(), {ts})",
            quiet=True,
        )

    seeded = zb.psql(f"SELECT count(*) FROM public.{FIX} WHERE some_text LIKE '{MARK}%'").strip()
    if seeded != str(len(seeds)):
        sys.exit(
            f"seeding failed: {seeded} of {len(seeds)} rows inserted into '{FIX}'.\n"
            "  Fix the fixture before reading anything below as a sweeper verdict."
        )

    print(f"seeded {len(seeds)} rows in '{FIX}' (threshold {THRESHOLD_MS // 60000} min)\n")

    env = dict(os.environ)
    env.pop("SYNC_RULES", None)  # the catalogue is the source under test
    env["SWEEP_ONLY_TABLES"] = FIX  # ⚠️ the scope: nothing else is swept
    env["GC_THRESHOLD_MS"] = str(THRESHOLD_MS)
    env["GC_INTERVAL_MS"] = "999999999"  # one pass, then it sleeps
    # The sweeper is a daemon: one pass, then it sleeps for GC_INTERVAL_MS. Timing out is
    # the expected end of a single-pass run, not a failure — the work is already done and
    # the output is on the pipe.
    try:
        proc = subprocess.run([str(zb.SWEEPER)], env=env, capture_output=True, text=True, timeout=15)
        out = proc.stdout + proc.stderr
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or b"").decode() + (e.stderr or b"").decode()

    failed = 0
    if f"sweeping {FIX} on tombstone column '{tombstone}'" not in out:
        zb.bad("the sweeper did not derive the table/column from the catalogue")
        print("   ", out.strip().splitlines()[:3])
        failed += 1
    else:
        zb.ok("table and tombstone column derived from zebridge_catalogue")

    swept = re.findall(r"GC: sweeping (\S+) on", out)
    if swept == [FIX]:
        zb.ok(f"SWEEP_ONLY_TABLES scoped the pass to '{FIX}' alone")
    else:
        zb.bad(f"the pass was not scoped to the fixture — swept: {swept}")
        failed += 1

    if "permission denied" in out:
        zb.bad("permission denied — bridge_writer needs DELETE (zebridge_grant_edge_writes)")
        failed += 1

    survivors = [
        line for line in zb.psql(
            f"SELECT some_text FROM public.{FIX} WHERE some_text LIKE '{MARK}%' ORDER BY some_text"
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
    return 1 if failed else 0


zb.run(main)

#!/usr/bin/env python3
"""Tombstone GC — does a running sweeper follow the catalogue?

The sweeper is the process nobody remembers: started once, left alone. So a table enabled
while it runs must be swept from its next pass, and a table dropped from the catalogue must
stop being swept — no restart. It reads `zebridge_catalogue` at the start of every pass.

This starts the sweeper scoped to a table that does not exist yet (it waits), enables that
table with an old tombstone and a live row, waits for the reap, then removes the table and
waits for the sweeper to say it stopped sweeping it.

Usage:  python scripts/scenarios/sweeper_catalogue.py

Needs `DATABASE_WRITER_URL` (the sweeper's own connection) and admin psql:

    set -a && . ./.env.bridge && set +a
"""

import os
import subprocess
import sys
import tempfile
import time

import zb

FIX = "zb_sweeper_late"       # a table this scenario owns: created mid-run, then dropped
MARK = "sweeper-catalogue"
INTERVAL_MS = 1000


def drop_fixture():
    zb.psql(f"DROP TABLE IF EXISTS public.{FIX} CASCADE", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{FIX}'", quiet=True)


def wait_for(log_path: str, text: str, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with open(log_path) as f:
            if text in f.read():
                return True
        time.sleep(0.2)
    return False


async def main():
    if not zb.SWEEPER.exists():
        sys.exit(f"{zb.SWEEPER} not built — run `zig build`")
    if not os.environ.get("DATABASE_WRITER_URL"):
        sys.exit("DATABASE_WRITER_URL is not set.\n  set -a && . ./.env.bridge && set +a")

    drop_fixture()
    zb.forget_table(FIX)
    env = dict(os.environ)
    env["SWEEP_ONLY_TABLES"] = FIX  # ⚠️ the scope: nothing else is swept
    env["GC_THRESHOLD_MS"] = "3600000"
    env["GC_INTERVAL_MS"] = str(INTERVAL_MS)

    log = tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False)
    proc = subprocess.Popen([str(zb.SWEEPER)], env=env, stdout=log, stderr=subprocess.STDOUT)
    failed = 0
    try:
        # 1. Nothing declared yet: the sweeper waits instead of exiting.
        if not wait_for(log.name, "nothing to sweep", 10):
            zb.bad("the sweeper did not report an empty sweep set")
            failed += 1
        time.sleep(2 * INTERVAL_MS / 1000)
        if proc.poll() is not None:
            zb.bad(f"the sweeper exited with nothing to sweep (code {proc.returncode})")
            return 1
        zb.ok("with nothing to sweep, the sweeper keeps running")

        # 2. The table is enabled while it runs, with one tombstone past the threshold.
        out = zb.psql(f"""
            CREATE TABLE public.{FIX} (uid uuid PRIMARY KEY, some_text text,
                updated_at timestamptz NOT NULL, deleted_at timestamptz);
            SELECT step || ':' || status FROM zebridge_enable('public.{FIX}',
                writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at',
                public_reason => 'sweeper catalogue scenario fixture',
                publication => '{zb.publication()}', dry_run => false);
            INSERT INTO public.{FIX} (uid, some_text, updated_at, deleted_at) VALUES
                (gen_random_uuid(), '{MARK}: old tombstone', now(), now() - interval '3 hours'),
                (gen_random_uuid(), '{MARK}: live row', now(), NULL);
        """)
        if "catalogue:done" not in out:
            zb.bad(f"zebridge_enable did not register the fixture:\n{out}")
            return 1

        if wait_for(log.name, f"sweeping {FIX} on tombstone column 'deleted_at'", 10):
            zb.ok("a table enabled mid-run joins the sweep set at the next pass")
        else:
            zb.bad("the running sweeper never picked up the new table")
            failed += 1

        if wait_for(log.name, f"reaped 1 tombstone(s) from {FIX}", 10):
            zb.ok("its old tombstone was reaped")
        else:
            zb.bad("the old tombstone was not reaped")
            failed += 1
        left = zb.psql(f"SELECT some_text FROM public.{FIX} ORDER BY some_text").strip()
        if left != f"{MARK}: live row":
            zb.bad(f"rows left: {left!r} — only the live row should be")
            failed += 1

        # 3. The table leaves the catalogue: the sweeper stops sweeping it, and lives on.
        drop_fixture()
        if wait_for(log.name, f"no longer sweeping {FIX}", 10):
            zb.ok("a table removed from the catalogue leaves the sweep set")
        else:
            zb.bad("the sweeper did not drop the removed table")
            failed += 1
        time.sleep(2 * INTERVAL_MS / 1000)
        if proc.poll() is not None:
            zb.bad(f"the sweeper died after the table left (code {proc.returncode})")
            failed += 1
        with open(log.name) as f:
            text = f.read()
        if "error(" in text:
            zb.bad("errors in the sweeper's log:")
            for line in text.splitlines():
                if "error(" in line:
                    print("   ", line)
            failed += 1
    finally:
        proc.terminate()
        proc.wait(timeout=10)
        drop_fixture()
        # A Debug build reports what it never freed when it stops: the sweep sets swapped
        # mid-run must all have been released.
        with open(log.name) as f:
            leaks = [line for line in f.read().splitlines() if "leaked" in line]
        os.unlink(log.name)
    if leaks:
        zb.bad(f"the sweeper leaked on stop: {leaks[:3]}")
        failed += 1
    else:
        zb.ok("stopped cleanly, nothing leaked")
    return 1 if failed else 0


zb.run(main)

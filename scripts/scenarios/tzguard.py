#!/usr/bin/env python3
"""`zebridge_timestamp_guard` refuses naive-timestamp columns — mechanically, at DDL time.

Version columns travel in §7.2's UTC wire format and are compared and clamped as absolute
instants; a `timestamp without time zone` lets two writers in different zones disagree
about which write is newer — silently, per row. The rule lived in prose until a migration
forgot it (NOTES.md, 2026-08-24); the guard is its mechanical form, same pattern as
`zebridge_publication_guard`: refuse inside the DDL transaction, so the migration fails
whole and nothing is half-applied.

Five claims, each checked end to end:

  1. the guard EXISTS — `render.py`'s lesson applies here too: an event trigger can
     silently vanish in a bad template render, and every check below would then pass
     for the wrong reason (nothing refused because nothing guards);
  2. CREATE TABLE with a naive column is refused, and the error names the column and
     the fix (`timestamptz`) — a refusal nobody can act on is half a guard;
  3. the same table with `timestamptz` passes;
  4. ALTER TABLE adding a naive column is refused, and the table keeps its old shape;
  5. the refusal is TRANSACTIONAL: a migration that creates a good table and then a bad
     one loses BOTH — quarantine means nothing half-applied, which is the property that
     distinguishes this from a warning someone reads later.

Plus the exemption that keeps migrations runnable at all: `zebridge_is_internal_table`
names (Ecto's `schema_migrations` is naive by design) must pass untouched — without it,
`mix ecto.migrate`'s first statement on a fresh database is refused and no migration can
ever run.

Usage:  python scripts/scenarios/tzguard.py
Creates and drops its own `tzguard_*` fixtures. Needs only psql (ZB_PSQL / default).
"""

import subprocess
import sys

import zb


def run_sql(sql: str):
    """Statement result with stderr, because the *refusal message* is under test too."""
    return subprocess.run(zb.PSQL.split() + ["-tA", "-v", "ON_ERROR_STOP=1", "-c", sql],
                          capture_output=True, text=True)


def main():
    failed = 0

    # ── 1. the guard exists ────────────────────────────────────────────────────
    have = zb.psql("SELECT 1 FROM pg_event_trigger WHERE evtname='zebridge_timestamp_guard_t' AND evtenabled <> 'D'").strip()
    if have != "1":
        zb.bad("zebridge_timestamp_guard_t is missing or disabled — every check below would "
               "pass for the wrong reason. Re-apply init.core.template.sql (render.py checks "
               "that a render doesn't eat it).")
        return 1
    zb.ok("the guard exists and is enabled")

    try:
        # ── 2. CREATE with naive timestamp: refused, and the message teaches ──
        r = run_sql("CREATE TABLE public.tzguard_bad (id serial PRIMARY KEY, at timestamp);")
        if r.returncode != 0 and "timestamptz" in r.stderr and "tzguard_bad" in r.stderr:
            zb.ok("CREATE with `timestamp` is refused, naming the table and the fix")
        elif r.returncode != 0:
            zb.bad(f"refused, but the message doesn't teach the fix: {r.stderr.strip()[:140]}")
            failed += 1
        else:
            zb.bad("a naive-timestamp CREATE was ACCEPTED — the guard is not guarding")
            failed += 1

        # ── 3. timestamptz passes ──────────────────────────────────────────────
        r = run_sql("CREATE TABLE public.tzguard_ok (id serial PRIMARY KEY, at timestamptz);")
        if r.returncode == 0:
            zb.ok("the same shape with `timestamptz` passes")
        else:
            zb.bad(f"a correct table was refused: {r.stderr.strip()[:140]}")
            failed += 1

        # ── 4. ALTER adding naive: refused, table keeps its shape ─────────────
        r = run_sql("ALTER TABLE public.tzguard_ok ADD COLUMN later timestamp;")
        cols = zb.psql("SELECT count(*) FROM pg_attribute WHERE attrelid='public.tzguard_ok'::regclass AND attname='later'").strip()
        if r.returncode != 0 and cols == "0":
            zb.ok("ALTER adding `timestamp` is refused and the column does not exist")
        else:
            zb.bad(f"ALTER path leaked: returncode={r.returncode}, column present={cols}")
            failed += 1

        # ── 5. the refusal is transactional: good DDL in the same txn dies too ─
        r = run_sql("BEGIN; CREATE TABLE public.tzguard_half (id int PRIMARY KEY); "
                    "CREATE TABLE public.tzguard_bad2 (id int PRIMARY KEY, at timestamp); COMMIT;")
        half = zb.psql("SELECT count(*) FROM pg_tables WHERE tablename='tzguard_half'").strip()
        if r.returncode != 0 and half == "0":
            zb.ok("a mixed migration rolls back WHOLE — the good table did not survive the bad one")
        else:
            zb.bad(f"half-applied migration: returncode={r.returncode}, tzguard_half exists={half}")
            failed += 1

        # ── exemption: internal tables stay naive-capable, or migrations break ─
        r = run_sql("ALTER TABLE public.schema_migrations ADD COLUMN tzguard_probe timestamp;")
        if r.returncode == 0:
            zb.psql("ALTER TABLE public.schema_migrations DROP COLUMN tzguard_probe", quiet=True)
            zb.ok("`schema_migrations` is exempt — `mix ecto.migrate` can still bootstrap")
        else:
            zb.bad(f"the exemption is broken; migrations cannot run on a fresh database: {r.stderr.strip()[:140]}")
            failed += 1
    finally:
        for t in ("tzguard_bad", "tzguard_ok", "tzguard_half", "tzguard_bad2"):
            zb.psql(f"DROP TABLE IF EXISTS public.{t}", quiet=True)

    return 1 if failed else 0


sys.exit(main())

#!/usr/bin/env python3
"""Reset the native stack to a known-good baseline — the cure for test
ordering/residue worries (§10cu).

A day of scenario runs leaves accumulation that makes the full battery
order-dependent: orphan replication slots, leftover probe tables and their
catalogue rows, stale KV schema keys (PURGE tombstones — §10bw), residue rows in
the fixture tables, and stray tenant mappings. This script lands the stack at the
baseline every scenario assumes, so a battery run measures the run, not the
archaeology.

It does NOT touch principals/creds (they survive `down.sh --clean`) or the JWT
stack. It is idempotent: safe to run against a healthy stack or a poisoned one.

  scripts/reprovision.py            # clean + ensure baseline
  scripts/reprovision.py --list     # show what it WOULD change, touch nothing

Run it against a running stack (PG + NATS up), with .env.bridge sourced. After it,
start the live bridge fresh — the boot republishes every catalogue schema clean.
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "scenarios"))
import zb  # noqa: E402

DRY = "--list" in sys.argv

# The canonical baseline the scenarios assume.
BASE_TABLES = ["test_types", "orders", "users", "salaries", "memo", "note_t",
               "counter_tenant", "counter_public"]
# principal -> tenant. zb_sweeper maps to every tenant (the sidecar's identity).
TENANTS = {"alice": "acme", "bob": "globex", "mary": "globex", "nina": "tango", "omar": "kilo"}
SWEEPER_TENANTS = ["acme", "globex", "tango", "kilo"]
# enable params per table: (version, tombstone, tiebreak, tenant), captured from
# the known-good catalogue. Empty string = not set.
ENABLE = {
    "test_types":     ("updated_at", "deleted_at", "last_writer", "tenant_id"),
    "orders":         ("updated_at", "", "", ""),
    "users":          ("updated_at", "", "", ""),
    "salaries":       ("updated_at", "", "", "tenant_id"),
    "memo":           ("updated_at", "", "", ""),
    "note_t":         ("updated_at", "", "", "tenant_id"),
    "counter_tenant": ("updated_at", "", "", "tenant_id"),
    "counter_public": ("updated_at", "", "", ""),
}


def psql(sql):
    return zb.psql(sql, quiet=True)


def note(msg):
    print(("  would: " if DRY else "  ") + msg, flush=True)


def main():
    pub = zb.publication()
    changed = 0

    # 1. base schema — apply the committed DDL (CREATE TABLE IF NOT EXISTS), so a
    #    stack wiped by down.sh --clean rebuilds the fixtures from source.
    schema = os.path.join(HERE, "baseline.schema.sql")
    have = set(x for x in psql(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' "
        f"AND tablename = ANY(ARRAY{BASE_TABLES})").split() if x)
    missing = [t for t in BASE_TABLES if t not in have]
    if missing:
        note(f"recreate missing base tables from baseline.schema.sql: {missing}")
        if not DRY:
            # zb.PSQL is the admin invocation string; -f applies the committed DDL.
            r = subprocess.run(zb.PSQL.split() + ["-v", "ON_ERROR_STOP=0", "-f", schema],
                               capture_output=True, text=True)
            if r.returncode != 0:
                print(r.stderr[-500:], file=sys.stderr)
        changed += 1

    # 2. drop leftover probe tables (anything public that is not base and not bridge-owned)
    leftovers = [t for t in psql(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' "
        "AND tablename NOT LIKE 'zebridge%'").split()
        if t and t not in BASE_TABLES]
    for t in leftovers:
        note(f"drop leftover table + catalogue row + KV schema key: {t}")
        if not DRY:
            psql(f"DELETE FROM zebridge_catalogue WHERE tbl = '{t}'")
            psql(f"DROP TABLE IF EXISTS public.\"{t}\" CASCADE")
            # a dropped table's schema key SHOULD carry a tombstone — it is gone
            subprocess.run(["nats", "--server", zb.nats_server(), "--creds",
                            zb.creds_for("bridge"), "kv", "purge", "schemas", t, "-f"],
                           capture_output=True)
        changed += 1

    # 3. drop every replication slot — scenarios and the live bridge make their own
    slots = [s for s in psql("SELECT slot_name FROM pg_replication_slots").split() if s]
    for s in slots:
        note(f"drop replication slot: {s}")
        if not DRY:
            psql(f"SELECT pg_drop_replication_slot('{s}')")
        changed += 1

    # 4. truncate the base fixtures to a clean slate (orders->users FK: CASCADE)
    note(f"truncate base fixtures to empty: {BASE_TABLES}")
    if not DRY:
        psql("TRUNCATE " + ", ".join(f"public.{t}" for t in BASE_TABLES) + " CASCADE")
    changed += 1

    # 5. tenant mappings — canonical set only, drop strays
    live = dict(x.split("|") for x in psql(
        "SELECT principal || '|' || tenant_id FROM zebridge_user_tenants "
        "WHERE principal <> 'zb_sweeper'").splitlines() if "|" in x)
    want = dict(TENANTS)
    for p, t in live.items():
        if want.get(p) != t:
            note(f"drop stray/incorrect tenant mapping: {p}->{t}")
            if not DRY:
                psql(f"DELETE FROM zebridge_user_tenants WHERE principal='{p}' AND tenant_id='{t}'")
            changed += 1
    for p, t in want.items():
        if live.get(p) != t:
            note(f"ensure tenant mapping: {p}->{t}")
            if not DRY:
                psql("INSERT INTO zebridge_user_tenants (principal, tenant_id) "
                     f"VALUES ('{p}','{t}') ON CONFLICT DO NOTHING")
            changed += 1
    for t in SWEEPER_TENANTS:
        if not DRY:
            psql("INSERT INTO zebridge_user_tenants (principal, tenant_id) "
                 f"VALUES ('zb_sweeper','{t}') ON CONFLICT DO NOTHING")

    # 6. ensure the base tables are enabled with the canonical params
    for t in BASE_TABLES:
        v, tomb, tie, ten = ENABLE[t]
        args = [f"'public.{t}'", "writable => true", f"version_col => '{v}'"]
        if tomb:
            args.append(f"tombstone_col => '{tomb}'")
        else:
            args.append("allow_physical_deletes => true")
        if tie:
            args.append(f"tiebreak_col => '{tie}'")
        if ten:
            args.append(f"tenant_col => '{ten}'")
        args += [f"publication => '{pub}'", "dry_run => false"]
        note(f"ensure enabled: {t}")
        if not DRY:
            out = psql(f"SELECT 1 FROM zebridge_enable({', '.join(args)})")
            if "error" in out.lower():
                print(f"  ⚠️ enable {t}: {out}", file=sys.stderr)

    print(f"\n{'DRY RUN — ' if DRY else ''}{changed} change(s). "
          "Start the live bridge fresh; boot republishes all schemas clean.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

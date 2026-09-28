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

  scripts/reprovision.py                     # clean + ensure baseline
  scripts/reprovision.py --list              # show what it WOULD change, touch nothing
  scripts/reprovision.py --drop-unknown      # also drop tables it does not recognise
  scripts/reprovision.py --drop-bridge-slot  # also drop the bridge's own slot (bridge stopped)

Run it against a running stack (PG + NATS up), with .env.bridge sourced. After it,
start the live bridge fresh — the boot republishes every catalogue schema clean.

What it will NOT do by default (§10jp — the first version did all of these once the
stack had grown past its 2026-09-05 baseline): drop a table it does not recognise as a
scenario probe (the examples' tables: 05's app_*, 06's fire_types, 08's pois, routes,
fuel_* and charge_points …), touch a table an EXTENSION owns (PostGIS's
spatial_ref_sys), drop an active replication slot or the bridge's own, or change the
tenant mappings of principals outside the fixture set (deleting a principal's last
mapping revokes it, §10dm).
"""
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "scenarios"))
import zb  # noqa: E402

DRY = "--list" in sys.argv
DROP_UNKNOWN = "--drop-unknown" in sys.argv
DROP_BRIDGE_SLOT = "--drop-bridge-slot" in sys.argv

# What the scenarios leave behind (scripts/scenarios/*.py), by name: zb_* probes (not
# zebridge_*, the bridge's own), the type fixtures (arr_t, blob_t …; note_t is base and
# never reaches this test), the migration probes, inject.py's crazy_N, and one-offs.
PROBE = re.compile(r"^(zb_.+|[a-z]+_t|mig_.+|crazy_\d+|tzguard_.+|ro_.+|rw_.+|sw_.+|test_types_v\d+"
                   r"|widgets|bench_users|decode_fixture|inc_rows)$")
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


def publication():
    """The bridge's publication: $BRIDGE_CDC_PUBLICATION, else the database's only one."""
    if os.environ.get("BRIDGE_CDC_PUBLICATION"):
        return zb.publication()
    pubs = [p for p in psql("SELECT pubname FROM pg_publication").split() if p]
    if len(pubs) != 1:
        sys.exit(f"set BRIDGE_CDC_PUBLICATION: the database has {len(pubs)} publications {pubs}")
    return pubs[0]


def main():
    pub = publication()
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

    # 2. drop leftover probe tables: public, not base, not the bridge's, not an
    #    extension's (pg_depend 'e': spatial_ref_sys is PostGIS's), and RECOGNISED as a
    #    probe — an unknown table is an example's until someone says otherwise.
    candidates = [t for t in psql(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relkind IN ('r','p') AND c.relname NOT LIKE 'zebridge%' "
        "AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass "
        "AND d.objid = c.oid AND d.deptype = 'e') ORDER BY 1").split()
        if t and t not in BASE_TABLES]
    leftovers = [t for t in candidates if PROBE.match(t) or DROP_UNKNOWN]
    for t in candidates:
        if t not in leftovers:
            print(f"  keep (not a known probe; --drop-unknown drops it): {t}", flush=True)
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

    # 3. drop scenario replication slots — never an ACTIVE one (a bridge is reading it)
    #    and never the bridge's own unless asked: a dropped bridge slot loses every change
    #    made before the bridge re-creates it.
    bridge_slot = os.environ.get("BRIDGE_CDC_SLOT", "my_slot")
    slots = []
    # (a boolean cast to text is 'true', not psql's display 't': say it in words)
    for row in psql("SELECT slot_name || '|' || CASE WHEN active THEN 'active' ELSE 'idle' END "
                    "FROM pg_replication_slots").splitlines():
        if "|" not in row:
            continue
        name, state = row.split("|")
        if state == "active":
            print(f"  keep (active): slot {name}", flush=True)
        elif name == bridge_slot and not DROP_BRIDGE_SLOT:
            print(f"  keep (the bridge's; --drop-bridge-slot drops it): slot {name}", flush=True)
        else:
            slots.append(name)
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

    # 5. tenant mappings — the FIXTURE principals only, as SETS (a principal may belong
    #    to several tenants). The canonical mapping goes in before any extra comes out,
    #    so a fixture principal never passes through zero memberships (that is a
    #    revocation, and its ban, §10dm). Other principals (events, mqttgw, pois …) are
    #    the examples' and are left alone.
    live = set()
    for x in psql("SELECT principal || '|' || tenant_id FROM zebridge_user_tenants").splitlines():
        if "|" in x:
            live.add(tuple(x.split("|")))
    for p, t in TENANTS.items():
        if (p, t) not in live:
            note(f"ensure tenant mapping: {p}->{t}")
            if not DRY:
                psql("INSERT INTO zebridge_user_tenants (principal, tenant_id) "
                     f"VALUES ('{p}','{t}') ON CONFLICT DO NOTHING")
            changed += 1
    for p, t in sorted(live):
        if p in TENANTS and TENANTS[p] != t:
            note(f"drop extra tenant mapping of a fixture principal: {p}->{t}")
            if not DRY:
                psql(f"DELETE FROM zebridge_user_tenants WHERE principal='{p}' AND tenant_id='{t}'")
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

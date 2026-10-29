#!/usr/bin/env python3
"""The producer keys two decisions on a table's TENANT column, whatever its tombstone.

    scripts/scenarios/run.py live -k shared_cut

Both were keyed on the tombstone column by mistake (found 2026-10-11, filtered_gap):

  1. the shared cut (§10lw): every tenant pair of a tenant-scoped table carries
     `shared_cutoff_seq`, its proof on CDC_PUBLIC. A table with no tombstone column
     shipped without it, and once CDC_PUBLIC pruned a message, a client following that
     table stayed blocked on CDC_PUBLIC for ever ("the gap stays open");
  2. the delta check (§10ek, f10afad): an UPDATE in one tenant's rows cuts that pair's
     next generation. On a table WITH a tombstone the check compared the tombstone
     column to the tenant name, never true: an update cut nothing.

Checked against a LIVE bridge with generations on (one cadence: GENERATION_CADENCE_SECONDS):

  1. every manifest `<tenant>.<table>` of a tenant-scoped table (tenant ≠ the open tenant)
     carries shared_cutoff_seq — including tables with no tombstone column;
  2. an UPDATE of one test_types row of a tenant moves that pair's manifest to a new
     generation within two cadences.

Needs a running bridge with generations, NATS_CREDS (bridge.creds), admin psql.
"""
import json
import os
import sys
import time

import zb

CADENCE = int(os.environ.get("GENERATION_CADENCE_SECONDS", "60"))


def manifest(key):
    r = zb.nats_cli("kv", "get", zb.kv_bucket("generations"), key, "--raw")
    if r.returncode != 0 or not r.stdout.strip():
        return None
    return json.loads(r.stdout)


def main():
    failed = 0
    open_tenant = zb.TOPOLOGY.get("open_tenant", "_default")

    # ── 1. the shared cut on every tenant pair of a tenant-scoped table ──────────
    rows = zb.psql("SELECT tbl, coalesce(tombstone_col::text, '') FROM zebridge_catalogue "
                   "WHERE tenant_col IS NOT NULL AND coalesce(generations, true) ORDER BY 1", quiet=True)
    tables = [l.split("|") for l in rows.splitlines() if l]
    tenants = [t for t in zb.tenants() if t != open_tenant]
    deadline = time.time() + 2 * CADENCE + 30
    missing = None
    while True:
        missing, checked = [], 0
        for tbl, tomb in tables:
            for t in tenants:
                m = manifest(f"{t}.{tbl}")
                if m is None:
                    continue  # no chain for this pair (no rows yet): nothing to prove
                checked += 1
                if not isinstance(m.get("shared_cutoff_seq"), int):
                    missing.append(f"{t}.{tbl}" + ("" if tomb else " (no tombstone)"))
        if not missing or time.time() > deadline:
            break
        time.sleep(5)
    if not missing:
        zb.ok(f"all {checked} tenant manifests carry shared_cutoff_seq, tables without a tombstone included")
    else:
        zb.bad(f"{len(missing)} tenant manifest(s) without shared_cutoff_seq after two cadences: "
               f"{', '.join(missing[:6])}{' …' if len(missing) > 6 else ''}")
        failed += 1

    # ── 2. an UPDATE in one tenant's rows cuts that pair's next generation ───────
    tenant = next((t for t in tenants
                   if zb.psql(f"SELECT count(*) FROM public.test_types WHERE tenant_id = '{t}'", quiet=True).strip() not in ("", "0")), None)
    if tenant is None and tenants:
        # A fresh stack holds no test_types rows (other scenarios of the group happen to leave some): write one, and let the
        # cut it causes land before the baseline is taken.
        tenant = tenants[0]
        key = f"{tenant}.test_types"
        gen_seen = (manifest(key) or {}).get("gen")
        zb.psql(f"INSERT INTO public.test_types (uid, some_text, tenant_id, inserted_at, updated_at) "
                f"VALUES (gen_random_uuid(), 'shared_cut seed', '{tenant}', now(), now())", quiet=True)
        deadline = time.time() + CADENCE + 30
        while time.time() < deadline and (manifest(key) or {}).get("gen") == gen_seen:
            time.sleep(5)
    if tenant is None:
        zb.bad("no tenant to update test_types for"); return 1
    key = f"{tenant}.test_types"
    before = manifest(key)
    if before is None:
        zb.bad(f"no manifest {key} to start from"); return 1
    gen0 = before.get("gen")
    zb.psql(f"UPDATE public.test_types SET some_text = 'shared_cut probe', updated_at = now() "
            f"WHERE uid = (SELECT uid FROM public.test_types WHERE tenant_id = '{tenant}' "
            f"AND deleted_at IS NULL ORDER BY uid LIMIT 1)", quiet=True)
    deadline = time.time() + 2 * CADENCE + 30
    gen = gen0
    while time.time() < deadline:
        gen = (manifest(key) or {}).get("gen", gen0)
        if gen != gen0:
            break
        time.sleep(5)
    if gen != gen0:
        zb.ok(f"an UPDATE in '{tenant}' cut {key} g{gen0} → g{gen} (test_types keeps a tombstone)")
    else:
        zb.bad(f"an UPDATE in '{tenant}' cut nothing: {key} still g{gen0} after two cadences ({2 * CADENCE + 30} s)")
        failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

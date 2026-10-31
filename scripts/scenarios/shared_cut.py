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
import pathlib
import sys
import tempfile
import time
import uuid

import msgpack
import zstandard

import zb

CADENCE = int(os.environ.get("GENERATION_CADENCE_SECONDS", "60"))


def manifest(key):
    r = zb.nats_cli("kv", "get", zb.kv_bucket("generations"), key, "--raw")
    if r.returncode != 0 or not r.stdout.strip():
        return None
    return json.loads(r.stdout)


def newest_object(m):
    """The document of the manifest's newest generation (its full, or the delta of that generation), decoded,
    or None: the object the cut published, read from the store the manifest names."""
    name = m["full"]["object"] if m["full"]["gen"] == m["gen"] else next(
        (d["object"] for d in m.get("deltas", []) if d.get("gen") == m["gen"]), None)
    if name is None:
        return None
    with tempfile.TemporaryDirectory() as tmp:
        path = pathlib.Path(tmp) / "obj"
        r = zb.nats_cli("object", "get", m["bucket"], name, "--output", str(path))
        if r.returncode != 0 or not path.exists():
            return None
        blob = path.read_bytes()
    if blob.startswith(b"\x28\xb5\x2f\xfd"):
        blob = zstandard.ZstdDecompressor().decompress(blob, max_output_size=1 << 30)
    return msgpack.unpackb(blob, raw=False, strict_map_key=False)


def carries(doc, uid, value):
    """Whether a chain document holds the row `uid` with some_text = `value`."""
    if not doc:
        return False
    cols = doc["columns"]
    if "uid" not in cols or "some_text" not in cols:
        return False
    iu, it = cols.index("uid"), cols.index("some_text")
    return any(str(r[iu]) == uid and r[it] == value for r in doc["rows"])


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

    # ── 2. an UPDATE in one tenant's rows cuts that pair's next generation, and that generation carries it ──
    #
    # The probe is a row this scenario owns, with a value no one else writes: a cut that merely happened (the
    # cadence ticks anyway) does not pass, only a generation whose own object holds the changed row does. The
    # UPDATE must say it changed exactly that row (RETURNING), or nothing was tested.
    if not tenants:
        zb.bad("no tenant to update test_types for"); return 1
    tenant = tenants[0]
    key = f"{tenant}.test_types"
    seeded = f"shared_cut seed {uuid.uuid4().hex[:12]}"
    marker = f"shared_cut probe {uuid.uuid4().hex[:12]}"
    gen_seen = (manifest(key) or {}).get("gen")
    uid = zb.psql(f"INSERT INTO public.test_types (uid, some_text, tenant_id, inserted_at, updated_at) "
                  f"VALUES (gen_random_uuid(), '{seeded}', '{tenant}', now(), now()) RETURNING uid", quiet=True).splitlines()
    uid = uid[0].strip() if uid else ""
    if len(uid) != 36:
        zb.bad(f"the probe row was not inserted: {uid!r}"); return 1
    try:
        # its own insert's cut lands first, so the baseline is the generation after it
        deadline = time.time() + 2 * CADENCE + 30
        while time.time() < deadline:
            m = manifest(key)
            if m and m.get("gen") != gen_seen and carries(newest_object(m), uid, seeded):
                break
            time.sleep(5)
        before = manifest(key)
        if before is None:
            zb.bad(f"no manifest {key} to start from"); return 1
        gen0 = before.get("gen")
        out = zb.psql(f"UPDATE public.test_types SET some_text = '{marker}', updated_at = now() "
                      f"WHERE uid = '{uid}' AND tenant_id = '{tenant}' RETURNING uid", quiet=True).splitlines()
        returned = [l.strip() for l in out if l.strip() and not l.startswith("UPDATE")]
        if returned != [uid]:
            zb.bad(f"the probe UPDATE did not change exactly the probe row: returned {returned}")
            failed += 1
        else:
            deadline = time.time() + 2 * CADENCE + 30
            gen, found = gen0, False
            while time.time() < deadline:
                m = manifest(key) or {}
                gen = m.get("gen", gen0)
                if gen != gen0 and carries(newest_object(m), uid, marker):
                    found = True
                    break
                time.sleep(5)
            if found:
                zb.ok(f"an UPDATE of the probe row in '{tenant}' cut {key} g{gen0} → g{gen}, and that generation carries the new value")
            else:
                zb.bad(f"no generation of {key} after g{gen0} carries the probe's new value within two cadences ({2 * CADENCE + 30} s); newest g{gen}")
                failed += 1
    finally:
        zb.psql(f"UPDATE public.test_types SET deleted_at = now(), updated_at = now() WHERE uid = '{uid}'", quiet=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

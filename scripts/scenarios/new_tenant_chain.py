#!/usr/bin/env python3
"""A new tenant whose rows were written before it existed gets its chains at its first
mapping, not at the producer's next tick.

    scripts/scenarios/run.py live -k new_tenant_chain

Rows written for a tenant before anyone is mapped to it are not on any stream (no stream
takes them yet, unmapped_tenant.py): they reach a device only through the tenant's first
chain. The producer built that chain at its next tick, up to GENERATION_CADENCE_SECONDS
after the first mapping, and until then the tenant's first device showed none of its rows
and held its CDC_PUBLIC position ("the gap stays open"). Found on leaf1, 2026-10-11: ten
minutes of an empty app for a new customer.

The bridge now kicks the producer when it creates a tenant's stream. Checked against a
LIVE bridge with generations on:

  1. two rows for a tenant nobody is mapped to, no stream, no chain;
  2. a first mapping: the stream is created, and the tenant's chain manifest for the
     table appears within 30 s — well inside the cadence — holding both rows.

Needs a running bridge with generations, a catalogue row for test_types with a tenant_col,
NATS_CREDS (bridge.creds), admin psql. Cleans up its rows, mapping, stream and chain.
"""
import json
import os
import sys
import time
import uuid

import zb

TENANT = "newchainten"
PRINCIPAL = "newchainprobe"
TABLE = "test_types"
LIMIT_S = 30


def manifest():
    r = zb.nats_cli("kv", "get", zb.kv_bucket("generations"), f"{TENANT}.{TABLE}", "--raw")
    return json.loads(r.stdout) if r.returncode == 0 and r.stdout.strip() else None


def stream_exists(name):
    return zb.nats_cli("stream", "info", name, "--json").returncode == 0


def cleanup(stream, rows):
    zb.psql("DELETE FROM public.test_types WHERE uid IN (" + ",".join(f"'{u}'" for u in rows) + ")", quiet=True)
    zb.purge_tombstones("test_types", TENANT)
    zb.psql(f"DELETE FROM public.zebridge_user_tenants WHERE tenant_id = '{TENANT}'", quiet=True)
    time.sleep(1.5)  # let the deletes publish before the stream goes
    zb.nats_cli("stream", "rm", stream, "-f")
    zb.nats_cli("kv", "del", zb.kv_bucket("generations"), f"{TENANT}.{TABLE}", "-f")
    zb.nats_cli("object", "rm", zb.TOPOLOGY["generations"]["bucket_prefix"] + TENANT, "-f")
    zb.drop_tenant_streams(TENANT)   # READINGS_ too
    zb.psql(f"DELETE FROM public.zebridge_generations WHERE tenant = '{TENANT}'", quiet=True)


def main():
    stream = zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + TENANT
    cadence = int(os.environ.get("GENERATION_CADENCE_SECONDS", "600"))
    rows = [uuid.uuid4() for _ in range(2)]
    cleanup(stream, [])
    if TENANT in zb.tenants() or stream_exists(stream) or manifest() is not None:
        sys.exit(f"'{TENANT}' still has a mapping, a stream or a chain — clean up a previous run first")

    failed = 0
    try:
        # ── 1. rows for a tenant that does not exist yet ─────────────────────────
        values = ",".join(f"('{u}','new tenant chain probe',now(),now(),'{TENANT}')" for u in rows)
        zb.psql(f"INSERT INTO public.test_types (uid,some_text,inserted_at,updated_at,tenant_id) VALUES {values}", quiet=True)
        time.sleep(3)
        if not stream_exists(stream) and manifest() is None:
            zb.ok(f"two rows for '{TENANT}' before any mapping: no stream, no chain (they wait for one)")
        else:
            zb.bad(f"'{TENANT}' has a stream or a chain before any mapping")
            failed += 1

        # ── 2. the first mapping: the chain follows within seconds ───────────────
        mapped_at = time.time()
        zb.psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) "
                f"VALUES ('{PRINCIPAL}', '{TENANT}') ON CONFLICT DO NOTHING", quiet=True)
        m = None
        while time.time() < mapped_at + LIMIT_S + 10:
            m = manifest()
            if m is not None:
                break
            time.sleep(1)
        took = time.time() - mapped_at
        if m is None:
            zb.bad(f"no chain for '{TENANT}'/'{TABLE}' {LIMIT_S + 10} s after the first mapping "
                   f"(the producer's cadence is {cadence} s): its first device would wait for it")
            failed += 1
        elif took > LIMIT_S:
            zb.bad(f"the chain came {took:.0f} s after the first mapping, over {LIMIT_S} s")
            failed += 1
        else:
            zb.ok(f"the first mapping made the stream and the chain in {took:.0f} s "
                  f"(g{m.get('gen')}), not at the next tick (cadence {cadence} s)")
    finally:
        cleanup(stream, rows)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

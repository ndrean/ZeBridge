#!/usr/bin/env python3
"""A tenant born at runtime — no config entry, no bridge restart, no reload.

NOTES.md §9's claim: the bridge's publisher is fully dynamic. Tenants are DATA
(`zebridge_user_tenants`), not config: the bridge derives the tenant list at boot only
for its stream reconciliation. At runtime the subject is built from the ROW's tenant
value, so a backend that (1) creates the `CDC_<TENANT>` stream itself and (2) inserts
the principal→tenant mapping can onboard a tenant with zero config changes.

This proves it end to end against a LIVE bridge, with a tenant deliberately absent
from every mapping the bridge has ever read:

  1. the new tenant's CDC stream is provisioned at runtime (what a backend would do);
  2. a `zebridge_user_tenants` mapping inserted NOW propagates to `$KV.tenants.<principal>`
     with no restart — the roster path is live, not boot-time;
  3. a row written with the new tenant lands in the new stream, on the subject the
     tenant value dictates — the bridge never knew this tenant existed.

⚠️ The stream MUST exist before the write. A tenant-routed row whose subject matches
no stream is the §2.19 failure shape: the publish gets no PubAck, retries, and enough
of that kills the bridge. Step (1) is not setup convenience — it is the contract.

Usage:  python scripts/scenarios/dyntenant.py
Needs a running bridge, a catalogue row for test_types with a tenant_col, NATS_CREDS
(bridge.creds — it provisions a stream), admin psql. Cleans up its streams, mapping
and rows.
"""

import json
import time
import sys

import zb

TENANT = "dynten"
PRINCIPAL = "dynprobe"


def stream_exists(name):
    return zb.nats_cli("stream", "info", name, "--json").returncode == 0


def stream_msgs(name):
    r = zb.nats_cli("stream", "info", name, "--json")
    return json.loads(r.stdout)["state"]["messages"] if r.returncode == 0 else -1


def main():
    failed = 0
    topo = zb.TOPOLOGY
    # A leftover `zb_sweeper` mapping is autogrant residue from a previous run (§10bv:
    # mapping ANY principal to a tenant also maps the sweeper, and this scenario's own
    # step 2 does exactly that). Clean it silently; only a REAL principal disqualifies.
    zb.psql(f"DELETE FROM public.zebridge_user_tenants "
            f"WHERE tenant_id = '{TENANT}' AND principal = 'zb_sweeper'", quiet=True)
    if TENANT in zb.tenants():
        sys.exit(f"'{TENANT}' is already mapped in zebridge_user_tenants — the whole point is that it is not")
    cdc_stream = topo["cdc_streams"]["tenant_prefix"] + TENANT
    cdc_prefix = topo["subjects"]["cdc_prefix"]

    if stream_exists(cdc_stream):
        sys.exit(f"{cdc_stream} already exists — clean up a previous run first")

    uid = __import__("uuid").uuid4()
    try:
        # ── 1. runtime stream provisioning (the backend's half of the contract) ──
        for name, subj in ((cdc_stream, f"{cdc_prefix}.{TENANT}.>"),):
            r = zb.nats_cli("stream", "add", name, f"--subjects={subj}", "--storage=file",
                            "--retention=limits", "--max-age=8d", "--replicas=1", "--defaults")
            if r.returncode != 0:
                sys.exit(f"could not create {name}: {r.stderr.strip()}")
        zb.ok(f"stream {cdc_stream} provisioned at runtime (bridge untouched)")

        # ── 2. the mapping propagates live to $KV.tenants ────────────────────────
        zb.psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) "
                f"VALUES ('{PRINCIPAL}', '{TENANT}') ON CONFLICT DO NOTHING", quiet=True)

        # ── 2b. the tenant is born SWEEPABLE (§10bv) ─────────────────────────────
        # The autogrant trigger maps `zb_sweeper` in the same transaction as the first
        # mapping — a tenant must never exist unswept-by-construction. Before the
        # trigger, this was the hole the audit could only report after the fact.
        swept = zb.psql(f"SELECT count(*) FROM public.zebridge_user_tenants "
                        f"WHERE principal = 'zb_sweeper' AND tenant_id = '{TENANT}'").strip()
        unreaped = zb.psql(f"SELECT count(*) FROM zebridge_audit_sweeper() "
                           f"WHERE tenant_id = '{TENANT}'").strip()
        if swept == "1" and unreaped == "0":
            zb.ok(f"born sweepable: mapping {PRINCIPAL} auto-granted zb_sweeper to '{TENANT}' "
                  "in the same transaction, and the audit has nothing to say")
        else:
            zb.bad(f"the newborn tenant is UNSWEPT (autogrant={swept}, audit rows={unreaped}) "
                   "— its tombstones would accumulate until a DBA notices")
            failed += 1
        deadline = time.time() + 15
        got = None
        while time.time() < deadline:
            r = zb.nats_cli("kv", "get", topo["kv"]["tenants"], PRINCIPAL, "--raw")
            if r.returncode == 0 and r.stdout.strip():
                got = r.stdout.strip()
                break
            time.sleep(0.5)
        if got == TENANT:
            zb.ok(f"$KV.tenants.{PRINCIPAL} → '{TENANT}' with no restart — the roster path is live")
        else:
            zb.bad(f"mapping did not propagate (got {got!r}) — is the bridge running?")
            failed += 1

        # ── 3. a row for the unknown tenant lands in its stream ──────────────────
        before = stream_msgs(cdc_stream)
        zb.psql("INSERT INTO public.test_types (uid,some_text,inserted_at,updated_at,tenant_id) "
                f"VALUES ('{uid}','dyntenant probe',now(),now(),'{TENANT}')", quiet=True)
        deadline = time.time() + 15
        while time.time() < deadline and stream_msgs(cdc_stream) <= before:
            time.sleep(0.5)
        n = stream_msgs(cdc_stream)
        if n > before:
            # --json: the body is msgpack and would blow up a text-mode pipe; the JSON
            # form base64-encodes it and carries the subject as a plain field.
            r = zb.nats_cli("stream", "get", cdc_stream, str(n), "--json")
            subj = json.loads(r.stdout).get("subject", "") if r.returncode == 0 else ""
            subj_ok = subj.startswith(f"{cdc_prefix}.{TENANT}.test_types.insert")
            if subj_ok:
                zb.ok(f"the write landed on {cdc_prefix}.{TENANT}.test_types.insert in {cdc_stream} "
                      "— routed from the ROW's tenant value, no config consulted")
            else:
                zb.bad(f"a message arrived but not on the expected subject:\n{r.stdout[:200]}")
                failed += 1
        else:
            zb.bad(f"nothing reached {cdc_stream} within 15s — check the bridge log for "
                   "publish retries (a missing stream would block, §2.19)")
            failed += 1
    finally:
        zb.psql(f"DELETE FROM public.test_types WHERE uid='{uid}'", quiet=True)
        # Both mappings: ours, and the one the autogrant trigger created from it.
        zb.psql(f"DELETE FROM public.zebridge_user_tenants WHERE principal='{PRINCIPAL}' "
                f"OR (principal = 'zb_sweeper' AND tenant_id = '{TENANT}')", quiet=True)
        time.sleep(1.5)  # let the delete's own events publish before the stream goes
        zb.nats_cli("stream", "rm", cdc_stream, "-f")

    return 1 if failed else 0


sys.exit(main())

#!/usr/bin/env python3
"""A row for a tenant nobody is mapped to yet: skipped, and the bridge keeps running.

A tenant's stream (`CDC_<tenant>`) exists once a principal is mapped to it. A DBA who
seeds a tenant's rows before inviting its first user writes rows whose subject no stream
takes: NATS answers 503. The bridge used to retry that batch until it stopped itself,
restart, meet the same row in the slot, and stop again, publishing nothing for ANY tenant
until someone enrolled into that one (found on the compose stack, 2026-10-11).

Now the event is skipped: nobody can read that tenant yet, and its first device seeds
from a full snapshot built after its stream exists, which holds the row. Checked against
a LIVE bridge:

  1. a row for an unmapped, streamless tenant does not stop the bridge (same uptime, past
     the whole retry budget), and the bridge does not create the stream for it;
  2. the slot is not stuck: a row for a mapped tenant still publishes;
  3. mapping a principal creates the tenant's stream, and its next row lands there.

Usage:  python scripts/scenarios/unmapped_tenant.py
Needs a running bridge on BRIDGE_PORT (27434), a catalogue row for test_types with a
tenant_col, NATS_CREDS (bridge.creds), admin psql. Cleans up its rows, mapping and stream.
"""

import json
import sys
import time
import urllib.request
import uuid

import zb

TENANT = "unmappedten"
PRINCIPAL = "unmappedprobe"


def status():
    """The bridge's /status, or None when it does not answer (it stopped)."""
    try:
        with urllib.request.urlopen(zb.http_base() + "/status", timeout=5) as r:
            return json.loads(r.read())
    except OSError:
        return None


def stream_msgs(name):
    r = zb.nats_cli("stream", "info", name, "--json")
    return json.loads(r.stdout)["state"]["messages"] if r.returncode == 0 else -1


def insert(uid, tenant, text):
    zb.psql("INSERT INTO public.test_types (uid,some_text,inserted_at,updated_at,tenant_id) "
            f"VALUES ('{uid}','{text}',now(),now(),'{tenant}')", quiet=True)


def main():
    failed = 0
    topo = zb.TOPOLOGY
    cdc_stream = topo["cdc_streams"]["tenant_prefix"] + TENANT
    zb.psql(f"DELETE FROM public.zebridge_user_tenants WHERE tenant_id = '{TENANT}'", quiet=True)
    if stream_msgs(cdc_stream) >= 0:
        sys.exit(f"{cdc_stream} exists — clean up a previous run first (nats stream rm {cdc_stream})")
    mapped = [t for t in zb.tenants() if t != TENANT]
    if not mapped:
        sys.exit("no mapped tenant to check the slot with — map one first")
    other = mapped[0]

    rows = [uuid.uuid4() for _ in range(3)]
    try:
        before = status()
        if before is None:
            sys.exit(f"no bridge answers on {zb.http_base()} — start one first")
        # ── 1. a row for the unmapped, streamless tenant ─────────────────────────
        insert(rows[0], TENANT, "unmapped tenant probe")
        # The retry budget is 6 tries with backoff 100..1600 ms (~3 s); a bridge that gives
        # up runs it again for its final flush while still answering /status: wait well
        # past both.
        time.sleep(20)
        after = status()
        if after is None:
            zb.bad(f"the bridge stopped after a row for the unmapped tenant '{TENANT}'")
            return 1
        else:
            if after["uptime_seconds"] >= before["uptime_seconds"] + 19:
                zb.ok(f"the bridge kept running past the retry budget (uptime {before['uptime_seconds']} → "
                      f"{after['uptime_seconds']} s): the row for '{TENANT}' did not stop it")
            else:
                zb.bad(f"the bridge restarted (uptime {before['uptime_seconds']} → {after['uptime_seconds']} s)")
                failed += 1
        if stream_msgs(cdc_stream) < 0:
            zb.ok(f"{cdc_stream} was not created for a tenant nobody is mapped to")
        else:
            zb.bad(f"{cdc_stream} exists: the bridge made a stream nobody can read")
            failed += 1

        # ── 2. the slot is not stuck behind it ───────────────────────────────────
        def published_now():
            st = status()
            return st["cdc_events_published"] if st else -1

        published = published_now()
        insert(rows[1], other, "slot liveness probe")
        deadline = time.time() + 15
        while time.time() < deadline and published_now() <= published:
            time.sleep(0.5)
        if published_now() > published:
            zb.ok(f"a row for the mapped tenant '{other}' still publishes: the slot moved past the skipped row")
        else:
            zb.bad(f"nothing published within 15 s after the skipped row: the slot is stuck")
            failed += 1

        # ── 3. the first mapping creates the stream; the next row lands in it ─────
        zb.psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) "
                f"VALUES ('{PRINCIPAL}', '{TENANT}') ON CONFLICT DO NOTHING", quiet=True)
        deadline = time.time() + 20
        while time.time() < deadline and stream_msgs(cdc_stream) < 0:
            time.sleep(0.5)
        if stream_msgs(cdc_stream) < 0:
            zb.bad(f"{cdc_stream} not created within 20 s of the first mapping")
            failed += 1
        else:
            n0 = stream_msgs(cdc_stream)
            insert(rows[2], TENANT, "after the first mapping")
            deadline = time.time() + 15
            while time.time() < deadline and stream_msgs(cdc_stream) <= n0:
                time.sleep(0.5)
            if stream_msgs(cdc_stream) > n0:
                zb.ok(f"the first mapping created {cdc_stream}, and the tenant's next row landed in it")
            else:
                zb.bad(f"{cdc_stream} exists but the tenant's next row never reached it")
                failed += 1
    finally:
        zb.psql("DELETE FROM public.test_types WHERE uid IN (" + ",".join(f"'{u}'" for u in rows) + ")", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_user_tenants WHERE tenant_id = '{TENANT}'", quiet=True)
        time.sleep(1.5)  # let the deletes publish before the stream goes
        zb.nats_cli("stream", "rm", cdc_stream, "-f")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

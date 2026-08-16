#!/usr/bin/env python3
"""One process, one NATS address.

Regression probe for a split brain: three components each resolved "where is NATS"
their own way. The publisher parsed `NATS_URL`; the snapshot listener borrowed the
publisher's parsed result; the mutation listener read `NATS_HOST` raw **and passed no
port**, so it dialled `NATS_HOST:4222` whatever the URL said.

Set `NATS_HOST=nats-server` (a name that resolves only inside compose) beside a working
`NATS_URL` and the bridge came up looking healthy — CDC flowing, snapshots served — with
ingress alone dead on `HostResolutionFailed`. That is the configuration this reproduces.

Needs a writer role for the mutation listener to start at all; without one the ingress
half of the check is skipped rather than silently passing.

Usage:  python scripts/scenarios/endpoint.py
"""

import os
import sys

import zb


async def main():
    log = "/tmp/zb_endpoint.log"
    bogus = os.environ.get("ZB_BOGUS_HOST", "nats-server")

    print(f"\nstarting a bridge with NATS_HOST={bogus} and NATS_URL={zb.NATS_URL}")
    with zb.Bridge(log, NATS_HOST=bogus, NATS_URL=zb.NATS_URL, LOG_LEVEL="debug") as br:
        ingress = bool(os.environ.get("POSTGRES_WRITER_USER") or os.environ.get("DATABASE_WRITER_URL"))
        needle = "Mutation listener: ✅ Ready" if ingress else "Snapshot listener: ✅ Consuming"
        reached = br.wait_for_log(needle, timeout=40)
        text = br.text()

    failed = 0
    if f"NATS endpoint: " not in text:
        zb.bad("the bridge never logged a resolved endpoint")
        return 1

    line = next(l for l in text.splitlines() if "NATS endpoint:" in l)
    print(f"  {line.split('info(bridge): ')[-1]}")

    # The deprecation warning names the bogus host on purpose, so it is not evidence of
    # anyone dialling it; every other mention would be.
    dialled = [l for l in text.splitlines() if bogus in l and "is ignored" not in l]
    if dialled:
        zb.bad(f"'{bogus}' reached a connection path — something resolves NATS_HOST on its own")
        for l in dialled:
            print(f"    {l}")
        failed = 1
    else:
        zb.ok(f"no component dialled '{bogus}' — NATS_URL won everywhere")

    # Logged at debug, so this probe forces LOG_LEVEL=debug: on a correctly split
    # environment the variable is absent and warning at every start would cry wolf.
    if f"NATS_HOST='{bogus}' is ignored" in text:
        zb.ok("and the dead variable is called out rather than silently dropped")
    else:
        zb.bad("NATS_HOST was set and the log said nothing (LOG_LEVEL=debug needed)")
        failed = 1

    if "HostResolutionFailed" in text:
        zb.bad("HostResolutionFailed — a component is still resolving its own address")
        failed = 1

    if not ingress:
        print("  ⓘ  ingress skipped: no POSTGRES_WRITER_USER/DATABASE_WRITER_URL set")
    elif reached:
        zb.ok("the mutation listener — the component that had it wrong — connected")
    else:
        zb.bad("the mutation listener never became ready")
        failed = 1

    return failed


zb.run(main)

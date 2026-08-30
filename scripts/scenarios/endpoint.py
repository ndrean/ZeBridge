#!/usr/bin/env python3
"""One process, one NATS address.

Regression probe for a split brain: the components each resolved "where is NATS"
their own way. The publisher parsed `NATS_URL`; the mutation listener read `NATS_HOST`
raw **and passed no port**, so it dialled `NATS_HOST:4222` whatever the URL said.

Set `NATS_HOST=nats-server` (a name that resolves nowhere on the native stack) beside
a working `NATS_URL` and the bridge came up looking healthy — CDC flowing — with
ingress alone dead on `HostResolutionFailed`. That is the configuration this reproduces.

The mutation listener — the component that had it wrong — only starts with a writer
role, so `DATABASE_WRITER_URL` is REQUIRED here: without it the probe would pass on the
publisher alone and prove nothing about the path under test.

Usage:  python scripts/scenarios/endpoint.py   (probe-bridge env: .env.bridge + bridge.creds)
"""

import os
import sys

import zb


async def main():
    log = "/tmp/zb_endpoint.log"
    bogus = os.environ.get("ZB_BOGUS_HOST", "nats-server")

    if not os.environ.get("DATABASE_WRITER_URL"):
        sys.exit(
            "DATABASE_WRITER_URL is not set — the mutation listener is the component this "
            "probe exists for, and it does not start without a writer role.\n"
            "  set -a && . ./.env.bridge && set +a"
        )

    print(f"\nstarting a bridge with NATS_HOST={bogus} and NATS_URL={zb.NATS_URL}")
    with zb.Bridge(log, NATS_HOST=bogus, NATS_URL=zb.NATS_URL, LOG_LEVEL="debug") as br:
        # The listener's own ready line; the endpoint line is checked separately below.
        reached = br.wait_for_log("Mutation listener: ✅ Ready", timeout=40)
        text = br.text()

    failed = 0
    if "NATS endpoint:" not in text:
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

    if reached:
        zb.ok("the mutation listener — the component that had it wrong — connected")
    else:
        zb.bad("the mutation listener never became ready")
        failed = 1

    return failed


zb.run(main)

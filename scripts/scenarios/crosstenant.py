#!/usr/bin/env python3
"""Can I read another tenant's data with my own credentials — by ANY primitive?

The documented path is a tenant-scoped consumer, and it behaves. That proves nothing. A
consumer is not a workflow, it is a client library with an API surface, and the question a
security model has to answer is not "does the intended route stay in its lane" but **"what
happens when someone takes a different route with the same credentials"**.

⚠️ This scenario is expected to FAIL until per-tenant streams land. That is the point: it
records a known hole as a test rather than as a conversation. The specific finding it exists
to pin down —

    a subject permission (`cdc.acme.>`) governs core SUBSCRIBE and nothing else.
    A JetStream consumer never subscribes to the subject it filters on: the server
    delivers to the reader's own _INBOX, and `filter_subject` is a stream-side
    selector the reader chooses. So the ACL is never consulted for it.

— which means the enforcement unit for reads is the **stream**, not the subject, and
`cdc.<tenant>.>` grants protect a path this client does not take.

Measured before per-tenant streams: alice (tenant `acme`) was refused a core subscription to
`cdc.globex.>` and then pulled three `cdc.globex.test_types.insert` messages through a
JetStream consumer, with the same credentials, in the same session.

Usage:
    NATS_URL=nats://alice:s3cret@127.0.0.1:4222 \
        python scripts/scenarios/crosstenant.py [victim_tenant]

Defaults to probing `globex` as whoever `NATS_URL` authenticates as.
"""

import asyncio
import sys

import nats
from nats.js.api import ConsumerConfig, DeliverPolicy

import zb

VICTIM = sys.argv[1] if len(sys.argv) > 1 else "globex"

leaks = []
async_errors = []


async def on_error(e):
    async_errors.append(str(e))


def leak(primitive, detail):
    leaks.append(primitive)
    zb.bad(f"{primitive}: REACHABLE")
    for line in detail.splitlines():
        print(f"      {line}")


def blocked(primitive, how):
    zb.ok(f"{primitive}: refused ({how})")


def principal() -> str:
    rest = zb.NATS_URL.split("://", 1)[-1]
    return rest.rsplit("@", 1)[0].split(":", 1)[0] if "@" in rest else "?"


async def main():
    who = principal()
    print(f"probing tenant '{VICTIM}' as principal '{who}'\n")

    nc = await nats.connect(zb.NATS_URL, error_cb=on_error)
    js = nc.jetstream()
    subject = f"cdc.{VICTIM}.>"

    # ── A. core subscribe — the only primitive a subject ACL actually governs ──
    async_errors.clear()
    try:
        await nc.subscribe(subject)
        await nc.flush(timeout=2)
    except Exception:
        pass
    await asyncio.sleep(0.5)
    if any("ermission" in e for e in async_errors):
        blocked(f"core SUB {subject}", "Permissions Violation")
    else:
        leak(f"core SUB {subject}",
             "The subscribe allow-list does not bound this principal to its own tenant.")

    # ── B. JetStream consumer on the SHARED stream, filtered at another tenant ─
    #
    # The primitive the real client uses. If this succeeds while A is refused, the subject
    # ACL is decorative for reads.
    # ⚠️ "I cannot reach it" and "it does not exist" are indistinguishable from a client —
    # both surface as a timeout, because a permissions violation on a request subject means
    # no reply rather than an error. Scoring them the same makes this test wrong in one
    # direction or the other: before the split it called a missing stream a refusal, after
    # the split it would call a genuine refusal inconclusive.
    #
    # zebridge_user_tenants declares which streams a deployment is supposed to have (the same list the bridge reconciles at boot), so it resolves
    # the ambiguity without needing admin credentials.
    prefix = (zb.TOPOLOGY.get("cdc_streams") or {}).get("tenant_prefix", "CDC_")
    declared = {t: f"{prefix}{t}" for t in zb.tenants()}
    victim_stream = declared.get(VICTIM, f"{prefix}{VICTIM}")
    victim_stream_declared = VICTIM in declared
    victim_stream_exists = None          # None = unknown, and unknown is not "safe"
    for stream in ("CDC", victim_stream):
        try:
            await js.stream_info(stream)
            if stream == victim_stream:
                victim_stream_exists = True
        except Exception:
            if stream == victim_stream:
                victim_stream_exists = False
            print(f"  – stream {stream} not reachable here — skipped")
            continue
        try:
            sub = await js.pull_subscribe(subject, durable=None, stream=stream)
            msgs = await sub.fetch(1, timeout=3)
            subs = ", ".join(m.subject for m in msgs)
            for m in msgs:
                await m.nak()
            leak(f"JS consumer on {stream} filtered {subject}",
                 f"pulled {len(msgs)} message(s): {subs}\n"
                 "filter_subject is chosen by the reader and is not a permission. The\n"
                 "enforcement unit is the stream, so this needs per-tenant streams.")
        except nats.errors.TimeoutError:
            # No message to read is not the same as being refused. Say so rather than
            # scoring an empty stream as a pass — that is how a test starts lying.
            print(f"  ⓘ  JS consumer on {stream} filtered {subject}: created, but nothing "
                  f"to pull (inconclusive — write a row for '{VICTIM}' and re-run)")
        except Exception as e:
            blocked(f"JS consumer on {stream} filtered {subject}", f"{type(e).__name__}: {str(e)[:60]}")

    # ── C. reading the other tenant's stream directly ─────────────────────────
    # ⚠️ A timeout is only a refusal if there is something there to refuse. Before the
    # per-tenant split `CDC_GLOBEX` does not exist, and scoring "no answer" as blocked would
    # make this line turn green now and stay green after the stream appears — the test would
    # start lying at exactly the moment it began to matter.
    if victim_stream_exists is False and victim_stream_declared:
        # Declared in grammar.json but unreachable by this principal: that is the split
        # working, not an absent stream.
        blocked(f"stream_info {victim_stream}", "declared in grammar.json but not reachable")
    elif victim_stream_exists is False:
        print(f"  ⓘ  stream_info {victim_stream}: not declared in grammar.json — N/A, not a pass")
    else:
        try:
            info = await js.stream_info(victim_stream)
            leak(f"stream_info {victim_stream}",
                 f"can read another tenant's stream metadata: {info.state.messages} messages")
        except Exception as e:
            blocked(f"stream_info {victim_stream}", type(e).__name__)

    # ── D. RETIRED (2026-08-27): the snapshot path ────────────────────────────
    # Snapshot-on-demand is gone (NOTES §10h/§10n); seeding reads generation
    # chains from the gen-<tenant> object stores, whose per-tenant isolation is
    # what tenant scoping now means on the read path.

    await nc.close()

    print()
    if leaks:
        print(f"\033[31m{len(leaks)} primitive(s) reach tenant '{VICTIM}'\033[0m")
        print("A rule that only holds for the documented primitive is a convention, not a control.\n")
        return 1
    print(f"\033[32mno primitive reached tenant '{VICTIM}'\033[0m\n")
    return 0


zb.run(main)

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

Exit code: only the probes that ARE supposed to be enforced today count — the
per-tenant CDC stream (C), the per-tenant generation object store (D'), and core
SUBSCRIBE (A). The JetStream-consumer-on-a-shared-stream probe (B) is the known
hole this file exists to record; it is printed loudly but does not fail the run.

Usage:
    NATS_CREDS=scripts/native/creds/alice.creds \
        python scripts/scenarios/crosstenant.py [victim_tenant]

Defaults to probing `globex` as the principal NATS_CREDS / ZB_PRINCIPAL names.
"""

import asyncio
import sys

import nats

import zb

VICTIM = sys.argv[1] if len(sys.argv) > 1 else "globex"

leaks = []            # every primitive that reached the victim
enforced_leaks = []   # the subset that is supposed to be closed TODAY → exit code
async_errors = []


async def on_error(e):
    async_errors.append(str(e))


def leak(primitive, detail, enforced=True):
    leaks.append(primitive)
    if enforced:
        enforced_leaks.append(primitive)
        zb.bad(f"{primitive}: REACHABLE")
    else:
        print(f"  \033[33m!\033[0m {primitive}: REACHABLE (known hole, recorded, not a verdict)")
    for line in detail.splitlines():
        print(f"      {line}")


def blocked(primitive, how):
    zb.ok(f"{primitive}: refused ({how})")


async def main():
    who = zb.require_principal()
    mine = zb.tenant_of(who)
    print(f"probing tenant '{VICTIM}' as principal '{who}' (tenant '{mine}')\n")
    if mine == VICTIM:
        sys.exit(f"'{who}' IS in tenant '{VICTIM}' — pick a victim tenant this principal is not mapped to")

    # connected as the CLIENT, with its creds — as the bridge every probe below is allowed
    nc = await nats.connect(zb.nats_server(), user_credentials=zb.creds_for(who), error_cb=on_error)
    js = nc.jetstream()
    subject = f"{zb.TOPOLOGY['subjects']['cdc_prefix']}.{VICTIM}.>"

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

    # ── B. JetStream consumer on the victim's stream, filtered at another tenant ─
    #
    # The primitive the real client uses. If this succeeds while A is refused, the subject
    # ACL is decorative for reads. (The monolithic "CDC" stream is gone — per-tenant
    # streams are the enforcement unit, so only the victim's stream is probed.)
    # ⚠️ "I cannot reach it" and "it does not exist" are indistinguishable from a client —
    # both surface as a timeout, because a permissions violation on a request subject means
    # no reply rather than an error. Scoring them the same makes this test wrong in one
    # direction or the other: before the split it called a missing stream a refusal, after
    # the split it would call a genuine refusal inconclusive.
    #
    # The tenant list is DATA — zebridge_user_tenants (grammar.json carries no tenants;
    # the bridge derives the same list for its stream reconciliation) — so it resolves
    # the ambiguity without needing admin credentials.
    prefix = (zb.TOPOLOGY.get("cdc_streams") or {}).get("tenant_prefix", "CDC_")
    declared = {t: f"{prefix}{t}" for t in zb.tenants()}
    victim_stream = declared.get(VICTIM, f"{prefix}{VICTIM}")
    victim_stream_declared = VICTIM in declared
    victim_stream_exists = None          # None = unknown, and unknown is not "safe"
    for stream in (victim_stream,):
        try:
            await js.stream_info(stream)
            victim_stream_exists = True
        except Exception:
            victim_stream_exists = False
            print(f"  – stream {stream} not reachable here — consumer probe skipped (see C)")
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
                 "enforcement unit is the stream — reaching a consumer here means the\n"
                 "per-tenant stream grant is wider than the tenant.",
                 enforced=False)
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
        # A tenant in zebridge_user_tenants (so the bridge reconciled its stream) but
        # unreachable by this principal: that is the split working, not an absent stream.
        blocked(f"stream_info {victim_stream}", "tenant exists in zebridge_user_tenants but the stream is not reachable")
    elif victim_stream_exists is False:
        print(f"  ⓘ  stream_info {victim_stream}: '{VICTIM}' is not in zebridge_user_tenants — N/A, not a pass")
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
    # what tenant scoping now means on the read path — probed next.

    # ── D'. the victim's generation object store ──────────────────────────────
    #
    # The seed path: chain objects live in OBJ_gen-<tenant> and the JWT grants
    # `$JS.API.DIRECT.GET.OBJ_gen-{{tag(tenant)}}.>` / CONSUMER.CREATE on ITS OWN
    # bucket only. Every API request to the victim's bucket must be denied — and
    # a denial on a request subject is a TIMEOUT, so the bucket's existence is
    # established first (bridge-side reconciliation creates one per tenant).
    gen_prefix = (zb.TOPOLOGY.get("generations") or {}).get("bucket_prefix", "gen-")
    victim_obj = f"OBJ_{gen_prefix}{VICTIM}"
    if not victim_stream_declared:
        print(f"  ⓘ  {victim_obj}: '{VICTIM}' is not in zebridge_user_tenants — N/A, not a pass")
    else:
        # stream-level reach: STREAM.INFO on the victim's bucket
        try:
            info = await asyncio.wait_for(js.stream_info(victim_obj), timeout=3)
            leak(f"STREAM.INFO {victim_obj}",
                 f"can read another tenant's generation store metadata: {info.state.messages} chunk(s)")
        except Exception as e:  # noqa: BLE001
            blocked(f"STREAM.INFO {victim_obj}", type(e).__name__)
        # DIRECT.GET: the exact request subject the seed client uses
        try:
            r = await nc.request(f"$JS.API.DIRECT.GET.{victim_obj}",
                                 b'{"last_by_subj":"$O.' + f"{gen_prefix}{VICTIM}".encode() + b'.M.>"}',
                                 timeout=3)
            body = r.data[:80]
            if r.headers and r.headers.get("Status") in ("404", "408"):
                leak(f"DIRECT.GET {victim_obj}", f"the request was ANSWERED ({r.headers.get('Status')}) — reachable, empty")
            else:
                leak(f"DIRECT.GET {victim_obj}", f"answered: {body!r}")
        except Exception as e:  # noqa: BLE001
            blocked(f"DIRECT.GET {victim_obj}", f"{type(e).__name__} (no reply = denied on the request subject)")
        # CONSUMER.CREATE: the other read primitive on an object store
        # Two timeouts mean opposite things: one from pull_subscribe is the CREATE request
        # never answered (denied on the request subject — the grant held); one from fetch
        # AFTER a successful subscribe is a consumer that exists on an empty bucket — a
        # reach. Conflating them scored a held grant as a leak.
        sub = None
        try:
            sub = await js.pull_subscribe(f"$O.{gen_prefix}{VICTIM}.>", durable=None, stream=victim_obj)
        except Exception as e:  # noqa: BLE001
            blocked(f"CONSUMER.CREATE {victim_obj}", f"{type(e).__name__} (no reply = denied on the request subject)")
        if sub is not None:
            try:
                msgs = await sub.fetch(1, timeout=2)
                for m in msgs:
                    await m.nak()
                leak(f"CONSUMER.CREATE {victim_obj}", f"a consumer was created on another tenant's store ({len(msgs)} msg pulled)")
            except nats.errors.TimeoutError:
                leak(f"CONSUMER.CREATE {victim_obj}", "consumer created, nothing to pull — the grant is still too wide")

    await nc.close()

    print()
    if leaks:
        print(f"\033[31m{len(leaks)} primitive(s) reach tenant '{VICTIM}'\033[0m"
              f" — {len(enforced_leaks)} of them on a path that is supposed to be closed")
        print("A rule that only holds for the documented primitive is a convention, not a control.\n")
        return 1 if enforced_leaks else 0
    print(f"\033[32mno primitive reached tenant '{VICTIM}'\033[0m\n")
    return 0


zb.run(main)

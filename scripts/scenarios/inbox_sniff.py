#!/usr/bin/env python3
"""The reply inbox is a read boundary: can I listen to what another principal receives?

JetStream does not deliver a pulled message, a KV answer or an object chunk on the
subject the reader filtered on. It delivers to the reader's own **inbox**, and the
subject ACL is never consulted for it. So while every client shared one inbox space —
`_INBOX.>`, the default of every NATS client — a principal could subscribe there and
read what every other principal was being handed. Measured on this stack before the
fix (NOTES §10fs, §10hm): bob captured acme's CDC rows, and a client credential
captured the bridge's own JetStream traffic.

The fix has two halves and this scenario checks both:

  A  the GRANT: a client may subscribe only to `_INBOX.<its own name>.>`; the shared
     space and another principal's subtree are refused by the server;
  B  the CLIENTS: every one of them sets its inbox prefix to `_INBOX.<principal>`, so
     a real request still completes under that narrower grant. A client that kept the
     default would be silently unable to receive any reply.

⚠️ A refusal here is a *server* verdict, not an exception: nats-py reports a
permissions violation through the error callback and the subscription simply never
receives. Both are collected.

Usage:
    ZB_PRINCIPAL=omar scripts/scenarios/.venv/bin/python scripts/scenarios/inbox_sniff.py
"""
import asyncio, sys
import nats
import zb


async def main() -> int:
    failed = 0
    me = zb.require_principal()
    others = [p for p in ("alice", "bob", "mary", "nina", "omar", "pois") if p != me]

    violations: list[str] = []

    async def on_error(e):
        violations.append(str(e).lower())

    nc = await nats.connect(zb.nats_server(), user_credentials=zb.creds_for(me), error_cb=on_error,
                            inbox_prefix=f"_INBOX.{me}".encode())

    # ── A. the grant ────────────────────────────────────────────────────────
    refused_shared = await _refused(nc, violations, "_INBOX.>")
    if refused_shared:
        zb.ok(f"'{me}' may NOT subscribe to the shared inbox space `_INBOX.>`")
    else:
        zb.bad(f"'{me}' subscribed to `_INBOX.>` — every principal's replies are readable"); failed += 1

    victim = others[0]
    refused_other = await _refused(nc, violations, f"_INBOX.{victim}.>")
    if refused_other:
        zb.ok(f"'{me}' may NOT subscribe to '{victim}'s inbox `_INBOX.{victim}.>`")
    else:
        zb.bad(f"'{me}' subscribed to `_INBOX.{victim}.>` — another principal's replies are readable"); failed += 1

    allowed_own = not await _refused(nc, violations, f"_INBOX.{me}.>")
    if allowed_own:
        zb.ok(f"'{me}' may subscribe to its OWN inbox `_INBOX.{me}.>`")
    else:
        zb.bad(f"'{me}' cannot subscribe to its own inbox — the grant does not match the prefix"); failed += 1

    # ── B. the clients ──────────────────────────────────────────────────────
    # The inbox a client generates must be inside its own grant, and a reply must
    # still arrive. `$JS.API.INFO` is the shortest round trip that uses the very
    # path the hole was about: the server answers on the reader's inbox.
    generated = nc.new_inbox()
    if generated.startswith(f"_INBOX.{me}."):
        zb.ok(f"the inbox this client generates is inside its grant ({generated})")
    else:
        zb.bad(f"the client generated {generated} — outside `_INBOX.{me}.`, replies will never arrive"); failed += 1

    try:
        reply = await nc.request("$JS.API.INFO", b"", timeout=5)
        answered = b"type" in reply.data
    except Exception as e:
        answered = False
        zb.bad(f"the JetStream round trip failed under the narrowed grant: {type(e).__name__} {e}"); failed += 1
    if answered:
        zb.ok("a JetStream reply still reaches this client under the narrowed grant")
    elif failed == 0:
        zb.bad("the JetStream reply did not arrive"); failed += 1

    await nc.close()
    return failed


async def _refused(nc, violations: list[str], subject: str) -> bool:
    """Subscribe and let the server answer. A refusal arrives on the error callback."""
    before = len(violations)
    try:
        await nc.subscribe(subject)
        await nc.flush()
    except Exception:
        return True
    await asyncio.sleep(0.4)
    return any(subject.lower() in v for v in violations[before:])


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

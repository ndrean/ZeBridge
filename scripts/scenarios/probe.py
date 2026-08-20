#!/usr/bin/env python3
"""Probing a table you can see but cannot touch — denied, and denied exactly once.

Schema descriptors are published to `$KV.schemas.>` for **every** table, and every
principal is granted the whole bucket. That is a deliberate call (NOTES.md §1.12): knowing
`orders` has a `price` column is a far smaller disclosure than its rows. But it hands an
attacker a well-formed message for a table they have no business writing — "what if I push
a bomb through something that seems to exist, just to see how the other side handles it?"

The disclosure is only defensible if the refusal is. What has to hold is **not** that the
probe is denied — `writable.py` already asserts that, including the row not landing and the
verdict naming the right SQLSTATE. What is missing, and what this scenario adds, is the
other half:

  1. the **premise** is real — the schema of a table this principal cannot write is in fact
     readable by it. Untested anywhere, and the whole concern rests on it;
  2. the refusal is **definitive**, carrying a status a client can act on;
  3. the refusal arrives **exactly once**. This is the one that matters. PROTOCOL.md §7.1
     says a client pops its queue only on a definitive reply, so a probe answered with
     *silence* is retried forever, and a probe answered *repeatedly* is a broker amplifying
     an attacker's single message. Either way the prober pays once and the system pays
     indefinitely.

⚠️ Deliberately runs as a **client principal**, not the bridge nkey — the bridge is allowed
to do everything this scenario exists to catch:

    NATS_URL=nats://alice:s3cret@127.0.0.1:4222 python scripts/scenarios/probe.py [table]

Defaults to `users`, the outbound-only fixture: published (so its schema is disclosed),
never granted (so every write must be refused).
"""

import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import msgpack
import zb

TABLE = sys.argv[1] if len(sys.argv) > 1 else "users"

# Long enough to outlast the bridge's retry budget, so "one verdict" is a claim about the
# whole delivery lifecycle and not about the first second of it.
WINDOW = float(os.environ.get("ZB_PROBE_WINDOW", "12"))
# A second, quieter window: a verdict arriving *here* means the message is still being
# redelivered after it was already answered.
QUIET = float(os.environ.get("ZB_PROBE_QUIET", "8"))


def principal() -> str:
    rest = zb.NATS_URL.split("://", 1)[-1]
    return rest.rsplit("@", 1)[0].split(":", 1)[0] if "@" in rest else "alice"


async def main():
    failed = 0
    who = principal()
    if not any(c in zb.NATS_URL for c in "@"):
        print("⚠️  No principal in NATS_URL — this scenario is meaningless as the bridge.\n"
              "    NATS_URL=nats://alice:s3cret@127.0.0.1:4222\n")

    nc = await zb.connect()
    js = nc.jetstream()

    # ── 1. the premise: is the schema actually disclosed to this principal? ────
    #
    # Reported as a finding, not a pass/fail. A deployment that has tightened the schema
    # bucket has removed the exposure rather than failed the test — but then the rest of
    # this scenario is testing a probe nobody can construct, so say so and stop.
    try:
        kv = await js.key_value(zb.TOPOLOGY["kv"]["schemas"])
        entry = await kv.get(TABLE)
        cols = json.loads(entry.value.decode())["pg"]["columns"]
        zb.ok(f"schema for '{TABLE}' is readable by '{who}': {len(cols)} columns disclosed")
        print(f"     → enough to build a well-formed write: {[c['name'] for c in cols][:4]}…")
    except Exception as err:
        print(f"  – schema for '{TABLE}' is NOT readable by '{who}' ({type(err).__name__})")
        print("    The disclosure this scenario is about does not exist here. Nothing to probe.")
        await nc.close()
        return 0

    # ── the probe ─────────────────────────────────────────────────────────────
    verdicts = []
    sub = await nc.subscribe(
        f"{zb.TOPOLOGY['subjects']['mutation_ack_prefix']}.{who}.>"
    )

    async def collect():
        async for msg in sub.messages:
            try:
                verdicts.append(json.loads(msg.data.decode()))
            except Exception:
                verdicts.append({"status": "<undecodable>"})

    task = asyncio.create_task(collect())

    version = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
    row_id = str(uuid.uuid4())
    # Dots are stripped: the id becomes a subject token in `mutation_ack.<who>.<msg_id>`.
    msg_id = f"probe-{row_id}".replace(".", "-")
    subject = zb.subject(
        zb.TOPOLOGY["subjects"]["mutations_prefix"], who, TABLE, "insert"
    )
    await js.publish(
        subject,
        msgpack.packb({
            "key": {"id": row_id},
            "data": {"id": row_id, "name": "probe", "email": "probe@example.com",
                     "inserted_at": version, "updated_at": version},
            "version": version,
            "client_id": "probe",
        }),
        headers={"Nats-Msg-Id": msg_id},
    )
    print(f"\n  probed {subject}  (msg_id {msg_id})")

    await asyncio.sleep(WINDOW)
    first = len(verdicts)
    await asyncio.sleep(QUIET)
    later = len(verdicts)
    task.cancel()
    await nc.close()

    print(f"  verdicts in {WINDOW:.0f}s: {first}   ... and {later - first} more in the next {QUIET:.0f}s\n")

    # ── 2. definitive, not silence ────────────────────────────────────────────
    if first == 0:
        zb.bad("no verdict at all — the probe was swallowed. A client following §7.1 "
               "retries this forever, so silence is the expensive answer, not the safe one.")
        failed += 1
    else:
        v = verdicts[0]
        status = str(v.get("status", "")).lower()
        if status in ("rejected", "failed"):
            zb.ok(f"refused definitively: status={status} reason={v.get('reason')!r} "
                  f"sqlstate={v.get('sqlstate')!r}")
        elif status == "accepted":
            zb.bad(f"'{TABLE}' ACCEPTED a probe from '{who}' — it is not outbound-only")
            failed += 1
        else:
            zb.bad(f"verdict carries no actionable status: {v!r}")
            failed += 1

    # ── 3. once, not a stream ─────────────────────────────────────────────────
    if first > 1:
        zb.bad(f"{first} verdicts for one probe — a single message is being amplified "
               "into a stream of replies")
        failed += 1
    elif later > first:
        zb.bad(f"{later - first} further verdict(s) after the answer — the message is "
               "still being redelivered once it was already refused")
        failed += 1
    elif first == 1:
        zb.ok("answered exactly once, and stayed quiet afterwards")

    # ── 4. and it changed nothing ─────────────────────────────────────────────
    landed = zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE name = 'probe'").strip()
    if landed == "0":
        zb.ok(f"no row reached '{TABLE}'")
    else:
        zb.bad(f"{landed} probe row(s) reached '{TABLE}'")
        failed += 1
        zb.psql(f"DELETE FROM public.{TABLE} WHERE name = 'probe'", quiet=True)

    return 1 if failed else 0


zb.run(main)

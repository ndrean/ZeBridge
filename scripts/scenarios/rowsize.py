#!/usr/bin/env python3
"""A row the change feed cannot carry must cost its sender a verdict, not cost everyone the table.

CDC packs each row into a fixed `2^BASE_BUF` buffer. NATS accepts up to `max_payload`,
which is far larger — 16 KiB against 1 MiB by default. Between the two sits every row a
client may legally write and the change feed cannot carry.

⚠️ **Before the ingress check, that gap was a denial of service any authorised writer could
mount.** Measured: one client sent a legal 50 KB row, PostgreSQL stored it, the client was
told **`accepted`** — and the CDC echo then suspended the table for every other consumer.
One bad write, everybody's table.

That is the wrong shape of failure. A table is suspended when its **shape** is wrong — no
primary key, an undecodable column type — which is a migration, and no client can cause it.
A bad *write* should cost its sender a "no thank you".

Four things checked:

  1. an oversized mutation is **refused**, and no row is written;
  2. **no table is suspended** by it — the failure stays with the sender;
  3. the limit is **published** as `max_row_bytes`, so a client can check before sending
     rather than discovering by rejection;
  4. a row just under the limit still **applies** — the guard is a ceiling, not a tax.

⚠️ The size is measured from what NATS delivered, never from a field the sender supplies:
the sender is exactly who it guards against. Same rule as `client_id` (stamped from the
envelope) and the principal (a subject token the broker vouched for).

Usage:  python scripts/scenarios/rowsize.py [table]

    NATS_CREDS=scripts/native/creds/omar.creds python scripts/scenarios/rowsize.py

Runs as a CLIENT principal against the long-running bridge (its /metrics is read for the
suspension check).
"""

import asyncio
import json
import sys
import urllib.request
import uuid
from datetime import datetime, timezone

import msgpack
import zb

TABLE = sys.argv[1] if len(sys.argv) > 1 else "test_types"


def schema(table: str) -> dict:
    """The schema descriptor from `$KV.schemas.<table>`, read with whatever credential
    the run carries — the bucket is granted to every principal."""
    raw = zb.kv_get("schemas", table)
    return json.loads(raw) if raw else {}


def refused_tables() -> float | None:
    try:
        with urllib.request.urlopen(f"{zb.http_base()}/metrics", timeout=4) as r:
            for line in r.read().decode().splitlines():
                if line.startswith("bridge_refused_tables "):
                    return float(line.split()[1])
    except Exception:  # noqa: BLE001
        return None
    return None


async def main():
    failed = 0
    who = zb.require_principal()

    # ── 3. the limit must be discoverable ──────────────────────────────────────
    #
    # ⚠️ Checked first, because everything below depends on it: a client cannot size a
    # write against a number it has no way to read. BASE_BUF is a deployment setting, so
    # hardcoding it in a client is wrong the moment an operator raises it.
    desc = schema(TABLE)
    limit = desc.get("max_row_bytes")
    if not isinstance(limit, int) or limit <= 0:
        sys.exit(
            f"the schema for '{TABLE}' publishes no usable `max_row_bytes` (got {limit!r}).\n"
            "  Without it a client has no way to check a write before sending."
        )
    zb.ok(f"the limit is published: max_row_bytes={limit:,}")

    required = [
        c for c in zb.psql(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{TABLE}' AND is_nullable='NO' AND column_default IS NULL"
        ).splitlines() if c
    ]
    tenant = zb.tenant_of(who)

    nc = await zb.connect_as(who)
    js = nc.jetstream()
    verdicts: dict[str, dict] = {}
    ack_prefix = zb.TOPOLOGY["subjects"]["mutation_ack_prefix"]
    sub = await nc.subscribe(f"{ack_prefix}.{who}.*")

    async def collect():
        # Verdicts are JSON on the wire.
        async for m in sub.messages:
            try:
                verdicts[m.subject.rsplit(".", 1)[-1]] = json.loads(m.data.decode())
            except Exception as err:  # noqa: BLE001
                verdicts[m.subject.rsplit(".", 1)[-1]] = {"status": "<undecodable>", "error": str(err)}

    task = asyncio.create_task(collect())
    await asyncio.sleep(0.5)

    async def send(text: str) -> tuple[str, dict | None]:
        uid = str(uuid.uuid4())
        v = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
        data = {"uid": uid, "some_text": text, "inserted_at": v, "updated_at": v}
        for c in required:
            if c == "tenant_id" and tenant:
                data[c] = tenant
            elif c not in data:
                data[c] = v
        msg_id = f"size-{uuid.uuid4().hex[:10]}"
        await js.publish(
            zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, TABLE, "insert"),
            msgpack.packb({"key": {"uid": uid}, "data": data, "version": v, "client_id": "c-size"}),
            headers={"Nats-Msg-Id": msg_id},
        )
        await asyncio.sleep(3.5)
        return uid, verdicts.get(msg_id)

    before = refused_tables()

    # ── 1 + 2. over the limit ──────────────────────────────────────────────────
    uid, verdict = await send("x" * (limit * 3))
    landed = zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE uid='{uid}'").strip()

    if verdict and verdict.get("reason") == "RowTooLargeToReplicate" and landed == "0":
        zb.ok("an oversized write is refused, and no row is written")
    elif landed != "0":
        zb.bad(
            f"the oversized row LANDED — its CDC echo cannot be packed, so the table is "
            "about to be suspended for every client"
        )
        failed += 1
        zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'", quiet=True)
    else:
        zb.bad(f"refused, but not as RowTooLargeToReplicate: {verdict}")
        failed += 1

    # ⚠️ The assertion the whole design turns on: the sender pays, nobody else does.
    after = refused_tables()
    if after is None:
        print("  ⓘ  /metrics unreachable — skipping the suspension check")
    elif before is not None and after > before:
        zb.bad(
            f"a table was suspended by one client's bad write (refused {before:.0f} → "
            f"{after:.0f}) — every reader loses it for a fault only the sender committed"
        )
        failed += 1
    else:
        zb.ok("and no table was suspended — the failure stayed with the sender")

    # ── 4. just under the limit still works ────────────────────────────────────
    #
    # A guard that refuses everything large would be safe and useless. The payload carries
    # framing and the other columns, so leave room rather than aiming at the exact byte.
    uid, verdict = await send("y" * (limit // 2))
    landed = zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE uid='{uid}'").strip()
    if landed == "1" and verdict and verdict.get("status") == "accepted":
        zb.ok(f"a {limit // 2:,}-byte row still applies — a ceiling, not a tax")
    else:
        zb.bad(f"a row well under the limit was refused: {verdict}")
        failed += 1
    zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'", quiet=True)

    task.cancel()
    await nc.close()
    return 1 if failed else 0


zb.run(main)

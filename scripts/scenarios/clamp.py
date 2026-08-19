#!/usr/bin/env python3
"""A future-dated version must not freeze a row — PROTOCOL.md §7.2.

Last-write-wins compares `stored.version < incoming.version`. A client whose clock is
wrong by a year does not write a wrong *value*, it writes a **sticky** one: the row now
rejects every later write, from every client, because nothing is greater than a year from
now. There is no error. The row simply stops accepting edits until wall-clock time passes
it, and the only cure is an out-of-band UPDATE by someone who noticed.

⚠️ Clock skew is not exotic — a browser's clock is set by its user. This needs one
misconfigured laptop, not an attack.

So §7.2 tells clients "do not send a future timestamp… **the bridge clamps and tells you
what it used**". That sentence was in the protocol before any code implemented it, which
is the worst kind of documentation bug: clients are written against it.

Four things checked:

  1. a version far in the future is **capped** at the database's clock, not stored as sent;
  2. the row is still **writable afterwards** — the actual damage, and the reason clamping
     beats rejecting: the client's data was never the problem, its clock was;
  3. the verdict **reports the value used**, so a client can correct itself rather than
     believing a version the database does not hold;
  4. a version inside the tolerance is left **exactly** as sent — a clamp that rounds
     every write would turn a clock question into an ordering one.

⚠️ Checked against **PostgreSQL's** `now()`, never this script's clock: that is the clock
the bridge compares with, and the two machines need not agree.

Usage:  python scripts/scenarios/clamp.py [table]
"""

import asyncio
import os
import re
import sys
import uuid
from datetime import datetime, timedelta, timezone

import msgpack
import zb

TABLE = sys.argv[1] if len(sys.argv) > 1 else "test_types"

# Must exceed config.Sync.version_future_tolerance ("5 seconds") by enough that scheduling
# jitter cannot explain the difference.
FUTURE = timedelta(days=365)
TOLERANCE_S = 5.0


def principal() -> str:
    rest = zb.NATS_URL.split("://", 1)[-1]
    return rest.rsplit("@", 1)[0].split(":", 1)[0] if "@" in rest else "alice"


def pg_now() -> datetime:
    raw = zb.psql("SELECT now() AT TIME ZONE 'UTC'").strip()
    return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


async def main():
    failed = 0
    who = principal()

    version_col = "updated_at"
    entry = next(
        (r for r in os.environ.get("SYNC_RULES", "").split(";") if r.strip().startswith(f"{TABLE}:")),
        None,
    )
    if entry:
        version_col = entry.split(":", 1)[1].split(",")[0].strip()

    coltype = zb.psql(
        "SELECT data_type FROM information_schema.columns "
        f"WHERE table_name='{TABLE}' AND column_name='{version_col}'"
    ).strip()
    print(f"version column: {version_col} ({coltype})\n")
    if "timestamp" not in coltype:
        sys.exit(
            f"'{version_col}' is {coltype}, which has no future to clamp. This scenario "
            "needs a timestamp version column."
        )

    required = [
        c for c in zb.psql(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{TABLE}' AND is_nullable='NO' AND column_default IS NULL"
        ).splitlines() if c
    ]
    tenant = zb.psql(
        f"SELECT tenant_id FROM zebridge_user_tenants WHERE principal='{who}' LIMIT 1"
    ).strip()

    nc = await zb.connect()
    js = nc.jetstream()
    verdicts: dict[str, dict] = {}
    ack_prefix = zb.TOPOLOGY["subjects"]["mutation_ack_prefix"]
    sub = await nc.subscribe(f"{ack_prefix}.{who}.*")

    async def collect():
        async for m in sub.messages:
            try:
                verdicts[m.subject.rsplit(".", 1)[-1]] = zb.decode(m.data)
            except Exception:  # noqa: BLE001
                pass

    task = asyncio.create_task(collect())
    await asyncio.sleep(0.5)

    async def write(uid: str, version: str, text: str) -> dict | None:
        data = {"uid": uid, "some_text": text, version_col: version, "inserted_at": version}
        for c in required:
            if c == "tenant_id" and tenant:
                data[c] = tenant
            elif c not in data:
                data[c] = version
        msg_id = f"clamp-{uuid.uuid4().hex[:12]}"
        await js.publish(
            zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, TABLE, "insert"),
            msgpack.packb({"key": {"uid": uid}, "data": data, "version": version, "client_id": "c-clamp"}),
            headers={"Nats-Msg-Id": msg_id},
        )
        await asyncio.sleep(3.0)
        return verdicts.get(msg_id)

    def stored_version(uid: str) -> datetime | None:
        raw = zb.psql(
            f"SELECT {version_col} AT TIME ZONE 'UTC' FROM public.{TABLE} WHERE uid='{uid}'"
        ).strip()
        return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc) if raw else None

    # ── 1 + 2 + 3. a year in the future ────────────────────────────────────────
    uid = str(uuid.uuid4())
    before = pg_now()
    sent = before + FUTURE
    verdict = await write(uid, iso(sent), "clamp: a year ahead")
    got = stored_version(uid)

    if got is None:
        zb.bad("the write never landed, so there is nothing to say about clamping")
        failed += 1
    else:
        drift = (got - before).total_seconds()
        if drift > TOLERANCE_S + 5:
            zb.bad(
                f"stored {got.isoformat()}, {drift/86400:.0f} days ahead of the database's "
                "clock — the row is frozen until then and no later write can move it"
            )
            failed += 1
        else:
            zb.ok(f"a version {FUTURE.days} days ahead was capped to now+{drift:.1f}s")

        # ⚠️ The assertion that matters, and note what it does NOT claim. The row is
        # unwritable for the length of the tolerance window — that is arithmetic, not a
        # bug: the stored version is `now + 5s`, so a write stamped `now` correctly loses
        # last-write-wins. What clamping buys is that the freeze is **bounded by the
        # tolerance instead of by the client's error**, which was a year. So wait it out.
        await asyncio.sleep(TOLERANCE_S + 2)
        later = pg_now()
        v2 = await write(uid, iso(later), "clamp: the write after")
        again = zb.psql(
            f"SELECT some_text FROM public.{TABLE} WHERE uid='{uid}'"
        ).strip()
        if again == "clamp: the write after":
            zb.ok("and the row still accepts a normal write afterwards")
        else:
            zb.bad(
                f"the row is frozen: a later write did not apply (still {again!r}). "
                "This is the whole failure mode clamping exists to prevent."
            )
            failed += 1

        # "…and tells you what it used" — half the promise, and the half a client needs
        # to correct its own state rather than believing a version PostgreSQL rejected.
        reported = (verdict or {}).get("version", "")
        if not verdict:
            zb.bad("no verdict at all for the clamped write")
            failed += 1
        elif not reported:
            zb.bad(
                "the verdict carries no `version`, so the client believes it stored the "
                "future value it sent — §7.2 promises otherwise"
            )
            failed += 1
        else:
            # ⚠️ And in the shape §7.2 documents, not PostgreSQL's own printing. A client
            # that stores `2026-08-19 05:54:11.363299+00` from a verdict holds a string
            # that will not match the CDC rendering of the same column, and its own
            # comparisons drift out of step with the feed without anything failing.
            wire = re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z?", reported)
            if not wire:
                zb.bad(
                    f"the verdict's version is {reported!r}, which is not the wire format "
                    "§7.2 documents (ISO 8601, `T`, six fractional digits). A client "
                    "cannot round-trip it."
                )
                failed += 1
            elif coltype.endswith("time zone") and not reported.endswith("Z"):
                zb.bad(f"a timestamptz version must carry `Z`: {reported!r}")
                failed += 1
            else:
                zb.ok(f"the verdict reports the stored version, in wire format ({reported})")

    zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'", quiet=True)

    # ── 4. inside the tolerance, untouched ─────────────────────────────────────
    #
    # Benign skew of a second or two is normal. Clamping it would silently rewrite
    # versions that arrived in a perfectly legitimate order.
    uid = str(uuid.uuid4())
    sent = pg_now() + timedelta(seconds=1)
    await write(uid, iso(sent), "clamp: barely ahead")
    got = stored_version(uid)
    if got is None:
        zb.bad("the within-tolerance write did not land")
        failed += 1
    elif abs((got - sent).total_seconds()) < 0.01:
        zb.ok("a version inside the tolerance was stored exactly as sent")
    else:
        zb.bad(
            f"a within-tolerance version was rewritten: sent {iso(sent)}, stored "
            f"{got.isoformat()} — clamping must not touch legitimate writes"
        )
        failed += 1
    zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'", quiet=True)

    task.cancel()
    await nc.close()
    return 1 if failed else 0


zb.run(main)

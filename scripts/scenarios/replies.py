#!/usr/bin/env python3
"""Every write gets a definitive reply, or a correct client can never pop its outbox.

PROTOCOL.md §7.1 gives clients a MUST: *"Pop the queue only on a definitive reply."* The
replies it names — `accepted`, `stale`, `row_deleted` — are **all successes at the SQL
level**, and they are not distinguishable from the row count, because two of the three are
zero rows.

⚠️ Until this was implemented the bridge published a verdict **only on failure**. A client
following the protocol exactly would therefore never pop a *successful* write: it would
resend it forever. Idempotently, thanks to `Nats-Msg-Id`, and therefore invisibly — no
error, no duplicate row, just an outbox that never drains and a queue that grows.

The three, and why the difference is not cosmetic:

  * `accepted` — rows changed. Pop and forget.
  * `stale` — zero rows, row still present and undeleted: someone else's newer version
    won. Pop **without reverting**; the winning row arrives over CDC. A client that
    hand-reverted here would fight the feed.
  * `row_deleted` — zero rows because there is no row to write: gone, or tombstoned. The
    one case last-write-wins cannot decide for the user, so it must be surfaced rather
    than swallowed.

Usage:  python scripts/scenarios/replies.py [table]

Needs the bridge running with the table's SYNC_RULES entry (a tombstone column is required
to produce `row_deleted` without a physical delete).
"""

import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import msgpack
import zb

TABLE = sys.argv[1] if len(sys.argv) > 1 else "test_types"


def principal() -> str:
    rest = zb.NATS_URL.split("://", 1)[-1]
    return rest.rsplit("@", 1)[0].split(":", 1)[0] if "@" in rest else "alice"


def rules() -> list[str]:
    entry = next(
        (r for r in os.environ.get("SYNC_RULES", "").split(";") if r.strip().startswith(f"{TABLE}:")),
        None,
    )
    return [c.strip() for c in entry.split(":", 1)[1].split(",")] if entry else []


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def pg_now() -> datetime:
    return datetime.fromisoformat(
        zb.psql("SELECT now() AT TIME ZONE 'UTC'").strip()
    ).replace(tzinfo=timezone.utc)


async def main():
    failed = 0
    who = principal()
    cols = rules()
    if len(cols) < 2:
        sys.exit(
            f"'{TABLE}' has no tombstone column in SYNC_RULES, so `row_deleted` cannot be\n"
            f"  produced without a physical delete.\n"
            f"    SYNC_RULES={TABLE}:updated_at,deleted_at,last_writer"
        )
    version_col, tombstone_col = cols[0], cols[1]

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
        msg_id = f"rep-{uuid.uuid4().hex[:12]}"
        await js.publish(
            zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, TABLE, "insert"),
            msgpack.packb({"key": {"uid": uid}, "data": data, "version": version, "client_id": "c-rep"}),
            headers={"Nats-Msg-Id": msg_id},
        )
        await asyncio.sleep(3.0)
        return verdicts.get(msg_id)

    def check(label: str, verdict: dict | None, want: str) -> bool:
        if verdict is None:
            zb.bad(f"{label}: no verdict at all — a client following §7.1 could never pop this write")
            return False
        got = verdict.get("status", "")
        if got == want:
            zb.ok(f"{label} → `{got}`")
            return True
        zb.bad(f"{label} → `{got}`, expected `{want}` (verdict: {verdict})")
        return False

    # ── accepted ───────────────────────────────────────────────────────────────
    uid = str(uuid.uuid4())
    v = check("a write that changes a row", await write(uid, iso(pg_now()), "replies: first"), "accepted")
    failed += 0 if v else 1

    # ── stale ──────────────────────────────────────────────────────────────────
    #
    # An OLDER version against the row just written. The guard refuses it, the row stays
    # exactly as it was, and the client must pop without reverting.
    old_version = iso(pg_now() - timedelta(hours=1))
    v = check("an older version against a live row", await write(uid, old_version, "replies: stale"), "stale")
    failed += 0 if v else 1

    text_now = zb.psql(f"SELECT some_text FROM public.{TABLE} WHERE uid='{uid}'").strip()
    if text_now == "replies: first":
        zb.ok("and the stale write changed nothing")
    else:
        zb.bad(f"a stale write modified the row: {text_now!r}")
        failed += 1

    # ── row_deleted ────────────────────────────────────────────────────────────
    #
    # Tombstoned out of band, as another client's delete would arrive. The row is still
    # physically present, which is exactly why the row count cannot tell these apart.
    zb.psql(
        f"UPDATE public.{TABLE} SET {tombstone_col} = now(), {version_col} = now() WHERE uid='{uid}'",
        quiet=True,
    )
    await asyncio.sleep(1)
    v = check(
        "a write against a tombstoned row",
        await write(uid, iso(pg_now() - timedelta(hours=1)), "replies: deleted"),
        "row_deleted",
    )
    failed += 0 if v else 1
    zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'", quiet=True)

    # ── row_deleted, the physical case ─────────────────────────────────────────
    #
    # Same verdict from the other cause: no row at all. A client cannot be asked to know
    # which kind of absence it hit.
    gone = str(uuid.uuid4())
    v = check(
        "an update to a row that never existed",
        await write(gone, iso(pg_now() - timedelta(hours=1)), "replies: never was"),
        "accepted",
    )
    # ⚠️ Deliberately `accepted`: an upsert on a missing row INSERTS it. This asserts the
    # bridge does not mistake "no previous row" for "deleted" — the difference between a
    # first write and a write against a grave.
    failed += 0 if v else 1
    zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{gone}'", quiet=True)

    # ── a run of writes must ALL be answered ───────────────────────────────────
    #
    # ⚠️ The regression test for NOTES §2.12. A pipelined connection that fails to leave
    # pipeline mode wedges the listener: it keeps logging, stays "connected", and answers
    # nothing. One write cannot tell the difference between that and a slow bridge — a run
    # of them can, because a wedged connection stops answering partway and never resumes.
    print("\nsoak: every write in a run is answered")
    n = 12
    ids = []
    for i in range(n):
        uid = str(uuid.uuid4())
        msg_id = f"soak-{uuid.uuid4().hex[:12]}"
        v = iso(pg_now())
        data = {"uid": uid, "some_text": f"soak {i}", version_col: v, "inserted_at": v}
        for c in required:
            if c == "tenant_id" and tenant:
                data[c] = tenant
            elif c not in data:
                data[c] = v
        await js.publish(
            zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, TABLE, "insert"),
            msgpack.packb({"key": {"uid": uid}, "data": data, "version": v, "client_id": "c-soak"}),
            headers={"Nats-Msg-Id": msg_id},
        )
        ids.append((msg_id, uid))

    for _ in range(40):
        if all(m in verdicts for m, _ in ids):
            break
        await asyncio.sleep(1)

    answered = [m for m, _ in ids if m in verdicts]
    if len(answered) == n:
        zb.ok(f"all {n} writes answered")
    else:
        zb.bad(
            f"only {len(answered)} of {n} writes were answered — ingress stalls partway, "
            "which is what a connection stuck in pipeline mode looks like from outside"
        )
        failed += 1
    for _, uid in ids:
        zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'", quiet=True)

    task.cancel()
    await nc.close()
    return 1 if failed else 0


zb.run(main)

#!/usr/bin/env python3
"""A write made offline is a write from the past — and the past must not win by arriving late.

An outbox replays writes that were composed while the client was disconnected. Between
composing and replaying, the row may have moved: someone else updated it, or deleted it,
or wrote the very same version. Every one of those is decided by LWW **on the version the
write carries**, not on when it happened to reach the bridge — otherwise reconnecting
would silently roll the row back to whatever the offline client last saw.

That is the property this scenario exists to prove, and it is not one the client can
verify about itself: the browser replays and sees the echo, which tells it the write was
processed, not that it was processed *correctly*. Only the row settles that.

⚠️ Versions are **pinned literals**, never `now()`. The whole matrix is orderings of two
versions, so a clock reading makes the outcome depend on how fast the harness ran — which
is how a race gets committed as a passing test.

Six cases, each seeded fresh:

  1. stale offline UPDATE  vs newer remote  → offline write refused, remote state kept
  2. newer offline UPDATE  vs older remote  → offline write applied
  3. equal versions                          → tiebreak decides, and the same writer wins
                                               whichever order the two arrive in
  4. newer offline DELETE  vs older remote  → row tombstoned
  5. stale offline UPDATE  vs remote DELETE → row stays deleted, **not resurrected**
  6. the same write replayed twice          → one row, second collapsed by dedup

Case 5 is the one worth the whole file. A client that was offline when the row was deleted
still holds an UPDATE for it; if that update resurrects the row, every deletion is undone
by whichever client happened to be offline at the time, and nothing reports it.

Case 6 is what makes replay *cheap* — and it has a **time limit**: JetStream dedup is a
window, not a ledger (120s by default). The scenario reports it, because past that point a
replay is a genuinely new message and `duplicate: true` never comes back.

⚠️ That is a weaker statement than "replay breaks". Past the window the write is still
correct — the upsert is idempotent and LWW refuses it as stale or as an unresolvable tie
with itself — but the *cheap* signal is gone: an outbox can no longer distinguish "already
landed" from "landed just now" and must wait for the verdict or the CDC echo instead. For
a client offline for hours, which is the real case, correctness rests on LWW and the
idempotent upsert, never on dedup.

Usage:  python scripts/scenarios/offline.py [table]

    NATS_URL=nats://alice:s3cret@127.0.0.1:4222 python scripts/scenarios/offline.py

Needs the bridge running with a tiebreak column, for case 3:

    SYNC_RULES=test_types:updated_at,deleted_at,last_writer
"""

import asyncio
import os
import sys
import uuid

import msgpack
import zb

TABLE = sys.argv[1] if len(sys.argv) > 1 else "test_types"

# Two client identities. Case 3 needs them ordered, and the rule is "higher id wins".
LOW, HIGH = "c-aaa", "c-zzz"

# Pinned wall-clock versions. Only their ORDER matters; the date is arbitrary and fixed so
# a rerun compares the same values.
DAY = "2026-08-16"
V_OLD, V_MID, V_NEW = f"{DAY}T09:00:00.000000", f"{DAY}T10:00:00.000000", f"{DAY}T11:00:00.000000"

# How long to wait for the bridge to apply a mutation before reading the row back. The
# mutation listener pulls with a 500ms timeout and PostgreSQL commits immediately, so this
# is slack, not a measured latency.
SETTLE = float(os.environ.get("ZB_SETTLE", "2.5"))


def principal() -> str:
    rest = zb.NATS_URL.split("://", 1)[-1]
    return rest.rsplit("@", 1)[0].split(":", 1)[0] if "@" in rest else "alice"


async def main():
    failed = 0
    who = principal()

    # ── configuration this scenario depends on ─────────────────────────────────
    rules = os.environ.get("SYNC_RULES", "")
    entry = next((r for r in rules.split(";") if r.strip().startswith(f"{TABLE}:")), None)
    cols = entry.split(":", 1)[1].split(",") if entry else []
    if len(cols) < 2:
        sys.exit(
            f"'{TABLE}' needs a version and a tombstone column in SYNC_RULES.\n"
            f"    SYNC_RULES={TABLE}:updated_at,deleted_at,last_writer"
        )
    version_col = cols[0].strip()
    tomb_col = cols[1].strip()
    tie_col = cols[2].strip() if len(cols) > 2 else None
    print(f"version={version_col}  tombstone={tomb_col}  tiebreak={tie_col or '(none)'}\n")

    # Read the shape from the catalog rather than assuming it — this table grew a NOT NULL
    # tenant_id, and a hardcoded row silently stopped inserting when it did.
    columns = zb.psql(
        f"SELECT attname FROM pg_attribute WHERE attrelid='public.{TABLE}'::regclass "
        "AND attnum>0 AND NOT attisdropped ORDER BY attnum"
    ).splitlines()
    if not columns:
        sys.exit(f"table '{TABLE}' not found — run the emitter's migrations first")

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
    base = zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, TABLE)

    # ── the dedup window: the shelf life of an idempotent replay ───────────────
    #
    # An outbox entry replayed inside this window is collapsed into the original write.
    # Replayed after it, the same bytes are a *second* write — harmless for an idempotent
    # upsert, but `duplicate: true` never comes back, so a client cannot tell "already
    # landed" from "landed just now", and any non-idempotent handling downstream breaks.
    try:
        info = await js.stream_info(zb.TOPOLOGY["streams"]["mutations"])
        raw = getattr(info.config, "duplicate_window", None) or 0
        # nats-py reports this in seconds when it reports it at all, and 0 when it does
        # not populate the field — which is not the same as "no dedup", so say so rather
        # than printing a number that reads as fact. The server default is 120s.
        window_s = raw / 1e9 if raw > 1e6 else raw
        if window_s:
            print(f"dedup window: {window_s:.0f}s — an outbox replayed later than this is "
                  f"no longer collapsed\n")
        else:
            print("dedup window: not reported by this client (server default is 120s)\n")
    except Exception:
        # Expected as a client principal: `$JS.API.STREAM.INFO.<stream>` is not in a
        # client's allow-list, and should not be. Re-run with the bridge nkey to see it.
        print("dedup window: not readable as a client principal — re-run with "
              "NATS_BRIDGE_NKEY_SEED to report it\n")

    def row(uid: str, text: str, version: str) -> dict:
        d = {
            "uid": uid, "age": 1, "temperature": 1.5, "price": "1.00000000",
            "is_true": True, "some_text": text, "tags": "{}", "matrix": "{}",
            "metadata": "{}", "inserted_at": V_OLD, version_col: version, tomb_col: None,
        }
        for c in required:
            if c == "tenant_id" and tenant:
                d[c] = tenant
            elif c not in d or d[c] is None:
                d.setdefault(c, version)
        return {c: d.get(c) for c in columns}

    async def send(op: str, uid: str, text: str, version: str,
                   client_id: str = LOW, msg_id: str | None = None,
                   tombstone: bool = False):
        """Publish one mutation. `msg_id` is reused verbatim to simulate a replay."""
        data = row(uid, text, version)
        if tombstone:
            data[tomb_col] = version
        payload = {"key": {"uid": uid}, "data": data,
                   "version": version, "client_id": client_id}
        hdr = {"Nats-Msg-Id": msg_id} if msg_id else None
        ack = await js.publish(f"{base}.{op}", msgpack.packb(payload), headers=hdr)
        await asyncio.sleep(SETTLE)
        return ack

    def read(uid: str):
        """(some_text, tombstoned, last_writer) — or (None, ...) when there is no row."""
        out = zb.psql(
            f"SELECT coalesce(some_text,'') || '|' || "
            f"CASE WHEN {tomb_col} IS NULL THEN 'live' ELSE 'dead' END || '|' || "
            f"coalesce({tie_col if tie_col else 'NULL'}::text,'') "
            f"FROM public.{TABLE} WHERE uid='{uid}'"
        ).strip()
        if "|" not in out:
            return None, None, None
        text, state, writer = out.split("|", 2)
        return text, state, writer

    def cleanup(uid: str):
        zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'", quiet=True)

    def sweep_debris():
        """Remove rows a previous *crashed* run left behind.

        Every row this scenario writes carries a version on the pinned DAY, which nothing
        real does — that date is the marker. Without this, one interrupted run leaves
        rows that the next run's counts and reads have to step around, and the failure
        shows up later as an unrelated assertion.
        """
        n = zb.psql(
            f"WITH d AS (DELETE FROM public.{TABLE} "
            f"WHERE {version_col}::date = DATE '{DAY}' RETURNING 1) SELECT count(*) FROM d",
            quiet=True,
        ).strip()
        if n and n != "0":
            print(f"  swept {n} row(s) left by an earlier interrupted run\n")

    sweep_debris()

    def check(label: str, cond: bool, detail: str):
        nonlocal failed
        if cond:
            zb.ok(f"{label}: {detail}")
        else:
            zb.bad(f"{label}: {detail}")
            failed += 1

    # ── 1. a stale offline UPDATE must not win by arriving late ────────────────
    uid = str(uuid.uuid4())
    await send("insert", uid, "seed", V_MID)
    await send("update", uid, "remote moved on", V_NEW)          # happened while offline
    await send("update", uid, "OFFLINE stale", V_OLD)            # the replay
    text, state, _ = read(uid)
    check("stale offline update", text == "remote moved on",
          f"row kept the newer remote write (got '{text}')")
    cleanup(uid)

    # ── 2. a newer offline UPDATE must still apply ─────────────────────────────
    uid = str(uuid.uuid4())
    await send("insert", uid, "seed", V_OLD)
    await send("update", uid, "remote", V_MID)
    await send("update", uid, "OFFLINE newer", V_NEW)
    text, _, _ = read(uid)
    check("newer offline update", text == "OFFLINE newer",
          f"replayed write applied (got '{text}')")
    cleanup(uid)

    # ── 3. equal versions resolve the same way in either order ─────────────────
    #
    # The offline case makes ties ordinary: the disconnected client wrote `stored + 1`
    # from the value it last saw, and so did whoever was online.
    if tie_col:
        for label, first, second in (("remote first", LOW, HIGH), ("replay first", HIGH, LOW)):
            uid = str(uuid.uuid4())
            await send("insert", uid, "seed", V_OLD)
            await send("update", uid, f"from {first}", V_NEW, client_id=first)
            await send("update", uid, f"from {second}", V_NEW, client_id=second)
            _, _, writer = read(uid)
            check(f"equal versions ({label})", writer == HIGH,
                  f"'{writer}' won — must be '{HIGH}' regardless of arrival order")
            cleanup(uid)
    else:
        print("  – equal-version case skipped: no tiebreak column in SYNC_RULES")

    # ── 4. an offline DELETE with a newer version tombstones the row ───────────
    uid = str(uuid.uuid4())
    await send("insert", uid, "seed", V_OLD)
    await send("update", uid, "remote", V_MID)
    await send("update", uid, "OFFLINE delete", V_NEW, tombstone=True)
    _, state, _ = read(uid)
    check("offline delete", state == "dead", f"row tombstoned (state '{state}')")
    cleanup(uid)

    # ── 5. a stale offline UPDATE must not resurrect a deleted row ─────────────
    #
    # The case that makes this file worth having. The client was offline when the row was
    # deleted, so its queue still holds an UPDATE for a row that no longer exists. If that
    # update wins, every delete is undone by whoever happened to be disconnected, silently.
    uid = str(uuid.uuid4())
    await send("insert", uid, "seed", V_OLD)
    await send("update", uid, "remote deleted it", V_NEW, tombstone=True)
    await send("update", uid, "OFFLINE resurrects?", V_MID)
    _, state, _ = read(uid)
    check("no resurrection", state == "dead",
          f"row stayed deleted against a stale offline update (state '{state}')")
    cleanup(uid)

    # ── 6. replaying the same write twice writes one row ───────────────────────
    #
    # The outbox reuses the original `Nats-Msg-Id` precisely so this collapses. A fresh id
    # per attempt would make every reconnect a second write.
    uid = str(uuid.uuid4())
    mid = f"offline-replay-{uid}"
    a1 = await send("insert", uid, "replayed once", V_MID, msg_id=mid)
    a2 = await send("insert", uid, "replayed once", V_MID, msg_id=mid)
    n = zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE uid='{uid}'").strip()
    check("idempotent replay", n == "1", f"exactly one row after two publishes (got {n})")
    dup = bool(getattr(a2, "duplicate", False))
    check("replay reported as duplicate", dup,
          "second PubAck carried duplicate=true — the signal an outbox pops on"
          if dup else
          f"second PubAck did NOT report a duplicate (seq {a1.seq} then {a2.seq}); "
          "outside the dedup window an outbox cannot tell a replay from a new write")
    cleanup(uid)

    await nc.close()
    print()
    return 1 if failed else 0


zb.run(main)

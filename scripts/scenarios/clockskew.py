#!/usr/bin/env python3
"""Clock skew under last-write-wins — the bias is bounded, audible, and convergent.

The most obvious argument against LWW is that it lets a CLOCK decide a data race.
Two clients edit one row; the one whose laptop runs four seconds fast wins edits it
did not make last. No attack, no bug — one misconfigured machine, and the ordering
the version column promises quietly stops being the order things happened in.

ZeBridge does not pretend to fix that. What it does — and what this scenario holds
it to — is bound the damage, make every loss audible, and keep replicas convergent:

  1. THEFT, bounded (fast clock inside the tolerance): a writer 4 s fast steals the
     row — an honest write later in REAL time verdicts `stale` — and the theft ends
     the moment the wall clock passes the stolen version. Inside the 5 s tolerance
     the clamp does not fire (clamp.py asserts exactness); the stolen window is the
     residue LWW cannot remove, and its size is the skew, capped by the tolerance.
  2. STARVATION vs THE RULE (slow clock): a naive wall-clock writer 30 s slow is
     `stale` against any fresh row, and stays stale for as long as its clock lags —
     wall-clock LWW starves it. The SAME lagging clock following §7.3's rule
     ("generate a version greater than the value you hold": max(now, seen + 1 µs) —
     libzb's `hlcVersion`, emulated here byte-for-byte) writes successfully. The
     wire cannot tell a skewed clock from a crafted version, which is why crafting
     one IS the test.
  3. CONVERGENCE: the CDC feed's last word on the row equals PostgreSQL's, and no
     stale write's payload ever reached the feed. Skew biases WHO wins — never WHAT
     the replicas converge to. Divergence would be the disaster; unfairness is the
     documented price of LWW.

⚠️ All comparisons are against **PostgreSQL's** `now()`, never this script's clock —
that is the clock the bridge clamps against, and the two machines need not agree.

Usage:  python scripts/scenarios/clockskew.py [table]   (NATS_CREDS=<client>.creds or ZB_PRINCIPAL)
"""

import asyncio
import sys
import uuid
from datetime import datetime, timedelta, timezone

import msgpack
import zb

TABLE = sys.argv[1] if len(sys.argv) > 1 else "test_types"

FAST_SKEW_S = 4.0        # inside config.Sync.version_future_tolerance ("5 seconds")
SLOW_SKEW = timedelta(seconds=30)
VERDICT_WAIT_S = 10.0
CDC_WAIT_S = 15.0


def pg_now() -> datetime:
    raw = zb.psql("SELECT now() AT TIME ZONE 'UTC'").strip()
    return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def tick(wire: str) -> str:
    """libzb core.nextVersion's tie arm: the held version plus one microsecond.
    Operates on the WIRE string, as the client does — no datetime round-trip."""
    dt = datetime.strptime(wire, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return iso(dt + timedelta(microseconds=1))


async def main():
    failed = 0
    who = zb.require_principal()
    rules = zb.require_rules(TABLE, "version")
    version_col = rules["version"]
    tenant = zb.psql(
        f"SELECT tenant_id FROM zebridge_user_tenants WHERE principal='{who}' LIMIT 1"
    ).strip()
    if not tenant:
        sys.exit(f"'{who}' has no tenant mapping — the CDC leg needs one")

    required = [
        c for c in zb.psql(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{TABLE}' AND is_nullable='NO' AND column_default IS NULL"
        ).splitlines() if c
    ]

    nc = await zb.connect_as(who)
    js = nc.jetstream()

    verdicts: dict[str, dict] = {}
    ack_prefix = zb.TOPOLOGY["subjects"]["mutation_ack_prefix"]
    ack_sub = await zb.subscribe(nc, f"{ack_prefix}.{who}.*")

    # The CDC leg listens from BEFORE the first write: convergence is judged on what
    # the feed actually said, not on a read-back after the dust settles.
    cdc_events: list[dict] = []
    cdc_sub = await zb.subscribe(nc, 
        f"{zb.TOPOLOGY['subjects']['cdc_prefix']}.{tenant}.{TABLE}.>"
    )

    async def collect_acks():
        async for m in ack_sub.messages:
            try:
                verdicts[m.subject.rsplit(".", 1)[-1]] = zb.decode(m.data)
            except Exception:  # noqa: BLE001
                pass

    async def collect_cdc():
        async for m in cdc_sub.messages:
            try:
                payload = zb.decode(m.data)
            except Exception:  # noqa: BLE001
                continue
            for ev in payload if isinstance(payload, list) else [payload]:
                if isinstance(ev, dict):
                    cdc_events.append(ev)

    tasks = [asyncio.create_task(collect_acks()), asyncio.create_task(collect_cdc())]
    await asyncio.sleep(0.5)

    async def write(uid: str, version: str, text: str) -> dict:
        data = {"uid": uid, "some_text": text, version_col: version, "inserted_at": version}
        for c in required:
            if c == "tenant_id":
                data[c] = tenant
            elif c not in data:
                data[c] = version
        msg_id = f"skew-{uuid.uuid4().hex[:12]}"
        await js.publish(
            zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, TABLE, "insert"),
            msgpack.packb({"key": {"uid": uid}, "data": data, "version": version,
                           "client_id": "c-skew"}),
            headers={"Nats-Msg-Id": msg_id},
        )
        deadline = asyncio.get_event_loop().time() + VERDICT_WAIT_S
        while msg_id not in verdicts and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.2)
        v = verdicts.get(msg_id)
        if v is None:
            zb.bad(f"no verdict for '{text}' within {VERDICT_WAIT_S}s — nothing below can be trusted")
            sys.exit(1)
        return v

    def pg_text(uid: str) -> str:
        return zb.psql(f"SELECT some_text FROM public.{TABLE} WHERE uid='{uid}'").strip()

    uid = str(uuid.uuid4())

    # ── 1. the theft, and its bound ────────────────────────────────────────────
    t0 = pg_now()
    stolen_until = t0 + timedelta(seconds=FAST_SKEW_S)
    v_thief = await write(uid, iso(stolen_until), "the thief")
    if v_thief.get("status") != "accepted" or v_thief.get("reason") == "version_clamped":
        zb.bad(f"a version {FAST_SKEW_S}s ahead should sail under the 5s tolerance "
               f"untouched, got {v_thief}")
        failed += 1
    else:
        zb.ok(f"a clock {FAST_SKEW_S}s fast writes unchallenged — inside the tolerance "
              "the clamp stays out of it, by design (clamp.py asserts the exactness)")

    v_honest = await write(uid, iso(pg_now()), "honest, later in real time")
    if v_honest.get("status") != "stale":
        zb.bad(f"the honest write later in REAL time should lose to the stolen version, "
               f"got {v_honest.get('status')!r} — is LWW comparing at all?")
        failed += 1
    elif pg_text(uid) != "the thief":
        zb.bad(f"verdict said stale but the row reads {pg_text(uid)!r}")
        failed += 1
    else:
        zb.ok("the theft is real — a write that happened LATER lost to a clock, and "
              "the loss is AUDIBLE: a definitive `stale`, so the outbox pops and the "
              "winner arrives over CDC instead of the client retrying forever")

    # The bound: the moment the wall clock passes the stolen version, honesty resumes.
    while pg_now() <= stolen_until:
        await asyncio.sleep(0.3)
    v_reclaim = await write(uid, iso(pg_now()), "honest reclaims")
    stolen_for = (pg_now() - t0).total_seconds()
    if v_reclaim.get("status") != "accepted" or pg_text(uid) != "honest reclaims":
        zb.bad(f"the row should be writable once real time passes the stolen version: "
               f"{v_reclaim} / row={pg_text(uid)!r}")
        failed += 1
    else:
        zb.ok(f"and the theft ENDED when the wall clock caught up (~{stolen_for:.1f}s): "
              "the frozen window is the skew itself, capped at the 5s tolerance — "
              "never the year a broken clock would have taken")

    # ── 2. starvation, and the rule that ends it ───────────────────────────────
    fresh = pg_now()
    v_fresh = await write(uid, iso(fresh), "fresh baseline")
    if v_fresh.get("status") != "accepted":
        zb.bad(f"baseline write refused: {v_fresh}")
        failed += 1
    seen = v_fresh.get("version") or iso(fresh)   # what a client ADOPTS from its ack

    v_naive = await write(uid, iso(pg_now() - SLOW_SKEW), "slow and naive")
    if v_naive.get("status") != "stale" or pg_text(uid) != "fresh baseline":
        zb.bad(f"a version 30s in the past should be stale against a fresh row, got "
               f"{v_naive.get('status')!r} / row={pg_text(uid)!r}")
        failed += 1
    else:
        zb.ok("a clock 30s slow, writing from its wrist, is starved: every edit is "
              "`stale` until the lag is fixed — wall-clock LWW has no mercy for it")

    hlc = max(iso(pg_now() - SLOW_SKEW), tick(seen))   # libzb hlcVersion, same clock
    v_hlc = await write(uid, hlc, "slow but disciplined")
    if v_hlc.get("status") != "accepted" or pg_text(uid) != "slow but disciplined":
        zb.bad(f"the §7.3 rule (max(now, seen+1µs)) should write through a 30s-slow "
               f"clock, got {v_hlc.get('status')!r} / row={pg_text(uid)!r}")
        failed += 1
    else:
        zb.ok("the SAME slow clock following §7.3 — version from the newest value "
              "SEEN, not from the wrist (libzb's hlcVersion) — writes through: the "
              "starvation was never the skew, it was deriving versions from a clock")

    # ── 3. convergence: the feed's last word equals PostgreSQL's ───────────────
    stored_wire = None
    raw = zb.psql(
        f"SELECT {version_col} AT TIME ZONE 'UTC' FROM public.{TABLE} WHERE uid='{uid}'"
    ).strip()
    if raw:
        stored_wire = iso(datetime.fromisoformat(raw).replace(tzinfo=timezone.utc))

    def row_events():
        return [ev for ev in cdc_events if (ev.get("data") or {}).get("uid") == uid]

    deadline = asyncio.get_event_loop().time() + CDC_WAIT_S
    while asyncio.get_event_loop().time() < deadline:
        evs = row_events()
        if evs and (evs[-1].get("data") or {}).get(version_col) == stored_wire:
            break
        await asyncio.sleep(0.3)

    evs = row_events()
    if not evs:
        zb.bad("no CDC event for the row at all — convergence cannot be judged")
        failed += 1
    else:
        last = evs[-1].get("data") or {}
        if last.get("some_text") == pg_text(uid) and last.get(version_col) == stored_wire:
            zb.ok(f"the feed's last word equals PostgreSQL's ({last.get('some_text')!r} "
                  f"@ {stored_wire}): skew biased who won, never what replicas hold")
        else:
            zb.bad(f"feed and database disagree: CDC says {last.get('some_text')!r} @ "
                   f"{last.get(version_col)!r}, PostgreSQL says {pg_text(uid)!r} @ "
                   f"{stored_wire!r} — this is divergence, the failure LWW must never have")
            failed += 1
        leaked = {(ev.get("data") or {}).get("some_text") for ev in evs} & {
            "honest, later in real time", "slow and naive"}
        if leaked:
            zb.bad(f"stale writes reached the feed: {leaked} — a rejected version "
                   "must leave no trace downstream")
            failed += 1
        else:
            zb.ok("and no stale write's payload ever reached the feed — losers "
                  "leave no trace, only verdicts")

    # teardown the way the product honours: soft delete on the tombstoned table
    zb.psql(f"UPDATE public.{TABLE} SET deleted_at = now(), {version_col} = now() "
            f"WHERE uid='{uid}'", quiet=True)
    for t in tasks:
        t.cancel()
    await nc.close()

    print("\nPASS" if not failed else f"\nFAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)

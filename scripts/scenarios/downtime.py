#!/usr/bin/env python3
"""Changes committed while the bridge is DOWN must still reach CDC on restart.

This is the guarantee a persistent replication slot exists to provide, and the one
nothing tested until 2026-08-26 — when a second consumer, diffed row by row against
Postgres, showed a replica holding rows Postgres no longer had.

The bug (NOTES.md finding 4): both the boot path and the reconnect path passed
`pg_current_wal_lsn()` — the WAL *head* — to `START_REPLICATION`, telling PostgreSQL
"start here" and skipping everything committed while the bridge was away. Nothing
errored. The slot was retaining the WAL correctly; the bridge stepped over it. Every
replica kept deleted rows forever.

⚠️ Why no existing scenario could catch it, which is the interesting part:

  * every scenario spins a FRESH probe slot, and for a brand-new slot the WAL head
    and `confirmed_flush_lsn` are the same position — "resume from head" and "resume
    from slot" are literally identical, so the bug is invisible by construction;
  * the scenarios that DO restart or sever a bridge (chaos.py) write and check
    AFTER the restart — testing the *after*, never the *during*;
  * the browser demo takes a fresh OPFS database on every load, so accumulated
    drift was erased before anyone could see it.

So this scenario deliberately does the one thing none of those do: it reuses ONE slot
across two bridge lifetimes and commits all three verbs into the gap between them.

  1. bridge A streams; a baseline row proves the pipeline and marks a stream position
  2. bridge A stops
  3. while DOWN: INSERT a row, UPDATE a row, DELETE a row
  4. bridge B starts on the SAME slot
  5. every one of those three changes must appear in CDC after the recorded position
  6. and again after `kill -9` — a CRASH, where the bridge writes nothing on the way
     out and never sends a final status update. The position must survive anyway,
     because it is not the bridge's to keep: the bridge ACKs only what JetStream has
     durably confirmed (`batch_pub.getLastConfirmedLsn()`, never the WAL position it
     merely received), and PostgreSQL records that as the slot's
     `confirmed_flush_lsn`. A crash therefore resumes slightly BEHIND — never ahead —
     which is at-least-once, and safe: LSN-derived msg ids let JetStream dedup and
     the client applier is idempotent.

⚠️ Owns the only bridge (it stops and starts one on a shared slot). Run it alone.

Usage:  python scripts/scenarios/downtime.py     (admin ZB_PSQL; NATS_CREDS=bridge)
"""

import asyncio
import subprocess
import sys
import uuid

from nats.js.api import ConsumerConfig, DeliverPolicy

import zb

LOG = "/tmp/zb_downtime_bridge.log"
TABLE = "memo"
CDC_STREAM = zb.TOPOLOGY["cdc_streams"]["public"]
FILTER = f"{zb.TOPOLOGY['subjects']['cdc_prefix']}.{TABLE}.>"


def mk_row(txt: str) -> str:
    uid = str(uuid.uuid4())
    zb.psql(
        f"INSERT INTO public.{TABLE} (uid, txt, updated_at) VALUES ('{uid}', '{txt}', now())",
        quiet=True,
    )
    return uid


async def events_since(js, start_seq: int) -> list[tuple[str, str]]:
    """(operation, uid) for every memo event at or after start_seq.

    Decodes the `.batch` shape too — a multi-row statement flushes as ONE message
    carrying an ARRAY of events, and a delete of several rows is exactly that.
    """
    sub = await js.pull_subscribe(
        FILTER,
        durable=None,
        stream=CDC_STREAM,
        config=ConsumerConfig(
            deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
            opt_start_seq=start_seq,
            filter_subject=FILTER,
        ),
    )
    out: list[tuple[str, str]] = []
    while True:
        try:
            batch = await sub.fetch(100, timeout=2)
        except Exception:  # noqa: BLE001
            break
        if not batch:
            break
        for msg in batch:
            decoded = zb.decode(msg.data)
            for ev in decoded if isinstance(decoded, list) else [decoded]:
                data = ev.get("data") or {}
                out.append((ev.get("operation"), data.get("uid")))
            await msg.ack()
    return out


async def main():
    failed = 0

    running = subprocess.run(["pgrep", "-f", "zig-out/bin/bridge"], capture_output=True, text=True)
    if running.stdout.strip():
        sys.exit("another bridge is already running — this scenario owns the only bridge")

    # A LEFTOVER slot would carry an old position and replay unrelated history into
    # the assertions. Start from a known-clean one; the point of this test is the slot
    # that survives between the two bridges BELOW, not one inherited from a past run.
    zb.psql(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='zb_probe' AND NOT active) "
        "THEN PERFORM pg_drop_replication_slot('zb_probe'); END IF; END $$",
        quiet=True,
    )

    keep = mk_row("downtime keep")       # will be UPDATEd while the bridge is down
    doomed = mk_row("downtime doomed")   # will be DELETEd while the bridge is down
    born = None                          # will be INSERTed while the bridge is down

    nc = await zb.connect()
    js = nc.jetstream()

    # ── 1. bridge A: stream, and mark where "during the outage" begins ──────────
    with zb.Bridge(LOG) as bridge:
        if not bridge.wait_for_log("Replication started successfully", timeout=60):
            zb.bad("bridge A never started — see " + LOG)
            await nc.close()
            return 1
        await asyncio.sleep(4)  # let the two fixture rows land

        info = await js.stream_info(CDC_STREAM)
        mark = info.state.last_seq + 1   # everything from here on is "after the mark"

        baseline = mk_row("downtime baseline")
        await asyncio.sleep(4)
        seen = await events_since(js, mark)
        if any(uid == baseline for _, uid in seen):
            zb.ok("baseline: with the bridge UP, a write reaches CDC (the pipeline is live)")
        else:
            zb.bad(f"baseline write never reached CDC — the rest cannot be trusted: {seen}")
            await nc.close()
            return 1

    # ── 2. the bridge is now DOWN. Commit all three verbs into the gap. ─────────
    gap_mark = (await js.stream_info(CDC_STREAM)).state.last_seq + 1

    born = mk_row("downtime born")
    zb.psql(f"UPDATE public.{TABLE} SET txt='downtime updated', updated_at=now() WHERE uid='{keep}'", quiet=True)
    zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{doomed}'", quiet=True)

    # ── 3. bridge B on the SAME slot: the retained WAL must replay ──────────────
    with zb.Bridge(LOG + ".2") as bridge2:
        if not bridge2.wait_for_log("Replication started successfully", timeout=60):
            zb.bad("bridge B never started — see " + LOG + ".2")
            await nc.close()
            return 1
        await asyncio.sleep(8)

        replayed = await events_since(js, gap_mark)
        got = {(op, uid) for op, uid in replayed}

        for verb, uid, label in (
            ("INSERT", born, "an INSERT made while the bridge was down"),
            ("UPDATE", keep, "an UPDATE made while the bridge was down"),
            ("DELETE", doomed, "a DELETE made while the bridge was down"),
        ):
            if (verb, uid) in got:
                zb.ok(f"{label} is replayed from the slot")
            else:
                zb.bad(f"{label} was LOST — never published (replicas diverge permanently)")
                failed += 1

    # ── 4. the same guarantee across a CRASH, not a clean stop ─────────────────
    crash_born = None
    with zb.Bridge(LOG + ".3") as bridge3:
        if not bridge3.wait_for_log("Replication started successfully", timeout=60):
            zb.bad("bridge C never started — see " + LOG + ".3")
            failed += 1
        else:
            await asyncio.sleep(3)
            crash_mark = (await js.stream_info(CDC_STREAM)).state.last_seq + 1
            # SIGKILL: no signal handler runs, no final status update is sent. Whatever
            # PostgreSQL has on the slot is all that survives.
            bridge3.proc.kill()
            bridge3.proc.wait(timeout=10)
            crash_born = mk_row("downtime crash-born")

    with zb.Bridge(LOG + ".4") as bridge4:
        if not bridge4.wait_for_log("Replication started successfully", timeout=60):
            zb.bad("bridge D never started — see " + LOG + ".4")
            failed += 1
        else:
            await asyncio.sleep(8)
            after_crash = {uid for _, uid in await events_since(js, crash_mark)}
            if crash_born in after_crash:
                zb.ok("a write made after `kill -9` is replayed — the position lives in the slot, not the process")
            else:
                zb.bad("a write made after `kill -9` was LOST — a crash loses the WAL gap")
                failed += 1

    await nc.close()

    for uid in (keep, doomed, born, baseline, crash_born):
        if uid:
            zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'", quiet=True)
    zb.psql(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='zb_probe' AND NOT active) "
        "THEN PERFORM pg_drop_replication_slot('zb_probe'); END IF; END $$",
        quiet=True,
    )

    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


zb.run(main)

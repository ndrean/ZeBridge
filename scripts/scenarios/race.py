#!/usr/bin/env python3
"""Concurrent-consumer races against nats.zig — the entry point under contention.

FOUND A REAL BUG (2026-08-26, fixed same day): the mutation listener's pull loop
slept 100 ms after EVERY message — a `for … else` meant as "sleep on timeout",
but Zig runs the else on every normal completion — capping the whole ingress
lane at ~10 writes/s regardless of backlog. Invisible to every single-write
scenario; this one's 24 concurrent writers exposed it in one run. Fixed in
mutation_listener.zig (sleep only when the fetch is empty; constant in Config).

Harness lessons paid for on the way (all encoded below): ZB_PSQL must point at
the host PG; publish failures must be LOUD (a swallowed error scored as
"24/24 wrong"); and Nats-Msg-Ids must be namespaced per run — MUTATIONS dedups
inside a 2-minute window, so a rerun with static ids gets clean PubAcks
(duplicate=true) while storing NOTHING.

The synchronous-per-connection structure is the safety ARGUMENT; this is the
test. Many principals hammer the mutation lane at once, THROUGH a NATS reconnect,
mixing well-formed and poison payloads — the conditions under which a race in the
bridge's use of nats.zig (shared publisher, meta cache, verdict path) would show
as a crash, a wedged consumer, a lost/duplicated apply, or a leak.

  1  N concurrent writers to counter_public (distinct keys) → every accepted
     write lands exactly once (no lost update, no double-apply); process alive;
  2  the same, DURING a NATS broker restart — writes queued across the blip
     converge once the broker returns, nothing crashes;
  3  a poison payload interleaved with legit ones from many senders does not
     wedge the queue (Finding 2's fix, under concurrency);
  4  `leaks` = 0 on the survivor.

⚠️ Owns the only bridge and restarts nats-server. Run with bridge creds for the
probe; writers connect as their own principals via the account (any valid creds).

Usage:  python scripts/scenarios/race.py   (admin ZB_PSQL; NATS_CREDS=bridge)
"""

import asyncio
import datetime
import os
import pathlib
import subprocess
import sys
import time

import msgpack

import zb

RUN = str(int(time.time()))  # msgid namespace: JetStream dedups Nats-Msg-Id inside the
# duplicate window — static ids made every rerun's publishes silent no-ops (PubAck
# duplicate=true, nothing stored) and scored 24/24-wrong with zero real writes.

LOG = "/tmp/zb_race_bridge.log"
N_WRITERS = 24
TABLE = "counter_public"
# The writers' identity: the creds file names the principal, and the subject must carry
# the same name or NATS refuses the publish (a refusal that surfaces as a timeout).
ADVERSARY_CREDS = os.environ.get("ADVERSARY_CREDS", "scripts/native/creds/omar.creds")
ADVERSARY = pathlib.Path(ADVERSARY_CREDS).stem
SUBJECT = zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], ADVERSARY, TABLE, "update")


def now_v():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


async def one_write(js, uid, value, msgid, raw=None):
    payload = raw if raw is not None else msgpack.packb(
        {"key": {"uid": uid}, "version": now_v(), "client_id": f"c-{value}",
         "data": {"uid": uid, "value": value, "updated_at": now_v()}})
    try:
        ack = await js.publish(SUBJECT, payload, headers={"Nats-Msg-Id": msgid})
        if getattr(ack, "duplicate", False):
            return f"duplicate msgid {msgid}: PubAck ok but message NOT stored"
        return None
    except Exception as e:
        return f"{type(e).__name__}: {e}"


async def main():
    failed = 0
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")

    zb.psql("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='zb_probe' AND NOT active) "
            "THEN PERFORM pg_drop_replication_slot('zb_probe'); END IF; END $$", quiet=True)

    with zb.Bridge(LOG) as bridge:
        if not bridge.wait_for_log("Replication started successfully", timeout=60):
            zb.bad("probe bridge never started — see " + LOG)
            return 1

        # a fresh row per writer: distinct keys, so a lost update is DETECTABLE
        # (every key must end at its writer's final value).
        keys = []
        for i in range(N_WRITERS):
            # Mint the key EXPLICITLY — do not rely on a column default being
            # present in this db state (the first run passed empty uids to the
            # writers and the bridge correctly rejected them 22P02, masking the
            # actual race test).
            # ⚠️ first LINE only: the host psql appends its "INSERT 0 1" command tag
            # to RETURNING output, so .strip() alone yields "<uuid>\nINSERT 0 1" —
            # which every mutation then carried as an invalid uuid key (22P02), and
            # the bridge correctly rejected all of them. The product was never wrong.
            out = zb.psql("INSERT INTO public.counter_public (uid, value, inserted_at, updated_at) "
                          "VALUES (gen_random_uuid(), 0, now(), now()) RETURNING uid", quiet=True).strip()
            uid = out.splitlines()[0].strip() if out else ""
            if not uid:
                sys.exit("could not mint a counter_public key — check the table")
            keys.append(uid)

        # the shared broker may still be settling from a prior scenario's restart —
        # wait for it before the writers connect (a ConnectionRefused here scored
        # the concurrency test 24/24-wrong when the real cause was "broker not up").
        for _ in range(20):
            try:
                probe = await zb.connect_as(ADVERSARY); await probe.close(); break
            except Exception:
                await asyncio.sleep(1)
        nc = await zb.connect_as(ADVERSARY)
        js = nc.jetstream()

        # ── 1. concurrent writers, distinct keys, each its final value ────────
        pub_errors = []
        async def writer(i):
            uid = keys[i]
            for r in range(5):
                err = await one_write(js, uid, i * 100 + r, f"race1-{RUN}-{i}-{r}")
                if err:
                    pub_errors.append(err)
            return i * 100 + 4  # the final value for this key
        finals = await asyncio.gather(*(writer(i) for i in range(N_WRITERS)))
        # A swallowed publish failure scored this test "24/24 keys wrong" when the
        # writes never entered the stream at all. Publishes must ACK before
        # convergence is worth scoring.
        if pub_errors:
            zb.bad(f"{len(pub_errors)}/{N_WRITERS * 5} publishes FAILED — first: {pub_errors[0]} "
                   f"(harness/wiring, not a convergence result)")
            failed += 1
        await asyncio.sleep(6)
        alive1 = bridge.proc.poll() is None
        mismatched = 0
        for i, uid in enumerate(keys):
            got = zb.psql(f"SELECT value FROM public.counter_public WHERE uid='{uid}'", quiet=True).strip()
            if got != str(finals[i]):
                if mismatched < 3:
                    print(f"      key[{i}] {uid}: got {got!r}, want {finals[i]!r}")
                mismatched += 1
        if alive1 and mismatched == 0:
            zb.ok(f"{N_WRITERS} concurrent writers × 5 writes: every key converged to its final value, no lost/torn update")
        else:
            zb.bad(f"concurrency race: {mismatched}/{N_WRITERS} keys wrong, alive={alive1}")
            failed += 1

        # (The across-a-broker-restart race was removed: restarting the SHARED
        # nats-server cascades into every other consumer, and chaos.py already
        # proves jitter survival on its OWN owned broker. race.py stays
        # non-invasive — pure concurrent contention against nats.zig.)

        # ── 3. poison interleaved with legit, many senders ────────────────────
        pub_errors3 = []
        async def mixed(i):
            if i % 4 == 0:
                err = await one_write(js, keys[i], 0, f"race3-{RUN}-poison-{i}", raw=msgpack.packb([1, 2])[:1] + b"\xff")
            else:
                err = await one_write(js, keys[i], 9000 + i, f"race3-{RUN}-{i}")
            if err:
                pub_errors3.append(err)
        await asyncio.gather(*(mixed(i) for i in range(N_WRITERS)))
        if pub_errors3:
            zb.bad(f"{len(pub_errors3)}/{N_WRITERS} phase-3 publishes FAILED — first: {pub_errors3[0]}")
            failed += 1
        await asyncio.sleep(6)
        legit_applied = sum(
            zb.psql(f"SELECT value FROM public.counter_public WHERE uid='{keys[i]}'", quiet=True).strip() == str(9000 + i)
            for i in range(N_WRITERS) if i % 4 != 0)
        expected = sum(1 for i in range(N_WRITERS) if i % 4 != 0)
        if legit_applied == expected:
            zb.ok(f"poison interleaved with legit under concurrency: all {expected} legit writes applied, poison did not wedge the queue")
        else:
            zb.bad(f"poison wedge under concurrency: {legit_applied}/{expected} legit writes applied")
            failed += 1

        await nc.close()

        # ── 4. the audit ──────────────────────────────────────────────────────
        if not zb.leaks_available():
            print("  ⓘ  `leaks` not available on this host — memory audit skipped")
        else:
            rep = subprocess.run(["leaks", "--nocontext", str(bridge.proc.pid)],
                                 capture_output=True, text=True).stdout
            if "0 leaks for 0 total leaked bytes" in rep:
                zb.ok("after the concurrency storm: `leaks` reports 0")
            else:
                zb.bad(f"leaks after races: {[l for l in rep.splitlines() if 'leaks for' in l]}")
                failed += 1

    for uid in keys:
        zb.psql(f"DELETE FROM public.counter_public WHERE uid='{uid}'", quiet=True)
    zb.psql("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='zb_probe' AND NOT active) "
            "THEN PERFORM pg_drop_replication_slot('zb_probe'); END IF; END $$", quiet=True)
    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


zb.run(main)

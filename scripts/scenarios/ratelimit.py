#!/usr/bin/env python3
"""The ingress rate limit (§10fk): one loud principal cannot crowd out another tenant.

  set -a && . ./.env.bridge && set +a
  export NATS_CREDS=scripts/native/creds/bridge.creds
  scripts/scenarios/.venv/bin/python scripts/scenarios/ratelimit.py

Owns the only bridge: starts a probe with MUTATION_RATE_PER_PRINCIPAL=20 and
MUTATION_RATE_BURST=20 (no other bridge may be running, or the two would split the
mutations). A tenant-scoped, writable table is created and enabled here and dropped
at the end.

What is checked: bob floods 200 inserts as fast as NATS takes them; the burst lands
at once and the rest are served at the rate — NAK'd with the delay of their place
in the queue, redelivered by JetStream once each — so every write gets exactly one
`accepted` verdict and the last one arrives about (200 − 20) / 20 s later; alice,
in another tenant, writes during the flood and gets her `accepted` verdict within
two seconds; libzb as bob, told to send 60 writes, lands all 60 in the master with
nothing lost and nothing re-sent, at the rate.

The stream's own cap (MUTATION_BACKLOG_PER_PRINCIPAL in up.sh, §10fk) is a fourth
phase, run only with ZB_RATELIMIT_CAP_PHASE=1 on a stack whose MUTATIONS stream was
created with a small cap (up.sh with MUTATION_BACKLOG_PER_PRINCIPAL=100): bob floods
past it, the excess is refused at publish (the PubAck errors — a client keeps the
entry), alice is answered, and the master holds exactly the cap plus what the rate
let through meanwhile.
"""
import asyncio, sys, time, uuid
import msgpack
import zb
from clients import Lib, fresh_sqlite

T = "rl_t"
FAILED = []
RATE, BURST, FLOOD = 20, 20, 200


def check(msg, cond):
    (zb.ok if cond else zb.bad)(msg)
    if not cond: FAILED.append(msg)


async def main():
    if zb.another_bridge_running():
        sys.exit("another bridge is running — this scenario owns the only one (stop it first)")
    zb.psql(f"DROP TABLE IF EXISTS public.{T}", quiet=True)
    zb.forget_table(T)  # the schema key too — a DROP after the probe bridge exits tombstones nobody
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), note text, tenant_id text NOT NULL, "
            f"inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)", quiet=True)
    zb.psql(f"SELECT public.zebridge_enable('public.{T}'::regclass, tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', "
            f"tombstone_col => 'deleted_at', tiebreak_col => NULL, publication => 'my_pub', dry_run => false)", quiet=True)

    with zb.Bridge("/tmp/zb_ratelimit_bridge.log", MUTATION_RATE_PER_PRINCIPAL=str(RATE), MUTATION_RATE_BURST=str(BURST)) as br:
        await asyncio.sleep(8)
        check("the probe bridge announces the limit", "ingress rate limit" in br.text())

        async def lane(who):
            nc = await zb.connect_as(who)
            js = nc.jetstream()
            verdicts = {}
            ack_prefix = zb.TOPOLOGY["subjects"]["mutation_ack_prefix"]
            sub = await zb.subscribe(nc, f"{ack_prefix}.{who}.*")

            async def collect():
                async for m in sub.messages:
                    try: verdicts.setdefault(m.subject.rsplit(".", 1)[-1], []).append(zb.decode(m.data))
                    except Exception: pass
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.3)
            tenant = zb.psql(f"SELECT tenant_id FROM zebridge_user_tenants WHERE principal = '{who}' LIMIT 1").strip()

            async def send(note):
                uid = str(uuid.uuid4()); now = time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime())
                msg_id = f"rl-{who}-{uuid.uuid4().hex[:10]}"
                await js.publish(zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, T, "insert"),
                                 msgpack.packb({"key": {"uid": uid}, "data": {"uid": uid, "note": note, "tenant_id": tenant, "updated_at": now}, "version": now, "client_id": f"c-{who}"}),
                                 headers={"Nats-Msg-Id": msg_id})
                return msg_id
            return nc, verdicts, task, send

        bob_nc, bob_v, bob_task, bob_send = await lane("bob")
        alice_nc, alice_v, alice_task, alice_send = await lane("alice")

        # ── bob floods; alice writes once, 300 ms into it ────────────────────────
        t0 = time.monotonic()
        bob_ids = []
        door = [0]  # publishes the stream refused (a cap smaller than the flood)

        async def flood():
            for i in range(FLOOD):
                try: bob_ids.append(await bob_send(f"flood {i}"))
                except Exception: door[0] += 1  # noqa: BLE001 — the PubAck error is the stream's cap
        alice_id = None
        alice_at = alice_done = None

        async def alice_once():
            nonlocal alice_id, alice_at, alice_done
            await asyncio.sleep(0.3)
            alice_at = time.monotonic()
            alice_id = await alice_send("alice, during the flood")
            while alice_id not in alice_v and time.monotonic() - alice_at < 10: await asyncio.sleep(0.05)
            alice_done = time.monotonic()
        await asyncio.gather(flood(), alice_once())
        flood_s = time.monotonic() - t0
        deadline = time.monotonic() + 40
        taken = len(bob_ids)
        while time.monotonic() < deadline and sum(1 for i in bob_ids if i in bob_v) < taken: await asyncio.sleep(0.2)
        served_s = time.monotonic() - t0
        got = [bob_v.get(i, [None])[0] for i in bob_ids]
        accepted = sum(1 for v in got if v and v.get("status") == "accepted")
        others = [v for v in got if v and v.get("status") != "accepted"]
        missing = sum(1 for v in got if v is None)
        dup = sum(1 for i in bob_ids if len(bob_v.get(i, [])) > 1)
        expect_s = max(taken - BURST, 0) / RATE
        check(f"bob's flood of {FLOOD} (sent in {flood_s:.1f} s; the stream took {taken}, refused {door[0]} at the door) was served at the rate: {accepted} accepted, {len(others)} other, {missing} without a verdict, {dup} answered twice, last verdict after {served_s:.1f} s (expected about {expect_s:.0f} s)",
              missing == 0 and dup == 0 and not others and accepted == taken and taken > BURST and expect_s * 0.7 <= served_s <= expect_s * 1.5 + 3)
        redeliveries = sum(1 for l in br.text().splitlines() if "rate limited" in l)
        check(f"the bridge refused and rescheduled rather than dropping ({redeliveries} log line(s))", redeliveries >= 1)
        landed = int(zb.psql(f"SELECT count(*) FROM {T} WHERE tenant_id = 'globex'", quiet=True).strip() or 0)
        check(f"the master holds every write the stream took ({landed})", landed == taken)
        av = alice_v.get(alice_id, [None])[0]
        check(f"alice's write during the flood: {av.get('status') if av else 'no verdict'} in {((alice_done - alice_at) * 1000) if alice_done else -1:.0f} ms",
              av is not None and av.get("status") == "accepted" and alice_done - alice_at < 2.0)

        for tk, nc in ((bob_task, bob_nc), (alice_task, alice_nc)):
            tk.cancel(); await nc.close()

        # ── libzb as bob: the outbox holds, nothing is lost ──────────────────────
        await asyncio.sleep(2)  # bob's bucket refills
        db = "/tmp/zb-ratelimit.sqlite3"; fresh_sqlite(db)
        em = Lib(db, [T], "py-ratelimit", principal="bob")
        n = 60
        for i in range(n):
            u = str(uuid.uuid4())
            em.mutate(T, "insert", {"uid": u}, {"uid": u, "note": f"libzb {i}", "tenant_id": "globex"})
        t1 = time.monotonic(); held = 0; last = None
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            last = em.flush(500)
            if last.get("sent", 0) == 0 and (last.get("settled", 0) == 0): held += 1
            pending = em.q("SELECT count(*) FROM _zebridge_outbox")[0][0]
            if pending == 0: break
            em.poll(200)
        elapsed = time.monotonic() - t1
        vc = (last or {}).get("verdicts", {})
        landed2 = int(zb.psql(f"SELECT count(*) FROM {T} WHERE note LIKE 'libzb %'", quiet=True).strip() or 0)
        check(f"libzb landed all {n} writes in {elapsed:.1f} s (about {(n - BURST) / RATE:.0f} s at the rate), outbox empty, verdicts {vc}, {held} empty flush(es)",
              landed2 == n and pending == 0 and vc.get("rate_limited", 0) == 0 and vc.get("accepted", 0) == n and elapsed >= (n - BURST) / RATE * 0.7)
        em.close()

        # ── the stream's cap: refused at the door, nobody else notices ───────────
        import json as _json, os as _os
        r = zb.nats_cli("stream", "info", zb.TOPOLOGY["streams"]["mutations"], "--json")
        cfg = _json.loads(r.stdout)["config"] if r.returncode == 0 else {}
        cap = int(cfg.get("max_msgs_per_subject") or -1)
        if _os.environ.get("ZB_RATELIMIT_CAP_PHASE") != "1":
            print(f"  · the stream's cap phase is off (ZB_RATELIMIT_CAP_PHASE=1 on a stack created with a small cap; this one: retention {cfg.get('retention')}, cap {cap}, discard {cfg.get('discard')})")
        elif cap <= 0 or cfg.get("retention") != "workqueue" or not cfg.get("discard_new_per_subject"):
            check(f"the cap phase needs workqueue retention, discard new per subject and a cap (this stream: {cfg.get('retention')}, cap {cap})", False)
        else:
            await asyncio.sleep(3)
            bob_nc, bob_v, bob_task, bob_send = await lane("bob")
            alice_nc, alice_v, alice_task, alice_send = await lane("alice")
            sent, refused = [], 0
            t2 = time.monotonic()
            for i in range(cap + 100):
                try:
                    sent.append(await bob_send(f"cap {i}"))
                except Exception as e:  # noqa: BLE001 — the PubAck error is the refusal
                    refused += 1
                    if refused == 1: print(f"    first refusal: {str(e)[:120]}")
            burst_s = time.monotonic() - t2
            a_at = time.monotonic(); aid = await alice_send("alice, past the cap")
            while aid not in alice_v and time.monotonic() - a_at < 10: await asyncio.sleep(0.05)
            av = alice_v.get(aid, [None])[0]
            check(f"past the cap ({cap}) bob's publishes are refused at the door: {refused} refused of {cap + 100} in {burst_s:.1f} s, {len(sent)} accepted into the stream",
                  refused >= 100 - RATE * (burst_s + 1) - BURST and refused > 0)
            check(f"alice is answered anyway: {av.get('status') if av else 'no verdict'} in {(time.monotonic() - a_at) * 1000:.0f} ms", av is not None and av.get("status") == "accepted")
            deadline = time.monotonic() + cap / RATE + 20
            while time.monotonic() < deadline and sum(1 for i in sent if i in bob_v) < len(sent): await asyncio.sleep(0.5)
            got = sum(1 for i in sent if bob_v.get(i, [{}])[0].get("status") == "accepted")
            check(f"every write the stream took was served: {got} of {len(sent)} accepted", got == len(sent))
            for tk, nc in ((bob_task, bob_nc), (alice_task, alice_nc)):
                tk.cancel(); await nc.close()

    zb.psql(f"DROP TABLE public.{T}", quiet=True)
    fresh_sqlite(db)
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(zb.run(main))

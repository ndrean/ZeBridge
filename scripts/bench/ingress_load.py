#!/usr/bin/env python3
"""Ingress load for scripts/bench/ingress.py: sensor_events inserts through NATS, verdicts timed.

The rows are 09-event's (examples/09-event/sensors.py); what varies is the write's ID and fate:

    legacy   Nats-Msg-Id = the reading's UUID hex, as sensors.py sends it
    zb1      a zb1 ID as libzb and zb-client-ts build it (escaped, framed, about 130 bytes)
    dup      phase 1 sends zb1 writes; after --dup-wait-s (past the duplicate window) phase 2
             resends the very same messages, which only the bridge itself can recognise
    reject   a --reject-share of the writes carries an unknown column: refused, and in a
             batch it fails the whole transaction, so the bridge replays that batch one by one
    single   one zb1 write at a time, each waiting for its own verdict: every write takes
             the listener's single-message path, so its latency is that path's alone

One JSON line on stdout at the end: sent, publish errors, verdicts by status, and verdict
latency percentiles (phase 2 apart for `dup`).
"""
import argparse, asyncio, datetime, json, math, random, time, uuid

import msgpack
import nats

TABLE = "sensor_events"


def iso(ts):
    return datetime.datetime.fromtimestamp(ts, datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def comp(s):
    """One message-ID component, escaped as the cores do (A-Z a-z 0-9 _ : pass, else ~HH)."""
    return "".join(chr(b) if b < 0x80 and (chr(b).isalnum() or chr(b) in "_:") else f"~{b:02X}" for b in s.encode())


def zb1_id(created_ms, client, key, version):
    return f"zb1-t{created_ms}-INSERT-{comp(client)}-{comp(TABLE)}-k1-{comp(key)}-{comp(version)}"


class Phase:
    def __init__(self):
        self.sent = 0
        self.errors = 0
        self.verdicts = {}
        self.ack_ms = []

    def summary(self):
        ms = sorted(self.ack_ms)
        p = lambda q: round(ms[min(len(ms) - 1, int(q * len(ms)))], 2) if ms else None
        return {"sent": self.sent, "publish_errors": self.errors, "verdicts": self.verdicts,
                "ack_p50_ms": p(.5), "ack_p90_ms": p(.9), "ack_p99_ms": p(.99), "acks": len(ms)}


async def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", required=True)
    ap.add_argument("--creds", required=True)
    ap.add_argument("--principal", default="bob")
    ap.add_argument("--tenant", default="globex")
    ap.add_argument("--variant", choices=("legacy", "zb1", "dup", "reject", "single"), required=True)
    ap.add_argument("--rate", type=float, required=True, help="writes per second offered by this process")
    ap.add_argument("--seconds", type=float, required=True)
    ap.add_argument("--first-id", type=int, default=0)
    ap.add_argument("--workers", type=int, default=100, help="concurrent publishers, as sensors are")
    ap.add_argument("--reject-share", type=float, default=0.02)
    ap.add_argument("--dup-wait-s", type=float, default=8.0)
    ap.add_argument("--principals", default="", help="comma-separated principals, one connection each (creds in --creds-dir)")
    ap.add_argument("--creds-dir", default="")
    a = ap.parse_args()

    phases = [Phase(), Phase()]
    sent_at, answered = {}, {}

    async def on_verdict(m):
        mid = m.subject.rsplit(".", 1)[-1]  # an ID never holds a dot
        if mid in answered:
            answered[mid].set()
        entry = sent_at.pop(mid, None)
        if entry is None:
            return  # another process's write
        t, ph = entry
        phases[ph].ack_ms.append((time.monotonic() - t) * 1000)
        try:
            status = json.loads(m.data).get("status", "?")
        except Exception:
            status = "unreadable"
        phases[ph].verdicts[status] = phases[ph].verdicts.get(status, 0) + 1

    # Writers spread over principals: one connection each, as devices are. Receipts are
    # grouped per principal, so how many principals share a batch matters.
    names = [x for x in a.principals.split(",") if x] or [a.principal]
    conns = []
    for name in names:
        creds = f"{a.creds_dir}/{name}.creds" if a.principals else a.creds
        nc = await nats.connect(a.url, user_credentials=creds, inbox_prefix=f"_INBOX.{name}".encode())
        await nc.subscribe(f"mutation_ack.{name}.*", cb=on_verdict)
        conns.append((name, nc, nc.jetstream()))
    period = a.workers / a.rate
    sent_log = []  # (msg_id, body) for the dup variant's phase 2

    async def publish(ph, mid, body, w=0):
        name, _, js = conns[w % len(conns)]
        sent_at[mid] = (time.monotonic(), ph)
        try:
            await js.publish(f"mutation.{name}.{TABLE}.insert", body, headers={"Nats-Msg-Id": mid})
            phases[ph].sent += 1
        except Exception:
            phases[ph].errors += 1
            sent_at.pop(mid, None)

    async def worker(w, until):
        sid = a.first_id + w
        k, t0 = 0, time.monotonic() + random.uniform(0, period)
        while True:
            due = t0 + k * period
            if due >= until:
                return
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            k += 1
            eid, now = uuid.uuid7(), time.time()
            row = {"event_id": str(eid), "tenant_id": a.tenant, "sensor_id": sid, "kind": "temperature",
                   "value": round(21 + 2 * math.cos(now), 3), "updated_at": iso(now)}
            if a.variant == "reject" and random.random() < a.reject_share:
                row["no_such_column"] = 1
            client = f"sensor-{sid}"
            body = msgpack.packb({"key": {"event_id": row["event_id"]}, "data": row, "version": row["updated_at"], "client_id": client})
            mid = eid.hex if a.variant == "legacy" else zb1_id(int(now * 1000), client, row["event_id"], row["updated_at"])
            if a.variant == "dup":
                sent_log.append((mid, body, w))
            await publish(0, mid, body, w)

    async def settle(ph, timeout=30):
        end = time.monotonic() + timeout
        while time.monotonic() < end and sum(phases[ph].verdicts.values()) < phases[ph].sent:
            await asyncio.sleep(.1)

    async def single(until):
        while time.monotonic() < until:
            eid, now = uuid.uuid7(), time.time()
            row = {"event_id": str(eid), "tenant_id": a.tenant, "sensor_id": a.first_id, "kind": "temperature",
                   "value": round(21 + 2 * math.cos(now), 3), "updated_at": iso(now)}
            client = f"sensor-{a.first_id}"
            body = msgpack.packb({"key": {"event_id": row["event_id"]}, "data": row, "version": row["updated_at"], "client_id": client})
            mid = zb1_id(int(now * 1000), client, row["event_id"], row["updated_at"])
            answered[mid] = asyncio.Event()
            await publish(0, mid, body)
            try:
                await asyncio.wait_for(answered[mid].wait(), 10)
            except asyncio.TimeoutError:
                pass
            answered.pop(mid, None)

    start = time.monotonic()
    if a.variant == "single":
        await single(start + a.seconds)
    else:
        await asyncio.gather(*(worker(w, start + a.seconds) for w in range(a.workers)))
    await settle(0)
    result = {"variant": a.variant, "offered_per_s": a.rate, "seconds": a.seconds, "phase1": phases[0].summary()}
    if a.variant == "dup":
        await asyncio.sleep(a.dup_wait_s)  # past the duplicate window: only the bridge can tell
        resend_start = time.monotonic()
        chunk = max(1, len(sent_log) // a.workers)

        async def resend(part):
            for i, (mid, body, w) in enumerate(part):
                due = resend_start + (i * a.workers) / a.rate
                await asyncio.sleep(max(0.0, due - time.monotonic()))
                await publish(1, mid, body, w)  # the same principal: a receipt is per principal

        await asyncio.gather(*(resend(sent_log[i:i + chunk]) for i in range(0, len(sent_log), chunk)))
        await settle(1)
        result["phase2_duplicates"] = phases[1].summary()
    result["unverdicted"] = len(sent_at)
    print(json.dumps(result), flush=True)
    for _, nc, _ in conns:
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())

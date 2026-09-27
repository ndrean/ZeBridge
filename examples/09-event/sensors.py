#!/usr/bin/env python3
"""09-event: simulated sensors. Each one publishes a reading every 20 ms as an edge write.

    scripts/scenarios/.venv/bin/python examples/09-event/sensors.py                     # 20 sensors × 50/s = 1,000 rows/s, 60 s
    scripts/scenarios/.venv/bin/python examples/09-event/sensors.py --sensors 100 --seconds 300

The path a device takes: a mutation on `mutation.<principal>.sensor_events.insert`, the
bridge writes it to PostgreSQL under the principal's tenant (RLS), and the row comes back
out as CDC to every replica. PostgreSQL is the only writer; nothing here touches it.

Each reading's key is a UUIDv7 minted here (`uuid.uuid7()`, Python 3.14): its first 48
bits are the reading's millisecond, so the replica knows WHEN a reading was taken without
a timestamp column, and "how old is the newest reading" measures the whole pipeline.

A sensor keeps its own clock (a deadline every period, not a sleep after each send), so a
slow publish does not stretch the rate. The bridge answers every write on
`mutation_ack.<principal>.<msg_id>`; the report counts those verdicts and their latency.

The sensors: sensor_id % 3 picks the kind — temperature (°C), humidity (%), pressure
(hPa). Each reads a cosine around its kind's base, period `--wave-s` (2 s) and its own
phase, plus a slow random walk about 10% of the wave's amplitude (mean-reverting, so it
wanders without running away), a little noise, and a rare spike (1 in 2,000) for the
`alarms` query. The
period makes the queries checkable by eye: a 1 s bucket holds half a wave, so the 1 s
average swings each second, while a 10 s moving average holds five whole waves and sits
flat on the base.
"""
import argparse, asyncio, datetime, json, math, os, pathlib, random, sys, time, uuid

import msgpack
import nats

ROOT = pathlib.Path(__file__).resolve().parents[2]
TABLE, TENANT = "sensor_events", "globex"
# kind, base, amplitude of the wave, noise (σ), spike
WALK = 0.10  # the random walk's spread, as a share of the wave's amplitude
KINDS = (("temperature", 21.0, 2.0, 0.05, 15.0), ("humidity", 45.0, 5.0, 0.2, 40.0), ("pressure", 1013.0, 3.0, 0.1, 30.0))


def iso(ts: float) -> str:
    """The version column as CDC renders a timestamptz (PROTOCOL §7.3)."""
    return datetime.datetime.fromtimestamp(ts, datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class Stats:
    def __init__(self):
        self.sent = 0
        self.pub_errors = 0
        self.verdicts: dict[str, int] = {}
        self.sent_at: dict[str, float] = {}
        self.ack_ms: list[float] = []

    def line(self, elapsed: float) -> str:
        ms = sorted(self.ack_ms) or [0.0]
        p = lambda q: ms[min(len(ms) - 1, int(q * len(ms)))]
        v = " ".join(f"{k} {n:,}" for k, n in sorted(self.verdicts.items()))
        return (f"{elapsed:6.1f} s  sent {self.sent:,} ({self.sent / max(elapsed, 1e-9):,.0f}/s)  verdicts: {v or '-'}  "
                f"ack p50 {p(0.5):.1f} ms p99 {p(0.99):.1f} ms  publish errors {self.pub_errors}")


async def sensor(js, st: Stats, principal: str, tenant: str, sid: int, period: float, wave: float, until: float):
    kind, base, amp, noise, spike = KINDS[sid % len(KINDS)]
    phase = random.uniform(0, 2 * math.pi)
    # The walk: each step pulls 0.1% back toward 0 (at 50 readings a second it drifts over
    # ~20 s, ten waves) and adds a kick sized so the walk's
    # spread settles at WALK × amp (an Ornstein-Uhlenbeck process, stepped per reading).
    drift, pull = 0.0, 0.001
    kick = WALK * amp * math.sqrt(2 * pull)
    subject = f"mutation.{principal}.{TABLE}.insert"
    k, t0 = 0, time.monotonic() + random.uniform(0, period)  # spread the sensors across the period
    while True:
        due = t0 + k * period
        if due >= until:
            return
        await asyncio.sleep(max(0.0, due - time.monotonic()))
        k += 1
        eid = uuid.uuid7()
        now = time.time()
        drift = drift * (1 - pull) + random.gauss(0, kick)
        reading = base + drift + amp * math.cos(2 * math.pi * now / wave + phase) + random.gauss(0, noise)
        if random.random() < 1 / 2000:
            reading += spike
        row = {"event_id": str(eid), "tenant_id": tenant, "sensor_id": sid, "kind": kind,
               "value": round(reading, 3), "updated_at": iso(now)}
        msg_id = eid.hex  # a subject token: no dots
        st.sent_at[msg_id] = time.monotonic()
        try:
            await js.publish(subject, msgpack.packb({"key": {"event_id": row["event_id"]}, "data": row,
                                                     "version": row["updated_at"], "client_id": f"sensor-{sid}"}),
                             headers={"Nats-Msg-Id": msg_id})
            st.sent += 1
        except Exception as e:
            st.pub_errors += 1
            st.sent_at.pop(msg_id, None)
            if st.pub_errors <= 3:
                print(f"sensor {sid}: publish failed: {type(e).__name__}: {e}", flush=True)


async def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    ap.add_argument("--principal", default="bob")
    ap.add_argument("--tenant", default="globex", help="the principal's tenant: every row carries it (RLS refuses another)")
    ap.add_argument("--creds", default="", help="default scripts/native/creds/<principal>.creds")
    ap.add_argument("--sensors", type=int, default=20)
    ap.add_argument("--first-id", type=int, default=0, help="the first sensor_id: several processes, disjoint ids")
    ap.add_argument("--ack-file", default="", help="also write every ack latency (ms), one per line, to merge runs")
    ap.add_argument("--period-ms", type=float, default=20.0)
    ap.add_argument("--seconds", type=float, default=60.0)
    ap.add_argument("--wave-s", type=float, default=2.0, help="the period of each sensor's cosine")
    a = ap.parse_args()
    creds = a.creds or str(ROOT / "scripts/native/creds" / f"{a.principal}.creds")

    nc = await nats.connect(a.url, user_credentials=creds, inbox_prefix=f"_INBOX.{a.principal}".encode())
    js = nc.jetstream()
    st = Stats()

    async def on_verdict(m):
        msg_id = m.subject.rsplit(".", 1)[-1]
        t = st.sent_at.pop(msg_id, None)
        if t is None:
            return  # another process's write: every sensors.py of a principal hears all its verdicts
        st.ack_ms.append((time.monotonic() - t) * 1000)
        try:
            status = json.loads(m.data).get("status", "?")
        except Exception:
            status = "unreadable"
        st.verdicts[status] = st.verdicts.get(status, 0) + 1

    await nc.subscribe(f"mutation_ack.{a.principal}.*", cb=on_verdict)
    rate = a.sensors * 1000 / a.period_ms
    print(f"{a.sensors} sensors × every {a.period_ms:g} ms = {rate:,.0f} readings/s for {a.seconds:g} s, "
          f"as {a.principal} on {a.tenant}", flush=True)

    start = time.monotonic()
    until = start + a.seconds
    tasks = [asyncio.create_task(sensor(js, st, a.principal, a.tenant, a.first_id + i, a.period_ms / 1000, a.wave_s, until))
             for i in range(a.sensors)]

    async def report():
        while True:
            await asyncio.sleep(5)
            print(st.line(time.monotonic() - start), flush=True)

    rep = asyncio.create_task(report())
    await asyncio.gather(*tasks)
    # The last verdicts: the bridge answers within a second or two of the last write.
    for _ in range(100):
        if sum(st.verdicts.values()) >= st.sent:
            break
        await asyncio.sleep(0.1)
    rep.cancel()
    print("done  " + st.line(a.seconds), flush=True)
    if a.ack_file:
        with open(a.ack_file, "w") as f:
            f.write("\n".join(f"{x:.3f}" for x in st.ack_ms))
    missing = st.sent - sum(st.verdicts.values())
    if missing:
        print(f"⚠️ {missing:,} writes without a verdict yet", flush=True)
    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())

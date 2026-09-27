#!/usr/bin/env python3
"""09-event: the MQTT gateway. MQTT readings in, mutations out.

    scripts/scenarios/.venv/bin/python examples/09-event/mqtt_gateway.py

nats-server speaks MQTT 3.1.1 on port 1883 (the `mqtt` block of nats-server-jwt.conf). A
device publishes the topic `sensors/<tenant>/<sensor_id>/<kind>`, which arrives in NATS as
the subject `sensors.<tenant>.<sensor_id>.<kind>`. A device cannot write a row itself:
MQTT carries no headers (the mutation needs `Nats-Msg-Id`), and the payload is whatever the
device sends. This gateway listens on `sensors.<tenant>.>` in a queue group (start more
to share the load) and turns each reading into an insert mutation, as its own principal
`mqttgw`, which PostgreSQL maps to the tenant (provision.py).

The payload, either form:
    21.43                                   a bare number: stamped with its arrival time
    {"value": 21.43, "ts_ms": 1790500000123}   the device's own time, in milliseconds

The row's key is a UUIDv7 built from that time, so the replica's "newest reading age"
measures from the device when it sends `ts_ms`. A reading that cannot be read (not a
number, an unknown kind) is counted and dropped, never written.

Two layers keep tenants apart. The device's credential may only PUBLISH `sensors.<its
tenant>.>` (NATS refuses anything else: "Publish Violation"). The gateway's may only
SUBSCRIBE there and write as `mqttgw`, whose tenant PostgreSQL's row-level security checks.
"""
import argparse, asyncio, datetime, json, os, pathlib, random, sys, time, uuid

import msgpack
import nats

ROOT = pathlib.Path(__file__).resolve().parents[2]
TABLE = "sensor_events"
KINDS = {"temperature", "humidity", "pressure"}


def uuid7_at(ms: int) -> uuid.UUID:
    """A UUIDv7 for a given millisecond: 48 bits of time, version 7, variant 10, random."""
    rand = random.getrandbits(74)
    n = (ms & (2**48 - 1)) << 80 | 0x7 << 76 | (rand >> 62) << 64 | 0b10 << 62 | (rand & (2**62 - 1))
    return uuid.UUID(int=n)


def iso(ts: float) -> str:
    """The version column as CDC renders a timestamptz (PROTOCOL §7.3)."""
    return datetime.datetime.fromtimestamp(ts, datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def reading(data: bytes, now_ms: int) -> tuple[float, int]:
    """(value, ms) from a bare number or {"value", "ts_ms"}. Raises ValueError otherwise."""
    text = data.decode().strip()
    if text.startswith("{"):
        j = json.loads(text)
        return float(j["value"]), int(j.get("ts_ms") or now_ms)
    return float(text), now_ms


async def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    ap.add_argument("--principal", default="mqttgw")
    ap.add_argument("--creds", default="", help="default scripts/native/creds/<principal>.creds")
    ap.add_argument("--tenant", default="globex")
    ap.add_argument("--in-flight", type=int, default=500, help="writes awaiting JetStream's acknowledgement at once")
    a = ap.parse_args()
    creds = a.creds or str(ROOT / "scripts/native/creds" / f"{a.principal}.creds")
    nc = await nats.connect(a.url, user_credentials=creds, inbox_prefix=f"_INBOX.{a.principal}".encode())
    js = nc.jetstream()
    subject = f"mutation.{a.principal}.{TABLE}.insert"
    gate = asyncio.Semaphore(a.in_flight)
    n = {"received": 0, "written": 0, "unreadable": 0, "publish_failed": 0}
    verdicts: dict[str, int] = {}

    async def on_verdict(m):
        try:
            s = json.loads(m.data).get("status", "?")
        except Exception:
            s = "unreadable"
        verdicts[s] = verdicts.get(s, 0) + 1

    async def write(sensor_id: int, kind: str, value: float, ms: int):
        eid = uuid7_at(ms)
        stamp = iso(time.time())
        row = {"event_id": str(eid), "tenant_id": a.tenant, "sensor_id": sensor_id, "kind": kind,
               "value": round(value, 3), "updated_at": stamp}
        try:
            await js.publish(subject, msgpack.packb({"key": {"event_id": row["event_id"]}, "data": row,
                                                     "version": stamp, "client_id": a.principal}),
                             headers={"Nats-Msg-Id": eid.hex})
            n["written"] += 1
        except Exception as e:
            n["publish_failed"] += 1
            if n["publish_failed"] <= 3:
                print(f"publish failed: {type(e).__name__}: {e}", flush=True)
        finally:
            gate.release()

    async def on_reading(m):
        n["received"] += 1
        parts = m.subject.split(".")  # sensors.<tenant>.<sensor_id>.<kind>
        try:
            if len(parts) != 4 or parts[1] != a.tenant or parts[3] not in KINDS:
                raise ValueError(f"unexpected subject {m.subject}")
            value, ms = reading(m.data, int(time.time() * 1000))
            sensor_id = int(parts[2])
        except (ValueError, KeyError, json.JSONDecodeError):
            n["unreadable"] += 1
            return
        await gate.acquire()
        asyncio.create_task(write(sensor_id, parts[3], value, ms))

    await nc.subscribe(f"mutation_ack.{a.principal}.*", cb=on_verdict)
    await nc.subscribe(f"sensors.{a.tenant}.>", queue="mqttgw", cb=on_reading)
    print(f"gateway: sensors.{a.tenant}.> → {subject}, as {a.principal}", flush=True)
    last = dict(n)
    while True:
        await asyncio.sleep(5)
        rate = (n["received"] - last["received"]) / 5
        v = " ".join(f"{k} {c:,}" for k, c in sorted(verdicts.items()))
        print(f"received {n['received']:,} ({rate:,.0f}/s)  written {n['written']:,}  verdicts: {v or '-'}  "
              f"unreadable {n['unreadable']}  publish failed {n['publish_failed']}", flush=True)
        last = dict(n)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

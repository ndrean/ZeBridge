#!/usr/bin/env python3
"""09-event: simulated MQTT devices. The same sensors as sensors.py, speaking plain MQTT.

    scripts/scenarios/.venv/bin/python examples/09-event/mqtt_sensors.py                    # 20 devices × 50/s, 60 s
    scripts/scenarios/.venv/bin/python examples/09-event/mqtt_sensors.py --qos 1 --bare     # QoS 1, bare numbers

Each device is its own MQTT connection (paho-mqtt, as on a real device), logs in with its
bearer JWT as the password (in operator mode MQTT cannot sign the server's nonce), and
publishes `sensors/<tenant>/<sensor_id>/<kind>` every 20 ms. mqtt_gateway.py turns each
reading into a row. The values are sensors.py's: a 2 s cosine around the kind's base, a
slow random walk, a little noise and a rare spike.

The payload is {"value": v, "ts_ms": t} (the device's own time, which becomes the row's
UUIDv7), or with --bare just the number, stamped by the gateway on arrival.
"""
import argparse, json, math, os, pathlib, random, re, sys, threading, time

import paho.mqtt.client as mqtt

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from sensors import KINDS, WALK  # the same signal as the NATS sensors


def jwt_of(creds_path: str) -> str:
    text = open(creds_path).read()
    return re.search(r"-----BEGIN NATS USER JWT-----\s*(\S+)\s*------END NATS USER JWT------", text).group(1)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--creds", default=str(ROOT / "scripts/native/creds/mqtt_globex.creds"))
    ap.add_argument("--tenant", default="globex")
    ap.add_argument("--sensors", type=int, default=20)
    ap.add_argument("--first-id", type=int, default=0)
    ap.add_argument("--period-ms", type=float, default=20.0)
    ap.add_argument("--seconds", type=float, default=60.0)
    ap.add_argument("--wave-s", type=float, default=2.0)
    ap.add_argument("--qos", type=int, default=0, choices=(0, 1))
    ap.add_argument("--bare", action="store_true", help="send the bare number, no device timestamp")
    a = ap.parse_args()
    jwt = jwt_of(a.creds)

    clients = []
    for i in range(a.sensors):
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"dev-{a.tenant}-{a.first_id + i}")
        c.username_pw_set("device", jwt)  # any non-empty user name; the JWT is the credential
        c.connect(a.host, a.port, keepalive=30)
        c.loop_start()
        clients.append(c)
    deadline = time.time() + 5
    while not all(c.is_connected() for c in clients) and time.time() < deadline:
        time.sleep(0.05)
    up = sum(c.is_connected() for c in clients)
    print(f"{up}/{a.sensors} devices connected over MQTT; every {a.period_ms:g} ms for {a.seconds:g} s, QoS {a.qos}", flush=True)
    if up < a.sensors:
        sys.exit("some devices could not connect: is the mqtt block on, and the JWT a bearer token?")

    period = a.period_ms / 1000
    sent = [0]
    lock = threading.Lock()

    def device(idx: int, c: mqtt.Client):
        sid = a.first_id + idx
        kind, base, amp, noise, spike = KINDS[sid % len(KINDS)]
        phase, drift, pull = random.uniform(0, 2 * math.pi), 0.0, 0.001
        kick = WALK * amp * math.sqrt(2 * pull)
        topic = f"sensors/{a.tenant}/{sid}/{kind}"
        t0 = time.monotonic() + random.uniform(0, period)
        end, k = t0 + a.seconds, 0
        while True:
            due = t0 + k * period
            if due >= end:
                return
            time.sleep(max(0.0, due - time.monotonic()))
            k += 1
            now = time.time()
            drift = drift * (1 - pull) + random.gauss(0, kick)
            v = base + drift + amp * math.cos(2 * math.pi * now / a.wave_s + phase) + random.gauss(0, noise)
            if random.random() < 1 / 2000:
                v += spike
            payload = f"{v:.3f}" if a.bare else json.dumps({"value": round(v, 3), "ts_ms": int(now * 1000)})
            c.publish(topic, payload, qos=a.qos)
            with lock:
                sent[0] += 1

    threads = [threading.Thread(target=device, args=(i, c), daemon=True) for i, c in enumerate(clients)]
    start = time.monotonic()
    for t in threads:
        t.start()
    while any(t.is_alive() for t in threads):
        time.sleep(5)
        el = time.monotonic() - start
        print(f"{el:6.1f} s  published {sent[0]:,} ({sent[0] / el:,.0f}/s)", flush=True)
    time.sleep(1)
    for c in clients:
        c.loop_stop()
        c.disconnect()
    print(f"done: {sent[0]:,} readings published over MQTT", flush=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""09-event: ask the event service, once or on a loop.

    scripts/scenarios/.venv/bin/python examples/09-event/ask.py freshness
    scripts/scenarios/.venv/bin/python examples/09-event/ask.py moving_avg '{"kind": "humidity", "window_s": 30}'
    scripts/scenarios/.venv/bin/python examples/09-event/ask.py per_minute
    scripts/scenarios/.venv/bin/python examples/09-event/ask.py alarms '{"kind": "temperature", "above": 30}'
    scripts/scenarios/.venv/bin/python examples/09-event/ask.py watch --seconds 60

The asker is `bob`, an ordinary client of globex: it holds no replica of the table (it is
on-demand: nothing seeds), it sends a question on `query.globex.<name>`, and libzb's
`zb_client_request` returns the answer, unpacking a compressed or object-stored one by
itself. PostgreSQL never sees a question.

`watch` asks `freshness` and `moving_avg` every second and prints, per sample, how old the
newest reading was when the service answered, the replica's row count and one sensor's
moving average; at the end, the age's percentiles. Run it while sensors.py writes.
"""
import argparse, ctypes, json, os, pathlib, statistics, sys, time

ROOT = pathlib.Path(__file__).resolve().parents[2]
LIB = ROOT / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if sys.platform == "darwin" else "libzbcore.so")
TABLE, TENANT = "sensor_events", "globex"


class Asker:
    def __init__(self, url: str, creds: str, principal: str, db: str):
        lib = ctypes.CDLL(str(LIB))
        lib.zb_free.argtypes = [ctypes.c_void_p]
        lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
        lib.zb_client_close.argtypes = [ctypes.c_uint64]
        for n, extra in (("sync", []), ("request", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint64])):
            f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + extra
        self.lib = lib
        self.h = lib.zb_client_connect(json.dumps({"natsUrl": url, "credsPath": creds, "principal": principal, "dbPath": db,
                                                   "ondemandTables": [TABLE], "clientId": "event-asker", "heartbeatMs": 0}).encode())
        if not self.h:
            sys.exit("libzb: open failed")
        self.take(lib.zb_client_sync(self.h))  # the schema arrives; nothing seeds

    def take(self, ptr):
        try:
            return json.loads(ctypes.string_at(ptr).decode())
        finally:
            self.lib.zb_free(ptr)

    def ask(self, name: str, payload: dict, timeout_ms: int = 5000) -> tuple[dict, float]:
        t0 = time.perf_counter()
        ans = self.take(self.lib.zb_client_request(self.h, f"query.{TENANT}.{name}".encode(), json.dumps(payload).encode(), timeout_ms))
        return ans, (time.perf_counter() - t0) * 1000

    def close(self):
        self.lib.zb_client_close(self.h)


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def watch(asker: Asker, seconds: float, every: float, sensor: int):
    ages, rtts, newest = [], [], None
    f: dict = {}
    kind = ("temperature", "humidity", "pressure")[sensor % 3]
    print(f"{'t':>6}  {'rows':>10}  {'newest age':>10}  {'readings/s':>10}  {'round trip':>10}  sensor {sensor} {kind}: 1 s avg / 10 s moving avg", flush=True)
    start = time.time()
    while time.time() - start < seconds:
        tick = time.time()
        f, rtt = asker.ask("freshness", {})
        m, _ = asker.ask("moving_avg", {"kind": kind, "window_s": 10, "since_s": 30, "sensor_id": sensor})
        if "error" in f:
            print(f"freshness: {f['error']}", flush=True)
        else:
            age = f.get("age_ms")
            # Only a sample with a NEW newest reading measures the lag; once the sensors
            # stop, the same reading just grows old.
            if age is not None and f.get("newest_ms") != newest:
                ages.append(age)
            newest = f.get("newest_ms")
            rtts.append(rtt)
            row = (m.get("rows") or [[None] * 5])[0]
            print(f"{tick - start:6.1f}  {f['rows']:>10,}  {age if age is not None else '-':>8} ms  {f['last_10s_per_s']:>10,}  "
                  f"{rtt:>7.1f} ms  {row[2]} / {row[4]}", flush=True)
        time.sleep(max(0.0, every - (time.time() - tick)))
    if f and "rows" in f:
        print(f"replica: {f['rows']:,} rows, sum(value) {f.get('sum_value')}", flush=True)
    if ages:
        print(f"newest-reading age over {len(ages)} samples with a new reading: p50 {pct(ages, .5):.0f} ms, p90 {pct(ages, .9):.0f} ms, "
              f"max {max(ages):.0f} ms; question round trip p50 {statistics.median(rtts):.1f} ms", flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("query", help="freshness | moving_avg | per_minute | alarms | watch")
    ap.add_argument("payload", nargs="?", default="{}", help="the question's parameters, JSON")
    ap.add_argument("--url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    ap.add_argument("--principal", default="bob")
    ap.add_argument("--creds", default="", help="default scripts/native/creds/<principal>.creds")
    ap.add_argument("--db", default="/tmp/events-asker.sqlite3", help="the asker's own (empty) local store")
    ap.add_argument("--seconds", type=float, default=60, help="watch: how long")
    ap.add_argument("--every", type=float, default=1.0, help="watch: seconds between samples")
    ap.add_argument("--sensor", type=int, default=0, help="watch: the sensor whose moving average is shown")
    a = ap.parse_args()
    asker = Asker(a.url, a.creds or str(ROOT / "scripts/native/creds" / f"{a.principal}.creds"), a.principal, a.db)
    try:
        if a.query == "watch":
            watch(asker, a.seconds, a.every, a.sensor)
        else:
            ans, rtt = asker.ask(a.query, json.loads(a.payload))
            print(json.dumps(ans, indent=1)[:4000])
            print(f"round trip {rtt:.1f} ms", file=sys.stderr)
    finally:
        asker.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Write-to-echo probe for scripts/bench/ingress.py's `echo` variant: a real libzb client.

One tenant of its own (the load generator writes another), so every CDC event this client
applies is the echo of its own write. One write at a time: `mutate` (the local optimistic
apply, the outbox, the publish), then `poll` until both the verdict (`outcomes`) and the
echo (`applied` > 0) arrived. One JSON line at the end: verdict and echo latency percentiles.
"""
import argparse, ctypes, datetime, json, sys, time, uuid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lib", required=True)
    ap.add_argument("--url", required=True)
    ap.add_argument("--creds", required=True)
    ap.add_argument("--principal", required=True)
    ap.add_argument("--tenant", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--gap-ms", type=float, default=50)
    ap.add_argument("--warmup", type=int, default=10)
    a = ap.parse_args()

    lib = ctypes.CDLL(a.lib)
    lib.zb_client_connect.argtypes, lib.zb_client_connect.restype = [ctypes.c_char_p], ctypes.c_uint64
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    lib.zb_free.argtypes = [ctypes.c_void_p]
    for fn, extra in (("sync", []), ("poll", [ctypes.c_uint64]),
                      ("mutate", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + fn)
        f.argtypes, f.restype = [ctypes.c_uint64] + extra, ctypes.c_void_p

    def take(p):
        try:
            return json.loads(ctypes.string_at(p))
        finally:
            lib.zb_free(p)

    opts = {"natsUrl": a.url, "credsPath": a.creds, "principal": a.principal, "jsDomain": "hub",
            "tables": ["sensor_events"], "dbPath": a.db, "heartbeatMs": 0}
    h = lib.zb_client_connect(json.dumps(opts).encode())
    assert h, "connect failed"
    assert "error" not in take(lib.zb_client_sync(h))
    # Seeded and tailing before the first probe: an unseeded table holds its events.
    end = time.monotonic() + 60
    while time.monotonic() < end:
        r = take(lib.zb_client_poll(h, 200))
        if not r.get("unseeded"):
            break
    verdict_ms, echo_ms, timeouts = [], [], 0
    for i in range(a.warmup + a.count):
        key = str(uuid.uuid4())
        now = datetime.datetime.now(datetime.UTC)
        row = {"event_id": key, "tenant_id": a.tenant, "sensor_id": 1, "kind": "probe", "value": float(i),
               "updated_at": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}
        t0 = time.perf_counter()
        m = take(lib.zb_client_mutate(h, b"sensor_events", b"INSERT", json.dumps({"event_id": key}).encode(), json.dumps(row).encode()))
        mid = m.get("msgId")
        tv = te = None
        deadline = time.monotonic() + 10
        while (tv is None or te is None) and time.monotonic() < deadline:
            r = take(lib.zb_client_poll(h, 5))
            t = time.perf_counter()
            if tv is None and any(o.get("msgId") == mid for o in r.get("outcomes", [])):
                tv = t
            if te is None and r.get("applied", 0) > 0:
                te = t
        if tv is None or te is None:
            timeouts += 1
            print(json.dumps({"probe": i, "verdict": tv is not None, "echo": te is not None, "msgId": mid}), file=sys.stderr, flush=True)
        elif i >= a.warmup:
            verdict_ms.append((tv - t0) * 1000)
            echo_ms.append((te - t0) * 1000)
        time.sleep(a.gap_ms / 1000)
    lib.zb_client_close(h)

    def pct(xs, q):
        xs = sorted(xs)
        return round(xs[min(len(xs) - 1, int(q * len(xs)))], 2) if xs else None
    print(json.dumps({"probes": len(echo_ms), "timeouts": timeouts,
                      "verdict_p50_ms": pct(verdict_ms, .5), "verdict_p90_ms": pct(verdict_ms, .9), "verdict_p99_ms": pct(verdict_ms, .99),
                      "echo_p50_ms": pct(echo_ms, .5), "echo_p90_ms": pct(echo_ms, .9), "echo_p99_ms": pct(echo_ms, .99)}))


if __name__ == "__main__":
    main()

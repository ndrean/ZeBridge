#!/usr/bin/env python3
"""09-event: a DuckDB replica of the sensor readings, answering time-series questions over NATS.

    scripts/scenarios/.venv/bin/python examples/09-event/event_service.py
    ZB_DB=/tmp/events-2.duckdb scripts/scenarios/.venv/bin/python examples/09-event/event_service.py --label second

One process, one connection, one loop, as in 08-map. libzb follows `sensor_events` into a
DuckDB file (the seed from the chain, then CDC) and answers `query.<tenant>.<name>` in the
queue group "events" from that same file, for every tenant in `--tenants`. The process that writes the replica is the one
that reads it: DuckDB allows one writer per file, and nothing else ever opens it. A second
instance (its own file) joins the queue group and the server spreads the questions.

The credential is `events`, a RESPONDER: it reads its tenants like a client and may answer
their queries, but cannot write (scripts/native/jwt-bootstrap.sh mints it, one tag per
tenant; provision.py maps it to them in PostgreSQL).

⚠️ One file holds every tenant's rows, so every query filters on the tenant the question
arrived on — `query.<tenant>.…`, a subject NATS let the asker publish to, never a field of
the payload. A query without that filter would answer acme's question with globex's data.

What TimescaleDB does with a hypertable and a continuous aggregate, this does on the
replica with plain SQL. The reading's time is its key (`uuid_extract_timestamp(event_id)`,
a UUIDv7). The queries, the parameters the asker sends, and what comes back:

  freshness   {}
      → rows, sum(value) (to check the replica against PostgreSQL), the newest reading's
        time, and its age when the question was answered: the pipeline's lag (sensor →
        NATS → bridge → PostgreSQL → CDC → replica), plus at most the gap between two readings
  moving_avg  {"kind": "temperature", "window_s": 10, "since_s": 60, "sensor_id": null, "series": false, "bucket_ms": 1000}
      → per sensor, buckets of bucket_ms (100 ms to 60 s) and the average over the last
        window_s seconds of buckets; the latest point per sensor, or the whole series of
        one sensor. 100 ms buckets show a 2 s wave itself; 1 s buckets average half of it.
  per_minute  {"kind": "temperature", "minutes": 10}
      → one row per minute: readings, sensors, avg/min/max/stddev — what a continuous
        aggregate materializes, recomputed on demand (a few ms at these sizes)
  alarms      {"kind": "temperature", "above": 30, "since_s": 60}
      → the sensors with readings above the threshold: how many, the highest, the last time

⚠️ The DuckDB engine hands numbers back as TEXT (as in 08-map); the answers convert them.
"""
import argparse, ctypes, json, math, os, pathlib, socket, sys, time

ROOT = pathlib.Path(__file__).resolve().parents[2]
LIB = ROOT / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if sys.platform == "darwin" else "libzbcore.so")
TABLE = "sensor_events"
TS = "uuid_extract_timestamp(event_id)"  # the reading's time, from its UUIDv7 key
KINDS = ("temperature", "humidity", "pressure")


def v7_boundary(t: float) -> str:
    """The smallest UUIDv7 of a moment (epoch seconds): its millisecond, version 7,
    variant 10, zeros. TimescaleDB's `to_uuidv7_boundary`.

    Every time filter compares the KEY with this, `event_id >= ?::UUID`, rather than
    `uuid_extract_timestamp(event_id) >= t`: DuckDB keeps a min and max key per block of
    rows and skips a block from those two numbers, while a function of the key hides them
    and every row is read. Same rows, measured 15× faster on a 20M-row filter. Built here
    and bound as a parameter because `zb_client_query` runs reads only (no CREATE MACRO),
    and from an epoch number so no time zone can shift the cut."""
    h = f"{int(t * 1000):012x}"
    return f"{h[:8]}-{h[8:12]}-7000-8000-000000000000"


def load_lib():
    lib = ctypes.CDLL(str(LIB))
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    for n, extra in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                     ("serve", [ctypes.c_char_p]), ("reply", [ctypes.c_uint64, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + extra
    return lib


class Card:
    """The libzb client; one thread drives it."""

    def __init__(self, lib, opts: dict):
        self.lib = lib
        self.h = lib.zb_client_connect(json.dumps(opts).encode())
        if not self.h:
            lib.zb_last_error.restype = ctypes.c_char_p
            sys.exit(f"libzb: open failed: {(lib.zb_last_error() or b'').decode()}")

    def take(self, ptr):
        try:
            return json.loads(ctypes.string_at(ptr).decode())
        finally:
            self.lib.zb_free(ptr)

    def sync(self):
        return self.take(self.lib.zb_client_sync(self.h))

    def poll(self, wait_ms: int):
        return self.take(self.lib.zb_client_poll(self.h, wait_ms))

    def query(self, sql: str, params=()):
        r = self.take(self.lib.zb_client_query(self.h, sql.encode(), json.dumps(list(params)).encode()))
        if "error" in r:
            raise RuntimeError(r["error"])
        return r

    def serve(self, tenants, queries, queue):
        return self.take(self.lib.zb_client_serve(self.h, json.dumps({"tenants": tenants, "queries": queries, "queue": queue}).encode()))

    def reply(self, req_id, answer):
        return self.take(self.lib.zb_client_reply(self.h, req_id, json.dumps(answer, default=str).encode()))


def num(x):
    """TEXT from the DuckDB engine → int or float (None stays None)."""
    if x is None:
        return None
    f = float(x)
    return int(f) if f.is_integer() and "." not in str(x) else f


def kind_of(q: dict) -> str:
    k = q.get("kind", "temperature")
    if k not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}")
    return k


def bounded(q: dict, key: str, default: int, lo: int, hi: int) -> int:
    v = int(q.get(key, default))
    if not lo <= v <= hi:
        raise ValueError(f"{key} must be within {lo}..{hi}")
    return v


def freshness(card: Card, tenant: str, q: dict) -> dict:
    now = time.time()
    # The newest reading is the largest key: one extraction, not one per row.
    r = card.query(f"SELECT count(*), epoch_ms(uuid_extract_timestamp(max(event_id))), "
                   f"count(*) FILTER (WHERE event_id >= ?::UUID), round(sum(value), 3) "
                   f"FROM {TABLE} WHERE deleted_at IS NULL AND tenant_id = ?", [v7_boundary(now - 10), tenant])
    rows, newest, last10, total = (num(x) for x in r["rows"][0])
    now_ms = now * 1000
    return {"rows": rows, "newest_ms": newest, "age_ms": round(now_ms - newest, 1) if newest else None,
            "last_10s_per_s": round((last10 or 0) / 10, 1), "sum_value": total}


def moving_avg(card: Card, tenant: str, q: dict) -> dict:
    kind = kind_of(q)
    # window_s seconds of buckets: the current one and the ones before it within the window.
    # A RANGE frame includes both ends, so the frame reaches back window - one bucket; with
    # 1 s buckets and window_s 10 that is 9 s, ten buckets.
    window = bounded(q, "window_s", 10, 1, 3600)   # inlined in the frame clause: validated as an int
    since = bounded(q, "since_s", 60, 1, 86400)
    bucket = bounded(q, "bucket_ms", 1000, 100, 60000)
    if (window * 1000) % bucket:
        raise ValueError("window_s must be a whole number of buckets")
    sensor = q.get("sensor_id")
    series = bool(q.get("series", False)) and sensor is not None
    now = time.time()
    # From `since` seconds ago up to the start of the current bucket: a bucket still
    # filling would drag the average.
    current = math.floor(now * 1000 / bucket) * bucket / 1000
    where = "tenant_id = ? AND kind = ? AND event_id >= ?::UUID AND event_id < ?::UUID"
    params = [tenant, kind, v7_boundary(now - since), v7_boundary(current)]
    if sensor is not None:
        where += " AND sensor_id = ?"
        params.append(int(sensor))
    last = "" if series else "QUALIFY row_number() OVER (PARTITION BY sensor_id ORDER BY t DESC) = 1"
    sql = (f"WITH b AS (SELECT sensor_id, time_bucket(INTERVAL {bucket} MILLISECOND, {TS}) AS t, avg(value) AS v, count(*) AS n "
           f"FROM {TABLE} WHERE deleted_at IS NULL AND {where} GROUP BY ALL), "
           f"m AS (SELECT *, avg(v) OVER (PARTITION BY sensor_id ORDER BY t "
           f"RANGE BETWEEN INTERVAL {window * 1000 - bucket} MILLISECOND PRECEDING AND CURRENT ROW) AS ma FROM b) "
           f"SELECT sensor_id, epoch_ms(t), round(v, 3), n, round(ma, 3) FROM m {last} ORDER BY sensor_id, t")
    r = card.query(sql, params)
    return {"kind": kind, "window_s": window, "bucket_ms": bucket, "columns": ["sensor_id", "t_ms", "avg", "n", "moving_avg"],
            "rows": [[num(x) for x in row] for row in r["rows"]]}


def per_minute(card: Card, tenant: str, q: dict) -> dict:
    kind = kind_of(q)
    minutes = bounded(q, "minutes", 10, 1, 1440)
    r = card.query(f"SELECT epoch_ms(time_bucket(INTERVAL 1 MINUTE, {TS})) AS m, count(*), count(DISTINCT sensor_id), "
                   f"round(avg(value), 3), round(min(value), 3), round(max(value), 3), round(stddev(value), 3) "
                   f"FROM {TABLE} WHERE deleted_at IS NULL AND tenant_id = ? AND kind = ? AND event_id >= ?::UUID "
                   f"GROUP BY m ORDER BY m", [tenant, kind, v7_boundary(time.time() - minutes * 60)])
    return {"kind": kind, "columns": ["minute_ms", "readings", "sensors", "avg", "min", "max", "stddev"],
            "rows": [[num(x) for x in row] for row in r["rows"]]}


def alarms(card: Card, tenant: str, q: dict) -> dict:
    kind = kind_of(q)
    above = float(q.get("above", 30))
    since = bounded(q, "since_s", 60, 1, 86400)
    r = card.query(f"SELECT sensor_id, count(*), round(max(value), 3), epoch_ms(uuid_extract_timestamp(max(event_id))) "
                   f"FROM {TABLE} WHERE deleted_at IS NULL AND tenant_id = ? AND kind = ? AND value > ? AND event_id >= ?::UUID "
                   f"GROUP BY sensor_id ORDER BY 2 DESC", [tenant, kind, above, v7_boundary(time.time() - since)])
    return {"kind": kind, "above": above, "columns": ["sensor_id", "readings", "highest", "last_ms"],
            "rows": [[num(x) for x in row] for row in r["rows"]]}


QUERIES = {"freshness": freshness, "moving_avg": moving_avg, "per_minute": per_minute, "alarms": alarms}


def serve(card: Card, tenants: list[str], queue: str, label: str) -> None:
    r = card.serve(tenants, sorted(QUERIES), queue)
    if "error" in r:
        sys.exit(f"serve refused: {r['error']}")
    print(f"answering {sorted(QUERIES)} for {tenants} in queue group {queue!r} ({r['serving']} subjects)", flush=True)
    applied = served = 0
    last = time.time()
    while True:
        rep = card.poll(50)
        if rep.get("error"):
            print(f"poll: {rep['error']}", flush=True)
            time.sleep(1)
            continue
        applied += rep.get("applied", 0) or 0
        for req in rep.get("requests", []):
            fn = QUERIES.get(req["name"])
            t0 = time.perf_counter()
            try:
                # The tenant comes from the subject the question arrived on, never the payload.
                ans = fn(card, req["tenant"], req.get("payload") or {}) if fn else {"error": f"unknown query {req['name']!r}", "known": sorted(QUERIES)}
            except Exception as e:  # a bad parameter is the asker's problem
                ans = {"error": f"{type(e).__name__}: {e}"}
            ans["ms"] = round((time.perf_counter() - t0) * 1000, 2)
            ans["answered_by"] = label
            card.reply(req["id"], ans)
            served += 1
        if time.time() - last >= 5:
            print(f"cdc applied {applied:,} in {time.time() - last:.0f} s ({applied / (time.time() - last):,.0f}/s), "
                  f"questions answered {served}", flush=True)
            applied, served, last = 0, 0, time.time()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    ap.add_argument("--creds", default=str(ROOT / "scripts/native/creds/events.creds"))
    ap.add_argument("--principal", default="events")
    ap.add_argument("--db", default=os.environ.get("ZB_DB", "/tmp/events-service.duckdb"))
    ap.add_argument("--tenants", default="globex,acme,tango,kilo", help="the tenants to answer for, comma-separated")
    ap.add_argument("--queue", default="events")
    ap.add_argument("--label", default=f"{socket.gethostname()}:{os.getpid()}")
    a = ap.parse_args()
    card = Card(load_lib(), {"natsUrl": a.url, "credsPath": a.creds, "principal": a.principal, "dbPath": a.db,
                             "engine": "duckdb", "tables": [TABLE], "clientId": f"event-service-{a.label}",
                             "heartbeatMs": 0, "seedStreaming": True})
    t0 = time.time()
    s = card.sync()
    if s.get("error"):
        sys.exit(f"sync: {s['error']}")
    print(f"replica {a.db}: {json.dumps(s)[:200]} in {time.time() - t0:.1f} s", flush=True)
    serve(card, [t.strip() for t in a.tenants.split(",") if t.strip()], a.queue, a.label)


if __name__ == "__main__":
    main()

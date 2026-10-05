#!/usr/bin/env python3
"""The airports service: a DuckDB replica of `airports`, answering the map over NATS.

libzb follows `airports` into a local DuckDB file (seeded from the snapshot, kept live by
the change stream) and answers named questions on `query._default.<name>`, the open
tenant every client may ask. PostgreSQL never sees a query.

    airports_in_view {"south", "west", "north", "east", "limit": 500}
      → the airports inside the box; a box that crosses the antimeridian (west > east)
        wraps. When more than `limit` are inside, one per cell of a grid over the box,
        so the answer spreads over the view (`complete`: false, `total`: how many).
    airports_near {"lat", "lng", "radius_km": 100, "limit": 20}
      → the nearest airports within the radius, with their distance.

Every answer: {"columns": [...], "rows": [[...], ...], "count", "complete", "ms"} — `ms`: the time
spent in this service on the question (the SQL, the rows to JSON and back).

    PYTHONPATH=zb-python/src python3 examples/10-airports/airport_service.py --creds airports.creds
"""
import argparse
import socket
import math
import os
import signal
import threading
import time

from zebridge import ZeBridge

TABLE = "airports"
COLUMNS = ["code", "icao", "name", "city", "country", "latitude", "longitude", "elevation", "time_zone"]


def airports_in_view(zb: ZeBridge, q: dict) -> dict:
    south, north = float(q["south"]), float(q["north"])
    west, east = float(q["west"]), float(q["east"])
    limit = min(int(q.get("limit", 500)), 5000)
    lng = "longitude BETWEEN ? AND ?" if west <= east else "(longitude >= ? OR longitude <= ?)"
    box = f"latitude BETWEEN ? AND ? AND {lng}"
    params = [south, north, west, east]
    t0 = time.time()
    total = zb.query(f"SELECT count(*) AS n FROM {TABLE} WHERE {box}", *params)[0]["n"]
    if int(total) <= limit:
        rows = zb.query(f"SELECT {', '.join(COLUMNS)} FROM {TABLE} WHERE {box} ORDER BY code", *params)
    else:
        # More than fit: one airport per cell of a grid over the box, so the answer is
        # spread over the view, the notable ones first (an ICAO code, a website).
        span = max(north - south, (east - west) % 360 or 360)
        cell = span / max(1.0, limit ** 0.5)
        rows = zb.query(
            f"SELECT {', '.join(COLUMNS)} FROM (SELECT *, row_number() OVER ("
            f"PARTITION BY floor(latitude / {cell}), floor(longitude / {cell}) "
            f"ORDER BY (icao IS NULL), (url IS NULL), code) AS rn FROM {TABLE} WHERE {box}) "
            f"WHERE rn = 1 LIMIT {limit}", *params)
    return shaped(rows, int(total) <= limit, t0, total=int(total))


def airports_near(zb: ZeBridge, q: dict) -> dict:
    lat, lng = float(q["lat"]), float(q["lng"])
    radius_km = float(q.get("radius_km", 100))
    limit = min(int(q.get("limit", 20)), 500)
    # A box around the circle first (plain comparisons), the exact distance only inside
    # it: half the time on this table, and it grows with the table.
    lng = (lng + 180) % 360 - 180  # a map may hand over -200 for 160
    dlat = radius_km / 111.32
    dlng = radius_km / (111.32 * max(0.01, math.cos(math.radians(lat))))
    west, east = lng - dlng, lng + dlng
    if dlng >= 180:
        lng_box = "TRUE"
    elif west < -180:   # the box crosses the date line: two halves
        lng_box = f"(longitude >= {west + 360} OR longitude <= {east})"
    elif east > 180:
        lng_box = f"(longitude >= {west} OR longitude <= {east - 360})"
    else:
        lng_box = f"longitude BETWEEN {west} AND {east}"
    box = f"latitude BETWEEN {lat - dlat} AND {lat + dlat} AND {lng_box}"
    # haversine, in kilometres
    dist = (f"2 * 6371 * asin(sqrt(pow(sin(radians(latitude - {lat}) / 2), 2) + "
            f"cos(radians({lat})) * cos(radians(latitude)) * pow(sin(radians(longitude - {lng}) / 2), 2)))")
    return answer(zb, f"SELECT {', '.join(COLUMNS)}, round({dist}, 1) AS distance_km FROM {TABLE} "
                      f"WHERE {box} AND {dist} <= {radius_km} ORDER BY distance_km LIMIT {limit + 1}",
                  [], limit)


def answer(zb: ZeBridge, sql: str, params: list, limit: int) -> dict:
    t0 = time.time()
    rows = zb.query(sql, *params)
    return shaped(rows[:limit], len(rows) <= limit, t0)


def shaped(rows: list, complete: bool, t0: float, **extra) -> dict:
    cols = list(rows[0].keys()) if rows else COLUMNS
    return {"columns": cols, "rows": [[r[c] for c in cols] for r in rows], "count": len(rows),
            "complete": complete, "ms": round((time.time() - t0) * 1000, 1), **extra}


QUERIES = {"airports_in_view": airports_in_view, "airports_near": airports_near}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    ap.add_argument("--creds", required=True, help="a responder's creds (bridge --mint-responder)")
    ap.add_argument("--db", default="/tmp/airports-service.duckdb")
    ap.add_argument("--queue", default="airports", help="instances in one queue group share the questions")
    ap.add_argument("--js-domain", default=os.environ.get("NATS_JS_DOMAIN") or None,
                    help="the hub's JetStream domain, when this service runs behind a leaf node")
    ap.add_argument("--name", default=os.environ.get("ZB_SERVICE_NAME") or socket.gethostname(),
                    help="said in every answer (`by`): which instance of the queue group answered")
    a = ap.parse_args()

    zb_ref: list = []

    # Questions are answered HERE, on libzb's own thread, the moment a poll hands them
    # over: query and reply run at once. Answered from another thread, each call would
    # wait for the worker's current poll to end — measured, ~100 ms per question.
    def on_change(r: dict) -> None:
        if r.get("applied"):
            print(f"cdc: {r['applied']} row(s) applied", flush=True)
        for req in r.get("requests") or []:
            zb = zb_ref[0]
            fn = QUERIES.get(req["name"])
            try:
                ans = fn(zb, req.get("payload") or {}) if fn else {"error": f"unknown query {req['name']!r}", "known": sorted(QUERIES)}
            except Exception as e:  # a bad parameter is the asker's problem
                ans = {"error": f"{type(e).__name__}: {e}"}
            ans["by"] = a.name
            zb.reply(req["id"], ans)
            print(f"{req['name']}: {ans.get('count', '?')} airport(s) in {ans.get('ms', '?')} ms", flush=True)

    t0 = time.time()
    # A question wakes libzb's poll when it lands, so the default 250 ms poll answers
    # at once and costs nothing when idle.
    opts = {"js_domain": a.js_domain} if a.js_domain else {}
    with ZeBridge(nats_url=a.url, creds_path=a.creds, db_path=a.db, engine="duckdb",
                  tables=[TABLE], client_id="airport-service", heartbeat_ms=0,
                  on_change=on_change, **opts) as zb:
        zb_ref.append(zb)
        held = zb.query(f"SELECT count(*) AS n FROM {TABLE}")[0]["n"]
        print(f"replica: {held} airports in {a.db}, {time.time() - t0:.1f} s", flush=True)
        r = zb.serve({"tenants": ["_default"], "queries": sorted(QUERIES), "queue": a.queue})
        print(f"answering {sorted(QUERIES)} on query._default.<name>, queue group {a.queue!r} ({r.get('serving')} subject(s))", flush=True)
        stop = threading.Event()
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        stop.wait()


if __name__ == "__main__":
    main()

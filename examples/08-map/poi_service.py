#!/usr/bin/env python3
"""The POI service: a DuckDB replica of all of France, answering "what is around me"
over NATS request/reply — PostgreSQL never sees a query (NOTES §10hj).

    examples/08-map/poi_service.py                    # bridge.creds, engine duckdb, queue group "pois"
    ZB_DB=/tmp/pois.duckdb examples/08-map/poi_service.py --tenants kilo,_default

One process, two halves on one libzb card (DuckDB allows one writer per file):
  * the replica: libzb follows `osm_pois` (a public table, 2.1M rows) — the seed from
    the chain, then CDC in bulk (§10hg) — polled from a thread;
  * the responder: nats-py, a queue-group subscription on `query.<tenant>.pois_near` for
    every tenant served; instances scale by starting another process.

Named queries only — the SQL lives here, the client sends parameters:
  pois_near  {"lat": 47.21, "lng": -1.55, "radius_m": 800, "kinds": ["amenity"], "limit": 500}
    → {"columns": [...], "rows": [[...], ...], "count": n, "ms": t}
The answer is the chain object's shape, so a phone applies it with `zb_client_ingest`.
A bounding box on (lat, lng) over the replica's index, then the haversine distance to
order and cut at the radius. Every libzb call is serialized through one lock: the card
is not made for two threads.
"""
import argparse, asyncio, ctypes, json, math, os, pathlib, sys, threading, time

ROOT = pathlib.Path(__file__).resolve().parents[2]
LIB = ROOT / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if sys.platform == "darwin" else "libzbcore.so")
TABLE = "osm_pois"
KINDS = ("amenity", "shop", "tourism", "man_made")


def load_lib():
    lib = ctypes.CDLL(str(LIB))
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, extra in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + extra
    return lib


class Card:
    def __init__(self, lib, opts: dict):
        self.lib, self.lock = lib, threading.Lock()
        self.h = lib.zb_client_open(json.dumps(opts).encode())
        if not self.h:
            sys.exit("libzb: open failed")

    def take(self, ptr):
        try:
            return json.loads(ctypes.string_at(ptr).decode())
        finally:
            self.lib.zb_free(ptr)

    def sync(self):
        with self.lock:
            return self.take(self.lib.zb_client_sync(self.h))

    def poll(self, wait_ms=1000):
        with self.lock:
            return self.take(self.lib.zb_client_poll(self.h, wait_ms))

    def query(self, sql, params=()):
        with self.lock:
            return self.take(self.lib.zb_client_query(self.h, sql.encode(), json.dumps(list(params)).encode()))


def pois_near(card: Card, q: dict) -> dict:
    lat, lng = float(q["lat"]), float(q["lng"])
    radius = float(q.get("radius_m", 1000))
    limit = int(q.get("limit", 500))
    kinds = [k for k in q.get("kinds", []) if k in KINDS]
    dlat = radius / 111_320.0
    dlng = radius / (111_320.0 * max(0.1, math.cos(math.radians(lat))))
    where = "lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?"
    params = [lat - dlat, lat + dlat, lng - dlng, lng + dlng]
    if kinds:
        where += " AND (" + " OR ".join(f"{k} IS NOT NULL" for k in kinds) + ")"
    # haversine, metres; ordered nearest first, cut at the radius
    dist = (f"2 * 6371000 * asin(sqrt(pow(sin(radians(lat - ({lat})) / 2), 2) + "
            f"cos(radians({lat})) * cos(radians(lat)) * pow(sin(radians(lng - ({lng})) / 2), 2)))")
    cols = ["osm_id", "cell", "amenity", "shop", "tourism", "man_made", "name", "name_en", "name_fr", "opening_hours",
            "beds", "rooms", "addr_full", "addr_housenumber", "addr_street", "addr_city", "source", "lat", "lng", "geom", "updated_at"]
    # `geom` travels as libzb's bytes marker ({"$bin": …}) — the phone's table declares it NOT NULL.
    sql = (f"SELECT {', '.join(cols)} FROM {TABLE} WHERE {where} AND {dist} <= {radius} "
           f"ORDER BY {dist} LIMIT {limit}")
    t0 = time.time()
    r = card.query(sql, params)
    if "error" in r:
        return {"error": r["error"]}
    # `complete`: the answer holds EVERY point of the area — a phone may then delete what
    # it holds there and the answer lacks. Cut by `limit`, it holds the nearest only.
    return {"columns": r["columns"], "rows": r["rows"], "count": len(r["rows"]), "complete": len(r["rows"]) < limit,
            "ms": round((time.time() - t0) * 1000, 1)}


QUERIES = {"pois_near": pois_near}


async def serve(card: Card, url: str, creds: str, tenants: list[str], queue: str):
    import nats
    nc = await nats.connect(url, user_credentials=creds, name="poi-service")
    served = 0

    async def handler(msg):
        nonlocal served
        name = msg.subject.rsplit(".", 1)[-1]
        fn = QUERIES.get(name)
        try:
            q = json.loads(msg.data or b"{}")
            ans = fn(card, q) if fn else {"error": f"unknown query {name!r}", "known": sorted(QUERIES)}
        except Exception as e:  # a bad parameter is the client's problem, not the service's
            ans = {"error": f"{type(e).__name__}: {e}"}
        await msg.respond(json.dumps(ans, default=str).encode())
        served += 1
        if served % 100 == 1:
            print(f"served {served} (last: {name}, {ans.get('count', '?')} rows, {ans.get('ms', '?')} ms)", flush=True)

    for t in tenants:
        for name in QUERIES:
            await nc.subscribe(f"query.{t}.{name}", queue=queue, cb=handler)
    print(f"answering {sorted(QUERIES)} for tenants {tenants} in queue group {queue!r}", flush=True)
    while True:
        await asyncio.sleep(3600)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    ap.add_argument("--creds", default=str(ROOT / "scripts" / "native" / "creds" / "bridge.creds"))
    ap.add_argument("--principal", default="bridge")
    ap.add_argument("--db", default=os.environ.get("ZB_DB", "/tmp/pois-service.duckdb"))
    ap.add_argument("--engine", default="duckdb")
    ap.add_argument("--tenants", default="kilo,_default")
    ap.add_argument("--queue", default="pois")
    a = ap.parse_args()
    lib = load_lib()
    card = Card(lib, {"url": a.url, "credsPath": a.creds, "principal": a.principal, "dbPath": a.db, "engine": a.engine,
                      "tables": [TABLE], "clientId": "poi-service", "heartbeatMs": 0, "seedStreaming": True})
    t0 = time.time()
    s = card.sync()
    print(f"replica: {json.dumps(s)[:200]} in {time.time() - t0:.1f} s", flush=True)

    def follow():
        # A short wait: the lock is held for the whole poll, and a query waits behind it.
        while True:
            r = card.poll(100)
            if r.get("applied"):
                print(f"cdc: {r['applied']} applied", flush=True)
            if r.get("error"):
                print(f"poll: {r['error']}", flush=True)
                time.sleep(2)

    threading.Thread(target=follow, daemon=True).start()
    asyncio.run(serve(card, a.url, a.creds, [t.strip() for t in a.tenants.split(",") if t.strip()], a.queue))


if __name__ == "__main__":
    main()

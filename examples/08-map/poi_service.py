#!/usr/bin/env python3
"""The map service: a DuckDB replica of all of France, answering "what is around me"
over NATS request/reply — PostgreSQL never sees a query (NOTES §10hj).

The dataset is OpenChargeMap's charge points (§10hr, `load_chargers.py`), not the HOT
OpenStreetMap export it replaced: one KIND of thing, each row worth showing.

    examples/08-map/poi_service.py                    # pois.creds (a RESPONDER principal, §10hk), engine duckdb, queue group "pois"
    ZB_DB=/tmp/pois.duckdb examples/08-map/poi_service.py --tenants kilo,_default

ONE process, ONE connection, ONE loop (§10hp). libzb both follows `charge_points` (a
public table, 16,173 rows — §10hr, the seed from the chain, then CDC) AND answers the
questions: `zb_client_serve` subscribes `query.<tenant>.<name>` in a queue group on the
client's own socket, `poll` hands over what arrived, `zb_client_reply` answers on the
asker's inbox. Instances scale by starting another process; the queue group makes the
server pick one.

This replaced a second NATS connection (nats-py) and a background thread, whose only
real cost was a LOCK: the card is not made for two threads, so a poll and a question
took turns. There is no lock here, and no `--serve-url`: one connection cannot answer
somewhere else than it listens.

Named queries only — the SQL lives here, the client sends parameters:
  chargers_near {"lat": 47.21, "lng": -1.55, "radius_m": 800, "min_kw": 0, "operational": true, "limit": 500}
    → {"columns": [...], "rows": [[...], ...], "count": n, "complete": bool, "ms": t}
  fuel_near  {"lat", "lng", "radius_m": 3000, "fuel": "SP95", "sort": "price", "limit": 20}
    → the stations selling that fuel within the radius, cheapest first, today's price
  tour       {"stops": [id, ...], "closed": true, "along_m": 80, "along_min_kw": 0, "fuel": "SP95", "fuel_m": 1500, "roads": true}
    → by road when Valhalla answers (the matrix orders, /route draws), straight lines otherwise
  route      {"points": [{"lat","lng"}, …], "costing": "auto"}
    → the road between the points: polyline, km, minutes, legs (Valhalla; an error without it)
    → the shortest round trip through the stops (exact to 9, 2-opt beyond, straight lines
      until Valhalla), its legs and polyline, and the POIs within along_m of the route
      as an answer a phone keeps like any other
The answer is the chain object's shape, so a phone applies it with `zb_client_ingest`.
A bounding box on (lat, lng) over the replica's index, then the haversine distance to
order and cut at the radius. Every libzb call is serialized through one lock: the card
is not made for two threads.
"""
import argparse, ctypes, json, math, os, pathlib, socket, sys, time, urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
LIB = ROOT / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if sys.platform == "darwin" else "libzbcore.so")
TABLE = "charge_points"
# The replica follows the fuel feed too (load_fuel.py): parents first.
TABLES = ["fuel_stations", "fuel_prices", "fuel_outages", TABLE]
# §10hr: what a charge point answer carries. `geom` travels as libzb's bytes marker.
CP_COLS = ["id", "ocm_id", "title", "address", "town", "postcode", "lat", "lng", "geom",
           "operator_id", "usage_type_id", "usage_cost", "status_type_id", "points",
           "connections", "max_power_kw", "level_id", "plugs", "phone", "url",
           "access_comments", "comments", "verified_at", "updated_at"]
FUELS = ("Gazole", "SP95", "SP98", "E10", "E85", "GPLc")


def load_lib():
    lib = ctypes.CDLL(str(LIB))
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, extra in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                     ("serve", [ctypes.c_char_p]), ("reply", [ctypes.c_uint64, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + extra
    return lib


class Card:
    """The libzb client. One thread drives it — §10hp removed the lock with the thread."""

    def __init__(self, lib, opts: dict):
        self.lib = lib
        self.h = lib.zb_client_open(json.dumps(opts).encode())
        if not self.h:
            sys.exit("libzb: open failed")

    def take(self, ptr):
        try:
            return json.loads(ctypes.string_at(ptr).decode())
        finally:
            self.lib.zb_free(ptr)

    def sync(self):
        return self.take(self.lib.zb_client_sync(self.h))

    def poll(self, wait_ms=1000):
        return self.take(self.lib.zb_client_poll(self.h, wait_ms))

    def query(self, sql, params=()):
        return self.take(self.lib.zb_client_query(self.h, sql.encode(), json.dumps(list(params)).encode()))

    def serve(self, tenants, queries, queue):
        return self.take(self.lib.zb_client_serve(self.h, json.dumps({"tenants": tenants, "queries": queries, "queue": queue}).encode()))

    def reply(self, req_id, answer):
        return self.take(self.lib.zb_client_reply(self.h, req_id, json.dumps(answer, default=str).encode()))


def chargers_near(card: Card, q: dict) -> dict:
    """{"lat", "lng", "radius_m": 1000, "min_kw": 0, "operational": true, "limit": 500}
    → the charge points around a position, nearest first, in the chain object's shape so
      a phone keeps them with `ingest`. `min_kw` asks for the fast ones only (43 kW is
      OpenChargeMap's rapid band); `operational` drops what the feed says is broken."""
    lat, lng = float(q["lat"]), float(q["lng"])
    radius = float(q.get("radius_m", 1000))
    limit = int(q.get("limit", 500))
    min_kw = float(q.get("min_kw", 0) or 0)
    dlat = radius / 111_320.0
    dlng = radius / (111_320.0 * max(0.1, math.cos(math.radians(lat))))
    where = "lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?"
    params = [lat - dlat, lat + dlat, lng - dlng, lng + dlng]
    if min_kw > 0:
        where += f" AND max_power_kw >= {min_kw}"
    if q.get("operational", False):
        where += " AND status_type_id = 50"
    # haversine, metres; ordered nearest first, cut at the radius
    dist = (f"2 * 6371000 * asin(sqrt(pow(sin(radians(lat - ({lat})) / 2), 2) + "
            f"cos(radians({lat})) * cos(radians(lat)) * pow(sin(radians(lng - ({lng})) / 2), 2)))")
    sql = (f"SELECT {', '.join(CP_COLS)} FROM {TABLE} WHERE {where} AND {dist} <= {radius} "
           f"ORDER BY {dist} LIMIT {limit}")
    t0 = time.time()
    r = card.query(sql, params)
    if "error" in r:
        return {"error": r["error"]}
    # `complete`: the answer holds EVERY point of the area — a phone may then delete what
    # it holds there and the answer lacks. Cut by `limit`, it holds the nearest only.
    return {"columns": r["columns"], "rows": r["rows"], "count": len(r["rows"]), "complete": len(r["rows"]) < limit,
            "ms": round((time.time() - t0) * 1000, 1)}


def haversine(a, b) -> float:
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 2 * 6371000 * math.asin(math.sqrt(h))


def tsp_order(points: list, closed: bool, matrix: list | None = None) -> list:
    """The voyageur de commerce over a handful of stops: exact for up to 9 (every permutation
    of the stops after the first), nearest-neighbour then 2-opt beyond. Straight-line
    distances — a road network (Valhalla) is the planned replacement of `haversine` here."""
    n = len(points)
    if n <= 2:
        return list(range(n))
    d = matrix if matrix is not None else [[haversine(points[i], points[j]) for j in range(n)] for i in range(n)]

    def length(order):
        t = sum(d[order[k]][order[k + 1]] for k in range(len(order) - 1))
        return t + (d[order[-1]][order[0]] if closed else 0)

    if n <= 9:
        import itertools
        best = min(itertools.permutations(range(1, n)), key=lambda perm: length((0,) + perm))
        return [0] + list(best)
    order, left = [0], set(range(1, n))
    while left:
        nxt = min(left, key=lambda j: d[order[-1]][j]); order.append(nxt); left.remove(nxt)
    improved = True
    while improved:
        improved = False
        for i in range(1, n - 1):
            for j in range(i + 1, n if closed else n - 1 + 1):
                if j >= n: continue
                cand = order[:i] + order[i:j + 1][::-1] + order[j + 1:]
                if length(cand) < length(order) - 1e-6:
                    order, improved = cand, True
    return order


def seg_distance_m(p, a, b) -> float:
    """Point to segment, metres, on a local flat approximation (fine over a few km)."""
    k = math.cos(math.radians(a[0]))
    px, py = (p[1] - a[1]) * k, p[0] - a[0]
    bx, by = (b[1] - a[1]) * k, b[0] - a[0]
    l2 = bx * bx + by * by
    t = 0.0 if l2 == 0 else max(0.0, min(1.0, (px * bx + py * by) / l2))
    dx, dy = px - t * bx, py - t * by
    return math.sqrt(dx * dx + dy * dy) * 111_320.0


def tour(card: Card, q: dict) -> dict:
    """{"stops": [osm_id, …] or [{"lat","lng"}, …], "closed": true, "along_m": 80,
        "along_kinds": [...], "along_limit": 300}
    → {"order": [...], "legs": [{"from","to","m"}], "total_m", "polyline": [[lat,lng],…],
       "along": {"columns","rows","count"} — the POIs within along_m of the route, the
       answer's usual shape, so a phone keeps them like any answer."""
    t0 = time.time()
    raw = q.get("stops") or []
    if len(raw) < 2:
        return {"error": "at least two stops"}
    stops = []
    ids = [x for x in raw if isinstance(x, str)]   # §10hr: the key is a uuid
    by_id = {}
    if ids:
        r = card.query(f"SELECT id, title, lat, lng FROM {TABLE} WHERE id IN ({', '.join('?' * len(ids))})", ids)
        if "error" in r:
            return {"error": r["error"]}
        by_id = {row[0]: row for row in r["rows"]}
    for x in raw:
        if isinstance(x, str):
            if x not in by_id:
                return {"error": f"unknown stop {x}"}
            _, name, lat, lng = by_id[x]
            stops.append({"id": x, "name": name, "lat": lat, "lng": lng})
        else:
            stops.append({"id": None, "name": x.get("name"), "lat": float(x["lat"]), "lng": float(x["lng"])})
    closed = bool(q.get("closed", True))
    pts = [(s["lat"], s["lng"]) for s in stops]
    roads = None
    if q.get("roads", True):
        # By road when Valhalla answers: the matrix orders the stops, /route draws the way.
        try:
            roads = road_matrix(pts, q.get("costing", "auto"))
        except Exception as e:
            roads = None
            road_error = str(e)[:100]
    order = tsp_order(pts, closed, matrix=roads)
    seq = [stops[i] for i in order]
    path = seq + ([seq[0]] if closed else [])
    legs = [{"from": path[k]["name"] or path[k]["id"], "to": path[k + 1]["name"] or path[k + 1]["id"],
             "m": round(haversine((path[k]["lat"], path[k]["lng"]), (path[k + 1]["lat"], path[k + 1]["lng"])))} for k in range(len(path) - 1)]
    polyline = [[p["lat"], p["lng"]] for p in path]
    by_road = None
    if roads is not None:
        try:
            rr = road_route([(p["lat"], p["lng"]) for p in path], q.get("costing", "auto"))
            polyline = rr["polyline"]
            for k, leg in enumerate(rr["legs"]):
                if k < len(legs):
                    legs[k]["m"] = round(leg["km"] * 1000); legs[k]["min"] = leg["min"]
            by_road = {"km": rr["km"], "min": rr["min"]}
        except Exception as e:
            road_error = str(e)[:100]
    along_m = float(q.get("along_m", 80))
    along = {"columns": [], "rows": [], "count": 0}
    # The corridor follows the way as drawn: the road polyline when there is one.
    way = [{"lat": a, "lng": b} for a, b in polyline]
    if along_m > 0:
        lats, lngs = [s["lat"] for s in way], [s["lng"] for s in way]
        dlat = along_m / 111_320.0
        dlng = along_m / (111_320.0 * max(0.1, math.cos(math.radians(sum(lats) / len(lats)))))
        # §10hr: what lies along the way is a charge point too — the corridor is where
        # you could actually stop, which is the question a driver has.
        min_kw = float(q.get("along_min_kw", 0) or 0)
        where = "lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?" + (f" AND max_power_kw >= {min_kw}" if min_kw > 0 else "")
        cols = CP_COLS
        r = card.query(f"SELECT {', '.join(cols)} FROM {TABLE} WHERE {where} LIMIT 20000", [min(lats) - dlat, max(lats) + dlat, min(lngs) - dlng, max(lngs) + dlng])
        if "error" not in r:
            li, lo = cols.index("lat"), cols.index("lng")
            stop_ids = {s["id"] for s in stops}
            near = [row for row in r["rows"] if row[0] not in stop_ids and
                    min(seg_distance_m((row[li], row[lo]), (way[k]["lat"], way[k]["lng"]), (way[k + 1]["lat"], way[k + 1]["lng"])) for k in range(len(way) - 1)) <= along_m]
            near = near[: int(q.get("along_limit", 300))]
            along = {"columns": cols, "rows": near, "count": len(near)}
    fuel = q.get("fuel")
    cheapest = None
    if fuel in FUELS:
        # The stations selling that fuel within fuel_m of any leg — the same corridor idea
        # over the fuel tables — the cheapest first, its detour from the nearest leg.
        fuel_m = float(q.get("fuel_m", 1500))
        lats, lngs = [s["lat"] for s in path], [s["lng"] for s in path]
        dlat = fuel_m / 111_320.0
        dlng = fuel_m / (111_320.0 * max(0.1, math.cos(math.radians(sum(lats) / len(lats)))))
        r = card.query(fuel_sql("s.lat BETWEEN ? AND ? AND s.lng BETWEEN ? AND ?", "0", fuel, "p.price", 500),
                       [min(lats) - dlat, max(lats) + dlat, min(lngs) - dlng, max(lngs) + dlng])
        if "error" not in r:
            c = r["columns"]; li, lo, pi = c.index("lat"), c.index("lng"), c.index("price")
            near = []
            for row in r["rows"]:
                detour = min(seg_distance_m((row[li], row[lo]), (path[k]["lat"], path[k]["lng"]), (path[k + 1]["lat"], path[k + 1]["lng"])) for k in range(len(path) - 1))
                if detour <= fuel_m:
                    near.append((row[pi], detour, row))
            near.sort(key=lambda x: (x[0], x[1]))
            cheapest = {"fuel": fuel, "columns": c + ["detour_m"], "rows": [row + [round(d)] for _, d, row in near[:5]], "count": len(near)}
    return {"order": [s["id"] if s["id"] is not None else [s["lat"], s["lng"]] for s in seq],
            "fuel": cheapest,
            "stops": [{"name": s["name"], "lat": s["lat"], "lng": s["lng"]} for s in seq],
            "legs": legs, "total_m": sum(l["m"] for l in legs), "closed": closed,
            "polyline": polyline, "by_road": by_road, "road_error": locals().get("road_error"), "along": along,
            "ms": round((time.time() - t0) * 1000, 1)}


def fuel_sql(where: str, dist: str, fuel: str, order: str, limit: int) -> str:
    """Stations with the price of one fuel: the two tables joined in the replica, live rows
    only (a replica never holds a tombstoned row), the outage of that fuel if any."""
    return (f"SELECT s.id, s.city, s.address, s.road_type, s.lat, s.lng, p.price, p.price_at, "
            f"o.kind AS outage, {dist} AS m "
            f"FROM fuel_stations s JOIN fuel_prices p ON p.station_id = s.id AND p.fuel = '{fuel}' "
            f"LEFT JOIN fuel_outages o ON o.station_id = s.id AND o.fuel = '{fuel}' "
            f"WHERE {where} ORDER BY {order} LIMIT {limit}")


def fuel_near(card: Card, q: dict) -> dict:
    """{"lat", "lng", "radius_m": 3000, "fuel": "SP95", "sort": "price"|"distance", "limit": 20}
    → the stations selling that fuel within the radius, with today's price, nearest or
      cheapest first, and an outage flag when the feed says the pump is dry."""
    t0 = time.time()
    lat, lng = float(q["lat"]), float(q["lng"])
    radius = float(q.get("radius_m", 3000))
    fuel = q.get("fuel", "SP95")
    if fuel not in FUELS:
        return {"error": f"unknown fuel {fuel!r}", "known": list(FUELS)}
    limit = int(q.get("limit", 20))
    dlat = radius / 111_320.0
    dlng = radius / (111_320.0 * max(0.1, math.cos(math.radians(lat))))
    dist = (f"2 * 6371000 * asin(sqrt(pow(sin(radians(s.lat - ({lat})) / 2), 2) + "
            f"cos(radians({lat})) * cos(radians(s.lat)) * pow(sin(radians(s.lng - ({lng})) / 2), 2)))")
    where = f"s.lat BETWEEN ? AND ? AND s.lng BETWEEN ? AND ? AND {dist} <= {radius}"
    order = "p.price, m" if q.get("sort", "price") == "price" else "m"
    r = card.query(fuel_sql(where, dist, fuel, order, limit), [lat - dlat, lat + dlat, lng - dlng, lng + dlng])
    if "error" in r:
        return {"error": r["error"]}
    return {"fuel": fuel, "columns": r["columns"], "rows": r["rows"], "count": len(r["rows"]), "ms": round((time.time() - t0) * 1000, 1)}


# ── roads: Valhalla (examples/08-map/valhalla) ──────────────────────────────────────

VALHALLA = os.environ.get("VALHALLA_URL", "http://localhost:8002")


def valhalla(path: str, body: dict, timeout=10.0) -> dict:
    req = urllib.request.Request(f"{VALHALLA}{path}", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def decode_polyline6(s: str) -> list:
    """Valhalla's shape: Google's polyline encoding at precision 6 → [[lat, lng], …]."""
    out, i, lat, lng = [], 0, 0, 0
    while i < len(s):
        for which in (0, 1):
            shift = result = 0
            while True:
                b = ord(s[i]) - 63; i += 1
                result |= (b & 0x1f) << shift; shift += 5
                if b < 0x20: break
            d = ~(result >> 1) if result & 1 else result >> 1
            if which == 0: lat += d
            else: lng += d
        out.append([lat / 1e6, lng / 1e6])
    return out


def road_route(points: list, costing="auto") -> dict:
    """Two or more points → the road between them: polyline, km, minutes, per-leg km."""
    r = valhalla("/route", {"locations": [{"lat": p[0], "lon": p[1]} for p in points], "costing": costing, "units": "kilometers"})
    trip = r["trip"]
    poly = []
    legs = []
    for leg in trip["legs"]:
        pts = decode_polyline6(leg["shape"])
        poly += pts if not poly else pts[1:]
        legs.append({"km": round(leg["summary"]["length"], 2), "min": round(leg["summary"]["time"] / 60, 1)})
    return {"polyline": poly, "km": round(trip["summary"]["length"], 2), "min": round(trip["summary"]["time"] / 60, 1), "legs": legs}


def road_matrix(points: list, costing="auto") -> list:
    """The distance matrix between the points, in km, from /sources_to_targets."""
    locs = [{"lat": p[0], "lon": p[1]} for p in points]
    r = valhalla("/sources_to_targets", {"sources": locs, "targets": locs, "costing": costing, "units": "kilometers"})
    return [[cell["distance"] if cell.get("distance") is not None else 1e9 for cell in row] for row in r["sources_to_targets"]]


def route(card: Card, q: dict) -> dict:
    """{"points": [{"lat","lng"}, …], "costing": "auto"|"bicycle"|"pedestrian"} → the road
    between them, in order: polyline, km, minutes, legs. Straight lines never: without
    Valhalla the answer says so."""
    t0 = time.time()
    pts = [(float(p["lat"]), float(p["lng"])) for p in q.get("points", [])]
    if len(pts) < 2:
        return {"error": "at least two points"}
    try:
        r = road_route(pts, q.get("costing", "auto"))
    except Exception as e:
        return {"error": f"valhalla unreachable or refused: {str(e)[:120]}", "valhalla": VALHALLA}
    r["ms"] = round((time.time() - t0) * 1000, 1)
    return r


QUERIES = {"chargers_near": chargers_near, "tour": tour, "fuel_near": fuel_near, "route": route}


def serve(card: Card, tenants: list[str], queue: str, label: str) -> None:
    """The whole service: subscribe, then one loop. A poll returns the CDC it applied AND
    the questions that arrived; each is answered from this same replica, on this same
    connection. The wait is short because it bounds how long a question can sit unseen."""
    r = card.serve(tenants, sorted(QUERIES), queue)
    if "error" in r:
        sys.exit(f"serve refused: {r['error']}")
    print(f"answering {sorted(QUERIES)} for tenants {tenants} in queue group {queue!r} "
          f"({r['serving']} subject(s), one connection)", flush=True)
    served = 0
    while True:
        rep = card.poll(100)
        if rep.get("error"):
            print(f"poll: {rep['error']}", flush=True)
            time.sleep(2)
            continue
        if rep.get("applied"):
            print(f"cdc: {rep['applied']} applied", flush=True)
        for q in rep.get("requests", []):
            fn = QUERIES.get(q["name"])
            try:
                ans = fn(card, q.get("payload") or {}) if fn else {"error": f"unknown query {q['name']!r}", "known": sorted(QUERIES)}
            except Exception as e:  # a bad parameter is the asker's problem, not the service's
                ans = {"error": f"{type(e).__name__}: {e}"}
            ans["answered_by"] = label  # §10hl: which instance answered — the leaf test counts these
            card.reply(q["id"], ans)
            served += 1
            if served % 100 == 1:
                print(f"served {served} (last: {q['name']}, {ans.get('count', '?')} rows, {ans.get('ms', '?')} ms)", flush=True)


def main():
    global VALHALLA
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    # §10hk: a responder principal — reads like a client, answers, cannot write. Minted by
    # jwt-bootstrap.sh (nsc) or scripts/native/mint_responder.py; tagged with the tenants it serves.
    ap.add_argument("--creds", default=str(ROOT / "scripts" / "native" / "creds" / "pois.creds"))
    ap.add_argument("--principal", default="pois")
    ap.add_argument("--db", default=os.environ.get("ZB_DB", "/tmp/pois-service.duckdb"))
    ap.add_argument("--engine", default="duckdb")
    ap.add_argument("--tenants", default="kilo,_default")
    ap.add_argument("--queue", default="pois")
    ap.add_argument("--label", default=f"{socket.gethostname()}:{os.getpid()}", help="the instance's name in every answer (answered_by)")
    ap.add_argument("--valhalla", default=VALHALLA, help="the routing engine (examples/08-map/valhalla); the tour and route go by road when it answers")
    a = ap.parse_args()
    VALHALLA = a.valhalla.rstrip("/")
    lib = load_lib()
    card = Card(lib, {"natsUrl": a.url, "credsPath": a.creds, "principal": a.principal, "dbPath": a.db, "engine": a.engine,
                      "tables": TABLES, "clientId": "poi-service", "heartbeatMs": 0, "seedStreaming": True})
    t0 = time.time()
    s = card.sync()
    print(f"replica: {json.dumps(s)[:200]} in {time.time() - t0:.1f} s", flush=True)

    serve(card, [t.strip() for t in a.tenants.split(",") if t.strip()], a.queue, a.label)


if __name__ == "__main__":
    main()

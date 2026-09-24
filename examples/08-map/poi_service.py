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
  tour       {"stops": [id, ...], "closed": true, "along_m": 80, "along_min_kw": 0, "fuel": "SP95", "fuel_m": 1500}
    → the shortest round trip through the stops (exact to 9, nearest-neighbour then 2-opt
      beyond), its legs and polyline, and the charge points within along_m of the way
  along_route {"points": [{"lat","lng"}, …], "corridor_m": 5000, "min_kw": 0, "fuel": "SP95", "limit": 500}
    → the way through the points, plus the charge points and stations within `corridor_m`
      of it, in TRAVEL order. A disc around a centre cannot say "along the way": its edge
      is wherever the row limit ran out, so it moves with local density (§10hx). A
      corridor's edge is the way, so the same route answers the same thing every time.

§10ie: the lines are STRAIGHT. Valhalla drew them by road until it was dropped — a 4 GB
extract, a 10 GB graph and a container per region, for something that said nothing about
replication, tenancy or a phone that can write. Measured before removing it, Nantes to
Angers: the road is 15% longer and its 5 km corridor holds 133 charge points against the
straight line's 113, differing by 36 found and 16 phantom.
The answer is the chain object's shape, so a phone applies it with `zb_client_ingest`.
A bounding box on (lat, lng) over the replica's index, then the haversine distance to
order and cut at the radius. Every libzb call is serialized through one lock: the card
is not made for two threads.
"""
import argparse, ctypes, json, math, os, pathlib, socket, sys, time

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
    lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, extra in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                     ("serve", [ctypes.c_char_p]), ("reply", [ctypes.c_uint64, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + extra
    return lib


class Card:
    """The libzb client. One thread drives it — §10hp removed the lock with the thread."""

    def __init__(self, lib, opts: dict):
        self.lib = lib
        self.h = lib.zb_client_connect(json.dumps(opts).encode())
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
    distances — straight-line, since §10ie dropped the road engine."""
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
    # §10ie: straight lines. The matrix that used to order these stops by road is gone
    # with Valhalla; `tsp_order` falls back to its own distances, which is what it did
    # whenever the engine was unreachable anyway.
    order = tsp_order(pts, closed, matrix=None)
    seq = [stops[i] for i in order]
    path = seq + ([seq[0]] if closed else [])
    legs = [{"from": path[k]["name"] or path[k]["id"], "to": path[k + 1]["name"] or path[k + 1]["id"],
             "m": round(haversine((path[k]["lat"], path[k]["lng"]), (path[k + 1]["lat"], path[k + 1]["lng"])))} for k in range(len(path) - 1)]
    polyline = [[p["lat"], p["lng"]] for p in path]
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
            "polyline": polyline, "straight": True, "along": along,
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



# ── the way between points ──────────────────────────────────────────────────────────


def straight_way(points: list) -> dict:
    """The way through the points as the crow flies: the points ARE the polyline.

    §10ie: this replaced Valhalla, which used to draw it by road. A routing engine is a
    4 GB extract, a 10 GB graph and a container per region, and it contributed nothing
    to what this demo is about — replication, tenancy, and a phone that can write. What
    it bought, measured on Nantes to Angers: a line 15% longer (92.4 km against 80.0)
    and a 5 km corridor holding 133 charge points against 113, differing by 36 found and
    16 phantom. Real for a driver, nothing for the architecture. `along_route` and `tour`
    take the straight line and say so; the corridor machinery never cared where the
    polyline came from.
    """
    legs = [{"km": round(haversine(points[k], points[k + 1]) / 1000, 2)} for k in range(len(points) - 1)]
    return {"polyline": [[p[0], p[1]] for p in points],
            "km": round(sum(leg["km"] for leg in legs), 2),
            "legs": legs, "straight": True}


def _num(x):
    """A replica value as a number. DuckDB returns numerics as text through libzb's
    JSON, and a distance that arrives as a string is a distance the host has to parse
    before it can sort by it."""
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def densify(shape: list, step: float) -> tuple:
    """The polyline as anchors EXACTLY `step` metres apart, each carrying how far along
    the road it sits.

    ⚠️ §10hx: this INTERPOLATES inside a segment; it does not subsample the vertices.
    Keeping "every vertex at least `step` from the last kept" bounds the spacing from
    BELOW and not from above, so the spacing ends up being whatever Valhalla's shape
    happens to be. Two anchors `d` apart cover only sqrt(W² - (d/2)²) of the half-width
    midway between them, and at d >= 2W they cover NOTHING: the corridor breaks in two.
    Measured on a synthetic motorway with vertices 15 km apart, a 2 km and a 5 km
    corridor both collapsed to zero width. Real shapes here have a median segment of
    39 m and a maximum of 1,130 m, so it only narrowed — a bug that hides behind
    someone else's data density is exactly the one to remove.

    With the spacing pinned at `step`, the corridor holds sqrt(1 - (step/2W)²) of its
    full half-width everywhere; at the default step = W/2 that is 96.8%."""
    out = [(shape[0][0], shape[0][1], 0.0)]
    carry = 0.0   # metres since the last anchor
    along = 0.0   # metres from the start of the route
    for i in range(len(shape) - 1):
        a, b = shape[i], shape[i + 1]
        d = haversine(a, b)
        if d <= 0:
            continue
        t = step - carry
        while t <= d:
            f = t / d
            out.append((a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f, along + t))
            t += step
        carry = (carry + d) % step
        along += d
    # The tail after the last whole step has no anchor of its own, so the corridor
    # narrows over the final stretch — measured: 950 m unanchored on a 74.95 km route.
    # The destination is exactly where someone looks for a charger, so anchor it.
    if out[-1][2] < along:
        out.append((shape[-1][0], shape[-1][1], along))
    return out, along


# One anchor is a row in a VALUES list and a pass over the candidates; past this many the
# step widens instead, so a 900 km route with a 200 m corridor degrades in accuracy
# rather than in the shape of the SQL.
MAX_ANCHORS = 4000


def _corridor_sql(inner: str, id_col: str, cols: str, anchors: list, width: float, limit: int) -> str:
    """`inner` (the bbox-filtered candidates) narrowed to what lies within `width` of the
    road, ordered by how far along the road it sits — travel order, not distance from
    the line. `arg_min` carries the winning anchor's distance-along out of the same
    aggregate that finds the nearest one."""
    vals = ",".join(f"({a[0]!r},{a[1]!r},{a[2]!r})" for a in anchors)
    dist = ("2 * 6371000 * asin(sqrt(pow(sin(radians(b.lat - a.alat) / 2), 2) + "
            "cos(radians(a.alat)) * cos(radians(b.lat)) * pow(sin(radians(b.lng - a.alng) / 2), 2)))")
    return (f"WITH anchor(alat, alng, am) AS (VALUES {vals}), box AS ({inner}), "
            f"near AS (SELECT b.{id_col} AS _id, min({dist}) AS m, arg_min(a.am, {dist}) AS along_m "
            f"         FROM box b, anchor a GROUP BY b.{id_col} HAVING m <= {width}) "
            f"SELECT {cols}, n.m, n.along_m FROM box b JOIN near n ON n._id = b.{id_col} "
            f"ORDER BY n.along_m LIMIT {limit}")


def along_route(card: Card, q: dict) -> dict:
    """{"points": [{"lat","lng"}, …], "costing": "auto", "corridor_m": 5000,
        "min_kw": 0, "operational": false, "fuel": "SP95"|null, "limit": 500}
    → the road between the points, plus the charge points and the fuel stations within
      `corridor_m` of it, in TRAVEL order.

    The disc that `chargers_near` draws cannot express "along the way": its edge is
    wherever the row limit ran out, so it moves with local density and a pan gives a
    different set (§10hx). A corridor's edge is the road, so the same route answers the
    same thing every time.

    `chargers.columns` is exactly CP_COLS, the chain object's shape, so the answer goes
    straight into an on-demand `charge_points` through `ingest`. The two distances live
    beside it in `charger_positions`, row for row, rather than as extra columns that
    would not fit the table.

    ⚠️ No scope is returned. The corridor is not a box, and the box around it holds rows
    a phone legitimately fetched for other views; sweeping it would throw them away."""
    t0 = time.time()
    pts = [(float(p["lat"]), float(p["lng"])) for p in q.get("points", [])]
    if len(pts) < 2:
        return {"error": "at least two points"}
    width = max(100.0, float(q.get("corridor_m", 5000)))
    limit = int(q.get("limit", 500))
    fuel = q.get("fuel") or None
    if fuel is not None and fuel not in FUELS:
        return {"error": f"unknown fuel {fuel!r}", "known": list(FUELS)}
    road = straight_way(pts)
    shape = road["polyline"]
    step = width / 2
    anchors, length = densify(shape, step)
    if len(anchors) > MAX_ANCHORS:
        step = length / MAX_ANCHORS
        anchors, length = densify(shape, step)
    # The corridor's worst half-width, as a fraction: the dip midway between anchors.
    held = math.sqrt(max(0.0, 1 - (step / (2 * width)) ** 2))

    lats = [p[0] for p in shape]
    lngs = [p[1] for p in shape]
    pad_lat = width / 111_320.0
    pad_lng = width / (111_320.0 * max(0.1, math.cos(math.radians(sum(lats) / len(lats)))))
    def bbox(pfx: str = "") -> str:
        return (f"{pfx}lat BETWEEN {min(lats) - pad_lat} AND {max(lats) + pad_lat} "
                f"AND {pfx}lng BETWEEN {min(lngs) - pad_lng} AND {max(lngs) + pad_lng}")

    where = bbox()
    min_kw = float(q.get("min_kw", 0) or 0)
    if min_kw > 0:
        where += f" AND max_power_kw >= {min_kw}"
    if q.get("operational", False):
        where += " AND status_type_id = 50"
    inner = f"SELECT {', '.join(CP_COLS)} FROM {TABLE} WHERE {where}"
    r = card.query(_corridor_sql(inner, "id", ", ".join(f"b.{c}" for c in CP_COLS), anchors, width, limit), [])
    if "error" in r:
        return {"error": r["error"]}
    n = len(CP_COLS)
    chargers = {"columns": CP_COLS, "rows": [row[:n] for row in r["rows"]],
                "count": len(r["rows"]), "complete": len(r["rows"]) < limit}
    # ⚠️ The DuckDB engine hands numbers back as TEXT, so these two arrive as "1234.5".
    # A distance a host has to parse before it can sort by it is not a distance.
    positions = [[_num(row[n]), _num(row[n + 1])] for row in r["rows"]]

    stations = None
    if fuel is not None:
        sbox = (f"SELECT s.id, s.city, s.address, s.road_type, s.lat, s.lng, p.price, p.price_at, o.kind AS outage "
                f"FROM fuel_stations s JOIN fuel_prices p ON p.station_id = s.id AND p.fuel = '{fuel}' "
                f"LEFT JOIN fuel_outages o ON o.station_id = s.id AND o.fuel = '{fuel}' "
                f"WHERE {bbox('s.')}")
        fr = card.query(_corridor_sql(sbox, "id", "b.*", anchors, width, limit), [])
        if "error" in fr:
            return {"error": fr["error"]}
        # the same two trailing columns, as numbers
        srows = [row[:-2] + [_num(row[-2]), _num(row[-1])] for row in fr["rows"]]
        stations = {"fuel": fuel, "columns": fr["columns"], "rows": srows, "count": len(srows)}

    return {"route": road, "corridor_m": width, "anchors": len(anchors),
            "anchor_step_m": round(step, 1), "width_held": round(held, 4),
            "chargers": chargers, "charger_positions": positions, "stations": stations,
            "ms": round((time.time() - t0) * 1000, 1)}


QUERIES = {"chargers_near": chargers_near, "tour": tour, "fuel_near": fuel_near, "along_route": along_route}


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
    a = ap.parse_args()
    lib = load_lib()
    card = Card(lib, {"natsUrl": a.url, "credsPath": a.creds, "principal": a.principal, "dbPath": a.db, "engine": a.engine,
                      "tables": TABLES, "clientId": "poi-service", "heartbeatMs": 0, "seedStreaming": True})
    t0 = time.time()
    s = card.sync()
    print(f"replica: {json.dumps(s)[:200]} in {time.time() - t0:.1f} s", flush=True)

    serve(card, [t.strip() for t in a.tenants.split(",") if t.strip()], a.queue, a.label)


if __name__ == "__main__":
    main()

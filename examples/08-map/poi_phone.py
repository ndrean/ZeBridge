#!/usr/bin/env python3
"""A phone, shaped in Python: libzb with `osm_pois` ON DEMAND (NOTES §10hj).

    examples/08-map/poi_phone.py --at 47.2184,-1.5536 --radius 600

The table is created from its descriptor but nothing seeds it and nothing is tailed:
the phone asks the service (`zb_client_request` on `query.<tenant>.pois_near`), keeps
the answer (`zb_client_ingest`, the chain's version-guarded upsert, with the asked
bounding box as the scope so what left that area is deleted locally), and reads its own
SQLite. Offline, it has every area it visited. Prints what it asked, what it got, what it
holds — and, with `--twice`, asks again to show the second answer changes only what moved.
"""
import argparse, base64, ctypes, json, math, os, pathlib, struct, sys, time

ROOT = pathlib.Path(__file__).resolve().parents[2]
LIB = ROOT / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if sys.platform == "darwin" else "libzbcore.so")
TABLE = "osm_pois"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--at", default="47.2184,-1.5536", help="lat,lng (default: Nantes)")
    ap.add_argument("--radius", type=float, default=600.0)
    ap.add_argument("--kinds", default="", help="comma list of amenity,shop,tourism,man_made")
    ap.add_argument("--tenant", default="kilo")
    ap.add_argument("--principal", default="omar")
    ap.add_argument("--creds", default=str(ROOT / "scripts" / "native" / "creds" / "omar.creds"))
    ap.add_argument("--db", default="/tmp/poi-phone.sqlite3")
    ap.add_argument("--url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    ap.add_argument("--twice", action="store_true")
    ap.add_argument("--edit", action="store_true", help="the edit story: add a POI here, rename it, remove it — asking the service after each")
    ap.add_argument("--tour", type=int, default=0, help="pick this many named POIs the phone holds and ask the service for the shortest round trip through them, with what lies along the way")
    ap.add_argument("--route", default="", help="lat,lng of a destination: the road from --at, by Valhalla through the service")
    ap.add_argument("--fuel", default="", help="a fuel (SP95, Gazole, E10, SP98, E85, GPLc): the cheapest stations within 3 km, and the cheapest along the tour")
    a = ap.parse_args()
    lat, lng = (float(x) for x in a.at.split(","))

    lib = ctypes.CDLL(str(LIB))
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, extra in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                     ("request", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint64]), ("ingest", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]),
                     ("mutate", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]), ("flush", [ctypes.c_uint64])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + extra

    def take(ptr):
        try:
            return json.loads(ctypes.string_at(ptr).decode())
        finally:
            lib.zb_free(ptr)

    h = lib.zb_client_open(json.dumps({"url": a.url, "credsPath": a.creds, "principal": a.principal, "dbPath": a.db,
                                       "ondemandTables": [TABLE], "clientId": "poi-phone", "heartbeatMs": 0}).encode())
    if not h:
        sys.exit("open failed")
    try:
        s = take(lib.zb_client_sync(h))          # the schema arrives; nothing seeds
        print("sync:", json.dumps(s)[:160])
        rounds = 2 if a.twice else 1
        for r in range(rounds):
            q = {"lat": lat, "lng": lng, "radius_m": a.radius, "limit": 2000}
            if a.kinds:
                q["kinds"] = a.kinds.split(",")
            t0 = time.time()
            ans = take(lib.zb_client_request(h, f"query.{a.tenant}.pois_near".encode(), json.dumps(q).encode(), 5000))
            dt = (time.time() - t0) * 1000
            if "error" in ans:
                sys.exit(f"request: {ans}")
            print(f"asked pois_near {a.at} r={a.radius:.0f} m → {ans['count']} rows{'' if ans.get('complete') else ' (cut by the limit)'} in {dt:.0f} ms round trip ({ans['ms']} ms in the service)")
            dlat = a.radius / 111_320.0
            dlng = a.radius / (111_320.0 * max(0.1, math.cos(math.radians(lat))))
            # The scope — "what I hold in this box and the answer lacks is gone" — only when the
            # answer is complete; an answer cut by the limit says nothing about the rest.
            scope = {"where": "lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?", "params": [lat - dlat, lat + dlat, lng - dlng, lng + dlng]} if ans.get("complete") else None
            ing = take(lib.zb_client_ingest(h, TABLE.encode(), json.dumps({"columns": ans["columns"], "rows": ans["rows"]}).encode(), json.dumps(scope).encode() if scope else b""))
            print(f"ingest: {ing}")
            held = take(lib.zb_client_query(h, f"SELECT count(*), count(name) FROM {TABLE}".encode(), b"[]"))
            print(f"the phone holds {held['rows'][0][0]} POIs, {held['rows'][0][1]} named")
            sample = take(lib.zb_client_query(h, f"SELECT name, coalesce(amenity, shop, tourism, man_made) AS what FROM {TABLE} WHERE name IS NOT NULL ORDER BY osm_id LIMIT 5".encode(), b"[]"))
            for row in sample["rows"]:
                print("   ", row)
            if r == 0 and rounds > 1:
                time.sleep(1)
        if a.edit:
            edit_story(lib, take, h, a, lat, lng)
        if a.fuel:
            fuel_story(lib, take, h, a, lat, lng)
        if a.tour:
            tour_story(lib, take, h, a, lat, lng)
        if a.route:
            route_story(lib, take, h, a, lat, lng)
    finally:
        lib.zb_client_close(h)


B32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def geohash(lat, lng, precision=5):
    lat_r, lng_r = [-90.0, 90.0], [-180.0, 180.0]
    out, bits, ch, even = "", 0, 0, True
    while len(out) < precision:
        r, v = (lng_r, lng) if even else (lat_r, lat)
        mid = (r[0] + r[1]) / 2
        if v >= mid:
            ch = ch * 2 + 1; r[0] = mid
        else:
            ch = ch * 2; r[1] = mid
        even = not even; bits += 1
        if bits == 5:
            out += B32[ch]; bits = ch = 0
    return out


def ewkb_point(lng, lat, srid=4326) -> str:
    """The bytes PostGIS speaks for a point with an SRID, little-endian, as libzb's $bin (base64)."""
    return base64.b64encode(struct.pack("<BIIdd", 1, 0x20000001, srid, lng, lat)).decode()


def edit_story(lib, take, h, a, lat, lng):
    """Add here, rename, remove — each write through libzb's outbox to the bridge, each step
    checked by asking the service (its DuckDB replica follows CDC) and by the phone's own row."""
    def ask(radius=60.0):
        q = {"lat": lat, "lng": lng, "radius_m": radius, "limit": 2000}
        ans = take(lib.zb_client_request(h, f"query.{a.tenant}.pois_near".encode(), json.dumps(q).encode(), 5000))
        if "error" in ans:
            sys.exit(f"request: {ans}")
        return ans

    def flush_and_wait(what):
        t0 = time.time()
        fl = take(lib.zb_client_flush(h, 8000))
        print(f"  {what}: flushed {fl} in {(time.time() - t0) * 1000:.0f} ms")
        time.sleep(1.5)  # the service's replica applies the CDC echo

    def in_answer(ans, osm_id):
        i = ans["columns"].index("osm_id"); j = ans["columns"].index("name")
        return next((row[j] for row in ans["rows"] if row[i] == osm_id), None)

    osm_id = -int(time.time())  # a client-minted id: negative, never an OSM one
    print(f"edit story at {lat},{lng} (osm_id {osm_id})")
    # An INSERT's values are the whole row, the key included: the bridge builds the statement
    # from `data` (the key alone addresses UPDATE and DELETE). `geom` as PostGIS's own bytes.
    values = {"osm_id": osm_id, "cell": geohash(lat, lng), "amenity": "cafe", "name": "Café ZeBridge", "lat": lat, "lng": lng, "geom": {"$bin": ewkb_point(lng, lat)}}
    m = take(lib.zb_client_mutate(h, "osm_pois".encode(), b"INSERT", json.dumps({"osm_id": osm_id}).encode(), json.dumps(values).encode()))
    print(f"  INSERT → {m}")
    flush_and_wait("insert")
    print(f"  the service sees: {in_answer(ask(), osm_id)!r}")
    m = take(lib.zb_client_mutate(h, "osm_pois".encode(), b"UPDATE", json.dumps({"osm_id": osm_id}).encode(), json.dumps({"name": "Café ZeBridge (renamed)"}).encode()))
    print(f"  UPDATE → {m}")
    flush_and_wait("rename")
    print(f"  the service sees: {in_answer(ask(), osm_id)!r}")
    m = take(lib.zb_client_mutate(h, "osm_pois".encode(), b"DELETE", json.dumps({"osm_id": osm_id}).encode(), b""))
    print(f"  DELETE → {m}")
    flush_and_wait("remove")
    print(f"  the service sees: {in_answer(ask(), osm_id)!r}")
    local = take(lib.zb_client_query(h, "SELECT count(*) FROM osm_pois WHERE osm_id = ?".encode(), json.dumps([osm_id]).encode()))
    print(f"  the phone holds it: {bool(local['rows'][0][0])}")





def fuel_story(lib, take, h, a, lat, lng):
    """The cheapest stations around, from the fuel feed the service's replica also follows."""
    q = {"lat": lat, "lng": lng, "radius_m": 3000, "fuel": a.fuel, "sort": "price", "limit": 5}
    t0 = time.time()
    ans = take(lib.zb_client_request(h, f"query.{a.tenant}.fuel_near".encode(), json.dumps(q).encode(), 5000))
    dt = (time.time() - t0) * 1000
    if "error" in ans:
        sys.exit(f"fuel_near: {ans}")
    c = ans["columns"]
    print(f"{a.fuel} within 3 km, cheapest first ({ans['count']} shown, {dt:.0f} ms round trip, {ans['ms']} ms in the service):")
    for row in ans["rows"]:
        print(f"    {float(row[c.index('price')]):.3f} €  {str(row[c.index('address')])[:30]:30} {str(row[c.index('city')])[:18]:18} {float(row[c.index('m')]):>6.0f} m" + (f"  ({row[c.index('outage')]} outage)" if row[c.index('outage')] else ""))


def route_story(lib, take, h, a, lat, lng):
    """Two points → the road between them, from Valhalla through the service (never straight lines)."""
    to_lat, to_lng = (float(x) for x in a.route.split(","))
    q = {"points": [{"lat": lat, "lng": lng}, {"lat": to_lat, "lng": to_lng}], "costing": "auto"}
    t0 = time.time()
    ans = take(lib.zb_client_request(h, f"query.{a.tenant}.route".encode(), json.dumps(q).encode(), 15000))
    dt = (time.time() - t0) * 1000
    if "error" in ans:
        sys.exit(f"route: {ans}")
    print(f"route {lat:.4f},{lng:.4f} → {to_lat:.4f},{to_lng:.4f}: {ans['km']} km, {ans['min']} min by road, {len(ans['polyline'])} points — {dt:.0f} ms round trip ({ans['ms']} ms in the service)")


def tour_story(lib, take, h, a, lat, lng):
    """The voyageur de commerce, from the phone: stops it already holds, the service's answer."""
    import random
    rows = take(lib.zb_client_query(h, "SELECT osm_id, name FROM osm_pois WHERE name IS NOT NULL AND amenity IS NOT NULL".encode(), b"[]"))["rows"]
    picks = random.Random(int(lat * 1e4)).sample(rows, min(a.tour, len(rows)))
    q = {"stops": [r[0] for r in picks], "closed": True, "along_m": 60, "along_limit": 200}
    if a.fuel:
        q["fuel"], q["fuel_m"] = a.fuel, 1500
    t0 = time.time()
    ans = take(lib.zb_client_request(h, f"query.{a.tenant}.tour".encode(), json.dumps(q).encode(), 10000))
    dt = (time.time() - t0) * 1000
    if "error" in ans:
        sys.exit(f"tour: {ans}")
    how = f"by road, {ans['by_road']['min']} min, {len(ans['polyline'])} points" if ans.get("by_road") else f"straight lines ({ans.get('road_error') or 'roads off'})"
    print(f"tour through {len(picks)} stops: {ans['total_m']} m round trip {how}, {len(ans['legs'])} legs, {ans['along']['count']} POIs within 60 m of the way — {dt:.0f} ms round trip ({ans['ms']} ms in the service)")
    for leg in ans["legs"]:
        print(f"    {str(leg['from'])[:34]:34} → {str(leg['to'])[:34]:34} {leg['m']:>6} m")
    if ans.get("fuel"):
        f = ans["fuel"]; c = f["columns"]
        print(f"  {f['fuel']} along the way: {f['count']} station(s) within 1.5 km of a leg" + (":" if f["rows"] else ""))
        for row in f["rows"][:3]:
            print(f"    {float(row[c.index('price')]):.3f} €  {str(row[c.index('address')])[:30]:30} {str(row[c.index('city')])[:18]:18} detour {row[c.index('detour_m')]:>5} m" + (f"  ({row[c.index('outage')]} outage)" if row[c.index('outage')] else ""))
    if ans["along"]["count"]:
        ing = take(lib.zb_client_ingest(h, "osm_pois".encode(), json.dumps({"columns": ans["along"]["columns"], "rows": ans["along"]["rows"]}).encode(), b""))
        print(f"  kept what lies along the way: {ing}")
        ni = ans["along"]["columns"].index("name"); ki = ans["along"]["columns"].index("amenity")
        names = [f"{r[ni]} ({r[ki] or 'poi'})" for r in ans["along"]["rows"] if r[ni]][:6]
        print("   ", "; ".join(names))


if __name__ == "__main__":
    main()

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
import argparse, ctypes, json, math, os, pathlib, sys, time

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
    a = ap.parse_args()
    lat, lng = (float(x) for x in a.at.split(","))

    lib = ctypes.CDLL(str(LIB))
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, extra in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                     ("request", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint64]), ("ingest", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p])):
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
    finally:
        lib.zb_client_close(h)


if __name__ == "__main__":
    main()

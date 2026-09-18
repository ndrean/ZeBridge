#!/usr/bin/env python3
"""The HOT OpenStreetMap points of interest of France as one replicated table (NOTES §10hj).

    examples/08-map/load_pois.py --create                # the table, its indexes
    examples/08-map/load_pois.py                         # load the export beside this file (2.1M points, a minute or two)
    examples/08-map/load_pois.py --geojson later.geojson # a later export: only the diff moves
    examples/08-map/load_pois.py --enable                # publish it as a PUBLIC table (a service replica; option B routes by cell)
    examples/08-map/load_pois.py --limit 100000          # a sample, for a quick look

The export is GeoJSON, one feature per line, 17 properties of which 3.8 are set on
average — 950 MB of text for ~250 MB of content — and it is read ONCE, streamed line by
line, never held in memory. Each point becomes one row of `osm_pois`, keyed by `osm_id`,
with its four tag columns (amenity, shop, tourism, man_made), names, opening hours,
address, a PostGIS point, and `cell`: the geohash-5 it falls in (~4.9 km), the map's
cell and option B's routing key (NOTES §10fp). `updated_at` is the LWW version,
`deleted_at` the tombstone. The upsert touches only rows whose values differ, so a
later export is absorbed as a diff — the same rule as load_fuel.py, and loading the
same file twice moves nothing.

Not enabled by default: with 34,196 cells, a tenant per cell is option A's shape and
34,196 streams — option B is the answer, and until it exists `--enable` publishes the
table as PUBLIC (one stream, one chain of all of France) for a service replica.
"""
import argparse, csv, json, os, pathlib, subprocess, sys, tempfile, time

HERE = pathlib.Path(__file__).resolve().parent
PSQL = os.environ.get("ZB_PSQL", "/opt/homebrew/opt/postgresql@18/bin/psql")
PUB = os.environ.get("BRIDGE_CDC_PUBLICATION", "my_pub")
DEFAULT_GEOJSON = HERE / "hotosm_fra_points_of_interest_points_geojson" / "hotosm_fra_points_of_interest_points_geojson.geojson"
B32 = "0123456789bcdefghjkmnpqrstuvwxyz"

DDL = """
CREATE TABLE IF NOT EXISTS public.osm_pois (
  osm_id           bigint PRIMARY KEY,
  cell             text NOT NULL,                -- geohash-5 of the point: the map's cell
  amenity          text,
  shop             text,
  tourism          text,
  man_made         text,
  name             text,
  name_en          text,
  name_fr          text,
  opening_hours    text,
  beds             integer,
  rooms            integer,
  addr_full        text,
  addr_housenumber text,
  addr_street      text,
  addr_city        text,
  source           text,
  lat              double precision NOT NULL,   -- the point again, as numbers: a phone's SQLite has no
  lng              double precision NOT NULL,   -- spatial functions, and a bounding box is a B-tree
  geom             geometry(Point, 4326) NOT NULL,
  updated_at       timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz
);
CREATE INDEX IF NOT EXISTS osm_pois_cell_idx ON public.osm_pois (cell);
CREATE INDEX IF NOT EXISTS osm_pois_latlng_idx ON public.osm_pois (lat, lng);
CREATE INDEX IF NOT EXISTS osm_pois_geom_idx ON public.osm_pois USING gist (geom);
"""

COLS = ["osm_id", "cell", "amenity", "shop", "tourism", "man_made", "name", "name_en", "name_fr", "opening_hours",
        "beds", "rooms", "addr_full", "addr_housenumber", "addr_street", "addr_city", "source", "lat", "lng"]
PROPS = {"name": "name", "name:en": "name_en", "name:fr": "name_fr", "amenity": "amenity", "man_made": "man_made", "shop": "shop",
         "tourism": "tourism", "opening_hours": "opening_hours", "beds": "beds", "rooms": "rooms", "addr:full": "addr_full",
         "addr:housenumber": "addr_housenumber", "addr:street": "addr_street", "addr:city": "addr_city", "source": "source"}

STAGE = "CREATE TEMP TABLE stage (" + ", ".join(
    f"{c} " + ("bigint" if c == "osm_id" else "integer" if c in ("beds", "rooms") else "double precision" if c in ("lat", "lng") else "text") for c in COLS
) + ") ON COMMIT DROP;\n"

VALUE_COLS = [c for c in COLS if c != "osm_id"]
APPLY = f"""
WITH up AS (
  INSERT INTO public.osm_pois AS t ({", ".join(COLS)}, geom)
  SELECT {", ".join(COLS)}, ST_SetSRID(ST_MakePoint(lng, lat), 4326) FROM stage
  ON CONFLICT (osm_id) DO UPDATE SET
    {", ".join(f"{c} = excluded.{c}" for c in VALUE_COLS)}, geom = excluded.geom, updated_at = now(), deleted_at = NULL
  WHERE ({", ".join(f"t.{c}" for c in VALUE_COLS)}, ST_AsEWKB(t.geom), t.deleted_at)
        IS DISTINCT FROM
        ({", ".join(f"excluded.{c}" for c in VALUE_COLS)}, ST_AsEWKB(excluded.geom), NULL::timestamptz)
  RETURNING (xmax = 0) AS inserted
),
gone AS (
  UPDATE public.osm_pois t SET deleted_at = now(), updated_at = now()
  WHERE t.deleted_at IS NULL AND NOT EXISTS (SELECT 1 FROM stage s WHERE s.osm_id = t.osm_id)
  RETURNING 1
)
SELECT (SELECT count(*) FROM up WHERE inserted) AS inserted, (SELECT count(*) FROM up WHERE NOT inserted) AS updated, (SELECT count(*) FROM gone) AS deleted;
"""


def geohash(lat: float, lng: float, precision: int = 5) -> str:
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


def psql(sql: str, *, file: pathlib.Path | None = None) -> str:
    cmd = [PSQL, "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-d", "postgres", "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1"]
    cmd += ["-f", str(file)] if file else ["-c", sql]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(r.stderr.strip())
    return r.stdout.strip()


def cell_text(v):
    if v is None:
        return "\\N"
    s = str(v)
    return s.replace("\\", "\\\\").replace("\t", " ").replace("\r", " ").replace("\n", " ")


def as_int(v):
    try:
        return int(str(v).strip()) if v is not None and str(v).strip() else None
    except ValueError:
        return None


def stage_file(src: pathlib.Path, dst: pathlib.Path, limit: int | None, precision: int) -> tuple[int, int]:
    """GeoJSON, one feature per line → COPY text, one row per point. Returns (rows, skipped)."""
    rows = skipped = 0
    with open(src, encoding="utf-8") as f, open(dst, "w") as out:
        for line in f:
            line = line.strip()
            if not line.startswith('{ "type": "Feature"') and not line.startswith('{"type": "Feature"'):
                continue
            if line.endswith(","):
                line = line[:-1]
            try:
                d = json.loads(line)
                g = d.get("geometry") or {}
                if g.get("type") != "Point":
                    skipped += 1
                    continue
                lon, lat = g["coordinates"][:2]
                p = d.get("properties") or {}
                osm_id = p.get("osm_id")
                if osm_id is None:
                    skipped += 1
                    continue
            except (ValueError, KeyError, TypeError):
                skipped += 1
                continue
            vals = {"osm_id": int(osm_id), "cell": geohash(float(lat), float(lon), precision), "lat": float(lat), "lng": float(lon)}
            for src_key, col in PROPS.items():
                v = p.get(src_key)
                vals[col] = as_int(v) if col in ("beds", "rooms") else (v if v not in (None, "") else None)
            out.write("\t".join(cell_text(vals[c]) for c in COLS) + "\n")
            rows += 1
            if limit and rows >= limit:
                break
    return rows, skipped


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--geojson", type=pathlib.Path, default=DEFAULT_GEOJSON)
    ap.add_argument("--create", action="store_true", help="create the table and its indexes, then load")
    ap.add_argument("--enable", action="store_true", help="publish osm_pois as a PUBLIC table (one stream, one chain of all of France)")
    ap.add_argument("--limit", type=int, default=None, help="load only the first N points — a SAMPLE, refused unless the table is empty (a partial snapshot soft-deletes everything else)")
    ap.add_argument("--precision", type=int, default=5, help="geohash length of `cell` (5 ≈ 4.9 km)")
    a = ap.parse_args()
    if a.create:
        psql(DDL)
        print("osm_pois: table and indexes ready")
    if a.limit:
        # A snapshot is authoritative: rows it lacks are soft-deleted. A sample is not a
        # snapshot — measured once, --limit 1 tombstoned 2,105,591 points.
        live = int(psql("SELECT count(*) FROM public.osm_pois WHERE deleted_at IS NULL") or "0")
        if live > a.limit:
            sys.exit(f"--limit {a.limit} on a table holding {live:,} live points would soft-delete the rest; load the full export instead")
    if a.enable:
        out = psql(f"SELECT step || ':' || status || ' ' || coalesce(detail, '') FROM zebridge_enable('public.osm_pois'::regclass, "
                   f"version_col => 'updated_at', tombstone_col => 'deleted_at', public_reason => 'OpenStreetMap points of interest, an open dataset', "
                   f"publication => '{PUB}', dry_run => false) WHERE status = 'ERROR'")
        if out:
            sys.exit(f"enable refused: {out}")
        print("osm_pois: enabled as a public table")
    with tempfile.TemporaryDirectory() as d:
        tsv = pathlib.Path(d) / "stage.tsv"
        t0 = time.time()
        rows, skipped = stage_file(a.geojson, tsv, a.limit, a.precision)
        t1 = time.time()
        script = pathlib.Path(d) / "load.sql"
        script.write_text("BEGIN;\n" + STAGE + f"\\copy stage FROM '{tsv}' WITH (FORMAT text)\n" + APPLY + "COMMIT;\n")
        out = psql("", file=script)
        t2 = time.time()
    ins, upd, dele = out.split("|")
    print(f"{a.geojson.name}: {rows:,} points staged in {t1 - t0:.0f} s ({skipped} skipped), applied in {t2 - t1:.0f} s")
    print(f"  inserted {int(ins):,}, updated {int(upd):,}, deleted {int(dele):,}")


if __name__ == "__main__":
    main()

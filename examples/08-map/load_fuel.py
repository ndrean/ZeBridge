#!/usr/bin/env python3
"""The French fuel-price feed as three replicated tables (NOTES §10hh).

    examples/08-map/load_fuel.py --create              # tables, PostGIS point, enable
    examples/08-map/load_fuel.py                       # load the snapshot beside this file
    examples/08-map/load_fuel.py --csv other.csv       # a later snapshot: only the diff moves
    examples/08-map/load_fuel.py --perturb 0.1         # a simulated day: 10% of prices move
    examples/08-map/load_fuel.py --close 50            # … and 50 stations close (soft-deleted, prices and outages with them)

The feed is a daily FULL snapshot, wide: one row per station with six price columns and
their timestamps, an outage start and kind per fuel, two JSON columns. Normalized here:

    fuel_stations (id, postcode, road_type, address, city, geom Point/4326, hours, services,
                   automate, departement, code_departement, region, code_region)
    fuel_prices   (station_id → fuel_stations, fuel, price, price_at)       PK (station_id, fuel)
    fuel_outages  (station_id → fuel_stations, fuel, started_at, kind)      PK (station_id, fuel)

Every table carries `updated_at` (the LWW version) and `deleted_at` (the tombstone,
PROTOCOL §7.5): a station gone from the snapshot, a fuel no longer sold, an outage over —
all soft-deleted, and revived when they come back. The upserts write ONLY rows whose
values differ (`IS DISTINCT FROM`), so PostgreSQL absorbs a day's snapshot as a diff and
CDC carries exactly that diff. Loading the same file twice moves nothing.

The load is one transaction: `\\copy` into a temp staging table, then the three upserts
and the soft-deletes as CTEs that count what they touched. psql only, as provision.py.
"""
import argparse, csv, json, os, pathlib, random, subprocess, sys, tempfile, time

HERE = pathlib.Path(__file__).resolve().parent
PSQL = os.environ.get("ZB_PSQL", "/opt/homebrew/opt/postgresql@18/bin/psql")
PUB = os.environ.get("BRIDGE_CDC_PUBLICATION", "my_pub")
DEFAULT_CSV = HERE / "prix-des-carburants-en-france-flux-instantane-v2.csv"
FUELS = ["Gazole", "SP95", "E85", "GPLc", "E10", "SP98"]
# the outage columns spell the fuel in lower case, except GPLc
OUTAGE_KEY = {"Gazole": "gazole", "SP95": "sp95", "E85": "e85", "GPLc": "GPLc", "E10": "e10", "SP98": "sp98"}

DDL = """
CREATE TABLE IF NOT EXISTS public.fuel_stations (
  id               bigint PRIMARY KEY,
  postcode         text,
  road_type        text,                       -- R route, A autoroute
  address          text,
  city             text,
  geom             geometry(Point, 4326) NOT NULL,
  lat              double precision NOT NULL,   -- the point as numbers too: a replica without spatial
  lng              double precision NOT NULL,   -- functions asks "within this box" on a B-tree
  hours            jsonb,
  services         jsonb,
  automate         boolean,
  departement      text,
  code_departement text,
  region           text,
  code_region      text,
  updated_at       timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz
);
CREATE TABLE IF NOT EXISTS public.fuel_prices (
  station_id bigint NOT NULL REFERENCES public.fuel_stations(id),
  fuel       text NOT NULL,
  price      numeric(6,3) NOT NULL,
  price_at   timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  PRIMARY KEY (station_id, fuel)
);
CREATE TABLE IF NOT EXISTS public.fuel_outages (
  station_id bigint NOT NULL REFERENCES public.fuel_stations(id),
  fuel       text NOT NULL,
  started_at timestamptz,
  kind       text NOT NULL,                    -- temporaire | definitive
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  PRIMARY KEY (station_id, fuel)
);
CREATE INDEX IF NOT EXISTS fuel_stations_geom_idx ON public.fuel_stations USING gist (geom);
"""

STAGE = """
CREATE TEMP TABLE stage (
  id bigint, lat double precision, lon double precision, postcode text, road_type text, address text, city text,
  hours jsonb, services jsonb, automate boolean, departement text, code_departement text, region text, code_region text,
  p_gazole numeric, at_gazole timestamptz, p_sp95 numeric, at_sp95 timestamptz, p_e85 numeric, at_e85 timestamptz,
  p_gplc numeric, at_gplc timestamptz, p_e10 numeric, at_e10 timestamptz, p_sp98 numeric, at_sp98 timestamptz,
  o_gazole timestamptz, k_gazole text, o_sp95 timestamptz, k_sp95 text, o_e85 timestamptz, k_e85 text,
  o_gplc timestamptz, k_gplc text, o_e10 timestamptz, k_e10 text, o_sp98 timestamptz, k_sp98 text
) ON COMMIT DROP;
"""

# The diff, as one statement of CTEs: each upsert touches only rows whose values differ,
# each soft-delete only live rows the snapshot no longer carries. The final SELECT is the
# report: what CDC will carry.
APPLY = """
WITH
unpivot_p AS (
  SELECT s.id AS station_id, f.fuel, f.price, f.price_at
  FROM stage s
  CROSS JOIN LATERAL (VALUES ('Gazole', s.p_gazole, s.at_gazole), ('SP95', s.p_sp95, s.at_sp95), ('E85', s.p_e85, s.at_e85),
                             ('GPLc', s.p_gplc, s.at_gplc), ('E10', s.p_e10, s.at_e10), ('SP98', s.p_sp98, s.at_sp98)) AS f(fuel, price, price_at)
  WHERE f.price IS NOT NULL
),
unpivot_o AS (
  SELECT s.id AS station_id, f.fuel, f.started_at, f.kind
  FROM stage s
  CROSS JOIN LATERAL (VALUES ('Gazole', s.o_gazole, s.k_gazole), ('SP95', s.o_sp95, s.k_sp95), ('E85', s.o_e85, s.k_e85),
                             ('GPLc', s.o_gplc, s.k_gplc), ('E10', s.o_e10, s.k_e10), ('SP98', s.o_sp98, s.k_sp98)) AS f(fuel, started_at, kind)
  WHERE f.kind IS NOT NULL AND f.kind <> ''
),
st AS (
  INSERT INTO public.fuel_stations AS t (id, postcode, road_type, address, city, geom, lat, lng, hours, services, automate, departement, code_departement, region, code_region)
  SELECT id, postcode, road_type, address, city, ST_SetSRID(ST_MakePoint(lon, lat), 4326), lat, lon, hours, services, automate, departement, code_departement, region, code_region
  FROM stage
  ON CONFLICT (id) DO UPDATE SET
    postcode = excluded.postcode, road_type = excluded.road_type, address = excluded.address, city = excluded.city,
    geom = excluded.geom, lat = excluded.lat, lng = excluded.lng, hours = excluded.hours, services = excluded.services, automate = excluded.automate,
    departement = excluded.departement, code_departement = excluded.code_departement, region = excluded.region, code_region = excluded.code_region,
    updated_at = now(), deleted_at = NULL
  WHERE (t.postcode, t.road_type, t.address, t.city, ST_AsEWKB(t.geom), t.lat, t.lng, t.hours, t.services, t.automate, t.departement, t.code_departement, t.region, t.code_region, t.deleted_at)
        IS DISTINCT FROM
        (excluded.postcode, excluded.road_type, excluded.address, excluded.city, ST_AsEWKB(excluded.geom), excluded.lat, excluded.lng, excluded.hours, excluded.services, excluded.automate, excluded.departement, excluded.code_departement, excluded.region, excluded.code_region, NULL::timestamptz)
  RETURNING (xmax = 0) AS inserted
),
st_gone AS (
  UPDATE public.fuel_stations t SET deleted_at = now(), updated_at = now()
  WHERE t.deleted_at IS NULL AND NOT EXISTS (SELECT 1 FROM stage s WHERE s.id = t.id)
  RETURNING 1
),
pr AS (
  INSERT INTO public.fuel_prices AS t (station_id, fuel, price, price_at)
  SELECT station_id, fuel, price, price_at FROM unpivot_p
  ON CONFLICT (station_id, fuel) DO UPDATE SET price = excluded.price, price_at = excluded.price_at, updated_at = now(), deleted_at = NULL
  WHERE (t.price, t.price_at, t.deleted_at) IS DISTINCT FROM (excluded.price, excluded.price_at, NULL::timestamptz)
  RETURNING (xmax = 0) AS inserted
),
pr_gone AS (
  UPDATE public.fuel_prices t SET deleted_at = now(), updated_at = now()
  WHERE t.deleted_at IS NULL AND NOT EXISTS (SELECT 1 FROM unpivot_p u WHERE u.station_id = t.station_id AND u.fuel = t.fuel)
  RETURNING 1
),
ou AS (
  INSERT INTO public.fuel_outages AS t (station_id, fuel, started_at, kind)
  SELECT station_id, fuel, started_at, kind FROM unpivot_o
  ON CONFLICT (station_id, fuel) DO UPDATE SET started_at = excluded.started_at, kind = excluded.kind, updated_at = now(), deleted_at = NULL
  WHERE (t.started_at, t.kind, t.deleted_at) IS DISTINCT FROM (excluded.started_at, excluded.kind, NULL::timestamptz)
  RETURNING (xmax = 0) AS inserted
),
ou_gone AS (
  UPDATE public.fuel_outages t SET deleted_at = now(), updated_at = now()
  WHERE t.deleted_at IS NULL AND NOT EXISTS (SELECT 1 FROM unpivot_o u WHERE u.station_id = t.station_id AND u.fuel = t.fuel)
  RETURNING 1
)
SELECT 'stations' AS tbl, (SELECT count(*) FROM st WHERE inserted) AS inserted, (SELECT count(*) FROM st WHERE NOT inserted) AS updated, (SELECT count(*) FROM st_gone) AS deleted
UNION ALL
SELECT 'prices', (SELECT count(*) FROM pr WHERE inserted), (SELECT count(*) FROM pr WHERE NOT inserted), (SELECT count(*) FROM pr_gone)
UNION ALL
SELECT 'outages', (SELECT count(*) FROM ou WHERE inserted), (SELECT count(*) FROM ou WHERE NOT inserted), (SELECT count(*) FROM ou_gone);
"""


def psql(sql: str, *, file: pathlib.Path | None = None) -> str:
    cmd = [PSQL, "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-d", "postgres", "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1"]
    cmd += ["-f", str(file)] if file else ["-c", sql]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(r.stderr.strip())
    return r.stdout.strip()


def create():
    psql(DDL)
    for t in ("fuel_stations", "fuel_prices", "fuel_outages"):
        out = psql(f"SELECT step || ':' || status || ' ' || coalesce(detail, '') FROM zebridge_enable('public.{t}'::regclass, "
                   f"version_col => 'updated_at', tombstone_col => 'deleted_at', public_reason => 'fuel prices, an open dataset', "
                   f"publication => '{PUB}', dry_run => false) WHERE status = 'ERROR'")
        if out:
            sys.exit(f"enable {t} refused: {out}")
        print(f"enabled {t}")


def num(v: str):
    return v.strip() if v and v.strip() else None


def load_rows(path: pathlib.Path, perturb: float, seed: int) -> list[list]:
    """The CSV as staging rows, in STAGE's column order; `perturb` moves that share of the
    prices by a few cents and stamps them now — a simulated later snapshot."""
    rng = random.Random(seed)
    now = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
    rows = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f, delimiter=";"):
            lat, lon = float(r["latitude"]) / 100000.0, float(r["longitude"]) / 100000.0
            row = [r["id"], lat, lon, num(r["Code postal"]), num(r["pop"]), num(r["Adresse"]), num(r["Ville"]),
                   num(r["horaires"]), num(r["services"]),
                   {"Oui": "t", "Non": "f"}.get(r["Automate 24-24 (oui/non)"].strip(), None),
                   num(r["Département"]), num(r["code_departement"]), num(r["Région"]), num(r["code_region"])]
            for fuel in FUELS:
                price, at = num(r[f"Prix {fuel}"]), num(r[f"Prix {fuel} mis à jour le"])
                if price is not None and perturb > 0 and rng.random() < perturb:
                    price = f"{float(price) + rng.choice([-0.03, -0.02, -0.01, 0.01, 0.02, 0.03]):.3f}"
                    at = now
                row += [price, at if price is not None else None]
            for fuel in FUELS:
                k = OUTAGE_KEY[fuel]
                row += [num(r[f"Début rupture {k} (si temporaire)"]), num(r[f"Type rupture {k}"])]
            rows.append(row)
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", type=pathlib.Path, default=DEFAULT_CSV)
    ap.add_argument("--create", action="store_true", help="create the three tables and enable them, then load")
    ap.add_argument("--perturb", type=float, default=0.0, help="move this share of the prices (0.1 = 10%%): a simulated later snapshot")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--close", type=int, default=0, help="leave this many random stations out of the snapshot: they close (soft-deleted with their prices and outages)")
    a = ap.parse_args()
    if a.create:
        create()
    rows = load_rows(a.csv, a.perturb, a.seed)
    if a.close:
        rng = random.Random(a.seed + 1000)
        gone = set(rng.sample(range(len(rows)), a.close))
        rows = [r for i, r in enumerate(rows) if i not in gone]
    with tempfile.TemporaryDirectory() as d:
        tsv = pathlib.Path(d) / "stage.tsv"
        # COPY's text format by hand: \N for NULL, backslashes doubled, no tabs or newlines in a cell.
        def cell(v):
            if v is None:
                return "\\N"
            if isinstance(v, str):
                return v.replace("\\", "\\\\").replace("\t", " ").replace("\r", " ").replace("\n", " ")
            return str(v)
        with open(tsv, "w") as f:
            for row in rows:
                f.write("\t".join(cell(v) for v in row) + "\n")
        script = pathlib.Path(d) / "load.sql"
        script.write_text("BEGIN;\n" + STAGE + f"\\copy stage FROM '{tsv}' WITH (FORMAT text)\n" + APPLY + "COMMIT;\n")
        t0 = time.time()
        out = psql("", file=script)
    print(f"{a.csv.name}: {len(rows)} stations staged{' (perturbed ' + str(a.perturb) + ')' if a.perturb else ''}{' (' + str(a.close) + ' closed)' if a.close else ''}, applied in {time.time() - t0:.1f} s")
    print(f"  {'table':10} {'inserted':>9} {'updated':>9} {'deleted':>9}")
    for line in out.splitlines():
        tbl, ins, upd, dele = line.split("|")
        print(f"  {tbl:10} {ins:>9} {upd:>9} {dele:>9}")


if __name__ == "__main__":
    main()

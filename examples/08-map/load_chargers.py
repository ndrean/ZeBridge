#!/usr/bin/env python3
"""OpenChargeMap's charge points as one replicated table (NOTES §10hr).

    examples/08-map/load_chargers.py --create     # the table, its indexes, published
    examples/08-map/load_chargers.py              # load poi-ocm.json (France: 16,173 points)
    examples/08-map/load_chargers.py --country 80 --json later.json   # a later export: the diff moves

It replaces `osm_pois` as the map's dataset. The HOT OpenStreetMap export was 2.1M
points of everything — pharmacies, benches, bars — and a map of it is noise. This is
one KIND of thing, it is what someone would actually look for, and every row carries
something worth showing: how fast the charger is, how many points, whether it works.

The export is the WORLD (272,598 points, 405 MB of JSON, read once into memory);
`--country` keeps one country by `AddressInfo.CountryID` (80 is France). Each survivor
becomes one row keyed by OpenChargeMap's own `ID`, with the address flattened out of
`AddressInfo` and the connections summarised: how many, the fastest in kW, and the
highest level. `updated_at` is the LWW version, `deleted_at` the tombstone, and the
upsert touches only rows whose values differ — so a later export is absorbed as a diff,
and loading the same file twice moves nothing.

The three codes kept as numbers are OpenChargeMap's reference ids. Two are worth
knowing: `status_type_id` 50 is operational (98% of France's rows), and `level_id` bands
the power — checked against this export, not assumed: level 1 medians 2 kW, level 2
medians 22 kW, level 3 medians 50 kW with a tenth percentile of 43.
"""
import argparse, json, os, pathlib, subprocess, sys, tempfile, time

HERE = pathlib.Path(__file__).resolve().parent
PSQL = os.environ.get("ZB_PSQL", "/opt/homebrew/opt/postgresql@18/bin/psql")
PUB = os.environ.get("BRIDGE_CDC_PUBLICATION", "my_pub")
DEFAULT_JSON = HERE / "poi-ocm.json"

DDL = """
CREATE TABLE IF NOT EXISTS public.charge_points (
  -- The key is the UUID the export already carries, not its integer ID: a client mints
  -- its own key for a charge point it adds (SECURITY §1.3), and it cannot mint a bigint
  -- without risking someone else's. The integer stays beside it, unique, for the export.
  id              uuid PRIMARY KEY,
  ocm_id          bigint UNIQUE,
  title           text NOT NULL,                -- what the map labels it
  address         text,
  town            text,
  postcode        text,
  lat             double precision NOT NULL,    -- the point as numbers: a phone's SQLite has no
  lng             double precision NOT NULL,    -- spatial functions, and a bounding box is a B-tree
  geom            geometry(Point, 4326) NOT NULL,
  operator_id     integer,
  usage_type_id   integer,                      -- 1 public, 4 membership, 5 private, 7 notice required
  usage_cost      text,
  status_type_id  integer,                      -- 50 = operational
  points          integer,                      -- NumberOfPoints: how many cars at once
  connections     integer,                      -- how many connectors are described
  max_power_kw    double precision,             -- the fastest connector here
  level_id        integer,                      -- 1 slow, 2 fast, 3 rapid (checked against the data)
  plugs           text,                         -- the connector type ids, comma separated
  phone           text,
  url             text,
  access_comments text,
  comments        text,
  verified_at     timestamptz,                  -- DateLastVerified: how much to trust the row
  updated_at      timestamptz NOT NULL DEFAULT now(),
  deleted_at      timestamptz
);
CREATE INDEX IF NOT EXISTS charge_points_latlng_idx ON public.charge_points (lat, lng);
CREATE INDEX IF NOT EXISTS charge_points_geom_idx ON public.charge_points USING gist (geom);
CREATE INDEX IF NOT EXISTS charge_points_power_idx ON public.charge_points (max_power_kw);
"""

COLS = ["id", "ocm_id", "title", "address", "town", "postcode", "lat", "lng", "operator_id", "usage_type_id",
        "usage_cost", "status_type_id", "points", "connections", "max_power_kw", "level_id", "plugs",
        "phone", "url", "access_comments", "comments", "verified_at"]
INT_COLS = {"ocm_id", "operator_id", "usage_type_id", "status_type_id", "points", "connections", "level_id"}
FLOAT_COLS = {"lat", "lng", "max_power_kw"}

STAGE = "CREATE TEMP TABLE stage (" + ", ".join(
    f"{c} " + ("uuid" if c == "id" else "bigint" if c == "ocm_id" else "integer" if c in INT_COLS else
               "double precision" if c in FLOAT_COLS else "timestamptz" if c == "verified_at" else "text") for c in COLS
) + ") ON COMMIT DROP;\n"

VALUE_COLS = [c for c in COLS if c != "id"]
APPLY = f"""
WITH up AS (
  INSERT INTO public.charge_points AS t ({", ".join(COLS)}, geom)
  SELECT {", ".join(COLS)}, ST_SetSRID(ST_MakePoint(lng, lat), 4326) FROM stage
  ON CONFLICT (id) DO UPDATE SET
    {", ".join(f"{c} = excluded.{c}" for c in VALUE_COLS)}, geom = excluded.geom, updated_at = now(), deleted_at = NULL
  WHERE ({", ".join(f"t.{c}" for c in VALUE_COLS)}, ST_AsEWKB(t.geom), t.deleted_at)
        IS DISTINCT FROM
        ({", ".join(f"excluded.{c}" for c in VALUE_COLS)}, ST_AsEWKB(excluded.geom), NULL::timestamptz)
  RETURNING (xmax = 0) AS inserted
),
gone AS (
  UPDATE public.charge_points t SET deleted_at = now(), updated_at = now()
  WHERE t.deleted_at IS NULL AND NOT EXISTS (SELECT 1 FROM stage s WHERE s.id = t.id)
  RETURNING 1
)
SELECT (SELECT count(*) FROM up WHERE inserted) AS inserted, (SELECT count(*) FROM up WHERE NOT inserted) AS updated, (SELECT count(*) FROM gone) AS deleted;
"""


def psql(sql: str, *, file: pathlib.Path | None = None) -> str:
    cmd = [PSQL, "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-d", "postgres", "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1"]
    cmd += ["-f", str(file)] if file else ["-c", sql]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(r.stderr.strip())
    return r.stdout.strip()


def cell(v):
    if v is None or v == "":
        return "\\N"
    s = str(v).strip()
    if not s:
        return "\\N"
    return s.replace("\\", "\\\\").replace("\t", " ").replace("\r", " ").replace("\n", " ")


def row_of(r: dict) -> dict | None:
    a = r.get("AddressInfo") or {}
    if a.get("Latitude") is None or a.get("Longitude") is None or r.get("ID") is None or not r.get("UUID"):
        return None
    conns = [c for c in (r.get("Connections") or []) if isinstance(c, dict)]
    powers = [c["PowerKW"] for c in conns if isinstance(c.get("PowerKW"), (int, float))]
    levels = [c["LevelID"] for c in conns if isinstance(c.get("LevelID"), int)]
    plugs = sorted({c["ConnectionTypeID"] for c in conns if isinstance(c.get("ConnectionTypeID"), int) and c["ConnectionTypeID"]})
    return {
        "id": r["UUID"], "ocm_id": int(r["ID"]),
        "title": (a.get("Title") or f"charge point {r['ID']}").strip(),
        "address": a.get("AddressLine1"), "town": a.get("Town"), "postcode": (a.get("Postcode") or "").strip() or None,
        "lat": float(a["Latitude"]), "lng": float(a["Longitude"]),
        "operator_id": r.get("OperatorID"), "usage_type_id": r.get("UsageTypeID"), "usage_cost": r.get("UsageCost"),
        "status_type_id": r.get("StatusTypeID"), "points": r.get("NumberOfPoints"),
        "connections": len(conns) or None,
        "max_power_kw": max(powers) if powers else None,
        "level_id": max(levels) if levels else None,
        "plugs": ",".join(str(p) for p in plugs) or None,
        "phone": a.get("ContactTelephone1"), "url": a.get("RelatedURL"),
        "access_comments": a.get("AccessComments"), "comments": r.get("GeneralComments"),
        "verified_at": r.get("DateLastVerified"),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--json", type=pathlib.Path, default=DEFAULT_JSON)
    ap.add_argument("--country", type=int, default=80, help="AddressInfo.CountryID to keep (80 = France); 0 = every country")
    ap.add_argument("--create", action="store_true", help="create the table and its indexes, then load")
    ap.add_argument("--enable", action="store_true", help="publish it: public (an open dataset) and WRITABLE from the edge")
    a = ap.parse_args()

    if a.create:
        psql(DDL)
        print("charge_points: table and indexes ready")
    if a.create or a.enable:
        out = psql(f"SELECT step || ':' || status || ' ' || coalesce(detail, '') FROM zebridge_enable('public.charge_points'::regclass, "
                   f"writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
                   f"public_reason => 'OpenChargeMap charge points, an open dataset', "
                   f"publication => '{PUB}', dry_run => false) WHERE status = 'ERROR'")
        if out:
            sys.exit(f"enable refused: {out}")
        print("charge_points: published — public, writable from the edge")

    t0 = time.time()
    doc = json.loads(a.json.read_text())
    t1 = time.time()
    rows = 0
    with tempfile.TemporaryDirectory() as d:
        tsv = pathlib.Path(d) / "stage.tsv"
        seen: set[str] = set()
        with open(tsv, "w") as out:
            for r in doc:
                if a.country and ((r.get("AddressInfo") or {}).get("CountryID") != a.country):
                    continue
                v = row_of(r)
                if v is None or v["id"] in seen:   # a duplicate key in one export would abort the COPY
                    continue
                seen.add(v["id"])
                out.write("\t".join(cell(v[c]) for c in COLS) + "\n")
                rows += 1
        t2 = time.time()
        script = pathlib.Path(d) / "load.sql"
        script.write_text("BEGIN;\n" + STAGE + f"\\copy stage FROM '{tsv}' WITH (FORMAT text)\n" + APPLY + "COMMIT;\n")
        out = psql("", file=script)
        t3 = time.time()
    ins, upd, dele = out.split("|")
    print(f"{a.json.name}: {len(doc):,} points read in {t1 - t0:.0f} s, {rows:,} kept for country {a.country or 'any'} in {t2 - t1:.0f} s, applied in {t3 - t2:.0f} s")
    print(f"  inserted {int(ins):,}, updated {int(upd):,}, deleted {int(dele):,}")
    print(psql("SELECT 'live ' || count(*) || ', rapid (≥43 kW) ' || count(*) FILTER (WHERE max_power_kw >= 43) || "
               "', operational ' || count(*) FILTER (WHERE status_type_id = 50) FROM public.charge_points WHERE deleted_at IS NULL"))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""The depot demo's trucks: one table, published, five trucks seeded.

    examples/14-depot/load_trucks.py                     # the local database
    ADMIN_DATABASE_URL=postgresql://… examples/14-depot/load_trucks.py   # a remote one (Supabase)
    examples/14-depot/load_trucks.py --check             # everything in a transaction, rolled back

Each truck is based at a depot: the charge point nearest one town, so `charge_points`
(examples/08-map/load_chargers.py) must be loaded first. `plan` is what the page saves when
a route is traced — {"from", "to", "km", "min", "shape"}, the shape as Valhalla encodes it —
so every open page sees the same plan, and redraws it without asking again.

Public, and writable from the edge: the page writes `plan`. Run it again at any time: the
table and its publication are checked, and a truck already there keeps its plan.
"""
import argparse, os, subprocess, sys

PSQL = os.environ.get("ZB_PSQL", "/opt/homebrew/opt/postgresql@18/bin/psql")
PUB = os.environ.get("BRIDGE_CDC_PUBLICATION", "my_pub")

# (id, name, the town its depot is nearest to, lat, lng)
TRUCKS = [
    ("truck-1", "Truck 1", "Nantes", 47.2184, -1.5536),
    ("truck-2", "Truck 2", "Angers", 47.4784, -0.5632),
    ("truck-3", "Truck 3", "Saint-Nazaire", 47.2735, -2.2138),
    ("truck-4", "Truck 4", "Cholet", 47.0600, -0.8790),
    ("truck-5", "Truck 5", "La Roche-sur-Yon", 46.6705, -1.4260),
]

DDL = """
CREATE TABLE IF NOT EXISTS public.trucks (
  id          text PRIMARY KEY,                                 -- 'truck-1'
  name        text NOT NULL,
  depot       uuid NOT NULL REFERENCES public.charge_points (id),
  plan        jsonb,                                            -- the traced route, or null
  updated_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz
);
"""

ENABLE = f"""
SELECT step || ':' || status || ' ' || coalesce(detail, '')
FROM zebridge_enable('public.trucks'::regclass,
  writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at',
  public_reason => 'the depot demo''s trucks', publication => '{PUB}', dry_run => false)
WHERE status = 'ERROR';
"""

# The nearest live charge point to each town, by squared degrees (scaled for the latitude):
# good enough to pick a depot, and it needs nothing but the lat/lng columns.
SEED = "INSERT INTO public.trucks (id, name, depot)\nVALUES\n" + ",\n".join(
    f"  ('{i}', '{n}', (SELECT id FROM public.charge_points WHERE deleted_at IS NULL "
    f"ORDER BY power(lat - {lat}, 2) + power((lng - {lng}) * cos(radians({lat})), 2) LIMIT 1))"
    for i, n, _, lat, lng in TRUCKS
) + "\nON CONFLICT (id) DO NOTHING;\n"

LIST = """
SELECT t.id || '  ' || t.name || '  depot: ' || c.title || coalesce(' (' || c.town || ')', '')
       || CASE WHEN t.plan IS NULL THEN '' ELSE '  plan: ' || (t.plan ->> 'km') || ' km' END
FROM public.trucks t JOIN public.charge_points c ON c.id = t.depot
WHERE t.deleted_at IS NULL ORDER BY t.id;
"""


def pg_env() -> dict:
    """ADMIN_DATABASE_URL as libpq's variables, so the password never reaches psql's
    command line; unset, the local database."""
    url = os.environ.get("ADMIN_DATABASE_URL")
    if not url:
        return {**os.environ, "PGHOST": "127.0.0.1", "PGPORT": "5432", "PGUSER": "postgres", "PGDATABASE": "postgres"}
    from urllib.parse import urlsplit, unquote, parse_qs
    u = urlsplit(url)
    env = {**os.environ, "PGHOST": u.hostname or "", "PGPORT": str(u.port or 5432),
           "PGUSER": unquote(u.username or ""), "PGDATABASE": (u.path or "/postgres").lstrip("/") or "postgres"}
    if u.password:
        env["PGPASSWORD"] = unquote(u.password)
    if "sslmode" in (q := parse_qs(u.query)):
        env["PGSSLMODE"] = q["sslmode"][0]
    return env


def psql(sql: str) -> str:
    r = subprocess.run([PSQL, "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True, env=pg_env())
    if r.returncode != 0:
        sys.exit(r.stderr.strip())
    return r.stdout.strip()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true", help="run everything in a transaction and roll it back")
    a = ap.parse_args()

    if psql("SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = 'charge_points'") != "1":
        sys.exit("no public.charge_points: load the chargers first (examples/08-map/load_chargers.py --create)")

    # One transaction: the table, its publication and the seed land together, or not at all.
    # A refused zebridge_enable raises, so nothing half-done is left behind.
    guard = f"""
DO $$ DECLARE refusal text; BEGIN
  SELECT string_agg(r, '; ') INTO refusal FROM ({ENABLE.strip().rstrip(';')}) AS e(r);
  IF refusal IS NOT NULL THEN RAISE EXCEPTION 'zebridge_enable refused trucks: %', refusal; END IF;
END $$;"""
    script = "BEGIN;\n" + DDL + guard + "\n" + SEED + LIST + ("ROLLBACK;" if a.check else "COMMIT;")
    out = psql(script)
    print("\n".join(l for l in out.splitlines() if l.strip()))
    print("(--check: rolled back, nothing kept)" if a.check else "trucks: published — public, writable from the edge")


if __name__ == "__main__":
    main()

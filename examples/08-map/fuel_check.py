#!/usr/bin/env python3
"""Do the fuel replicas equal PostgreSQL? (NOTES §10hh)

    examples/08-map/fuel_check.py --sqlite /tmp/fuel-libzb.sqlite3 --duckdb /tmp/fuel.duckdb --sqlite /tmp/fuel-ts.sqlite3

For each replica and each of the three tables: the live row count (a replica drops
tombstoned rows, so it is compared with PostgreSQL's `deleted_at IS NULL` rows), and a
checksum that moves with every value a day's snapshot can change — stations: the sum of
ids and the automate count; prices: the sum of prices in thousandths; outages: the count
per kind. A DuckDB file is opened read-only, so the follower must have exited (`--once`).
Exit code 1 when any replica differs.
"""
import argparse, os, pathlib, sqlite3, subprocess, sys

PSQL = os.environ.get("ZB_PSQL", "/opt/homebrew/opt/postgresql@18/bin/psql")
CHECKS = {
    "fuel_stations": "SELECT count(*), coalesce(sum(id), 0), coalesce(sum(CASE WHEN automate THEN 1 ELSE 0 END), 0) FROM fuel_stations{where}",
    "fuel_prices":   "SELECT count(*), coalesce(sum(CAST(ROUND(price * 1000) AS BIGINT)), 0), count(DISTINCT station_id) FROM fuel_prices{where}",
    "fuel_outages":  "SELECT count(*), coalesce(sum(CASE WHEN kind = 'temporaire' THEN 1 ELSE 0 END), 0), coalesce(sum(CASE WHEN kind = 'definitive' THEN 1 ELSE 0 END), 0) FROM fuel_outages{where}",
}


def pg(table: str) -> tuple:
    sql = CHECKS[table].format(where=" WHERE deleted_at IS NULL").replace("CAST(ROUND(price * 1000) AS BIGINT)", "ROUND(price * 1000)::bigint")
    r = subprocess.run([PSQL, "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-d", "postgres", "-XAtq", "-F", "|", "-c", sql], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(r.stderr.strip())
    return tuple(int(float(x)) for x in r.stdout.strip().split("|"))


def replica(path: pathlib.Path, table: str) -> tuple:
    if path.suffix == ".duckdb":
        import duckdb
        con = duckdb.connect(str(path), read_only=True)
        try:
            return tuple(int(float(x)) for x in con.execute(CHECKS[table].format(where="")).fetchone())
        finally:
            con.close()
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        # SQLite keeps `automate` as 0/1 and numeric as REAL text; the same SQL reads both.
        return tuple(int(float(x)) for x in con.execute(CHECKS[table].format(where="")).fetchone())
    finally:
        con.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sqlite", action="append", type=pathlib.Path, default=[])
    ap.add_argument("--duckdb", action="append", type=pathlib.Path, default=[])
    a = ap.parse_args()
    truth = {t: pg(t) for t in CHECKS}
    bad = 0
    print(f"{'replica':34} {'table':14} {'rows':>7} {'checksum a':>12} {'checksum b':>11}  verdict")
    for t, v in truth.items():
        print(f"{'PostgreSQL (live rows)':34} {t:14} {v[0]:>7} {v[1]:>12} {v[2]:>11}")
    for path in a.sqlite + a.duckdb:
        for t, want in truth.items():
            try:
                got = replica(path, t)
            except Exception as e:
                got, verdict = ("?", "?", "?"), f"unreadable: {str(e)[:60]}"
                bad += 1
            else:
                verdict = "equal" if got == want else f"DIFFERS (want {want})"
                bad += got != want
            print(f"{path.name:34} {t:14} {got[0]:>7} {got[1]:>12} {got[2]:>11}  {verdict}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()

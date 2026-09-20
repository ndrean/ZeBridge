#!/usr/bin/env python3
"""The shared route: one row two people edit at once (NOTES §10ho).

    examples/08-map/load_routes.py --create       # the table, enabled writable, and the demo row

`routes.doc` is a jsonb MAP OF REGISTERS — `{"start": {"v": {"lat", "lng"}, "t", "w"},
"end": {...}}` — the cooperative document over plain LWW (§10cr's map-of-LWW-registers,
measured). The row itself is ordinary: `updated_at` is its version, `last_writer` its
tiebreak, `deleted_at` its tombstone, and the bridge treats it like any other row. What
makes two editors converge is app-level: each ships the union of its OWN registers merged
into the last document it saw (core.mergeRegisters, the same rule in both client
libraries), and reconciles until what it observes contains what it wrote. The row race
decides who must merge; the register race decides which value survives.

Public — every principal reads it, any principal may write it — because the demo is a
phone on libzb and a browser on zb-client-ts, two principals of two tenants, editing one
route.
"""
import argparse, os, pathlib, subprocess, sys

PSQL = os.environ.get("ZB_PSQL", "/opt/homebrew/opt/postgresql@18/bin/psql")
PUB = os.environ.get("BRIDGE_CDC_PUBLICATION", "my_pub")
ROUTE_ID = "11111111-1111-4111-8111-111111111111"   # the demo row, known to every editor

DDL = """
CREATE TABLE IF NOT EXISTS public.routes (
  id          uuid PRIMARY KEY,
  name        text NOT NULL,
  doc         jsonb NOT NULL DEFAULT '{}'::jsonb,   -- {key: {v, t, w}} — the registers
  updated_at  timestamptz NOT NULL DEFAULT now(),
  last_writer text,
  deleted_at  timestamptz
);
"""


def psql(sql: str) -> str:
    r = subprocess.run([PSQL, "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-d", "postgres", "-X", "-A", "-t", "-q",
                        "-v", "ON_ERROR_STOP=1", "-c", sql], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(r.stderr.strip())
    return r.stdout.strip()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--create", action="store_true", help="create the table, enable it writable and public, insert the demo row")
    a = ap.parse_args()
    if not a.create:
        ap.print_help(); return
    psql(DDL)
    out = psql(f"SELECT step || ':' || status || ' ' || coalesce(detail, '') FROM zebridge_enable('public.routes'::regclass, "
               f"writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', tiebreak_col => 'last_writer', "
               f"public_reason => 'a shared route, the cooperative-editing demo', publication => '{PUB}', dry_run => false) "
               f"WHERE status = 'ERROR'")
    if out:
        sys.exit(f"enable refused: {out}")
    psql(f"INSERT INTO public.routes (id, name, doc) VALUES ('{ROUTE_ID}', 'demo', '{{}}'::jsonb) ON CONFLICT (id) DO NOTHING")
    print(f"routes: table enabled (public, writable), demo row {ROUTE_ID}")
    print(psql("SELECT tbl || ' seed_epoch=' || seed_epoch FROM zebridge_catalogue WHERE tbl = 'routes'"))


if __name__ == "__main__":
    main()

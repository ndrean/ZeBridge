#!/usr/bin/env python3
"""A PostgreSQL replica (§10fd): libzb with `dbUrl` instead of a SQLite file.

  set -a && . ./.env.bridge && set +a
  scripts/scenarios/.venv/bin/python scripts/scenarios/pgreplica.py

Needs a running bridge, PostGIS on the master, and a second database on the dev
server named by ZB_REPLICA_URL (default postgres://postgres@127.0.0.1:5432/zb_replica)
with PostGIS installed. A master table with every shape the wire carries — integers,
numeric, boolean, text[] and int[][], jsonb, bytea, a PostGIS point, timestamptz — is
created and enabled here and dropped at the end; the replica's copy is left.

What is checked: the replica table is created from the descriptor's `pg` block with
PostgreSQL's own types; the seed lands natively (tags[2], matrix[2][1], the jsonb
operator, the bytea length, ST_AsText of the point, numeric with its scale); a row
inserted after the seed arrives over CDC the same way; a write from the client with
arrays, bytes and a point lands in the master and echoes back natively; the C card's
query returns typed cells (integer, boolean, the bytes marker).
"""
import base64, os, struct, sys, time, uuid
import zb
from clients import Lib

T = "pgr_t"
TENANT = "globex"
REPLICA = os.environ.get("ZB_REPLICA_URL", "postgres://postgres@127.0.0.1:5432/zb_replica")
FAILED = []


def check(msg, cond):
    (zb.ok if cond else zb.bad)(msg)
    if not cond: FAILED.append(msg)


def rq(sql):
    """A query on the REPLICA database, through psql."""
    import subprocess
    r = subprocess.run([zb.PSQL.split()[0], REPLICA, "-X", "-q", "-A", "-t", "-c", sql], capture_output=True, text=True)
    if r.returncode != 0: print(f"    replica psql: {r.stderr.strip()[:200]}")
    return r.stdout.strip()


def ewkb_point(lon, lat, srid=4326):
    return b"\x01" + struct.pack("<I", 0x20000001) + struct.pack("<I", srid) + struct.pack("<dd", lon, lat)


async def main():
    zb.psql(f"DROP TABLE IF EXISTS public.{T}", quiet=True)
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), n int, price numeric(12,4), ok boolean, tags text[], matrix int[][], "
            f"meta jsonb, tile bytea, geom geometry(Point, 4326), note text, tenant_id text NOT NULL, inserted_at timestamptz NOT NULL DEFAULT now(), "
            f"updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)", quiet=True)
    zb.psql(f"SELECT public.zebridge_enable('public.{T}'::regclass, tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', "
            f"tombstone_col => 'deleted_at', tiebreak_col => NULL, publication => 'my_pub', dry_run => false)", quiet=True)
    zb.ok(f"{T} created and enabled on the master")
    zb.psql(f"INSERT INTO {T} (n, price, ok, tags, matrix, meta, tile, geom, note, tenant_id) VALUES "
            f"(7, 12.5000, true, ARRAY['a', 'b c'], ARRAY[[1, 2], [3, 4]], '{{\"src\": \"seed\", \"k\": 1}}', decode('00ff10', 'hex'), ST_SetSRID(ST_MakePoint(2.3522, 48.8566), 4326), 'seed', '{TENANT}')", quiet=True)
    time.sleep(2)
    rq(f"DROP TABLE IF EXISTS {T} CASCADE")

    # ── the replica: libzb on PostgreSQL ────────────────────────────────────────
    em = Lib("/tmp/unused.sqlite3", [T], "py-pgr", principal="bob", db_url=REPLICA)
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline and not em.q(f"SELECT 1 FROM {T} WHERE note = 'seed'"): em.poll(300)
    types = rq(f"SELECT string_agg(column_name || ':' || udt_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = '{T}'")
    check(f"the replica table has PostgreSQL's own types ({types})", "tags:_text" in types and "meta:jsonb" in types and "tile:bytea" in types and "geom:geometry" in types and "price:numeric" in types and "ok:bool" in types)
    r = rq(f"SELECT n, price::text, ok, tags[2], matrix[2][1], meta->>'src', length(tile), ST_AsText(geom), ST_SRID(geom) FROM {T} WHERE note = 'seed'")
    check(f"the seed landed natively: {r}", r == "7|12.5000|t|b c|3|seed|3|POINT(2.3522 48.8566)|4326")

    # ── CDC after the seed ──────────────────────────────────────────────────────
    zb.psql(f"INSERT INTO {T} (n, price, ok, tags, matrix, meta, tile, geom, note, tenant_id) VALUES "
            f"(8, 0.2500, false, ARRAY['x'], ARRAY[[5, 6]], '{{\"src\": \"cdc\"}}', decode('deadbeef', 'hex'), ST_SetSRID(ST_MakePoint(13.405, 52.52), 4326), 'cdc', '{TENANT}')", quiet=True)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not em.q(f"SELECT 1 FROM {T} WHERE note = 'cdc'"): em.poll(300)
    r = rq(f"SELECT n, price::text, ok, tags[1], matrix[1][2], meta->>'src', length(tile), ST_AsText(geom) FROM {T} WHERE note = 'cdc'")
    check(f"a row over CDC lands natively: {r}", r == "8|0.2500|f|x|6|cdc|4|POINT(13.405 52.52)")

    # ── a write from the client ─────────────────────────────────────────────────
    u = str(uuid.uuid4())
    tile = bytes(range(16))
    em.mutate(T, "insert", {"uid": u}, {"uid": u, "n": 9, "price": "3.2500", "ok": True, "tags": ["p", "q r"], "matrix": [[7, 8]], "meta": {"src": "libzb"},
                                       "tile": {"$bin": base64.b64encode(tile).decode()}, "geom": {"$bin": base64.b64encode(ewkb_point(4.9, 52.37)).decode()}, "note": "from libzb", "tenant_id": TENANT})
    fl = em.flush(3000)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and zb.psql(f"SELECT count(*) FROM {T} WHERE uid = '{u}'", quiet=True).strip() != "1": time.sleep(0.5)
    m = zb.psql(f"SELECT n, price::text, ok, tags::text, matrix::text, meta->>'src', encode(tile, 'hex'), ST_AsText(geom) FROM {T} WHERE uid = '{u}'", quiet=True).strip()
    check(f"the client's write landed in the master: {m} (verdicts {fl.get('verdicts')})", m == '9|3.2500|t|{p,"q r"}|{{7,8}}|libzb|000102030405060708090a0b0c0d0e0f|POINT(4.9 52.37)')
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        em.poll(300)
        r = rq(f"SELECT tags[2], matrix[1][2], encode(tile, 'hex'), ST_AsText(geom) FROM {T} WHERE uid = '{u}'")
        if r.startswith("q r|8|"): break
    check(f"its echo is native in the replica: {r}", r == "q r|8|000102030405060708090a0b0c0d0e0f|POINT(4.9 52.37)")

    # ── the C card's query, typed ───────────────────────────────────────────────
    rows = em.q(f"SELECT n, ok, tile, price FROM {T} WHERE note = 'seed'")
    check(f"the C card returns typed cells from PostgreSQL: {rows[0] if rows else '?'}",
          bool(rows) and rows[0][0] == 7 and rows[0][1] is True and isinstance(rows[0][2], dict) and rows[0][2].get("$bin") == base64.b64encode(bytes.fromhex("00ff10")).decode() and rows[0][3] == "12.5000")
    em.close()
    zb.psql(f"DROP TABLE public.{T}", quiet=True)
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(zb.run(main))

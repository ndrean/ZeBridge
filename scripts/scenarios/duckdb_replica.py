#!/usr/bin/env python3
"""A DuckDB replica (§10fl): libzb with `engine: duckdb` — the micro-VM worker's store.

  set -a && . ./.env.bridge && set +a
  scripts/scenarios/.venv/bin/python scripts/scenarios/duckdb_replica.py

Needs a running bridge, PostGIS and pgvector on the master, libzb built (`cd libzb
&& zig build -Doptimize=ReleaseFast`), DuckDB installed for the engine, and the
`duckdb` Python package in the venv. A master table with every shape the wire
carries — integers, numeric, boolean, text[] and int[][], jsonb, bytea, a PostGIS
point, a vector(3), timestamptz — is created and enabled here and dropped at the end.

What is checked: the replica table is created with DuckDB's types from the
descriptor's `pg` block (VARCHAR[], JSON, BLOB, FLOAT[3], DECIMAL(12,4), TIMESTAMP
WITH TIME ZONE); the seed lands natively through the appender (a list element, a
nested list element, a JSON field, the bytes' lengths, the vector's element, the
numeric with its scale); a row inserted after the seed arrives over CDC the same
way; a write from the client with arrays, bytes and a vector lands in the master and
echoes back natively; then libzb closes and DuckDB itself opens the file — the
worker's second act — for a group-by and a Parquet export that reads back whole.
"""
import base64, os, struct, sys, time, uuid
import zb
from clients import Lib

T = "dk_t"
TENANT = "globex"
DB = "/tmp/zb-duckdb.duckdb"
FAILED = []


def check(msg, cond):
    (zb.ok if cond else zb.bad)(msg)
    if not cond: FAILED.append(msg)


def ewkb_point(lon, lat, srid=4326):
    return b"\x01" + struct.pack("<I", 0x20000001) + struct.pack("<I", srid) + struct.pack("<dd", lon, lat)


def b64(b): return {"$bin": base64.b64encode(b).decode()}


async def main():
    zb.psql(f"DROP TABLE IF EXISTS public.{T}", quiet=True)
    zb.forget_table(T)  # the schema key too — a DROP after the probe bridge exits tombstones nobody
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), n int, price numeric(12,4), ok boolean, tags text[], matrix int[][], "
            f"meta jsonb, tile bytea, geom geometry(Point, 4326), emb vector(3), note text, tenant_id text NOT NULL, inserted_at timestamptz NOT NULL DEFAULT now(), "
            f"updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)", quiet=True)
    zb.psql(f"SELECT public.zebridge_enable('public.{T}'::regclass, tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', "
            f"tombstone_col => 'deleted_at', tiebreak_col => NULL, publication => 'my_pub', dry_run => false)", quiet=True)
    zb.ok(f"{T} created and enabled on the master")
    zb.psql(f"INSERT INTO {T} (n, price, ok, tags, matrix, meta, tile, geom, emb, note, tenant_id) VALUES "
            f"(7, 12.5000, true, ARRAY['a', 'b c'], ARRAY[[1, 2], [3, 4]], '{{\"src\": \"seed\", \"k\": 1}}', decode('00ff10', 'hex'), ST_SetSRID(ST_MakePoint(2.3522, 48.8566), 4326), '[1,2,3]', 'seed', '{TENANT}')", quiet=True)
    time.sleep(2)
    for f in (DB, DB + ".wal"):
        if os.path.exists(f): os.remove(f)

    # ── the replica: libzb on DuckDB ────────────────────────────────────────────
    em = Lib(DB, [T], "py-duckdb", principal="bob", engine="duckdb")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline and not em.q(f"SELECT 1 FROM {T} WHERE note = 'seed'"): em.poll(300)
    types = {r[0]: r[1] for r in em.q(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{T}'")}
    check(f"the replica table has DuckDB's types ({types})",
          types.get("tags") == "VARCHAR[]" and types.get("meta") == "JSON" and types.get("tile") == "BLOB" and types.get("geom") == "BLOB"
          and types.get("emb") == "FLOAT[3]" and types.get("price") == "DECIMAL(12,4)" and types.get("ok") == "BOOLEAN" and types.get("updated_at") == "TIMESTAMP WITH TIME ZONE")
    r = em.q(f"SELECT n, price, ok, tags[2], matrix[2][1], json_extract_string(meta, '$.src'), octet_length(tile), octet_length(geom), emb[3] FROM {T} WHERE note = 'seed'")
    r = r[0] if r else None
    check(f"the seed landed natively through the appender: {r}", r == [7, "12.5000", True, "b c", 3, "seed", 3, 25, 3.0])

    # ── CDC after the seed ──────────────────────────────────────────────────────
    zb.psql(f"INSERT INTO {T} (n, price, ok, tags, matrix, meta, tile, geom, emb, note, tenant_id) VALUES "
            f"(8, 0.2500, false, ARRAY['x'], ARRAY[[5, 6]], '{{\"src\": \"cdc\"}}', decode('deadbeef', 'hex'), ST_SetSRID(ST_MakePoint(13.405, 52.52), 4326), '[4,5,6]', 'cdc', '{TENANT}')", quiet=True)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not em.q(f"SELECT 1 FROM {T} WHERE note = 'cdc'"): em.poll(300)
    r = em.q(f"SELECT n, price, ok, tags[1], matrix[1][2], json_extract_string(meta, '$.src'), octet_length(tile), emb[1] FROM {T} WHERE note = 'cdc'")
    r = r[0] if r else None
    check(f"a row over CDC lands natively: {r}", r == [8, "0.2500", False, "x", 6, "cdc", 4, 4.0])

    # ── a write from the client ─────────────────────────────────────────────────
    u = str(uuid.uuid4())
    tile = bytes(range(16))
    em.mutate(T, "insert", {"uid": u}, {"uid": u, "n": 9, "price": "3.2500", "ok": True, "tags": ["p", "q r"], "matrix": [[7, 8]], "meta": {"src": "libzb"},
                                       "tile": b64(tile), "geom": b64(ewkb_point(4.9, 52.37)), "emb": b64(struct.pack("<3f", 7, 8, 9)), "note": "from libzb", "tenant_id": TENANT})
    fl = em.flush(3000)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and zb.psql(f"SELECT count(*) FROM {T} WHERE uid = '{u}'", quiet=True).strip() != "1": time.sleep(0.5)
    m = zb.psql(f"SELECT n, price::text, ok, tags::text, matrix::text, meta->>'src', encode(tile, 'hex'), ST_AsText(geom), emb::text FROM {T} WHERE uid = '{u}'", quiet=True).strip()
    check(f"the client's write landed in the master: {m} (verdicts {fl.get('verdicts')})", m == '9|3.2500|t|{p,"q r"}|{{7,8}}|libzb|000102030405060708090a0b0c0d0e0f|POINT(4.9 52.37)|[7,8,9]')
    deadline = time.monotonic() + 30
    r = None
    while time.monotonic() < deadline:
        em.poll(300)
        r = em.q(f"SELECT tags[2], matrix[1][2], octet_length(tile), emb[2] FROM {T} WHERE uid = '{u}'")
        if r and r[0][0] == "q r": break
    check(f"its echo is native in the replica: {r[0] if r else None}", bool(r) and r[0] == ["q r", 8, 16, 8.0])
    rows = em.q(f"SELECT n, ok, tile, price FROM {T} WHERE note = 'seed'")
    check(f"the C card returns typed cells from DuckDB: {rows[0] if rows else '?'}",
          bool(rows) and rows[0][0] == 7 and rows[0][1] is True and isinstance(rows[0][2], dict) and rows[0][2].get("$bin") == base64.b64encode(bytes.fromhex("00ff10")).decode() and rows[0][3] == "12.5000")
    em.close()

    # ── the worker's second act: DuckDB itself opens the file ───────────────────
    import duckdb
    con = duckdb.connect(DB, read_only=True)
    n, avg = con.execute(f"SELECT count(*), avg(n) FROM {T}").fetchone()
    check(f"DuckDB opens the replica after libzb closed it: {n} rows, avg(n) {avg}", n == 3 and abs(avg - 8.0) < 1e-9)
    pq = "/tmp/zb-duckdb-dk_t.parquet"
    if os.path.exists(pq): os.remove(pq)
    con.execute(f"COPY (SELECT * FROM {T}) TO '{pq}' (FORMAT PARQUET)")
    back = con.execute(f"SELECT count(*), sum(n) FROM read_parquet('{pq}')").fetchone()
    check(f"a Parquet export reads back whole: {back}", back == (3, 24))
    con.close()

    zb.psql(f"DROP TABLE public.{T}", quiet=True)
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(zb.run(main))

#!/usr/bin/env python3
"""Bytes end to end (§10ex): a bytea column and a PostGIS geometry column, from
PostgreSQL to a BLOB in the replica and back.

  set -a && . ./.env.bridge && set +a
  scripts/scenarios/.venv/bin/python scripts/scenarios/blobs.py

Needs a running bridge (the extension must be installed when it boots: it reads the
geometry OIDs from pg_type then) and PostGIS. The table `blob_t` is created and
enabled here, and dropped at the end.

What is checked, in order: the replica declares BLOB for both columns; a fresh libzb
seed carries every byte of a tile and a point's EWKB, exactly; a row inserted
after the seed arrives over CDC with the same bytes; a write from the client — bytes
as the `{"$bin": base64}` marker — lands in PostgreSQL as the same bytea and the same
point; the Node client sees the same bytes; the chain audit finds the objects exact.
"""
import base64, os, struct, subprocess, sys, time, uuid
import zb
from clients import Lib, Node, fresh_sqlite

T = "blob_t"
TENANT = "globex"
FAILED = []


def check(msg, cond):
    (zb.ok if cond else zb.bad)(msg)
    if not cond: FAILED.append(msg)


def b64(s):
    """PostgreSQL's base64 rendering wraps at 76 columns; the marker's does not."""
    return s.replace("\n", "")


def ewkb_point(lon, lat, srid=4326):
    """Little-endian EWKB for a 2-D point with an SRID: byte order, type|SRID flag, srid, x, y."""
    return b"\x01" + struct.pack("<I", 0x20000001) + struct.pack("<I", srid) + struct.pack("<dd", lon, lat)


def pg_bytes(where):
    r = zb.psql(f"SELECT replace(encode(tile, 'base64'), E'\\n', ''), replace(encode(ST_AsEWKB(geom), 'base64'), E'\\n', ''), note FROM {T} WHERE {where}", quiet=True).strip().split("|")
    return b64(r[0]), b64(r[1]), r[2]


async def main():
    if "postgis" not in zb.psql("SELECT extname FROM pg_extension", quiet=True):
        zb.bad("PostGIS is not installed (CREATE EXTENSION postgis)"); return 1
    zb.psql(f"DROP TABLE IF EXISTS public.{T}", quiet=True)
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), tile bytea, geom geometry(Point, 4326), note text, "
            f"tenant_id text NOT NULL, inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)", quiet=True)
    en = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM public.zebridge_enable('public.{T}'::regclass, tenant_col => 'tenant_id', writable => true, "
                 f"version_col => 'updated_at', tombstone_col => 'deleted_at', tiebreak_col => NULL, publication => 'my_pub', dry_run => false)", quiet=True)
    zb.ok(f"{T} created and enabled ({'error' not in en and 'refused' not in en})")
    # Three rows: a 1,500-byte tile of a repeating pattern, every byte value once, an empty
    # tile. Sized for the change-feed budget: the width guard measures the TEXT form of a row
    # (a bytea counts twice) against 2^BASE_BUF, 4 KB on the dev bridge — a real 300 KB tile
    # was refused at INSERT. Tiles that size need BASE_BUF 19 or more.
    zb.psql(f"INSERT INTO {T} (tile, geom, note, tenant_id) VALUES "
            f"(decode(repeat('00ff10fe7f80', 250), 'hex'), ST_SetSRID(ST_MakePoint(2.3522, 48.8566), 4326), 'paris 300k', '{TENANT}'), "
            f"((SELECT decode(string_agg(lpad(to_hex(i), 2, '0'), ''), 'hex') FROM generate_series(0, 255) i), ST_SetSRID(ST_MakePoint(-0.1276, 51.5072), 4326), 'london 256', '{TENANT}'), "
            f"(''::bytea, NULL, 'empty', '{TENANT}')", quiet=True)
    time.sleep(2)

    # ── a fresh libzb replica seeds it ──────────────────────────────────────────
    db = "/tmp/zb-blobs.sqlite3"; fresh_sqlite(db)
    em = Lib(db, [T], "py-blobs", principal="bob")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and em.q(f"SELECT count(*) FROM {T}")[0][0] < 3: em.poll(300)
    types = {r[0]: r[1] for r in em.q(f"SELECT name, type FROM pragma_table_info('{T}')")}
    check(f"the replica declares BLOB for tile and geom (tile {types.get('tile')}, geom {types.get('geom')})", types.get("tile") == "BLOB" and types.get("geom") == "BLOB")
    rows = em.q(f"SELECT note, tile, geom, typeof(tile), typeof(geom), length(tile) FROM {T} ORDER BY note")
    got = {r[0]: r for r in rows}
    ok = True
    for note in ("paris 300k", "london 256", "empty"):
        want_tile, want_geom, _ = pg_bytes(f"note = '{note}'")
        r = got[note]
        tile_b64 = r[1]["$bin"] if isinstance(r[1], dict) else None
        geom_b64 = r[2]["$bin"] if isinstance(r[2], dict) else (None if r[2] is None else "?")
        same = tile_b64 == want_tile and (geom_b64 == want_geom or (r[2] is None and want_geom == ""))
        ok &= same and r[3] == "blob"
        if not same: print(f"    {note}: tile {str(tile_b64)[:40]}… vs PG {want_tile[:40]}…; geom {geom_b64} vs {want_geom}")
    check(f"seeded bytes are PostgreSQL's bytes: 1,500-byte tile, the 256 byte values, an empty tile, two EWKB points (typeof blob, {got['paris 300k'][5]} bytes)", ok and got["paris 300k"][5] == 1500)

    # ── CDC after the seed ──────────────────────────────────────────────────────
    zb.psql(f"INSERT INTO {T} (tile, geom, note, tenant_id) VALUES (decode('deadbeef00', 'hex'), ST_SetSRID(ST_MakePoint(13.405, 52.52), 4326), 'berlin cdc', '{TENANT}')", quiet=True)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not em.q(f"SELECT 1 FROM {T} WHERE note = 'berlin cdc'"): em.poll(300)
    r = em.q(f"SELECT tile, geom FROM {T} WHERE note = 'berlin cdc'")
    want_tile, want_geom, _ = pg_bytes("note = 'berlin cdc'")
    check("a row inserted after the seed arrives over CDC with the same bytes (bin on the wire)", bool(r) and r[0][0].get("$bin") == want_tile and r[0][1].get("$bin") == want_geom)

    # ── a write from the client: the marker in, bytea and geometry out ──────────
    uid = str(uuid.uuid4())
    tile = bytes(range(256)) * 4 + b"\x00\x00"
    geom = ewkb_point(4.9041, 52.3676)
    # The key names the row for the outbox and the local apply; the VALUES are the row
    # PostgreSQL inserts — the key column must be in both, or the column default wins.
    v = em.mutate(T, "insert", {"uid": uid}, {"uid": uid, "tile": {"$bin": base64.b64encode(tile).decode()}, "geom": {"$bin": base64.b64encode(geom).decode()}, "note": "amsterdam from libzb", "tenant_id": TENANT})
    fl = em.flush(3000)
    print(f"    mutate → {v}; flush → {fl}")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and zb.psql(f"SELECT count(*) FROM {T} WHERE uid = '{uid}'", quiet=True).strip() != "1": time.sleep(0.5)
    pg = zb.psql(f"SELECT replace(encode(tile, 'base64'), E'\\n', ''), ST_AsText(geom), ST_SRID(geom) FROM {T} WHERE uid = '{uid}'", quiet=True).strip().split("|")
    if len(pg) != 3:
        by_note = zb.psql(f"SELECT uid, note, tenant_id, length(tile), ST_AsText(geom) FROM {T} WHERE note LIKE 'amsterdam%'", quiet=True).strip()
        print(f"    PostgreSQL answered {pg!r}; rows by note: {by_note!r}")
    check(f"the client's bytes landed in PostgreSQL: bytea equal, geometry {pg[1] if len(pg) > 1 else '?'} SRID {pg[2] if len(pg) > 2 else '?'} (verdicts {fl.get('verdicts')})",
             len(pg) == 3 and b64(pg[0]) == base64.b64encode(tile).decode() and pg[1] == "POINT(4.9041 52.3676)" and pg[2] == "4326")
    # and the echo: the replica holds the same bytes the server does, as blobs
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        em.poll(300)
        r = em.q(f"SELECT tile, geom, typeof(tile) FROM {T} WHERE uid = ?", [uid])
        if r and r[0][2] == "blob" and r[0][0].get("$bin") == base64.b64encode(tile).decode(): break
    check("the CDC echo of the client's own write is byte-identical in the replica", bool(r) and r[0][0].get("$bin") == base64.b64encode(tile).decode() and r[0][1].get("$bin") == base64.b64encode(geom).decode())

    # ── the Node client ─────────────────────────────────────────────────────────
    ndb = "/tmp/zb-blobs-node.sqlite3"; fresh_sqlite(ndb)
    nd = Node(ndb, log="/tmp/zb_blobs_node.log", principal="mary")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and (nd.q(f"SELECT count(*) FROM {T}") or [[0]])[0][0] < 5: time.sleep(1)
    nr = nd.q(f"SELECT tile, geom FROM {T} WHERE note = 'paris 300k'")
    want_tile, want_geom, _ = pg_bytes("note = 'paris 300k'")
    ntile = nr[0][0] if nr else None
    node_ok = isinstance(ntile, dict) and ntile.get("$bin") == want_tile and nr[0][1].get("$bin") == want_geom
    check(f"the Node client holds the same bytes ({'marker' if isinstance(ntile, dict) else type(ntile).__name__})", node_ok)
    nd.close(); em.close()

    # ── the objects themselves ──────────────────────────────────────────────────
    r = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "chain_audit.py"), "--tenant", TENANT, "--table", T, "--no-replay"], capture_output=True, text=True)
    check("the chain audit finds the full and deltas exact (bytea as \\x hex, geometry as EWKB hex)", "✓ the chain mirrors PostgreSQL" in r.stdout)
    if "✓ the chain mirrors PostgreSQL" not in r.stdout: print(r.stdout[-1500:])
    zb.psql(f"DROP TABLE public.{T}", quiet=True)
    fresh_sqlite(db); fresh_sqlite(ndb)
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(zb.run(main))

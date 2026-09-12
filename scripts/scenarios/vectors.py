#!/usr/bin/env python3
"""pgvector and bit(n) on the wire (§10fg): the bridge normalises them to BLOBs
sqlite-vec reads as they are, and renders a client's BLOB back for PostgreSQL.

  set -a && . ./.env.bridge && set +a
  scripts/scenarios/.venv/bin/python scripts/scenarios/vectors.py

Needs a running bridge, pgvector on the master and on the zb_replica database
(ZB_REPLICA_URL, default postgres://postgres@127.0.0.1:5432/zb_replica), and sqlite-vec
in the scenario venv. A table with a vector(3), a halfvec(3), a sparsevec(5), a bit(8),
a bit(3) and a varbit(12) is created and enabled here and dropped at the end.

What is checked: the descriptor maps the vector types and bit(n) to BLOB and varbit to
TEXT; in a libzb SQLite replica the seed's vector is 12 bytes of little-endian float32
that sqlite-vec's vec_distance_L2 and vec_to_json read with no conversion, the halfvec
is float16s, the sparsevec is the dim/nnz/indices/values shape, bit(8) and bit(3) are
their packed bytes (vec_bit reads them), varbit is '0101' text; a row over CDC arrives
the same way; a client's write of all six as bytes lands in the master in pgvector's
own text form, bit(3) trimmed to three bits; the audit finds the chain exact; a
PostgreSQL replica (libzb dbUrl) holds pgvector's own types, seeded through COPY and
fed over CDC.
"""
import base64, os, struct, sqlite3, subprocess, sys, time, uuid
import zb
from clients import Lib, fresh_sqlite

T = "vec_t"
TENANT = "globex"
REPLICA = os.environ.get("ZB_REPLICA_URL", "postgres://postgres@127.0.0.1:5432/zb_replica")
FAILED = []


def check(msg, cond):
    (zb.ok if cond else zb.bad)(msg)
    if not cond: FAILED.append(msg)


def rq(sql):
    r = subprocess.run([zb.PSQL.split()[0], REPLICA, "-X", "-q", "-A", "-t", "-c", sql], capture_output=True, text=True)
    if r.returncode != 0: print(f"    replica psql: {r.stderr.strip()[:200]}")
    return r.stdout.strip()


def b64(b): return {"$bin": base64.b64encode(b).decode()}
def unb64(cell): return base64.b64decode(cell["$bin"]) if isinstance(cell, dict) and "$bin" in cell else None


def sparsevec(dim, pairs):
    """The wire shape: u32 dim, u32 nnz, nnz u32 0-based indices, nnz float32s, little-endian."""
    idx = [i for i, _ in pairs]; vals = [v for _, v in pairs]
    return struct.pack("<II", dim, len(pairs)) + struct.pack(f"<{len(idx)}I", *idx) + struct.pack(f"<{len(vals)}f", *vals)


async def main():
    zb.psql(f"DROP TABLE IF EXISTS public.{T}", quiet=True)
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), emb vector(3), half halfvec(3), sv sparsevec(5), bits bit(8), b3 bit(3), vb varbit(12), "
            f"note text, tenant_id text NOT NULL, inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)", quiet=True)
    en = zb.psql(f"SELECT step || ': ' || detail FROM public.zebridge_enable('public.{T}'::regclass, tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', "
                 f"tombstone_col => 'deleted_at', tiebreak_col => NULL, publication => 'my_pub', dry_run => false)", quiet=True)
    check(f"{T} enabled on the master (pgvector types, bit, varbit)", "publication" in en and "columns:" not in en)
    zb.psql(f"INSERT INTO {T} (emb, half, sv, bits, b3, vb, note, tenant_id) VALUES "
            f"('[1,2,3]', '[0.5,-1,1.5]', '{{2:1.5,5:-0.25}}/5', B'10100101', B'101', B'1010', 'seed', '{TENANT}')", quiet=True)
    time.sleep(3)

    import json
    desc = json.loads(zb.kv_get("schemas", T) or "{}")
    stypes = {c["name"]: c["type"] for c in desc.get("sqlite", {}).get("columns", [])}
    ptypes = {c["name"]: c["type"] for c in desc.get("pg", {}).get("columns", [])}
    check(f"the descriptor maps vector/halfvec/sparsevec/bit(n) to BLOB and varbit to TEXT: {stypes}",
          all(stypes.get(c) == "BLOB" for c in ("emb", "half", "sv", "bits", "b3")) and stypes.get("vb") == "TEXT")
    check(f"the pg block keeps pgvector's own types: {ptypes}", ptypes.get("emb") == "vector(3)" and ptypes.get("sv") == "sparsevec(5)" and ptypes.get("b3") == "bit(3)")

    # ── a SQLite replica, read by sqlite-vec as is ──────────────────────────────
    db = "/tmp/zb-vectors.sqlite3"; fresh_sqlite(db)
    em = Lib(db, [T], "py-vectors", principal="bob")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and not em.q(f"SELECT 1 FROM {T} WHERE note = 'seed'"): em.poll(300)
    row = em.q(f"SELECT emb, half, sv, bits, b3, vb FROM {T} WHERE note = 'seed'")
    r = row[0] if row else [None] * 6
    emb, half, sv, bits, b3 = (unb64(c) for c in r[:5]); vb = r[5]
    check(f"vector: 12 bytes of little-endian float32 ({emb.hex() if emb else None})", emb is not None and struct.unpack("<3f", emb) == (1.0, 2.0, 3.0))
    check("halfvec: little-endian float16s", half is not None and struct.unpack("<3e", half) == (0.5, -1.0, 1.5))
    check(f"sparsevec: dim, nnz, 0-based indices, values ({sv.hex() if sv else None})", sv is not None and sv == sparsevec(5, [(1, 1.5), (4, -0.25)]))
    check("bit(8) is its packed byte, bit(3) padded to a byte, varbit its text", bits == b"\xa5" and b3 == b"\xa0" and vb == "1010")
    import sqlite_vec
    ro = sqlite3.connect(f"file:{db}?mode=ro", uri=True); ro.enable_load_extension(True); sqlite_vec.load(ro); ro.enable_load_extension(False)
    vj, d0, d1, ham = ro.execute(f"SELECT vec_to_json(emb), vec_distance_L2(emb, '[1,2,3]'), vec_distance_L2(emb, '[1,2,4]'), vec_distance_hamming(vec_bit(bits), vec_bit(X'a4')) FROM {T} WHERE note = 'seed'").fetchone()
    check(f"sqlite-vec reads the BLOBs with no conversion: vec_to_json {vj}, L2 to [1,2,3] {d0}, to [1,2,4] {d1}, hamming {ham}", vj == "[1.000000,2.000000,3.000000]" and d0 == 0.0 and d1 == 1.0 and ham == 1.0)
    ro.close()

    # ── CDC after the seed ──────────────────────────────────────────────────────
    zb.psql(f"INSERT INTO {T} (emb, half, sv, bits, b3, vb, note, tenant_id) VALUES ('[4,5,6]', '[2,3,4]', '{{1:0.5}}/5', B'11110000', B'110', B'1', 'cdc', '{TENANT}')", quiet=True)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not em.q(f"SELECT 1 FROM {T} WHERE note = 'cdc'"): em.poll(300)
    row = em.q(f"SELECT emb, sv, bits, b3, vb FROM {T} WHERE note = 'cdc'")
    r = row[0] if row else [None] * 5
    check("a row over CDC lands in the same shapes", r[0] is not None and struct.unpack("<3f", unb64(r[0])) == (4.0, 5.0, 6.0) and unb64(r[1]) == sparsevec(5, [(0, 0.5)]) and unb64(r[2]) == b"\xf0" and unb64(r[3]) == b"\xc0" and r[4] == "1")

    # ── a client's write: bytes in, pgvector's text form in the master ──────────
    u = str(uuid.uuid4())
    em.mutate(T, "insert", {"uid": u}, {"uid": u, "emb": b64(struct.pack("<3f", 7, 8, 9)), "half": b64(struct.pack("<3e", 0.25, 0.5, 0.75)), "sv": b64(sparsevec(5, [(2, 2.5)])),
                                       "bits": b64(b"\x0f"), "b3": b64(b"\xe0"), "vb": "11", "note": "from libzb", "tenant_id": TENANT})
    fl = em.flush(3000)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and zb.psql(f"SELECT count(*) FROM {T} WHERE uid = '{u}'", quiet=True).strip() != "1": time.sleep(0.5)
    m = zb.psql(f"SELECT emb::text, half::text, sv::text, bits::text, b3::text, vb::text FROM {T} WHERE uid = '{u}'", quiet=True).strip()
    check(f"the client's write landed in the master in pgvector's text form, bit(3) trimmed: {m} (verdicts {fl.get('verdicts')})", m == "[7,8,9]|[0.25,0.5,0.75]|{3:2.5}/5|00001111|111|11")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        em.poll(300)
        row = em.q(f"SELECT emb, b3 FROM {T} WHERE uid = '{u}'")
        if row and unb64(row[0][0]) == struct.pack("<3f", 7, 8, 9): break
    check("its echo is the same BLOB in the replica", bool(row) and unb64(row[0][0]) == struct.pack("<3f", 7, 8, 9) and unb64(row[0][1]) == b"\xe0")
    em.close()

    # ── the audit: the chain mirrors PostgreSQL, vectors compared in pgvector's text ──
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and not zb.kv_get("generations", f"{TENANT}.{T}"): time.sleep(2)
    a = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "chain_audit.py"), "--tenant", TENANT, "--table", T, "--pub", "my_pub"], capture_output=True, text=True)
    check("the audit finds the chain exact, replay included", "✓ the chain mirrors PostgreSQL" in a.stdout)
    if "✓ the chain mirrors PostgreSQL" not in a.stdout: print(a.stdout[-1200:])

    # ── a PostgreSQL replica: pgvector's own types, seeded through COPY, fed over CDC ──
    rq("CREATE EXTENSION IF NOT EXISTS vector")
    rq(f"DROP TABLE IF EXISTS {T} CASCADE")
    pg = Lib("/tmp/unused.sqlite3", [T], "py-vectors-pg", principal="bob", db_url=REPLICA)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and not pg.q(f"SELECT 1 FROM {T} WHERE uid = '{u}'"): pg.poll(300)
    types = rq(f"SELECT string_agg(column_name || ':' || udt_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = '{T}'")
    check(f"the replica table has pgvector's own types ({types})", "emb:vector" in types and "half:halfvec" in types and "sv:sparsevec" in types and "bits:bit" in types and "vb:varbit" in types)
    r = rq(f"SELECT emb::text, half::text, sv::text, bits::text, b3::text, vb::text FROM {T} WHERE note = 'seed'")
    check(f"the seed landed natively through COPY: {r}", r == "[1,2,3]|[0.5,-1,1.5]|{2:1.5,5:-0.25}/5|10100101|101|1010")
    r = rq(f"SELECT emb::text, b3::text FROM {T} WHERE uid = '{u}'")
    check(f"and the client's row: {r}", r == "[7,8,9]|111")
    zb.psql(f"INSERT INTO {T} (emb, half, sv, bits, b3, vb, note, tenant_id) VALUES ('[0.1,0.2,0.3]', '[1,1,1]', '{{5:9}}/5', B'00000001', B'001', B'0', 'cdc2', '{TENANT}')", quiet=True)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not pg.q(f"SELECT 1 FROM {T} WHERE note = 'cdc2'"): pg.poll(300)
    r = rq(f"SELECT emb::text, half::text, sv::text, bits::text, b3::text, vb::text FROM {T} WHERE note = 'cdc2'")
    check(f"a row over CDC lands natively in the PostgreSQL replica: {r}", r == "[0.1,0.2,0.3]|[1,1,1]|{5:9}/5|00000001|001|0")
    pg.close()

    zb.psql(f"DROP TABLE public.{T}", quiet=True)
    fresh_sqlite(db)
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(zb.run(main))

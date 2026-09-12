#!/usr/bin/env python3
"""Arrays as JSON text on the wire (§10ey), read by both engines.

  set -a && . ./.env.bridge && set +a
  scripts/scenarios/.venv/bin/python scripts/scenarios/arrays.py

Needs a running bridge. A table with text[], int[][], bool[] and numeric[] columns is
created and enabled here, dropped at the end.

What is checked: a SQLite replica (libzb, tenant globex) holds each array as JSON text
that SQLite's own `json_extract` / `json_array_length` read, on the seed and over CDC;
a PostgreSQL-engine replica (the Node worker on PGlite, tenant acme) holds native
arrays — `tags[2]`, `matrix[2][1]` — from the same wire; a write with arrays from
each client lands in PostgreSQL as native arrays and echoes back in each engine's
own shape; the chain audit finds the objects exact.
"""
import os, sys, time, uuid
import zb
from clients import Lib, Node, fresh_sqlite

T = "arr_t"
FAILED = []


def check(msg, cond):
    (zb.ok if cond else zb.bad)(msg)
    if not cond: FAILED.append(msg)


def insert(note, tenant):
    zb.psql(f"INSERT INTO {T} (tags, matrix, flags, prices, note, tenant_id) VALUES "
            f"(ARRAY['a', 'b c', NULL], ARRAY[[1, 2], [3, 4]], ARRAY[true, false], ARRAY[1.50, 2]::numeric[], '{note}', '{tenant}')", quiet=True)


async def main():
    zb.psql(f"DROP TABLE IF EXISTS public.{T}", quiet=True)
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), tags text[], matrix int[][], flags bool[], prices numeric[], note text, "
            f"tenant_id text NOT NULL, inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)", quiet=True)
    en = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM public.zebridge_enable('public.{T}'::regclass, tenant_col => 'tenant_id', writable => true, "
                 f"version_col => 'updated_at', tombstone_col => 'deleted_at', tiebreak_col => NULL, publication => 'my_pub', dry_run => false)", quiet=True)
    zb.ok(f"{T} created and enabled")
    insert("seed globex", "globex"); insert("seed acme", "acme")
    time.sleep(2)

    # ── SQLite: libzb on globex ─────────────────────────────────────────────────
    db = "/tmp/zb-arrays.sqlite3"; fresh_sqlite(db)
    em = Lib(db, [T], "py-arrays", principal="bob")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and not em.q(f"SELECT 1 FROM {T} WHERE note = 'seed globex'"): em.poll(300)
    r = em.q(f"SELECT tags, json_extract(tags, '$[1]'), json_array_length(matrix), json_extract(matrix, '$[1][0]'), flags, prices, json_type(tags) FROM {T} WHERE note = 'seed globex'")
    check(f"SQLite holds JSON text SQLite reads: tags {r[0][0] if r else '?'}, $[1] = {r[0][1] if r else '?'}, matrix length {r[0][2] if r else '?'}, $[1][0] = {r[0][3] if r else '?'}, flags {r[0][4] if r else '?'}, prices {r[0][5] if r else '?'}",
          bool(r) and r[0][0] == '["a","b c",null]' and r[0][1] == "b c" and r[0][2] == 2 and r[0][3] == 3 and r[0][4] == "[true,false]" and r[0][5] == '["1.50","2"]' and r[0][6] == "array")
    insert("cdc globex", "globex")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not em.q(f"SELECT 1 FROM {T} WHERE note = 'cdc globex'"): em.poll(300)
    r = em.q(f"SELECT json_extract(tags, '$[0]'), json_extract(matrix, '$[0][1]') FROM {T} WHERE note = 'cdc globex'")
    check("a row inserted after the seed arrives over CDC as JSON text", bool(r) and r[0][0] == "a" and r[0][1] == 2)
    # a write with arrays from the SQLite client: real arrays in the payload
    u = str(uuid.uuid4())
    em.mutate(T, "insert", {"uid": u}, {"uid": u, "tags": ["p", "q r"], "matrix": [[5, 6]], "flags": [False], "prices": ["3.25"], "note": "from libzb", "tenant_id": "globex"})
    fl = em.flush(3000)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and zb.psql(f"SELECT count(*) FROM {T} WHERE uid = '{u}'", quiet=True).strip() != "1": time.sleep(0.5)
    pg = zb.psql(f"SELECT tags::text, matrix::text, flags::text, prices::text FROM {T} WHERE uid = '{u}'", quiet=True).strip()
    check(f"the SQLite client's arrays landed in PostgreSQL as native arrays: {pg} (verdicts {fl.get('verdicts')})", pg == '{p,"q r"}|{{5,6}}|{f}|{3.25}')
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        em.poll(300)
        r = em.q(f"SELECT tags, json_extract(tags, '$[1]') FROM {T} WHERE uid = ?", [u])
        if r and r[0][0] == '["p","q r"]': break
    check("its echo is the same JSON text the optimistic write stored (no phantom difference)", bool(r) and r[0][0] == '["p","q r"]' and r[0][1] == "q r")
    em.close()

    # ── PostgreSQL engine: the Node worker on PGlite, tenant acme ──────────────
    os.environ["ZB_ENGINE"] = "pglite"
    ndir = "/tmp/zb-arrays-pglite"
    os.system(f"rm -rf {ndir}")
    nd = Node(ndir, log="/tmp/zb_arrays_node.log", principal="alice")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline and not (nd.q(f"SELECT 1 FROM {T} WHERE note = 'seed acme'") or []): time.sleep(1)
    r = nd.q(f"SELECT tags[2], matrix[2][1], flags[1], prices[1]::text, array_length(tags, 1) FROM {T} WHERE note = 'seed acme'")
    check(f"PGlite holds native arrays from the same wire: tags[2] = {r[0][0] if r else '?'}, matrix[2][1] = {r[0][1] if r else '?'}, flags[1] = {r[0][2] if r else '?'}, prices[1] = {r[0][3] if r else '?'}",
          bool(r) and r[0][0] == "b c" and r[0][1] == 3 and r[0][2] is True and r[0][3] == "1.50" and r[0][4] == 3)
    insert("cdc acme", "acme")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not (nd.q(f"SELECT 1 FROM {T} WHERE note = 'cdc acme'") or []): time.sleep(0.5)
    r = nd.q(f"SELECT tags[1], matrix[1][2] FROM {T} WHERE note = 'cdc acme'")
    check("a CDC row arrives in PGlite as a native array (JSON text converted on apply)", bool(r) and r[0][0] == "a" and r[0][1] == 2)
    u2 = str(uuid.uuid4())
    nd.mutate(T, "insert", {"uid": u2}, {"uid": u2, "tags": ["x", "y z"], "matrix": [[7, 8]], "flags": [True], "prices": ["4.5"], "note": "from pglite", "tenant_id": "acme"})
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and zb.psql(f"SELECT count(*) FROM {T} WHERE uid = '{u2}'", quiet=True).strip() != "1": time.sleep(0.5)
    pg = zb.psql(f"SELECT tags::text, matrix::text FROM {T} WHERE uid = '{u2}'", quiet=True).strip()
    r = nd.q(f"SELECT tags[2], matrix[1][2] FROM {T} WHERE uid = '{u2}'")
    check(f"the PGlite client's arrays landed in PostgreSQL ({pg}) and its own replica reads them natively", pg == '{x,"y z"}|{{7,8}}' and bool(r) and r[0][0] == "y z" and r[0][1] == 8)
    nd.close()
    os.environ.pop("ZB_ENGINE", None)

    # ── the objects ─────────────────────────────────────────────────────────────
    import subprocess
    a = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "chain_audit.py"), "--tenant", "globex", "--table", T, "--no-replay"], capture_output=True, text=True)
    check("the chain audit finds the objects exact (arrays against PostgreSQL's to_json)", "✓ the chain mirrors PostgreSQL" in a.stdout)
    if "✓ the chain mirrors PostgreSQL" not in a.stdout: print(a.stdout[-1200:])
    zb.psql(f"DROP TABLE public.{T}", quiet=True)
    fresh_sqlite(db); os.system(f"rm -rf {ndir}")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(zb.run(main))

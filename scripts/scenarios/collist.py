#!/usr/bin/env python3
"""Publication column lists (§10ff): a tsvector never travels, and a column the
DBA leaves out does not either — the descriptor, the chain, CDC and the audit agree.

  set -a && . ./.env.bridge && set +a
  scripts/scenarios/.venv/bin/python scripts/scenarios/collist.py

Needs a running bridge. A table with a body, a STORED tsvector over it, a column
the caller leaves out (`columns => …`), and the usual tenancy columns is created
and enabled here and dropped at the end.

What is checked: zebridge_enable reports the tsvector left out and the publication's
column list lacks both; the descriptor in KV names neither; a libzb replica has
neither column, and seeds the body; a row over CDC arrives without them; a write of
the body from the client lands and the master recomputes its tsvector; the audit
finds the chain exact over the published columns; re-running zebridge_enable after
ADD COLUMN refreshes the list. A stray second publication naming the table with a
narrower list (uid, tenant_id, secret) exists throughout — another bridge's, or a leftover —
and must not shrink this bridge's descriptor: the filters key on the bridge's
publication, not on every publication that names the table.
"""
import json, sys, time, uuid
import zb
from clients import Lib, fresh_sqlite

T = "col_t"
TENANT = "globex"
FAILED = []


def check(msg, cond):
    (zb.ok if cond else zb.bad)(msg)
    if not cond: FAILED.append(msg)


def attnames():
    return zb.psql(f"SELECT array_to_string(attnames, ',') FROM pg_publication_tables WHERE pubname = 'my_pub' AND tablename = '{T}'", quiet=True).strip()


async def main():
    zb.psql(f"DROP TABLE IF EXISTS public.{T}", quiet=True)
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), body text, fts tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(body, ''))) STORED, "
            f"secret text, tenant_id text NOT NULL, inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)", quiet=True)
    zb.psql("DROP PUBLICATION IF EXISTS col_stray", quiet=True)
    # (uid, tenant_id, secret): a column list must cover the replica identity, which
    # zebridge_enable sets to the (key, tenant) index, or PostgreSQL refuses every UPDATE.
    zb.psql(f"CREATE PUBLICATION col_stray FOR TABLE public.{T} (uid, tenant_id, secret)", quiet=True)
    en = zb.psql(f"SELECT step || ': ' || detail FROM public.zebridge_enable('public.{T}'::regclass, tenant_col => 'tenant_id', writable => true, "
                 f"columns => ARRAY['uid', 'body', 'fts', 'tenant_id', 'inserted_at', 'updated_at', 'deleted_at']::name[], "
                 f"version_col => 'updated_at', tombstone_col => 'deleted_at', tiebreak_col => NULL, publication => 'my_pub', dry_run => false)", quiet=True)
    check(f"zebridge_enable left the tsvector out: {[l for l in en.splitlines() if l.startswith('columns')]}", "columns: 1 column(s) left out" in en and "fts" in en)
    names = attnames()
    check(f"the publication's column list has neither fts nor secret: {names}", names == "uid,body,tenant_id,inserted_at,updated_at,deleted_at")
    zb.psql(f"INSERT INTO {T} (body, secret, tenant_id) VALUES ('the quick brown fox', 'hush', '{TENANT}')", quiet=True)
    time.sleep(3)

    desc = json.loads(zb.kv_get("schemas", T) or "{}")
    dcols = [c["name"] for c in desc.get("sqlite", {}).get("columns", [])]
    check(f"the descriptor names the bridge's publication's columns only, the stray publication's narrower list ignored: {dcols}", dcols == ["uid", "body", "tenant_id", "inserted_at", "updated_at", "deleted_at"])

    db = "/tmp/zb-collist.sqlite3"; fresh_sqlite(db)
    em = Lib(db, [T], "py-collist", principal="bob")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and not em.q(f"SELECT 1 FROM {T}"): em.poll(300)
    rcols = em.cols(T)
    check(f"the replica has neither column: {rcols}", "fts" not in rcols and "secret" not in rcols and "body" in rcols)
    r = em.q(f"SELECT body FROM {T}")
    check("the seed carried the body", bool(r) and r[0][0] == "the quick brown fox")
    zb.psql(f"INSERT INTO {T} (body, secret, tenant_id) VALUES ('lazy dog', 'hush 2', '{TENANT}')", quiet=True)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not em.q(f"SELECT 1 FROM {T} WHERE body = 'lazy dog'"): em.poll(300)
    check("a row over CDC arrives with the published columns", bool(em.q(f"SELECT 1 FROM {T} WHERE body = 'lazy dog'")))
    u = str(uuid.uuid4())
    em.mutate(T, "insert", {"uid": u}, {"uid": u, "body": "jumps quickly", "tenant_id": TENANT})
    fl = em.flush(3000)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and zb.psql(f"SELECT count(*) FROM {T} WHERE uid = '{u}'", quiet=True).strip() != "1": time.sleep(0.5)
    m = zb.psql(f"SELECT body, fts::text FROM {T} WHERE uid = '{u}'", quiet=True).strip()
    check(f"the client's write landed and the master computed its tsvector: {m} (verdicts {fl.get('verdicts')})", m == "jumps quickly|'jump':1 'quick':2")
    em.close()

    # The chain comes at the producer's next tick — wait for its manifest before the audit.
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and not zb.kv_get("generations", f"{TENANT}.{T}"): time.sleep(2)
    import os, subprocess
    a = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "chain_audit.py"), "--tenant", TENANT, "--table", T, "--pub", "my_pub", "--no-replay"], capture_output=True, text=True)
    check("the audit finds the chain exact over the published columns", "✓ the chain mirrors PostgreSQL" in a.stdout)
    if "✓ the chain mirrors PostgreSQL" not in a.stdout: print(a.stdout[-800:])

    # a column added later joins the list when zebridge_enable runs again
    zb.psql(f"ALTER TABLE {T} ADD COLUMN extra int", quiet=True)
    before = attnames()
    en2 = zb.psql(f"SELECT step || ': ' || detail FROM public.zebridge_enable('public.{T}'::regclass, tenant_col => 'tenant_id', writable => true, "
                  f"version_col => 'updated_at', tombstone_col => 'deleted_at', tiebreak_col => NULL, publication => 'my_pub', dry_run => false)", quiet=True)
    after = attnames()
    check(f"ADD COLUMN, then zebridge_enable again: the list refreshed ({before} → {after})", "extra" not in before and "extra" in after and "fts" not in after)
    zb.psql(f"DROP TABLE public.{T}", quiet=True)
    zb.psql("DROP PUBLICATION col_stray", quiet=True)
    fresh_sqlite(db)
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(zb.run(main))

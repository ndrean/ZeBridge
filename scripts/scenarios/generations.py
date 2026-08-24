#!/usr/bin/env python3
"""`zebridge_generations` — the generation producer's memory, and its whole contract.

NOTES.md §1.13 (delta generations): the producer's state lives in PostgreSQL because
"the bridge never reads its own output back". This table is that state, and its
contract is more than a shape:

  1. the shape itself — PK (tenant, tbl, gen), `cutoff_lsn` a real `pg_lsn`;
  2. **invisible to clients** — not published, and its DDL swallowed by
     `zebridge_is_internal_table`: no `zebridge_ddl_events` row, no `$KV.schemas` key.
     A client must never build a local replica of producer bookkeeping;
  3. **the reader can run the build recipe** — LSN read BEFORE the REPEATABLE READ
     snapshot, content query and bookkeeping INSERT in that same transaction, on the
     read role's single deliberate write grant;
  4. **append-only by privilege** — the reader holds INSERT+DELETE (pruning) but no
     UPDATE: history cannot be rewritten, only extended and pruned;
  5. **the writer holds nothing** — generation bookkeeping is not ingress.

Usage:  python scripts/scenarios/generations.py
Needs POSTGRES_BRIDGE_* / POSTGRES_WRITER_* creds in the env (source .env.admin).
Cleans up its rows via the DELETE grant it is testing.
"""

import os
import subprocess
import sys

import zb

PGBIN = "/opt/homebrew/opt/postgresql@18/bin/psql"
HOST = os.environ.get("ZB_PG_HOST", "127.0.0.1")
PORT = os.environ.get("ZB_PG_PORT", "5432")


def as_role(user_env, pw_env, sql):
    user = os.environ.get(user_env)
    pw = os.environ.get(pw_env)
    if not (user and pw):
        sys.exit(f"{user_env}/{pw_env} not set — source .env.admin")
    return subprocess.run([PGBIN, "-h", HOST, "-p", PORT, "-U", user, "-d", "postgres",
                           "-tA", "-v", "ON_ERROR_STOP=1"],
                          input=sql, capture_output=True, text=True,
                          env={**os.environ, "PGPASSWORD": pw})


def reader(sql):
    return as_role("POSTGRES_BRIDGE_USER", "POSTGRES_BRIDGE_PASSWORD", sql)


def writer(sql):
    return as_role("POSTGRES_WRITER_USER", "POSTGRES_WRITER_PASSWORD", sql)


def main():
    failed = 0

    # ── 1. shape ───────────────────────────────────────────────────────────────
    pk = zb.psql("SELECT string_agg(a.attname, ',' ORDER BY k.ord) FROM pg_index i "
                 "JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true "
                 "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum "
                 "WHERE i.indrelid='public.zebridge_generations'::regclass AND i.indisprimary").strip()
    lsn_type = zb.psql("SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
                       "WHERE attrelid='public.zebridge_generations'::regclass AND attname='cutoff_lsn'").strip()
    if pk == "tenant,tbl,gen" and lsn_type == "pg_lsn":
        zb.ok("shape: PRIMARY KEY (tenant, tbl, gen), cutoff_lsn is a real pg_lsn")
    else:
        zb.bad(f"shape drifted: pk=({pk}), cutoff_lsn={lsn_type}")
        failed += 1

    # ── 2. invisible to clients ────────────────────────────────────────────────
    ddl = zb.psql("SELECT count(*) FROM zebridge_ddl_events WHERE table_name='zebridge_generations'").strip()
    pub = zb.psql("SELECT count(*) FROM pg_publication_tables WHERE tablename='zebridge_generations'").strip()
    kv = zb.nats_cli("kv", "get", zb.TOPOLOGY["kv"]["schemas"], "zebridge_generations", "--raw")
    if ddl == "0" and pub == "0" and kv.returncode != 0:
        zb.ok("invisible to clients: no DDL event, not published, no $KV.schemas key")
    else:
        zb.bad(f"leaked: ddl_events={ddl}, published={pub}, kv={'present' if kv.returncode == 0 else 'absent'}")
        failed += 1

    try:
        # ── 3. the reader runs the build recipe, LSN-before-snapshot ──────────
        r = reader("""
SELECT pg_current_wal_lsn() AS l \\gset
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM public.users;
INSERT INTO public.zebridge_generations (tenant, tbl, gen, cutoff_version, cutoff_lsn)
VALUES ('_default', 'users', 1, now(), :'l');
COMMIT;
SELECT gen || '@' || cutoff_lsn FROM public.zebridge_generations WHERE tenant='_default' AND tbl='users';
""")
        if r.returncode == 0 and "1@" in r.stdout:
            zb.ok(f"the reader built generation 1 with the LSN-before-snapshot recipe ({r.stdout.strip().splitlines()[-1]})")
        else:
            zb.bad(f"recipe failed as bridge_reader: {r.stderr.strip()[:160]}")
            failed += 1

        # ── PK: the chain cannot fork ──────────────────────────────────────────
        r = reader("INSERT INTO public.zebridge_generations (tenant, tbl, gen, cutoff_version, cutoff_lsn) "
                   "VALUES ('_default', 'users', 1, now(), pg_current_wal_lsn());")
        if r.returncode != 0 and "duplicate key" in r.stderr:
            zb.ok("a second generation 1 is refused by the primary key — the chain cannot fork")
        else:
            zb.bad("duplicate (tenant, tbl, gen) was accepted")
            failed += 1

        # ── 4. append-only by privilege ────────────────────────────────────────
        r = reader("UPDATE public.zebridge_generations SET cutoff_version = now() WHERE gen = 1;")
        if r.returncode != 0 and "permission denied" in r.stderr:
            zb.ok("the reader cannot UPDATE — history is append-only by privilege, not convention")
        else:
            zb.bad(f"reader UPDATE was not refused: {r.stderr.strip()[:120]}")
            failed += 1

        # ── 5. the writer holds nothing here ───────────────────────────────────
        r = writer("INSERT INTO public.zebridge_generations (tenant, tbl, gen, cutoff_version, cutoff_lsn) "
                   "VALUES ('_default', 'users', 99, now(), pg_current_wal_lsn());")
        if r.returncode != 0 and "permission denied" in r.stderr:
            zb.ok("bridge_writer is refused entirely — generation bookkeeping is not ingress")
        else:
            zb.bad(f"writer INSERT was not refused: {r.stderr.strip()[:120]}")
            failed += 1
    finally:
        # cleanup through the very grant that pruning will use
        r = reader("DELETE FROM public.zebridge_generations WHERE tenant='_default' AND tbl='users';")
        if r.returncode == 0:
            zb.ok("cleanup via the reader's DELETE grant — the pruning path works")
        else:
            zb.bad(f"pruning DELETE failed: {r.stderr.strip()[:120]}")
            failed += 1

    return 1 if failed else 0


sys.exit(main())

#!/usr/bin/env python3
"""The database refuses a client write to a bridge-owned table, whatever the bridge let through.

A client's write reaches PostgreSQL as the bridge's writer role with `zb.principal` set to the client
(`set_config(…, true)`, transaction-local). The sweeper sets it to `zb_sweeper` for its session. Nothing
else the bridge does to its own tables (enrollment, the producer, revoke, the DDL trigger) sets it, and a
DBA's psql does not. So a write to `zebridge_*` under any other principal is a client's, and the init SQL
puts a statement-level trigger on every such table that refuses it.

Until then the only guard in front of `zebridge_gc_watermark` (and the other bridge tables) was
`mutation_listener.isForbiddenTable()`: with it disabled a client moved the GC floor (`writable.py` step 4).
This is the layer behind it, in the database.

On a scratch database with both init templates applied (no bridge, no NATS):

  1. every `zebridge_*` table in `public` refuses INSERT, UPDATE, DELETE and TRUNCATE under a client principal
     (statement-level, so it needs no rows), with SQLSTATE 42501 and the table named;
  2. the same four statements pass with no principal (a DBA, enrollment, the producer);
  3. and pass as `zb_sweeper`;
  4. a table that is not the bridge's is untouched by the rule (a user table accepts a client principal);
  5. the receipts schema `zebridge_private` is left out on purpose: the receipts are written inside the
     client's own transaction, with the principal set.

Needs `envsubst`, psql, the template variables and ADMIN_DATABASE_URL (default the local postgres superuser).
"""
import importlib.util
import os
import re
import subprocess
import sys

import zb

ADMIN_URL = os.environ.get("ADMIN_DATABASE_URL", "postgres://postgres@127.0.0.1:5432/postgres")
SCRATCH = "zb_bridge_table_guard_scratch"

_derive = zb.ROOT / "scripts" / "zb-derive-env.py"
_spec = importlib.util.spec_from_file_location("zb_derive_env", _derive)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
_mod.derive_into(os.environ)


def url(db: str | None = None) -> str:
    return ADMIN_URL if db is None else re.sub(r"/[^/]+$", f"/{db}", ADMIN_URL)


def sql(text: str, db: str | None = SCRATCH, stop: bool = True) -> subprocess.CompletedProcess:
    cmd = ["psql", url(db), "-tA", "-F", "|"] + (["-v", "ON_ERROR_STOP=1"] if stop else [])
    return subprocess.run(cmd + ["-c", text], capture_output=True, text=True)


# One DO block per principal: each bridge table, each of the four statements, in a sub-block that catches
# the error. `refused` counts the 42501s that name the table; `other` lists anything else (a statement that
# was not refused, or failed for another reason), which is what the test reports.
PROBE = """
DO $probe$
DECLARE
    t text; col text; stmt text; refused int := 0; passed int := 0; floor_guard int := 0; other text := '';
    stmts text[];
BEGIN
    PERFORM set_config('zb.principal', %(principal)s, false);
    FOR t, col IN
        SELECT c.relname, (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum LIMIT 1)
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname LIKE 'zebridge\\_%%' ORDER BY 1
    LOOP
        stmts := ARRAY[
            format('INSERT INTO public.%%I SELECT * FROM public.%%I WHERE false', t, t),
            format('UPDATE public.%%I SET %%I = %%I WHERE false', t, col, col),
            format('DELETE FROM public.%%I WHERE false', t),
            format('TRUNCATE public.%%I CASCADE', t)];
        FOREACH stmt IN ARRAY stmts LOOP
            BEGIN
                EXECUTE stmt;
                passed := passed + 1;
                RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'rollback the probe';
            EXCEPTION
                WHEN SQLSTATE '42501' THEN
                    IF SQLERRM LIKE '%%' || t || '%%' THEN refused := refused + 1;
                    ELSE other := other || format('[%%s: 42501 without the table name: %%s]', t, SQLERRM); END IF;
                WHEN SQLSTATE '23514' THEN
                    -- the GC floor's own guard: DELETE and TRUNCATE are refused for EVERYONE, a DBA too
                    IF t = 'zebridge_gc_watermark' AND SQLERRM LIKE '%%never removed%%' THEN floor_guard := floor_guard + 1;
                    ELSE other := other || format('[%%s: 23514 %%s]', t, SQLERRM); END IF;
                WHEN SQLSTATE 'P0001' THEN NULL;
                WHEN OTHERS THEN other := other || format('[%%s: %%s %%s]', left(stmt, 30), SQLSTATE, SQLERRM);
            END;
        END LOOP;
    END LOOP;
    RAISE NOTICE 'RESULT refused=%% passed=%% floor=%% other=%%', refused, passed, floor_guard, other;
END
$probe$;
"""


def probe(principal: str) -> dict:
    lit = "''" if principal == "" else "'" + principal + "'"
    r = sql(PROBE % {"principal": lit}, stop=False)
    m = re.search(r"RESULT refused=(\d+) passed=(\d+) floor=(\d+) other=(.*)", r.stderr)
    if not m:
        return {"error": (r.stderr or r.stdout).strip()[-400:]}
    return {"refused": int(m.group(1)), "passed": int(m.group(2)), "floor": int(m.group(3)), "other": m.group(4).strip()}


async def main():
    failed = 0

    def check(label: str, cond: bool, detail: str = ""):
        nonlocal failed
        if cond:
            zb.ok(label)
        else:
            zb.bad(f"{label}\n      {detail}")
            failed += 1

    sql(f"DROP DATABASE IF EXISTS {SCRATCH}", db=None, stop=False)
    if sql(f"CREATE DATABASE {SCRATCH}", db=None).returncode != 0:
        zb.bad(f"could not create {SCRATCH} — is ADMIN_DATABASE_URL right?")
        return 1
    try:
        env = dict(os.environ, TARGET_DB=SCRATCH)
        for template in ("init.core.template.sql", "init.write.template.sql"):
            rendered = subprocess.run(["envsubst"], stdin=open(zb.ROOT / template),
                                      capture_output=True, text=True, env=env).stdout
            r = subprocess.run(["psql", url(SCRATCH), "-v", "ON_ERROR_STOP=1", "-q"],
                               input=rendered, capture_output=True, text=True)
            if r.returncode != 0:
                zb.bad(f"{template} did not apply: {r.stderr.strip()[-300:]}")
                return 1

        n = int(sql("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname LIKE 'zebridge\\_%'").stdout.strip() or 0)
        check(f"0. the init SQL created the bridge's tables (found {n}, expected at least 10)", n >= 10, f"found {n}")

        # 1. a client principal: every statement on every bridge table is refused
        res = probe("alice")
        check(f"1. under a client principal every INSERT, UPDATE, DELETE and TRUNCATE on all {n} bridge tables is refused (42501, table named; the GC floor's own DELETE and TRUNCATE guard answers first for that table)",
              res.get("refused") + res.get("floor") == n * 4 and res.get("floor") == 2 and res.get("passed") == 0 and not res.get("other"), str(res))

        # 2. no principal: a DBA, enrollment, the producer
        res = probe("")
        check("2. with no principal the same statements pass (nothing refused but the floor's own DELETE and TRUNCATE)",
              res.get("refused") == 0 and res.get("floor") == 2 and res.get("passed") == n * 4 - 2 and not res.get("other"), str(res))

        # 3. the sweeper
        res = probe("zb_sweeper")
        check("3. as zb_sweeper they pass too",
              res.get("refused") == 0 and res.get("floor") == 2 and res.get("passed") == n * 4 - 2 and not res.get("other"), str(res))

        # 4. a user table is not touched by the rule
        sql("CREATE TABLE public.plain_user_table (id int PRIMARY KEY, v int)")
        r = sql("SELECT set_config('zb.principal', 'alice', false); INSERT INTO public.plain_user_table VALUES (1, 1)", stop=False)
        check("4. a user table accepts a write under a client principal",
              r.returncode == 0 and "INSERT 0 1" in r.stdout, (r.stderr or r.stdout).strip()[-200:])

        # 5. the receipts schema stays writable inside a client's transaction
        r = sql("SELECT set_config('zb.principal', 'alice', false); "
                "UPDATE zebridge_private.receipt_policy SET min_accepted_ms = min_accepted_ms WHERE false", stop=False)
        check("5. zebridge_private (the receipts, written in the client's own transaction) is left out of the rule",
              r.returncode == 0, (r.stderr or r.stdout).strip()[-200:])
    finally:
        sql(f"DROP DATABASE IF EXISTS {SCRATCH}", db=None, stop=False)
    return 1 if failed else 0


zb.run(main)

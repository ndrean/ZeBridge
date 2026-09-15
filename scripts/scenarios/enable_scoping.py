#!/usr/bin/env python3
"""zebridge_enable's scoping decision — every legal way in, and the refusals (NOTES §10fx).

    scripts/scenarios/run.py offline -k enable_scoping

A table is published only once it is scoped: a tenant column (writable or read-only),
a recorded public reason, or a one-tenant publication. Until 2026-09-14 the check
accepted a tenant column only with `writable => true`, so a READ-ONLY tenant table was
refused as "published unscoped" and the branch that scopes its reads never ran — seen
once and misread as the guard working (NOTES §10cp, the salaries run). Asserted here, on
a scratch database rendered from the templates:

  1. a read-only tenant table enables: RLS on, the catalogue names the tenant column,
     the table is in the publication, the reads branch reported;
  2. the same call as a dry run changes nothing and reports no ERROR;
  3. a nullable tenant column is refused up front, with nothing applied;
  4. a table with no tenant, no public reason and no write grant is still refused;
  5. a writable tenant table still enables with its guards and write scoping;
  6. a public read-only table still enables;
  7. enable builds an index that leads with the version column (NOTES §10gc), and the
     dry run of check 2 built none;
  8. an index that already leads with the version column is reused, not duplicated;
  9. the schema a DDL publishes lists the table's other indexes but not <table>_zb_version:
     clients never read by version (§10gd);
 10. version_index => false builds no index and says the step was skipped.

Needs `envsubst`, psql, the template variables (`.env.admin`, `.env.bridge`) and
ADMIN_DATABASE_URL (default the local postgres superuser).
"""
import importlib.util
import os
import re
import subprocess
import sys

import zb

ADMIN_URL = os.environ.get("ADMIN_DATABASE_URL", "postgres://postgres@127.0.0.1:5432/postgres")
SCRATCH = "zb_enable_scoping_scratch"
PUB = "p_scoping"

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


def enable(table: str, args: str) -> list[tuple[str, str, str]]:
    r = sql(f"SELECT step, status, detail FROM public.zebridge_enable('public.{table}'::regclass, {args})", stop=False)
    rows = [tuple((line.split("|", 2) + ["", ""])[:3]) for line in r.stdout.splitlines() if line]
    if r.returncode != 0:
        rows.append(("exception", "EXCEPTION", r.stderr.strip()[-300:]))
    return rows


def published(table: str) -> bool:
    return sql(f"SELECT 1 FROM pg_publication_tables WHERE pubname = '{PUB}' AND tablename = '{table}'").stdout.strip() == "1"


def rls(table: str) -> bool:
    return sql(f"SELECT relrowsecurity FROM pg_class WHERE oid = 'public.{table}'::regclass").stdout.strip() == "t"


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
        sql(f"SELECT * FROM public.zebridge_create_publication('{PUB}')")
        sql("""
            CREATE TABLE public.ro_tenant (uid uuid PRIMARY KEY, tenant_id text NOT NULL, v int, updated_at timestamptz NOT NULL);
            CREATE TABLE public.ro_dry    (uid uuid PRIMARY KEY, tenant_id text NOT NULL, v int, updated_at timestamptz NOT NULL);
            CREATE TABLE public.ro_null   (uid uuid PRIMARY KEY, tenant_id text,          v int, updated_at timestamptz NOT NULL);
            CREATE TABLE public.ro_bare   (uid uuid PRIMARY KEY, v int, updated_at timestamptz NOT NULL);
            CREATE TABLE public.rw_tenant (uid uuid PRIMARY KEY, tenant_id text NOT NULL, v int, updated_at timestamptz NOT NULL, deleted_at timestamptz);
            CREATE TABLE public.ro_public (uid uuid PRIMARY KEY, v int, updated_at timestamptz NOT NULL);
            CREATE TABLE public.ro_indexed (uid uuid PRIMARY KEY, tenant_id text NOT NULL, v int, updated_at timestamptz NOT NULL);
            CREATE INDEX ro_indexed_mine ON public.ro_indexed (updated_at, uid);
            CREATE TABLE public.ro_optout (uid uuid PRIMARY KEY, tenant_id text NOT NULL, v int, updated_at timestamptz NOT NULL);
        """)

        # 1. read-only tenant table
        rows = enable("ro_tenant", f"tenant_col => 'tenant_id', publication => '{PUB}', dry_run => false")
        errors = [r for r in rows if r[1] in ("ERROR", "EXCEPTION")]
        cat = sql("SELECT tenant_col FROM public.zebridge_catalogue WHERE tbl = 'ro_tenant'").stdout.strip()
        reads = any(r[0] == "rls" and "scope_reads_by_tenant" in r[2] for r in rows)
        check("1. a read-only tenant table enables: RLS on, catalogue tenant_col=tenant_id, published, reads scoped",
              not errors and rls("ro_tenant") and cat == "tenant_id" and published("ro_tenant") and reads,
              f"errors={errors} rls={rls('ro_tenant')} catalogue={cat!r} published={published('ro_tenant')} reads_step={reads}")

        # 2. the same as a dry run
        rows = enable("ro_dry", f"tenant_col => 'tenant_id', publication => '{PUB}'")
        errors = [r for r in rows if r[1] in ("ERROR", "EXCEPTION")]
        check("2. the dry run reports no ERROR and changes nothing (no RLS, not published, no catalogue row)",
              not errors and not rls("ro_dry") and not published("ro_dry")
              and sql("SELECT count(*) FROM public.zebridge_catalogue WHERE tbl = 'ro_dry'").stdout.strip() == "0",
              f"errors={errors} rls={rls('ro_dry')} published={published('ro_dry')}")

        # 3. nullable tenant column
        rows = enable("ro_null", f"tenant_col => 'tenant_id', publication => '{PUB}', dry_run => false")
        check("3. a nullable tenant column is refused up front, nothing applied",
              any(r[1] == "ERROR" and "nullable" in r[2] for r in rows) and not rls("ro_null") and not published("ro_null"),
              f"rows={rows}")

        # 4. unscoped
        rows = enable("ro_bare", f"publication => '{PUB}', dry_run => false")
        check("4. no tenant, no public reason, not writable: still refused as unscoped, naming read-only tenant tables",
              any(r[1] == "ERROR" and "published unscoped" in r[2] and "read-only" in r[2] for r in rows) and not published("ro_bare"),
              f"rows={rows}")

        # 5. writable tenant table
        rows = enable("rw_tenant", f"writable => true, tenant_col => 'tenant_id', version_col => 'updated_at', "
                                   f"tombstone_col => 'deleted_at', publication => '{PUB}', dry_run => false")
        errors = [r for r in rows if r[1] in ("ERROR", "EXCEPTION")]
        steps = {r[0] for r in rows}
        check("5. a writable tenant table still enables with grants, guards and write scoping",
              not errors and {"grants", "guards", "rls"} <= steps and rls("rw_tenant") and published("rw_tenant"),
              f"errors={errors} steps={sorted(steps)}")

        # 6. public read-only table
        rows = enable("ro_public", f"public_reason => 'scenario: readable by every consumer', publication => '{PUB}', dry_run => false")
        errors = [r for r in rows if r[1] in ("ERROR", "EXCEPTION")]
        check("6. a public read-only table still enables", not errors and published("ro_public"), f"errors={errors}")

        # 7. the version index
        def version_indexes(table: str) -> list[str]:
            out = sql(f"""SELECT i.indexrelid::regclass FROM pg_index i
                           JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
                          WHERE i.indrelid = 'public.{table}'::regclass AND a.attname = 'updated_at'""").stdout
            return [l for l in out.splitlines() if l]
        check("7. enable built the version index; the dry run built none",
              version_indexes("ro_tenant") == ["ro_tenant_zb_version"] and version_indexes("rw_tenant") == ["rw_tenant_zb_version"]
              and version_indexes("ro_dry") == [],
              f"ro_tenant={version_indexes('ro_tenant')} rw_tenant={version_indexes('rw_tenant')} ro_dry={version_indexes('ro_dry')}")

        # 8. an existing index is reused
        rows = enable("ro_indexed", f"tenant_col => 'tenant_id', publication => '{PUB}', dry_run => false")
        step = [r for r in rows if r[0] == "version index"]
        check("8. an index already leading with the version column is reported and not duplicated",
              step and step[0][1] == "already" and "ro_indexed_mine" in step[0][2]
              and version_indexes("ro_indexed") == ["ro_indexed_mine"],
              f"step={step} indexes={version_indexes('ro_indexed')}")

        # 9. the schema sent to clients leaves the version index out
        sql("CREATE INDEX ro_tenant_by_v ON public.ro_tenant (v); ALTER TABLE public.ro_tenant ADD COLUMN extra int")
        names = sql("""SELECT string_agg(ix->>'name', ',' ORDER BY ix->>'name') FROM (
                          SELECT schema_def FROM public.zebridge_ddl_events WHERE table_name = 'ro_tenant'
                          ORDER BY id DESC LIMIT 1) e, jsonb_array_elements(e.schema_def->'indexes') ix""").stdout.strip()
        check("9. the published schema lists ro_tenant_by_v and not ro_tenant_zb_version",
              "ro_tenant_by_v" in names.split(",") and "ro_tenant_zb_version" not in names.split(","),
              f"indexes published: {names!r}")

        # 10. the opt-out
        rows = enable("ro_optout", f"tenant_col => 'tenant_id', version_index => false, publication => '{PUB}', dry_run => false")
        step = [r for r in rows if r[0] == "version index"]
        errors = [r for r in rows if r[1] in ("ERROR", "EXCEPTION")]
        check("10. version_index => false: enabled, step skipped, no index built",
              not errors and step and step[0][1] == "skipped" and version_indexes("ro_optout") == [] and published("ro_optout"),
              f"errors={errors} step={step} indexes={version_indexes('ro_optout')}")
    finally:
        sql(f"DROP DATABASE IF EXISTS {SCRATCH}", db=None, stop=False)
    return 1 if failed else 0


zb.run(main)

#!/usr/bin/env python3
"""The incarnation's move into zebridge_incarnations, on disposable databases: the
migration gate for a table identity that names objects for ever.

  A  fresh install: the new init SQL on an empty database — the table, the grants, a
     catalogue without the old columns, a first enable at incarnation 1 with no re-seed.
  B  upgrade from the layout before (commit 531cc88: the incarnation on the catalogue):
     a history built there (one table re-created: incarnation 2, epoch 1; one not) keeps
     its identities, OIDs and epochs through the new init SQL; the old columns go.
  C  re-applying the init SQL changes no identity and no epoch.
  D  removing a catalogue entry and enabling the table again cannot reset its identity:
     the incarnation goes on from where it was (so object names never repeat), whether
     the table was re-created before the new enable or not.
  E  grants and wiring: the reader may read the table, the reader and the writer may call
     zebridge_observe_incarnation and nobody else may; the catalogue still rides the
     publication after its columns were dropped.

Needs the shared PostgreSQL (127.0.0.1:5432, superuser postgres), git (the old templates)
and a built bridge (--init-sql renders the new ones). No NATS, no bridge process.

  scripts/scenarios/.venv/bin/python scripts/scenarios/incarnation_migration.py
"""
import os, re, subprocess, sys, uuid
import zb
from hub_fallback_stage2 import BRIDGE

OLD = "531cc88"   # the last layout with the incarnation on zebridge_catalogue
PUB = "im_pub"
PSQL = [zb._psql_binary(), "-X", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-Atq"]
failed = 0


def check(label, cond):
    global failed
    (zb.ok if cond else zb.bad)(label)
    failed += not cond
    return cond


def sql(db, statement):
    r = subprocess.run([*PSQL, "-d", db], input=statement, capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"{db}: {r.stderr[-1500:]}")
    return r.stdout.strip()


class Db:
    def __init__(self, label):
        self.name = f"zb_im_{label}_{uuid.uuid4().hex[:6]}"
        self.r, self.w = self.name + "_r", self.name + "_w"
        sql("postgres", f'CREATE DATABASE "{self.name}"')

    def drop(self):
        subprocess.run([*PSQL, "-d", "postgres"], capture_output=True, text=True,
                       input=f'DROP DATABASE IF EXISTS "{self.name}" WITH (FORCE); DROP ROLE IF EXISTS "{self.r}"; DROP ROLE IF EXISTS "{self.w}";')

    def env(self):
        return dict(os.environ, DATABASE_READER_URL=f"postgres://{self.r}:x@127.0.0.1:5432/{self.name}",
                    DATABASE_WRITER_URL=f"postgres://{self.w}:x@127.0.0.1:5432/{self.name}", TARGET_DB=self.name,
                    BRIDGE_CDC_PUBLICATION=PUB)

    def init_new(self):
        rendered = subprocess.run([str(BRIDGE), "--init-sql"], env=self.env(), capture_output=True, text=True, check=True).stdout
        sql(self.name, rendered)

    def init_old(self):
        """The core and write templates as they were at OLD, filled as --init-sql fills them."""
        values = {"POSTGRES_READER_USER": self.r, "POSTGRES_READER_PASSWORD": "x", "POSTGRES_WRITER_USER": self.w,
                  "POSTGRES_WRITER_PASSWORD": "x", "TARGET_DB": self.name, "OPEN_TENANT": zb.TOPOLOGY["open_tenant"],
                  "LEAF_PREFIX": "lf-", "RECEIPT_COMPLETION_GUARD": "true"}
        text = ""
        for f in ("init.core.template.sql", "init.write.template.sql"):
            text += subprocess.run(["git", "show", f"{OLD}:{f}"], cwd=zb.ROOT, capture_output=True, text=True, check=True).stdout + "\n"
        missing = sorted(set(re.findall(r"\$\{([A-Z_]+)\}", text)) - set(values))
        if missing:
            raise RuntimeError(f"old templates use unknown placeholders: {missing}")
        sql(self.name, re.sub(r"\$\{([A-Z_]+)\}", lambda m: values[m.group(1)], text))
        # Its publication as the bridge of that day made it: the old SQL did not.
        sql(self.name, f"SELECT count(*) FROM zebridge_create_publication('{PUB}');")

    def table(self, t):
        sql(self.name, f"CREATE TABLE public.{t} (id text PRIMARY KEY, updated_at timestamptz NOT NULL DEFAULT now());")

    def enable(self, t):
        sql(self.name, f"SELECT count(*) FROM zebridge_enable('public.{t}'::regclass, public_reason => 'migration test', "
                       f"publication => '{PUB}', dry_run => false);")

    def recreate(self, t):
        sql(self.name, f"DROP TABLE public.{t};")
        self.table(t)

    def oid(self, t):
        return sql(self.name, f"SELECT coalesce(to_regclass('public.{t}')::oid::text, '')")

    def ident(self, t, where="zebridge_incarnations"):
        """(incarnation, recorded OID, seed epoch) — the identity read from `where`."""
        r = sql(self.name, f"SELECT x.incarnation, coalesce(x.incarnation_relid::text, ''), c.seed_epoch "
                           f"FROM public.zebridge_catalogue c LEFT JOIN public.{where} x ON x.tbl = c.tbl WHERE c.tbl = '{t}'")
        if not r:
            return None
        inc, rel, ep = r.split("|")
        return (int(inc) if inc else None, rel, int(ep))

    def has_column(self, table, col):
        return sql(self.name, f"SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' "
                              f"AND table_name = '{table}' AND column_name = '{col}'") != "0"


def main():
    dbs = []
    try:
        # ── A: fresh install ─────────────────────────────────────────────────
        a = Db("fresh"); dbs.append(a)
        a.init_new()
        check("A fresh: zebridge_incarnations exists, the catalogue carries no incarnation column",
              sql(a.name, "SELECT to_regclass('public.zebridge_incarnations') IS NOT NULL") == "t"
              and not a.has_column("zebridge_catalogue", "incarnation") and not a.has_column("zebridge_catalogue", "incarnation_relid"))
        a.table("t1"); a.enable("t1")
        check(f"A fresh: a first enable records incarnation 1 with the table's OID and no re-seed: {a.ident('t1')}",
              a.ident("t1") == (1, a.oid("t1"), 0))
        a.recreate("t1")
        check(f"A fresh: a recreate through the DDL path is incarnation 2 and one epoch move: {a.ident('t1')}",
              a.ident("t1") == (2, a.oid("t1"), 1))
        a.enable("t1")   # as a recreate is completed: back in the publication
        check(f"A fresh: enabling the re-created table again moves nothing: {a.ident('t1')}", a.ident("t1") == (2, a.oid("t1"), 1))

        # ── E: grants and wiring (on the fresh install) ─────────────────────
        nobody = a.name + "_nobody"
        sql(a.name, f'CREATE ROLE "{nobody}";')
        try:
            priv = sql(a.name, f"SELECT has_table_privilege('{a.r}', 'public.zebridge_incarnations', 'SELECT'), "
                               f"has_function_privilege('{a.r}', 'public.zebridge_observe_incarnation(text, oid)', 'EXECUTE'), "
                               f"has_function_privilege('{a.w}', 'public.zebridge_observe_incarnation(text, oid)', 'EXECUTE'), "
                               f"has_function_privilege('{nobody}', 'public.zebridge_observe_incarnation(text, oid)', 'EXECUTE'), "
                               f"has_table_privilege('{nobody}', 'public.zebridge_incarnations', 'UPDATE')")
            check(f"E grants: reader reads the table, reader and writer call the function, another role neither: {priv}",
                  priv == "t|t|t|f|f")
        finally:
            sql(a.name, f'DROP ROLE "{nobody}";')
        rides = sql(a.name, f"SELECT string_agg(tablename, ',' ORDER BY tablename) FROM pg_publication_tables WHERE pubname = '{PUB}' "
                            f"AND tablename IN ('zebridge_catalogue', 'zebridge_ddl_events', 't1')")
        check(f"E wiring: the catalogue, the DDL events and the table ride the publication: {rides}",
              rides == "t1,zebridge_catalogue,zebridge_ddl_events")

        # ── B: upgrade from the layout before ───────────────────────────────
        b = Db("upgrade"); dbs.append(b)
        b.init_old()
        check(f"B before ({OLD}): the catalogue carries the incarnation",
              b.has_column("zebridge_catalogue", "incarnation") and sql(b.name, "SELECT to_regclass('public.zebridge_incarnations') IS NULL") == "t")
        for t in ("t1", "t2"):
            b.table(t); b.enable(t)
        b.recreate("t1")
        before = {t: b.ident(t, "zebridge_catalogue") for t in ("t1", "t2")}
        check(f"B before: t1 re-created (incarnation 2, epoch 1), t2 not (1, 0): {before}",
              before["t1"][0] == 2 and before["t1"][2] == 1 and before["t2"][0] == 1 and before["t2"][2] == 0)
        b.init_new()
        after = {t: b.ident(t) for t in ("t1", "t2")}
        check(f"B after: identities, OIDs and epochs kept through the move: {before} -> {after}", after == before)
        check("B after: the old catalogue columns are gone",
              not b.has_column("zebridge_catalogue", "incarnation") and not b.has_column("zebridge_catalogue", "incarnation_relid"))
        status = sql(b.name, f"SELECT status FROM zebridge_observe_incarnation('t1', '{b.oid('t1')}'::oid)")
        check(f"B after: the live OID observes as current, nothing moves: {status}, {b.ident('t1')}",
              status == "current" and b.ident("t1") == after["t1"])
        rides = sql(b.name, f"SELECT count(*) FROM pg_publication_tables WHERE pubname = '{PUB}' AND tablename = 'zebridge_catalogue'")
        check(f"B after: the catalogue still rides the publication once its columns were dropped ({rides})", rides == "1")

        # ── C: re-applying ───────────────────────────────────────────────────
        b.init_new()
        again = {t: b.ident(t) for t in ("t1", "t2")}
        check(f"C reapply: no identity or epoch moves: {after} -> {again}", again == after)

        # ── D: remove the catalogue entry, enable again ─────────────────────
        prev = b.ident("t1")
        sql(b.name, "DELETE FROM public.zebridge_catalogue WHERE tbl = 't1';")
        b.recreate("t1")          # not catalogued now: the DDL path observes nothing
        b.enable("t1")
        d1 = b.ident("t1")
        check(f"D re-enable after a recreate: the incarnation goes on ({prev[0]} -> {d1[0]}), never back to 1; the OID is the live one",
              d1[0] == prev[0] + 1 and d1[1] == b.oid("t1"))
        names = {f"t1-i{prev[0]}-g1-full", f"t1-i{d1[0]}-g1-full"}
        check(f"D object names cannot repeat: {sorted(names)}", len(names) == 2)
        prev2 = b.ident("t2")
        sql(b.name, "DELETE FROM public.zebridge_catalogue WHERE tbl = 't2';")
        b.enable("t2")            # the same table, re-catalogued
        d2 = b.ident("t2")
        check(f"D re-enable of the same table: its identity stays ({prev2[0]} -> {d2[0]}, OID kept)",
              d2[0] == prev2[0] and d2[1] == prev2[1])
    finally:
        for d in dbs:
            d.drop()
    print("PASS" if not failed else f"FAIL ({failed})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

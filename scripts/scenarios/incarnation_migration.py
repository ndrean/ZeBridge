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
     the incarnation goes on from where it was (so object names never repeat), and the
     seed epoch never comes back to a value a replica already holds — above it after a
     recreate, equal without one; a lowered epoch is raised back to its floor.
  E  grants and wiring: the reader may read the table, the reader and the writer may call
     zebridge_observe_incarnation and nobody else may; the catalogue still rides the
     publication after its columns were dropped.
  F  infrastructure: zebridge_incarnations is an internal table, and its creation enters
     no DDL event — on the upgrade too, where the DDL capture is already installed.
  G  concurrent recreation observation and reseed/upsert: catalogue precedes identity,
     both operations finish, and the new incarnation and both epoch moves survive.
  H  an insert waiting behind a catalogue deletion preserves the newly committed floor.

Needs the shared PostgreSQL (127.0.0.1:5432, superuser postgres), git (the old templates)
and a built bridge (--init-sql renders the new ones). No NATS, no bridge process.

  scripts/scenarios/.venv/bin/python scripts/scenarios/incarnation_migration.py
"""
import os, re, subprocess, sys, time, uuid
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

    def floor(self, t):
        return int(sql(self.name, f"SELECT seed_epoch FROM public.zebridge_incarnations WHERE tbl = '{t}'") or -1)

    def internal(self):
        """(zebridge_is_internal_table says so, DDL events recorded for it)."""
        return (sql(self.name, "SELECT public.zebridge_is_internal_table('zebridge_incarnations')") == "t",
                int(sql(self.name, "SELECT count(*) FROM public.zebridge_ddl_events WHERE table_name = 'zebridge_incarnations'")))

    def has_column(self, table, col):
        return sql(self.name, f"SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' "
                              f"AND table_name = '{table}' AND column_name = '{col}'") != "0"


def concurrency(db, upsert=False):
    """Hold the identity row; prove the observer owns the catalogue before waiting.

    A reseed or conflicting INSERT must wait on the observer, not take the catalogue
    and wait on the identity holder. This lock graph deterministically rejects the old
    order, without depending on PostgreSQL's choice of deadlock victim.
    """
    tag = "upsert" if upsert else "reseed"
    names = {k: f"im_{tag}_{k}_{uuid.uuid4().hex[:6]}" for k in ("holder", "observer", "writer")}
    children = []

    def start(which, statement, interactive=False):
        p = subprocess.Popen([*PSQL, "-d", db.name], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True)
        children.append(p)
        p.stdin.write(f"SET application_name = '{names[which]}'; SET statement_timeout = '15s'; " + statement + "\n")
        p.stdin.flush()
        if not interactive:
            p.stdin.close()
            p.stdin = None
        return p

    def wait_for(query):
        end = time.monotonic() + 8
        while time.monotonic() < end:
            if sql(db.name, query) == "t":
                return True
            time.sleep(0.05)
        return False

    try:
        # Restore-style recreation: the producer's observer, not DDL capture, must
        # advance the identity and epoch while the catalogue writer competes with it.
        sql(db.name, "ALTER EVENT TRIGGER zebridge_ddl_trigger DISABLE; ALTER EVENT TRIGGER zebridge_drop_trigger DISABLE;")
        db.recreate("t1")
        sql(db.name, "ALTER EVENT TRIGGER zebridge_ddl_trigger ENABLE; ALTER EVENT TRIGGER zebridge_drop_trigger ENABLE; "
                    f"ALTER PUBLICATION {PUB} ADD TABLE public.t1;")
        before = db.ident("t1")
        holder = start("holder", "BEGIN; SELECT 1 FROM zebridge_incarnations WHERE tbl = 't1' FOR UPDATE;", True)
        if not wait_for(f"SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = '{names['holder']}' AND state = 'idle in transaction')"):
            raise RuntimeError("identity lock barrier not reached")
        observer = start("observer", f"SELECT status FROM zebridge_observe_incarnation('t1', '{db.oid('t1')}'::oid);")
        if not wait_for(f"SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = '{names['observer']}' AND wait_event_type = 'Lock')"):
            raise RuntimeError("observer did not reach identity lock barrier")
        probe = subprocess.run([*PSQL, "-d", db.name], capture_output=True, text=True,
                               input="SELECT 1 FROM zebridge_catalogue WHERE tbl = 't1' FOR UPDATE NOWAIT;")
        check(f"G {tag}: observer holds catalogue while waiting for identity",
              probe.returncode != 0 and "could not obtain lock" in probe.stderr)
        statement = ("INSERT INTO zebridge_catalogue SELECT * FROM zebridge_catalogue WHERE tbl = 't1' "
                     "ON CONFLICT (tbl) DO UPDATE SET seed_epoch = zebridge_catalogue.seed_epoch + 1;" if upsert else
                     "SELECT count(*) FROM zebridge_reseed('t1');")
        writer = start("writer", statement)
        ordered = wait_for(f"SELECT EXISTS (SELECT 1 FROM pg_stat_activity w, pg_stat_activity o "
                           f"WHERE w.application_name = '{names['writer']}' AND o.application_name = '{names['observer']}' "
                           "AND o.pid = ANY(pg_blocking_pids(w.pid)))")
        check(f"G {tag}: catalogue writer waits on observer (catalogue before identity)", ordered)
        check(f"G {tag}: blocked writer has not attempted an identity write",
              sql(db.name, f"SELECT NOT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid) "
                  f"WHERE a.application_name = '{names['writer']}' AND l.relation = 'zebridge_incarnations'::regclass "
                  "AND l.mode = 'RowExclusiveLock')") == "t")
        holder.stdin.write("COMMIT;\n")
        holder.stdin.close(); holder.stdin = None
        outputs = [p.communicate(timeout=20) for p in children]
        errors = [err for p, (_, err) in zip(children, outputs) if p.returncode]
        check(f"G {tag}: both operations finish without deadlock: {errors}", not errors)
        after = db.ident("t1")
        check(f"G {tag}: one new incarnation, both epoch moves retained and floor recorded: {before} -> {after}",
              after == (before[0] + 1, db.oid("t1"), before[2] + 2) and db.floor("t1") == after[2])
    finally:
        for p in children:
            if p.poll() is None:
                p.kill()
            p.communicate()


def insert_after_delete(db):
    """The BEFORE read can precede a wait on an uncommitted catalogue deletion."""
    row = sql(db.name, "SELECT row_to_json(c)::text FROM zebridge_catalogue c WHERE tbl = 't1'")
    epoch = db.ident("t1")[2]
    tag = "im_delete_" + uuid.uuid4().hex[:6]
    holder = subprocess.Popen([*PSQL, "-d", db.name], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True)
    writer = None
    try:
        holder.stdin.write(f"SET application_name = '{tag}_holder'; BEGIN; "
                           "UPDATE zebridge_catalogue SET seed_epoch = seed_epoch + 1 WHERE tbl = 't1'; "
                           "DELETE FROM zebridge_catalogue WHERE tbl = 't1';\n")
        holder.stdin.flush()
        def wait(query):
            end = time.monotonic() + 8
            while time.monotonic() < end:
                if sql(db.name, query) == 't': return True
                time.sleep(0.05)
            return False
        if not wait(f"SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = '{tag}_holder' AND state = 'idle in transaction')"):
            raise RuntimeError("delete barrier not reached")
        writer = subprocess.Popen([*PSQL, "-d", db.name], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True)
        escaped = row.replace("'", "''")
        writer.stdin.write(f"SET application_name = '{tag}_writer'; SET statement_timeout = '15s'; "
                           f"INSERT INTO zebridge_catalogue SELECT * FROM json_populate_record(NULL::zebridge_catalogue, '{escaped}');\n")
        writer.stdin.close(); writer.stdin = None
        check("H insert waits behind the uncommitted deletion after reading the old floor",
              wait(f"SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = '{tag}_writer' AND wait_event_type = 'Lock')"))
        holder.stdin.write("COMMIT;\n"); holder.stdin.close(); holder.stdin = None
        h_out, h_err = holder.communicate(timeout=20)
        w_out, w_err = writer.communicate(timeout=20)
        check(f"H insert finishes and preserves the newly committed floor ({epoch} -> {db.floor('t1')})",
              holder.returncode == writer.returncode == 0 and db.ident("t1")[2] == db.floor("t1") == epoch + 1)
        if holder.returncode or writer.returncode:
            raise RuntimeError(h_err + w_err)
    finally:
        for p in (holder, writer):
            if p is not None:
                if p.poll() is None: p.kill()
                p.communicate()


def main():
    dbs = []
    try:
        # ── A: fresh install ─────────────────────────────────────────────────
        a = Db("fresh"); dbs.append(a)
        a.init_new()
        check("A fresh: zebridge_incarnations exists, the catalogue carries no incarnation column",
              sql(a.name, "SELECT to_regclass('public.zebridge_incarnations') IS NOT NULL") == "t"
              and not a.has_column("zebridge_catalogue", "incarnation") and not a.has_column("zebridge_catalogue", "incarnation_relid"))
        check(f"F fresh: zebridge_incarnations is internal, no DDL event for it: {a.internal()}", a.internal() == (True, 0))
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
        check(f"B after: the epoch floors start at the catalogue's epochs: t1 {b.floor('t1')}, t2 {b.floor('t2')}",
              (b.floor("t1"), b.floor("t2")) == (before["t1"][2], before["t2"][2]))
        check(f"F upgrade (DDL capture installed before): zebridge_incarnations is internal, no DDL event for it: {b.internal()}",
              b.internal() == (True, 0))
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
        check(f"C reapply: no identity or epoch moves: {after} -> {again}, floors {b.floor('t1')}, {b.floor('t2')}",
              again == after and (b.floor("t1"), b.floor("t2")) == (after["t1"][2], after["t2"][2]))

        # ── D: remove the catalogue entry, enable again ─────────────────────
        prev = b.ident("t1")
        sql(b.name, "DELETE FROM public.zebridge_catalogue WHERE tbl = 't1';")
        b.recreate("t1")          # not catalogued now: the DDL path observes nothing
        b.enable("t1")
        d1 = b.ident("t1")
        check(f"D re-enable after a recreate: the incarnation goes on ({prev[0]} -> {d1[0]}), never back to 1; the OID is the live one",
              d1[0] == prev[0] + 1 and d1[1] == b.oid("t1"))
        check(f"D re-enable after a recreate: the epoch moves above the one replicas hold ({prev[2]} -> {d1[2]}), floor {b.floor('t1')}",
              d1[2] == prev[2] + 1 and b.floor("t1") == d1[2])
        names = {f"t1-i{prev[0]}-g1-full", f"t1-i{d1[0]}-g1-full"}
        check(f"D object names cannot repeat: {sorted(names)}", len(names) == 2)
        prev2 = b.ident("t2")
        sql(b.name, "DELETE FROM public.zebridge_catalogue WHERE tbl = 't2';")
        b.enable("t2")            # the same table, re-catalogued
        d2 = b.ident("t2")
        check(f"D re-enable of the same table: its identity stays ({prev2[0]} -> {d2[0]}, OID kept)",
              d2[0] == prev2[0] and d2[1] == prev2[1])
        check(f"D re-enable of the same table: its epoch stays ({prev2[2]} -> {d2[2]})", d2[2] == prev2[2])
        sql(b.name, "UPDATE public.zebridge_catalogue SET seed_epoch = 0 WHERE tbl = 't1';")
        check(f"D a lowered epoch is raised back to its floor: {b.ident('t1')[2]} (floor {b.floor('t1')})",
              b.ident("t1")[2] == d1[2] == b.floor("t1"))
        # The two catalogue entry paths must obey the same lock order.
        concurrency(a)
        concurrency(a, upsert=True)
        insert_after_delete(a)
    finally:
        for d in dbs:
            d.drop()
    print("PASS" if not failed else f"FAIL ({failed})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

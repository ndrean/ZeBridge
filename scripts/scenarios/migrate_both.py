"""Every migration shape, under BOTH clients at once (NOTES §10dg, MIGRATIONS.md).

A libzb client (Python host, polling) and a zb-client-ts client (Node, durable,
schema watch) follow the same two tables while PostgreSQL migrates them — no client
restart, no bridge restart. After each shape the two replicas are asked what they
hold and compared with PostgreSQL:

  1. ADD COLUMN … DEFAULT 'x'     the constant default fills the OLD rows locally (§10df)
  2. RENAME COLUMN                 the value survives (the trigger's rename hint, §1.2)
  3. DROP COLUMN                   gone, the row survives
  4. ADD COLUMN … DEFAULT now()    the silent divergence, then zebridge_reseed() closes it
  5. the re-key (bigserial → uuid) parent rebuilt EMPTY + child re-typed, both re-seeded
                                   through the seed epoch the DDL trigger bumps (§10dg)
  6. DROP TABLE                    the tombstone drops both replicas' tables
"""
import sys, time, uuid
import zb
from clients import Lib, Node, both, fresh_sqlite

LOG = "/tmp/zb_migrate_both_bridge.log"
PUB = zb.publication()
P, C = "mig_parent", "mig_child"
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")


def teardown():
    for sql in (f"DROP TABLE IF EXISTS public.{C}", f"DROP TABLE IF EXISTS public.{P}",
                f"DELETE FROM public.zebridge_catalogue WHERE tbl IN ('{P}','{C}')",
                # the producer sweeps a departed table's chain on its next tick (§10dg);
                # here too, so a run never depends on the previous run's bridge having ticked
                f"DELETE FROM public.zebridge_generations WHERE tbl IN ('{P}','{C}')"):
        zb.psql(sql, quiet=True)


def main():
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")
    failed = 0
    def check(label, cond):
        nonlocal failed
        label = time.strftime("%H:%M:%S ") + label
        if cond: zb.ok(label)
        else: zb.bad(label); failed += 1
        sys.stdout.flush()

    teardown()
    zb.psql(f"CREATE TABLE public.{P} (id bigserial PRIMARY KEY, label text, {COMMON})")
    zb.psql(f"CREATE TABLE public.{C} (id bigserial PRIMARY KEY, parent_id bigint NOT NULL REFERENCES public.{P}(id), note text, {COMMON})")
    py = nd = None
    for db in ("/tmp/zb-migrate-both-py.sqlite3", "/tmp/zb-migrate-both-node.sqlite3"):
        fresh_sqlite(db)
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            for t in (P, C):
                out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{t}', {ENABLE})")
                check(f"zebridge_enable({t}) live: {out.strip()[:60]}…", "error" not in out.lower())
            py = Lib("/tmp/zb-migrate-both-py.sqlite3", [P, C], "py-migrate-both")
            nd = Node("/tmp/zb-migrate-both-node.sqlite3", "/tmp/zb_migrate_both_node.log")
            tenant = py.tenant
            check(f"both clients up, tenant {tenant} / {nd.tenant}", nd.tenant == tenant)
            zb.psql(f"INSERT INTO {P} (label, tenant_id) SELECT 'p' || g, '{tenant}' FROM generate_series(1,3) g")
            zb.psql(f"INSERT INTO {C} (parent_id, note, tenant_id) SELECT p.id, 'c' || p.id, '{tenant}' FROM {P} p")
            # 150 s: the Node client's connect waits 90 s for any followed table without a
            # chain (the dev database's suspended probe tables — CLIENTS.md divergence 3).
            dt = both(py, nd, lambda c: c.q(f"SELECT count(*) FROM {P}")[0][0] == 3 and c.q(f"SELECT count(*) FROM {C}")[0][0] == 3, 150)
            counts = lambda c: f"{c.q(f'SELECT count(*) FROM {P}')[0][0]}/{c.q(f'SELECT count(*) FROM {C}')[0][0]}"
            held = f"inbox py {py.q('SELECT count(*) FROM _zbz_inbox')[0][0]}, node {nd.q('SELECT count(*) FROM _zebridge_inbox')[0][0]}"
            check(f"§0 both replicas hold 3 parents + 3 children ({dt} s; parents/children py {counts(py)}, node {counts(nd)}; {held})", dt is not None)

            # ── 1. ADD COLUMN with a constant default ──
            zb.psql(f"ALTER TABLE {P} ADD COLUMN kind text NOT NULL DEFAULT 'plain'")
            dt = both(py, nd, lambda c: "kind" in c.cols(P) and c.q(f"SELECT count(*) FROM {P} WHERE kind = 'plain'")[0][0] == 3)
            check(f"§1 ADD COLUMN … DEFAULT 'plain': both replicas fill the 3 OLD rows locally, no re-seed ({dt} s)", dt is not None)

            # ── 2. RENAME COLUMN ──
            zb.psql(f"UPDATE {P} SET label = 'kept-' || label, updated_at = now()")
            both(py, nd, lambda c: c.q(f"SELECT count(*) FROM {P} WHERE label LIKE 'kept-%'")[0][0] == 3)
            zb.psql(f"ALTER TABLE {P} RENAME COLUMN label TO title")
            dt = both(py, nd, lambda c: "title" in c.cols(P) and "label" not in c.cols(P) and c.q(f"SELECT count(*) FROM {P} WHERE title LIKE 'kept-%'")[0][0] == 3)
            check(f"§2 RENAME COLUMN label → title: renamed in both, the values survive ({dt} s)", dt is not None)

            # ── 3. DROP COLUMN ──
            zb.psql(f"ALTER TABLE {P} DROP COLUMN kind")
            dt = both(py, nd, lambda c: "kind" not in c.cols(P) and c.q(f"SELECT count(*) FROM {P}")[0][0] == 3)
            check(f"§3 DROP COLUMN kind: gone in both, the rows survive ({dt} s)", dt is not None)

            # ── 4. a VOLATILE default: the silent divergence, then the lever ──
            zb.psql(f"ALTER TABLE {P} ADD COLUMN stamp timestamptz DEFAULT now()")
            both(py, nd, lambda c: "stamp" in c.cols(P))
            nulls = lambda c: c.q(f"SELECT count(*) FROM {P} WHERE stamp IS NULL")[0][0]
            check(f"§4a ADD COLUMN … DEFAULT now(): both replicas hold NULL where PostgreSQL holds a timestamp (py {nulls(py)}, node {nulls(nd)})", nulls(py) == 3 and nulls(nd) == 3)
            bumped = zb.psql(f"SELECT tbl || '=' || seed_epoch FROM zebridge_reseed('{P}')").strip()
            dt = both(py, nd, lambda c: nulls(c) == 0, 90)
            check(f"§4b zebridge_reseed({P}) → {bumped}: both re-seeded from the producer's forced full, NULLs gone ({dt} s; py {nulls(py)}, node {nulls(nd)})", dt is not None)

            # ── 5. the re-key: bigserial → uuid on the parent, the child's FK follows ──
            e0 = zb.psql(f"SELECT string_agg(tbl || '=' || seed_epoch, ',' ORDER BY tbl) FROM zebridge_catalogue WHERE tbl IN ('{P}','{C}')").strip()
            zb.psql(f"""BEGIN;
                ALTER TABLE {P} ADD COLUMN new_id uuid NOT NULL DEFAULT gen_random_uuid();
                ALTER TABLE {C} ADD COLUMN new_parent_id uuid;
                UPDATE {C} c SET new_parent_id = p.new_id FROM {P} p WHERE p.id = c.parent_id;
                ALTER TABLE {C} DROP COLUMN parent_id;
                ALTER TABLE {P} DROP COLUMN id;
                ALTER TABLE {P} RENAME COLUMN new_id TO id;
                ALTER TABLE {P} ADD PRIMARY KEY (id);
                ALTER TABLE {C} RENAME COLUMN new_parent_id TO parent_id;
                ALTER TABLE {C} ALTER COLUMN parent_id SET NOT NULL;
                ALTER TABLE {C} ADD FOREIGN KEY (parent_id) REFERENCES {P}(id);
                COMMIT;""")
            # the old pk took the replica identity with it: re-run the (idempotent) enable
            zb.psql(f"SELECT step FROM zebridge_enable('public.{P}', {ENABLE})", quiet=True)
            e1 = zb.psql(f"SELECT string_agg(tbl || '=' || seed_epoch, ',' ORDER BY tbl) FROM zebridge_catalogue WHERE tbl IN ('{P}','{C}')").strip()
            check(f"§5a the DDL trigger bumped both seed epochs, once each: {e0} → {e1}", e1 == ",".join(f"{k}={int(v) + 1}" for k, v in (x.split("=") for x in e0.split(","))))
            pg_ids = zb.psql(f"SELECT string_agg(id::text, ',' ORDER BY title) FROM {P}").strip()
            def rekeyed(c):
                ids = ",".join(str(r[0]) for r in c.q(f"SELECT id FROM {P} ORDER BY title"))
                joined = c.q(f"SELECT count(*) FROM {C} c JOIN {P} p ON p.id = c.parent_id")[0][0]
                return ids == pg_ids and joined == 3
            dt = both(py, nd, rekeyed, 120)
            check(f"§5b both replicas re-keyed: parent ids are PostgreSQL's uuids, all 3 children join by the new key ({dt} s)", dt is not None)
            ty = lambda c, t, col: (c.q(f"SELECT type FROM pragma_table_info('{t}') WHERE name = ?", [col]) or [[None]])[0][0]
            check(f"§5c local shapes moved: parent id py={ty(py, P, 'id')} node={ty(nd, P, 'id')}; child fk py={ty(py, C, 'parent_id')} node={ty(nd, C, 'parent_id')}",
                  all(x == "TEXT" for x in (ty(py, P, 'id'), ty(nd, P, 'id'), ty(py, C, 'parent_id'), ty(nd, C, 'parent_id'))))
            nid = str(uuid.uuid4())
            zb.psql(f"INSERT INTO {P} (id, title, tenant_id) VALUES ('{nid}', 'p4', '{tenant}'); INSERT INTO {C} (parent_id, note, tenant_id) VALUES ('{nid}', 'c4', '{tenant}')")
            dt = both(py, nd, lambda c: bool(c.q(f"SELECT 1 FROM {C} c JOIN {P} p ON p.id = c.parent_id WHERE p.id = ?", [nid])))
            check(f"§5d CDC keyed by the uuid lands in both, parent and child join ({dt} s)", dt is not None)

            # ── 6. DROP TABLE ──
            zb.psql(f"DROP TABLE {C}")
            dt = both(py, nd, lambda c: C not in [r[0] for r in c.q("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", [C])], 30)
            check(f"§6 DROP TABLE {C}: the tombstone dropped both local tables ({dt} s)", dt is not None)
            # Tear down WHILE the bridge runs: a drop and a catalogue delete committed
            # after it stops are replayed by the next bridge at boot, and a scenario
            # that counts that bridge's reloads (livebirth) then sees one too many.
            py.close(); nd.close(); py = nd = None
            teardown()
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

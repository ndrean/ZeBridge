"""Two parents re-keyed in ONE transaction, a child referencing both (NOTES §10dg, item 1
of the client-test list of 2026-09-06).

The re-key proofs so far moved one parent. Here `mig_a` and `mig_b` (bigserial keys)
both move to uuid in the same migration, and `mig_c` references both. Expected:

  * the DDL trigger bumps each of the three seed epochs EXACTLY once — the closure of
    `mig_a` reaches `mig_c`, the closure of `mig_b` reaches it again, and `mig_c`'s own
    re-type would be a third time: the transaction-local marker collapses them;
  * both clients (libzb polling, zb-client-ts in Node) rebuild both parents empty,
    re-type the child's two FK columns, re-seed all three, and every child row joins
    BOTH parents by the new keys; CDC keyed by the new uuids lands afterwards.
"""
import sys, time, uuid
import zb
from clients import Lib, Node, both, fresh_sqlite

LOG = "/tmp/zb_rekey_two_parents_bridge.log"
A, B, C = "mig_a", "mig_b", "mig_c"
PUB = zb.publication()
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")


def epochs():
    return zb.psql(f"SELECT string_agg(tbl || '=' || seed_epoch, ',' ORDER BY tbl) FROM zebridge_catalogue WHERE tbl IN ('{A}','{B}','{C}')").strip()


def teardown():
    for sql in (f"DROP TABLE IF EXISTS public.{C}", f"DROP TABLE IF EXISTS public.{A}", f"DROP TABLE IF EXISTS public.{B}",
                f"DELETE FROM public.zebridge_catalogue WHERE tbl IN ('{A}','{B}','{C}')",
                f"DELETE FROM public.zebridge_generations WHERE tbl IN ('{A}','{B}','{C}')"):
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
    zb.psql(f"CREATE TABLE public.{A} (id bigserial PRIMARY KEY, name text, {COMMON})")
    zb.psql(f"CREATE TABLE public.{B} (id bigserial PRIMARY KEY, name text, {COMMON})")
    zb.psql(f"CREATE TABLE public.{C} (id bigserial PRIMARY KEY, a_id bigint NOT NULL REFERENCES public.{A}(id), "
            f"b_id bigint NOT NULL REFERENCES public.{B}(id), note text, {COMMON})")
    for db in ("/tmp/zb-rekey2-py.sqlite3", "/tmp/zb-rekey2-node.sqlite3"):
        fresh_sqlite(db)
    py = nd = None
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            for t in (A, B, C):
                out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{t}', {ENABLE})")
                check(f"zebridge_enable({t}) live: {out.strip()[:50]}…", "error" not in out.lower())
            py = Lib("/tmp/zb-rekey2-py.sqlite3", [A, B, C], "py-rekey2")
            nd = Node("/tmp/zb-rekey2-node.sqlite3", "/tmp/zb_rekey_two_parents_node.log")
            tenant = py.tenant
            zb.psql(f"INSERT INTO {A} (name, tenant_id) SELECT 'a' || g, '{tenant}' FROM generate_series(1,3) g")
            zb.psql(f"INSERT INTO {B} (name, tenant_id) SELECT 'b' || g, '{tenant}' FROM generate_series(1,3) g")
            zb.psql(f"INSERT INTO {C} (a_id, b_id, note, tenant_id) SELECT a.id, b.id, 'c' || a.id || b.id, '{tenant}' FROM {A} a JOIN {B} b ON b.id = 4 - a.id")
            counts = lambda c: tuple(c.q(f"SELECT count(*) FROM {t}")[0][0] for t in (A, B, C))
            dt = both(py, nd, lambda c: counts(c) == (3, 3, 3), 150)
            check(f"§0 both replicas hold 3 + 3 parents and 3 children (py {counts(py)}, node {counts(nd)}; {dt} s)", dt is not None)
            e0 = epochs()

            # ── the migration: both parents to uuid, the child's two FKs remapped, one transaction ──
            zb.psql(f"""BEGIN;
                ALTER TABLE {A} ADD COLUMN new_id uuid NOT NULL DEFAULT gen_random_uuid();
                ALTER TABLE {B} ADD COLUMN new_id uuid NOT NULL DEFAULT gen_random_uuid();
                ALTER TABLE {C} ADD COLUMN new_a_id uuid, ADD COLUMN new_b_id uuid;
                UPDATE {C} c SET new_a_id = a.new_id FROM {A} a WHERE a.id = c.a_id;
                UPDATE {C} c SET new_b_id = b.new_id FROM {B} b WHERE b.id = c.b_id;
                ALTER TABLE {C} DROP COLUMN a_id, DROP COLUMN b_id;
                ALTER TABLE {A} DROP COLUMN id;
                ALTER TABLE {B} DROP COLUMN id;
                ALTER TABLE {A} RENAME COLUMN new_id TO id;
                ALTER TABLE {B} RENAME COLUMN new_id TO id;
                ALTER TABLE {A} ADD PRIMARY KEY (id);
                ALTER TABLE {B} ADD PRIMARY KEY (id);
                ALTER TABLE {C} RENAME COLUMN new_a_id TO a_id;
                ALTER TABLE {C} RENAME COLUMN new_b_id TO b_id;
                ALTER TABLE {C} ALTER COLUMN a_id SET NOT NULL, ALTER COLUMN b_id SET NOT NULL;
                ALTER TABLE {C} ADD FOREIGN KEY (a_id) REFERENCES {A}(id), ADD FOREIGN KEY (b_id) REFERENCES {B}(id);
                COMMIT;""")
            for t in (A, B):
                zb.psql(f"SELECT step FROM zebridge_enable('public.{t}', {ENABLE})", quiet=True)
            e1 = epochs()
            want = ",".join(f"{k}={int(v) + 1}" for k, v in (x.split("=") for x in e0.split(",")))
            check(f"§1 each of the three seed epochs moved EXACTLY once: {e0} → {e1}", e1 == want)

            pg = zb.psql(f"SELECT string_agg(c.note || ':' || a.name || ':' || b.name, ',' ORDER BY c.note) FROM {C} c JOIN {A} a ON a.id = c.a_id JOIN {B} b ON b.id = c.b_id").strip()
            def converged(c):
                rows = c.q(f"SELECT c.note || ':' || a.name || ':' || b.name FROM {C} c JOIN {A} a ON a.id = c.a_id JOIN {B} b ON b.id = c.b_id ORDER BY c.note")
                return ",".join(r[0] for r in rows) == pg and c.q(f"SELECT count(*) FROM {C}")[0][0] == 3
            dt = both(py, nd, converged, 150)
            check(f"§2 both replicas: every child joins BOTH parents by the new uuid keys ({dt} s): {pg}", dt is not None)
            ty = lambda c, t, col: (c.q(f"SELECT type FROM pragma_table_info('{t}') WHERE name = ?", [col]) or [[None]])[0][0]
            shapes = [ty(c, t, col) for c in (py, nd) for t, col in ((A, "id"), (B, "id"), (C, "a_id"), (C, "b_id"))]
            check(f"§3 both parents re-keyed and both FK columns re-typed on both replicas: {shapes}", all(x == "TEXT" for x in shapes))

            na, nb = str(uuid.uuid4()), str(uuid.uuid4())
            zb.psql(f"INSERT INTO {A} (id, name, tenant_id) VALUES ('{na}', 'a4', '{tenant}'); INSERT INTO {B} (id, name, tenant_id) VALUES ('{nb}', 'b4', '{tenant}'); "
                    f"INSERT INTO {C} (a_id, b_id, note, tenant_id) VALUES ('{na}', '{nb}', 'c44', '{tenant}')")
            dt = both(py, nd, lambda c: bool(c.q(f"SELECT 1 FROM {C} c JOIN {A} a ON a.id = c.a_id JOIN {B} b ON b.id = c.b_id WHERE c.note = 'c44'")))
            check(f"§4 CDC after the re-key: a child of two new uuid parents lands and joins on both replicas ({dt} s)", dt is not None)
            py.close(); nd.close(); py = nd = None
            teardown()
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

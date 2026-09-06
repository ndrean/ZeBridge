"""A client offline across a re-key (NOTES §10dg, item 2 of the client-test list).

Every re-key proof so far had the clients CONNECTED when the descriptor moved: they saw
the intermediate descriptors one by one, then the epoch. An offline client meets the
final descriptor (new key shape, new epoch) and the producer's new full all at once,
at its next connect, on a replica file that still holds the old-keyed rows. Both
clients (libzb polling, zb-client-ts in Node) are closed on their durable files, the
parent is re-keyed and rows are written under the new key while they are away, and
they reopen on the SAME files. Expected: the parent rebuilt empty from its shape
record, the child re-typed, both re-seeded from the fresh full, the rows written while
offline present, CDC keyed by the new uuid flowing afterwards.
"""
import sys, time, uuid
import zb
from clients import Lib, Node, both, fresh_sqlite

LOG = "/tmp/zb_rekey_offline_bridge.log"
P, C = "mig_p", "mig_k"
PUB = zb.publication()
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")
PY_DB, NODE_DB = "/tmp/zb-rekey-offline-py.sqlite3", "/tmp/zb-rekey-offline-node.sqlite3"


def epochs():
    return zb.psql(f"SELECT string_agg(tbl || '=' || seed_epoch, ',' ORDER BY tbl) FROM zebridge_catalogue WHERE tbl IN ('{P}','{C}')").strip()


def teardown():
    for sql in (f"DROP TABLE IF EXISTS public.{C}", f"DROP TABLE IF EXISTS public.{P}",
                f"DELETE FROM public.zebridge_catalogue WHERE tbl IN ('{P}','{C}')",
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
    zb.psql(f"CREATE TABLE public.{P} (id bigserial PRIMARY KEY, title text, {COMMON})")
    zb.psql(f"CREATE TABLE public.{C} (id bigserial PRIMARY KEY, parent_id bigint NOT NULL REFERENCES public.{P}(id), note text, {COMMON})")
    fresh_sqlite(PY_DB); fresh_sqlite(NODE_DB)
    py = nd = None
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            for t in (P, C):
                out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{t}', {ENABLE})")
                check(f"zebridge_enable({t}) live: {out.strip()[:50]}…", "error" not in out.lower())
            py = Lib(PY_DB, [P, C], "py-rekey-offline")
            nd = Node(NODE_DB, "/tmp/zb_rekey_offline_node.log")
            tenant = py.tenant
            zb.psql(f"INSERT INTO {P} (title, tenant_id) SELECT 'p' || g, '{tenant}' FROM generate_series(1,3) g")
            zb.psql(f"INSERT INTO {C} (parent_id, note, tenant_id) SELECT p.id, 'c' || p.id, '{tenant}' FROM {P} p")
            counts = lambda c: (c.q(f"SELECT count(*) FROM {P}")[0][0], c.q(f"SELECT count(*) FROM {C}")[0][0])
            dt = both(py, nd, lambda c: counts(c) == (3, 3), 150)
            check(f"§0 both replicas hold 3 parents + 3 children (py {counts(py)}, node {counts(nd)}; {dt} s)", dt is not None)
            shape0 = py.q(f"SELECT key_shape FROM _zbz_shape WHERE tbl = '{P}'")[0][0]
            check(f"§0 both replicas recorded the old key shape: {shape0}", shape0 == '[["id","INTEGER"]]' and nd.q(f"SELECT key_shape FROM _zebridge_shape WHERE tbl = '{P}'")[0][0] == shape0)

            # ── both clients go OFFLINE, their files kept ──
            py.close(); nd.close(); py = nd = None
            check("§1 both clients closed on their durable files", True)

            e0 = epochs()
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
            zb.psql(f"SELECT step FROM zebridge_enable('public.{P}', {ENABLE})", quiet=True)
            e1 = epochs()
            check(f"§2 re-keyed while both were away; epochs {e0} → {e1}", e1 == ",".join(f"{k}={int(v) + 1}" for k, v in (x.split("=") for x in e0.split(","))))
            # rows written under the NEW key, still while they are away
            nid = str(uuid.uuid4())
            zb.psql(f"INSERT INTO {P} (id, title, tenant_id) VALUES ('{nid}', 'p4-offline', '{tenant}'); INSERT INTO {C} (parent_id, note, tenant_id) VALUES ('{nid}', 'c4-offline', '{tenant}')")
            zb.psql(f"UPDATE {P} SET title = 'p1-offline', updated_at = now() WHERE title = 'p1'")
            # let the producer build the post-re-key full before anyone reconnects
            t0 = time.monotonic()
            while time.monotonic() - t0 < 30:
                if zb.psql(f"SELECT count(*) FROM zebridge_generations WHERE tenant = '{tenant}' AND tbl = '{P}' AND seed_epoch = {int(e1.split(',')[1].split('=')[1])} AND has_full") not in ("", "0"): break
                time.sleep(1)
            check(f"§2 the producer built the parent's full under the new epoch while they were away ({round(time.monotonic() - t0, 1)} s)", time.monotonic() - t0 < 30)

            # ── back: same files, the new descriptor, epoch and full all at once ──
            py = Lib(PY_DB, [P, C], "py-rekey-offline")
            nd = Node(NODE_DB, "/tmp/zb_rekey_offline_node2.log")
            pg = zb.psql(f"SELECT string_agg(c.note || ':' || p.title, ',' ORDER BY c.note) FROM {C} c JOIN {P} p ON p.id = c.parent_id").strip()
            def converged(c):
                rows = c.q(f"SELECT c.note || ':' || p.title FROM {C} c JOIN {P} p ON p.id = c.parent_id ORDER BY c.note")
                return ",".join(r[0] for r in rows) == pg and c.q(f"SELECT count(*) FROM {P}")[0][0] == 4
            dt = both(py, nd, converged, 150)
            check(f"§3 reopened on the same files: both replicas converged — children join by the uuid key, the rows written while away included ({dt} s): {pg}", dt is not None)
            ty = lambda c, t, col: (c.q(f"SELECT type FROM pragma_table_info('{t}') WHERE name = ?", [col]) or [[None]])[0][0]
            check(f"§3 local shapes moved on both: parent id py={ty(py, P, 'id')} node={ty(nd, P, 'id')}; child fk py={ty(py, C, 'parent_id')} node={ty(nd, C, 'parent_id')}",
                  all(x == "TEXT" for x in (ty(py, P, 'id'), ty(nd, P, 'id'), ty(py, C, 'parent_id'), ty(nd, C, 'parent_id'))))
            shape1 = py.q(f"SELECT key_shape FROM _zbz_shape WHERE tbl = '{P}'")[0][0]
            check(f"§3 the shape records moved: {shape1}", shape1 == '[["id","TEXT"]]' and nd.q(f"SELECT key_shape FROM _zebridge_shape WHERE tbl = '{P}'")[0][0] == shape1)
            wm = (py.q(f"SELECT seed_epoch FROM _zbz_generations WHERE tbl = '{P}'") or [[None]])[0][0]
            check(f"§3 watermarks carry the new epoch (py {wm})", str(wm) == e1.split(',')[1].split('=')[1])

            nid2 = str(uuid.uuid4())
            zb.psql(f"INSERT INTO {P} (id, title, tenant_id) VALUES ('{nid2}', 'p5', '{tenant}'); INSERT INTO {C} (parent_id, note, tenant_id) VALUES ('{nid2}', 'c5', '{tenant}')")
            dt = both(py, nd, lambda c: bool(c.q(f"SELECT 1 FROM {C} c JOIN {P} p ON p.id = c.parent_id WHERE c.note = 'c5'")))
            check(f"§4 CDC after the reconnect, keyed by the uuid: lands and joins on both ({dt} s)", dt is not None)
            py.close(); nd.close(); py = nd = None
            teardown()
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

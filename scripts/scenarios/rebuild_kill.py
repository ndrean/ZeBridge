"""A client killed in the middle of a local migration (NOTES §10dj, item 5).

A migration on the replica is several SQLite statements, each atomic, none of them one
transaction: drop the view, drop the table (or rename it aside), create the new one,
copy the rows, rename, record the shape, drop the watermark. A host killed between
any two leaves the file in a state no code path ever wrote on purpose. Those states
are exactly the prefixes of that sequence, so this scenario builds each one by hand
on the closed client's file and reopens the client on it — for a rebuild that keeps
the rows (a foreign key added; no epoch move) and for a re-key (epoch moved):

  P1  killed after DROP TABLE            table absent, watermark and shape record stale
  P2  killed before the RENAME           rows sit in <table>__migrating, no <table>
  P3  killed after the watermark went    table old, no watermark
  P4  the shape record is missing        table old, epoch moved, nothing to compare with

Both clients, every state: the replica must equal PostgreSQL afterwards, with no
`__migrating` leftover and a clean foreign-key check.
"""
import shutil, sqlite3, sys, time, uuid
import zb
from clients import Lib, Node, both, fresh_sqlite

LOG = "/tmp/zb_rebuild_kill_bridge.log"
P, C = "mig_rk", "mig_rk_c"
PUB = zb.publication()
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")
PY_DB, NODE_DB = "/tmp/zb-rebuild-kill-py.sqlite3", "/tmp/zb-rebuild-kill-node.sqlite3"
NAMES = {"py": {"wm": "_zbz_generations", "shape": "_zbz_shape"}, "node": {"wm": "_zebridge_generations", "shape": "_zebridge_shape"}}


def teardown():
    for sql in (f"DROP TABLE IF EXISTS public.{C}", f"DROP TABLE IF EXISTS public.{P}",
                f"DELETE FROM public.zebridge_catalogue WHERE tbl IN ('{P}','{C}')",
                f"DELETE FROM public.zebridge_generations WHERE tbl IN ('{P}','{C}')"):
        zb.psql(sql, quiet=True)


def snapshot(db, tag):
    for ext in ("", "-wal", "-shm"):
        try: shutil.copy(db + ext, f"{db}.{tag}{ext}")
        except FileNotFoundError:
            try: __import__("os").remove(f"{db}.{tag}{ext}")
            except FileNotFoundError: pass


def restore(db, tag):
    fresh_sqlite(db)
    for ext in ("", "-wal", "-shm"):
        try: shutil.copy(f"{db}.{tag}{ext}", db + ext)
        except FileNotFoundError: pass


def leave(db, who, state, table):
    """Put the closed file into prefix state `state` for `table`, by hand."""
    n = NAMES[who]
    con = sqlite3.connect(db); con.execute("PRAGMA foreign_keys = OFF")
    if state == "P1":
        con.execute(f"DROP VIEW IF EXISTS {table}_view"); con.execute(f"DROP TABLE IF EXISTS {table}")
    elif state == "P2":
        con.execute(f"DROP VIEW IF EXISTS {table}_view"); con.execute(f"ALTER TABLE {table} RENAME TO {table}__migrating")
    elif state == "P3":
        con.execute(f"DELETE FROM {n['wm']} WHERE tbl = ?", (table,))
    elif state == "P4":
        con.execute(f"DELETE FROM {n['shape']} WHERE tbl = ?", (table,))
    con.commit(); con.close()


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
    zb.psql(f"CREATE TABLE public.{P} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), title text, {COMMON})")
    # the child starts WITHOUT its foreign key: adding it later is the rebuild-that-keeps-rows
    zb.psql(f"CREATE TABLE public.{C} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), p_id uuid NOT NULL, note text, {COMMON})")
    fresh_sqlite(PY_DB); fresh_sqlite(NODE_DB)
    py = nd = None
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            for t in (P, C):
                out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{t}', {ENABLE})")
                check(f"zebridge_enable({t}) live: {out.strip()[:50]}…", "error" not in out.lower())
            tenant = zb.tenant_of("omar")
            zb.psql(f"INSERT INTO {P} (title, tenant_id) SELECT 'p' || g, '{tenant}' FROM generate_series(1,3) g")
            zb.psql(f"INSERT INTO {C} (p_id, note, tenant_id) SELECT p.uid, 'c-' || p.title, '{tenant}' FROM {P} p")

            def open_both():
                return Lib(PY_DB, [P, C], "py-rebuild-kill"), Node(NODE_DB, "/tmp/zb_rebuild_kill_node.log")
            def pg_state():
                return zb.psql(f"SELECT string_agg(c.note || '=' || p.title, ',' ORDER BY c.note) FROM {C} c JOIN {P} p ON p.uid = c.p_id WHERE c.deleted_at IS NULL").strip()
            def replica(c):
                rows = c.q(f"SELECT c.note || '=' || p.title FROM {C} c JOIN {P} p ON p.uid = c.p_id WHERE c.deleted_at IS NULL ORDER BY c.note")
                return ",".join(r[0] for r in rows)
            def clean(c):
                return not c.q("SELECT name FROM sqlite_master WHERE name LIKE '%__migrating'") and len(c.q("PRAGMA foreign_key_check")) == 0
            py, nd = open_both()
            want = pg_state()
            dt = both(py, nd, lambda c: replica(c) == want, 150)
            check(f"§0 both replicas hold the family ({dt} s): {want}", dt is not None)
            py.close(); nd.close(); py = nd = None
            snapshot(PY_DB, "m0"); snapshot(NODE_DB, "m0")

            # ── M1: the child gains its foreign key → a rebuild that keeps the rows, no epoch move ──
            zb.psql(f"ALTER TABLE {C} ADD CONSTRAINT {C}_p_fk FOREIGN KEY (p_id) REFERENCES {P}(uid)")
            nid = str(uuid.uuid4())
            zb.psql(f"INSERT INTO {P} (uid, title, tenant_id) VALUES ('{nid}', 'p4', '{tenant}'); INSERT INTO {C} (p_id, note, tenant_id) VALUES ('{nid}', 'c-p4', '{tenant}')")
            e = zb.psql(f"SELECT string_agg(tbl || '=' || seed_epoch, ',' ORDER BY tbl) FROM zebridge_catalogue WHERE tbl IN ('{P}','{C}')").strip()
            check(f"§M1 foreign key added upstream, a family written meanwhile; epochs unchanged ({e})", e == f"{P}=0,{C}=0")
            want = pg_state()
            for state in ("P1", "P2"):
                restore(PY_DB, "m0"); restore(NODE_DB, "m0")
                leave(PY_DB, "py", state, C); leave(NODE_DB, "node", state, C)
                py, nd = open_both()
                dt = both(py, nd, lambda c: replica(c) == want and clean(c), 150)
                check(f"§M1 {state}: reopened on the interrupted file, both converged and clean ({dt} s): py [{replica(py)}], node [{replica(nd)}]", dt is not None)
                py.close(); nd.close(); py = nd = None
            snapshot(PY_DB, "m1"); snapshot(NODE_DB, "m1")   # the last converged state, M1 applied

            # ── M2: the parent re-keyed to (tenant_id, uid) → epoch moves ──
            zb.psql(f"ALTER TABLE {P} DROP CONSTRAINT {P}_pkey CASCADE, ADD PRIMARY KEY (tenant_id, uid)")
            zb.psql(f"ALTER TABLE {C} ADD CONSTRAINT {C}_p_fk2 FOREIGN KEY (tenant_id, p_id) REFERENCES {P}(tenant_id, uid)")
            zb.psql(f"SELECT step FROM zebridge_enable('public.{P}', {ENABLE})", quiet=True)
            nid2 = str(uuid.uuid4())
            zb.psql(f"INSERT INTO {P} (uid, title, tenant_id) VALUES ('{nid2}', 'p5', '{tenant}'); INSERT INTO {C} (p_id, note, tenant_id) VALUES ('{nid2}', 'c-p5', '{tenant}')")
            e = zb.psql(f"SELECT string_agg(tbl || '=' || seed_epoch, ',' ORDER BY tbl) FROM zebridge_catalogue WHERE tbl IN ('{P}','{C}')").strip()
            # the child is NOT in the closure here: the parent's pk drop cascaded its FK away
            # first, and its own values never changed — no re-seed owed, and none asked
            check(f"§M2 parent re-keyed to (tenant_id, uid) upstream; the parent's epoch moved, the child's did not ({e})", e == f"{P}=1,{C}=0")
            time.sleep(6)  # the producer's fulls under the new epoch
            want = pg_state()
            for state in ("P1", "P2", "P3", "P4"):
                restore(PY_DB, "m1"); restore(NODE_DB, "m1")
                leave(PY_DB, "py", state, P); leave(NODE_DB, "node", state, P)
                py, nd = open_both()
                dt = both(py, nd, lambda c: replica(c) == want and clean(c), 150)
                shape = lambda c, t: (c.q(f"SELECT key_shape FROM {t} WHERE tbl = ?", [P]) or [[None]])[0][0]
                check(f"§M2 {state}: reopened on the interrupted file, both converged and clean ({dt} s); key shapes py {shape(py, '_zbz_shape')}, node {shape(nd, '_zebridge_shape')}",
                      dt is not None and shape(py, "_zbz_shape") == shape(nd, "_zebridge_shape") == '[["tenant_id","TEXT"],["uid","TEXT"]]')
                py.close(); nd.close(); py = nd = None
            teardown()
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

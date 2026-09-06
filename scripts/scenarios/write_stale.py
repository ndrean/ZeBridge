"""A client writes against a table that is outdated by the time the write lands
(NOTES §10dg, item 3 of the client-test list).

Both clients (libzb, zb-client-ts in Node) write while the BRIDGE is down: the writes
apply optimistically on the replica and sit in the MUTATIONS stream with no verdict.
PostgreSQL migrates meanwhile — the bridge is what carries a migration to a client, so
the clients still hold the old shape. Then the bridge returns and the parked writes
meet the new schema:

  A. an UPDATE setting a column that was DROPPED;
  B. an INSERT lacking a column that became NOT NULL with no default;
  C. an UPDATE on a table whose primary key GAINED a column (a re-key).

What must hold at the end, whatever each verdict is: both replicas equal PostgreSQL
(no ghost row from an optimistic apply, no revert resurrecting a row a re-seed
removed), nothing left pending in either outbox, and both clients still answering.
"""
import sys, time, uuid
import zb
from clients import Lib, Node, both, fresh_sqlite

LOG, LOG2 = "/tmp/zb_write_stale_bridge.log", "/tmp/zb_write_stale_bridge2.log"
W, K = "mig_w", "mig_w2"
PUB = zb.publication()
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")
PY_DB, NODE_DB = "/tmp/zb-write-stale-py.sqlite3", "/tmp/zb-write-stale-node.sqlite3"


def teardown():
    for sql in (f"DROP TABLE IF EXISTS public.{W}", f"DROP TABLE IF EXISTS public.{K}",
                f"DELETE FROM public.zebridge_catalogue WHERE tbl IN ('{W}','{K}')",
                f"DELETE FROM public.zebridge_generations WHERE tbl IN ('{W}','{K}')"):
        zb.psql(sql, quiet=True)


def outbox_left(c, tbl_like):
    names = [r[0] for r in c.q("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ?", [tbl_like])]
    return sum(c.q(f"SELECT count(*) FROM {n}")[0][0] for n in names) if names else -1


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
    zb.psql(f"CREATE TABLE public.{W} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), title text, note text, {COMMON})")
    zb.psql(f"CREATE TABLE public.{K} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), title text, {COMMON})")
    fresh_sqlite(PY_DB); fresh_sqlite(NODE_DB)
    py = nd = None
    rows = {t: [str(uuid.uuid4()) for _ in range(3)] for t in (W, K)}
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            for t in (W, K):
                out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{t}', {ENABLE})")
                check(f"zebridge_enable({t}) live: {out.strip()[:50]}…", "error" not in out.lower())
            py = Lib(PY_DB, [W, K], "py-write-stale")
            nd = Node(NODE_DB, "/tmp/zb_write_stale_node.log")
            tenant = py.tenant
            for i, u in enumerate(rows[W]):
                zb.psql(f"INSERT INTO {W} (uid, title, note, tenant_id) VALUES ('{u}', 'w{i}', 'n{i}', '{tenant}')")
            for i, u in enumerate(rows[K]):
                zb.psql(f"INSERT INTO {K} (uid, title, tenant_id) VALUES ('{u}', 'k{i}', '{tenant}')")
            counts = lambda c: (c.q(f"SELECT count(*) FROM {W}")[0][0], c.q(f"SELECT count(*) FROM {K}")[0][0])
            dt = both(py, nd, lambda c: counts(c) == (3, 3), 150)
            check(f"§0 both replicas hold the rows (py {counts(py)}, node {counts(nd)}; {dt} s)", dt is not None)

        # ── the bridge is DOWN; the clients write, optimistically, into the void ──
        new_py, new_nd = str(uuid.uuid4()), str(uuid.uuid4())
        m1 = py.mutate(W, "UPDATE", {"uid": rows[W][0]}, {"note": "stale-note-py"})          # A
        m2 = py.mutate(W, "INSERT", {"uid": new_py}, {"uid": new_py, "title": "new-py", "tenant_id": tenant})  # B
        m3 = py.mutate(K, "UPDATE", {"uid": rows[K][0]}, {"title": "k0-py"})                 # C
        f = py.flush(3000)
        nd.mutate(W, "UPDATE", {"uid": rows[W][1]}, {"note": "stale-note-node"})
        nd.mutate(W, "INSERT", {"uid": new_nd}, {"uid": new_nd, "title": "new-node", "tenant_id": tenant})
        nd.mutate(K, "UPDATE", {"uid": rows[K][1]}, {"title": "k1-node"})
        time.sleep(2)
        check(f"§1 six writes queued with the bridge down: libzb flush sent={f.get('sent')} settled={f.get('settled')}; optimistic copies applied (py {counts(py)}, node {counts(nd)})",
              f.get("sent") == 3 and f.get("settled") == 0 and counts(py) == (4, 3) and counts(nd) == (4, 3))
        check(f"§1 outboxes hold them: py {outbox_left(py, '%outbox%')}, node {outbox_left(nd, '_zebridge_outbox')}",
              outbox_left(py, '%outbox%') == 3 and outbox_left(nd, '_zebridge_outbox') == 3)

        # ── PostgreSQL migrates while the bridge is down ──
        zb.psql(f"ALTER TABLE {W} DROP COLUMN note")
        zb.psql(f"ALTER TABLE {W} ADD COLUMN priority integer NOT NULL DEFAULT 0; ALTER TABLE {W} ALTER COLUMN priority DROP DEFAULT")
        zb.psql(f"ALTER TABLE {K} DROP CONSTRAINT {K}_pkey, ADD PRIMARY KEY (tenant_id, uid)")
        check("§2 migrated meanwhile: note dropped, priority NOT NULL without default, the key of the second table gained tenant_id", True)

        with zb.Bridge(LOG2, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge2:
            if not bridge2.wait_for_log("Generation producer started", timeout=30):
                zb.bad("restarted bridge never started its producer"); print(bridge2.text()[-1500:]); return 1
            zb.psql(f"SELECT step FROM zebridge_enable('public.{K}', {ENABLE})", quiet=True)
            # let the verdicts land and the schemas move
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                py.poll()
                if outbox_left(py, '%outbox%') == 0 and outbox_left(nd, '_zebridge_outbox') == 0: break
                time.sleep(0.3)
            check(f"§3 every parked write got its verdict: outboxes py {outbox_left(py, '%outbox%')}, node {outbox_left(nd, '_zebridge_outbox')}",
                  outbox_left(py, '%outbox%') == 0 and outbox_left(nd, '_zebridge_outbox') == 0)
            pg_w = zb.psql(f"SELECT string_agg(title || ':' || priority, ',' ORDER BY title) FROM {W} WHERE deleted_at IS NULL").strip()
            pg_k = zb.psql(f"SELECT string_agg(title, ',' ORDER BY title) FROM {K} WHERE deleted_at IS NULL").strip()
            print(f"  · PostgreSQL now: {W} = {pg_w}; {K} = {pg_k}")
            def converged(c):
                w = ",".join(f"{r[0]}:{r[1]}" for r in c.q(f"SELECT title, priority FROM {W} WHERE deleted_at IS NULL ORDER BY title"))
                k = ",".join(r[0] for r in c.q(f"SELECT title FROM {K} WHERE deleted_at IS NULL ORDER BY title"))
                return w == pg_w and k == pg_k
            dt = both(py, nd, converged, 150)
            wv = lambda c: ",".join(f"{r[0]}:{r[1]}" for r in c.q(f"SELECT title, priority FROM {W} WHERE deleted_at IS NULL ORDER BY title"))
            kv = lambda c: ",".join(r[0] for r in c.q(f"SELECT title FROM {K} WHERE deleted_at IS NULL ORDER BY title"))
            check(f"§4 both replicas equal PostgreSQL, no ghost row, no resurrected row ({dt} s): py [{wv(py)} | {kv(py)}], node [{wv(nd)} | {kv(nd)}]", dt is not None)
            check(f"§4 the second table's key moved on both: py {py.q(f'SELECT key_shape FROM _zbz_shape WHERE tbl = ?', [K])[0][0]}, node {nd.q(f'SELECT key_shape FROM _zebridge_shape WHERE tbl = ?', [K])[0][0]}",
                  py.q(f"SELECT key_shape FROM _zbz_shape WHERE tbl = ?", [K])[0][0] == nd.q(f"SELECT key_shape FROM _zebridge_shape WHERE tbl = ?", [K])[0][0] == '[["tenant_id","TEXT"],["uid","TEXT"]]')
            # a fresh, correct write goes through afterwards
            good = str(uuid.uuid4())
            py.mutate(W, "INSERT", {"uid": good}, {"uid": good, "title": "after-py", "priority": 7, "tenant_id": tenant}); py.flush(5000)
            nd.mutate(W, "INSERT", {"uid": str(uuid.uuid4())}, {"uid": str(uuid.uuid4()), "title": "after-node", "priority": 8, "tenant_id": tenant})
            dt = both(py, nd, lambda c: c.q(f"SELECT count(*) FROM {W} WHERE title LIKE 'after-%'")[0][0] == 2, 60)
            pg_after = zb.psql(f"SELECT count(*) FROM {W} WHERE title LIKE 'after-%'").strip()
            check(f"§5 writes in the NEW shape land on both, from both ({dt} s); PostgreSQL has {pg_after}", dt is not None)
            py.close(); nd.close(); py = nd = None
            teardown()
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

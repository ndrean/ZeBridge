"""A two-level cascade delete while the middle rows are HELD (NOTES §10dg, item 4).

Three tables, grandparent → parent → child, physical cascade deletes. The rows are
inserted CHILD FIRST inside one transaction (the FKs are DEFERRABLE INITIALLY DEFERRED,
so PostgreSQL allows it), which puts them in the WAL — and on the CDC stream — in the
order child, parent, grandparent: each replica must HOLD the child (no parent yet) and
the parent (no grandparent yet) in its inbox, then apply the grandparent and release
both. That is the hold path, deterministic.

Then the race: the same reverse insert and the grandparent's DELETE in ONE transaction,
so the cascade's deletes reach the replica while the child and the parent are still
held. A retry that replays a held INSERT after its DELETE went by would resurrect a
row PostgreSQL no longer has. Expected on both clients: nothing of that family exists,
the inbox is empty, `PRAGMA foreign_key_check` is clean. Finally an ordinary cascade
delete of an applied family, for the baseline.
"""
import sys, time, uuid
import zb
from clients import Lib, Node, both, fresh_sqlite

LOG = "/tmp/zb_cascade_held_bridge.log"
G, P, C = "mig_g", "mig_p2", "mig_c2"
PUB = zb.publication()
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now()")
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tiebreak_col => 'last_writer', "
          f"allow_physical_deletes => true, generations => true, publication => '{PUB}', dry_run => false")
PY_DB, NODE_DB = "/tmp/zb-cascade-held-py.sqlite3", "/tmp/zb-cascade-held-node.sqlite3"


def teardown():
    for sql in (f"DROP TABLE IF EXISTS public.{C}", f"DROP TABLE IF EXISTS public.{P}", f"DROP TABLE IF EXISTS public.{G}",
                f"DELETE FROM public.zebridge_catalogue WHERE tbl IN ('{G}','{P}','{C}')",
                f"DELETE FROM public.zebridge_generations WHERE tbl IN ('{G}','{P}','{C}')"):
        zb.psql(sql, quiet=True)


def family(tenant):
    g, p, c = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    # child first, then parent, then grandparent — legal only because the FKs are deferred
    sql = (f"INSERT INTO {C} (uid, p_id, note, tenant_id) VALUES ('{c}', '{p}', 'c', '{tenant}'); "
           f"INSERT INTO {P} (uid, g_id, name, tenant_id) VALUES ('{p}', '{g}', 'p', '{tenant}'); "
           f"INSERT INTO {G} (uid, name, tenant_id) VALUES ('{g}', 'g', '{tenant}');")
    return g, p, c, sql


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
    zb.psql(f"CREATE TABLE public.{G} (uid uuid PRIMARY KEY, name text, {COMMON})")
    zb.psql(f"CREATE TABLE public.{P} (uid uuid PRIMARY KEY, g_id uuid NOT NULL REFERENCES public.{G}(uid) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED, name text, {COMMON})")
    zb.psql(f"CREATE TABLE public.{C} (uid uuid PRIMARY KEY, p_id uuid NOT NULL REFERENCES public.{P}(uid) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED, note text, {COMMON})")
    fresh_sqlite(PY_DB); fresh_sqlite(NODE_DB)
    py = nd = None
    try:
        # RING_BUFFER_COUNT at its floor: a family larger than the ring is published as
        # several messages, which is how a hold crosses a batch boundary (§4, §5).
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5", RING_BUFFER_COUNT="1024") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            for t in (G, P, C):
                out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{t}', {ENABLE})")
                check(f"zebridge_enable({t}) live: {out.strip()[:50]}…", "error" not in out.lower())
            py = Lib(PY_DB, [G, P, C], "py-cascade-held")
            nd = Node(NODE_DB, "/tmp/zb_cascade_held_node.log")
            tenant = py.tenant
            counts = lambda c: tuple(c.q(f"SELECT count(*) FROM {t}")[0][0] for t in (G, P, C))
            inbox = lambda c, t: c.q(f"SELECT count(*) FROM {t}")[0][0]
            fk_bad = lambda c: len(c.q("PRAGMA foreign_key_check"))
            dt = both(py, nd, lambda c: counts(c) == (0, 0, 0), 150)
            check(f"§0 both clients up, three empty tables ({dt} s)", dt is not None)

            # ── 1. the hold path: child, parent, grandparent in that order ──
            g1, p1, c1, sql = family(tenant)
            zb.psql(f"BEGIN; {sql} COMMIT;")
            dt = both(py, nd, lambda c: counts(c) == (1, 1, 1) and fk_bad(c) == 0, 60)
            held_py = "missing-parent" in open("/tmp/zb_cascade_held.out").read() if False else None
            check(f"§1 reverse-order family applied on both after the holds released (py {counts(py)}, node {counts(nd)}; {dt} s); inboxes empty: py {inbox(py, '_zbz_inbox')}, node {inbox(nd, '_zebridge_inbox')}",
                  dt is not None and inbox(py, "_zbz_inbox") == 0 and inbox(nd, "_zebridge_inbox") == 0)

            # ── 2. the race: reverse insert AND the cascade delete in one transaction ──
            g2, p2, c2, sql = family(tenant)
            zb.psql(f"BEGIN; {sql} DELETE FROM {G} WHERE uid = '{g2}'; COMMIT;")
            pg = tuple(int(zb.psql(f"SELECT count(*) FROM {t} WHERE uid = '{u}'") or 0) for t, u in ((G, g2), (P, p2), (C, c2)))
            check(f"§2 PostgreSQL: the family was inserted and cascade-deleted in one transaction, nothing remains {pg}", pg == (0, 0, 0))
            def gone(c):
                return all(not c.q(f"SELECT 1 FROM {t} WHERE uid = ?", [u]) for t, u in ((G, g2), (P, p2), (C, c2)))
            # give the retry path every chance to resurrect: several polls, then look
            time.sleep(3); both(py, nd, lambda c: True, 3)
            ok2 = gone(py) and gone(nd) and inbox(py, "_zbz_inbox") == 0 and inbox(nd, "_zebridge_inbox") == 0 and fk_bad(py) == 0 and fk_bad(nd) == 0
            check(f"§2 no ghost, no orphan on either replica: family present py {tuple(bool(py.q(f'SELECT 1 FROM {t} WHERE uid = ?', [u])) for t, u in ((G, g2), (P, p2), (C, c2)))}, node {tuple(bool(nd.q(f'SELECT 1 FROM {t} WHERE uid = ?', [u])) for t, u in ((G, g2), (P, p2), (C, c2)))}; inboxes py {inbox(py, '_zbz_inbox')}, node {inbox(nd, '_zebridge_inbox')}; fk violations py {fk_bad(py)}, node {fk_bad(nd)}", ok2)

            # ── 3. baseline: cascade-delete the applied family ──
            zb.psql(f"DELETE FROM {G} WHERE uid = '{g1}'")
            dt = both(py, nd, lambda c: counts(c) == (0, 0, 0) and fk_bad(c) == 0, 60)
            check(f"§3 an ordinary two-level cascade delete empties all three on both (py {counts(py)}, node {counts(nd)}; {dt} s)", dt is not None)

            # ── 4. across BATCHES: 1500 children first, then the parent and grandparent ──
            # More rows than the ring holds, so the children arrive in messages of their
            # own — the deferred batch cannot help, the hold path must: children held,
            # parent held, grandparent applied, everything released in passes.
            g4, p4 = str(uuid.uuid4()), str(uuid.uuid4())
            zb.psql(f"""BEGIN;
                INSERT INTO {C} (uid, p_id, note, tenant_id) SELECT gen_random_uuid(), '{p4}', 'c' || g, '{tenant}' FROM generate_series(1, 1500) g;
                INSERT INTO {P} (uid, g_id, name, tenant_id) VALUES ('{p4}', '{g4}', 'p4', '{tenant}');
                INSERT INTO {G} (uid, name, tenant_id) VALUES ('{g4}', 'g4', '{tenant}');
                COMMIT;""")
            dt = both(py, nd, lambda c: counts(c) == (1, 1, 1500) and fk_bad(c) == 0, 120)
            check(f"§4 a 1500-child family, children first across several messages, applied on both (py {counts(py)}, node {counts(nd)}; {dt} s); inboxes py {inbox(py, '_zbz_inbox')}, node {inbox(nd, '_zebridge_inbox')}",
                  dt is not None and inbox(py, "_zbz_inbox") == 0 and inbox(nd, "_zebridge_inbox") == 0)

            # ── 5. the race across batches: the same, and the grandparent deleted in the same transaction ──
            g5, p5 = str(uuid.uuid4()), str(uuid.uuid4())
            zb.psql(f"""BEGIN;
                INSERT INTO {C} (uid, p_id, note, tenant_id) SELECT gen_random_uuid(), '{p5}', 'x' || g, '{tenant}' FROM generate_series(1, 1500) g;
                INSERT INTO {P} (uid, g_id, name, tenant_id) VALUES ('{p5}', '{g5}', 'p5', '{tenant}');
                INSERT INTO {G} (uid, name, tenant_id) VALUES ('{g5}', 'g5', '{tenant}');
                DELETE FROM {G} WHERE uid = '{g5}';
                COMMIT;""")
            dt = both(py, nd, lambda c: counts(c) == (1, 1, 1500) and inbox(c, "_zbz_inbox" if c is py else "_zebridge_inbox") == 0 and fk_bad(c) == 0, 120)
            check(f"§5 inserted and cascade-deleted across several messages: nothing of it remains, nothing held, no orphan (py {counts(py)}, node {counts(nd)}; inboxes py {inbox(py, '_zbz_inbox')}, node {inbox(nd, '_zebridge_inbox')}; {dt} s)", dt is not None)
            zb.psql(f"DELETE FROM {G} WHERE uid = '{g4}'")
            dt = both(py, nd, lambda c: counts(c) == (0, 0, 0) and fk_bad(c) == 0, 120)
            check(f"§6 the big family cascade-deleted, all three empty on both ({dt} s)", dt is not None)
            py.close(); nd.close(); py = nd = None
            teardown()
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

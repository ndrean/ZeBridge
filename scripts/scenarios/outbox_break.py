"""The application tries to break the client's own bookkeeping (NOTES §10di).

A client's replica file holds two kinds of tables: the data, which is the feed's, and
the bookkeeping — the outbox of writes awaiting a verdict, the stream positions, the
inbox of held events, the shape record. `query()` is the one API that runs SQL the
application wrote. libzb answers it on a second SQLite connection opened READONLY;
the TypeScript client does the same on Node (a second better-sqlite3 handle) and
guards by statement shape where there is one handle. This scenario is the attack:
with the bridge down and one write queued in each outbox, every write anyone could
type — on the outbox, the positions, the shape record, the inbox, a data table, a
pragma, a CTE feeding DML, a second statement after a semicolon — must be refused on
both clients, reads must keep working, and when the bridge returns the queued write
must still be there to land in PostgreSQL.
"""
import sys, time, uuid
import zb
from clients import Lib, Node, both, fresh_sqlite

LOG, LOG2 = "/tmp/zb_outbox_break_bridge.log", "/tmp/zb_outbox_break_bridge2.log"
T = "mig_ob"
PUB = zb.publication()
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")
PY_DB, NODE_DB = "/tmp/zb-outbox-break-py.sqlite3", "/tmp/zb-outbox-break-node.sqlite3"

ATTACKS = [
    ("DELETE FROM _zebridge_outbox",                                   "delete the outbox"),
    ("UPDATE _zebridge_outbox SET attempts = 99",                      "poison the outbox"),
    ("DELETE FROM {seq}",                                               "erase the stream positions"),
    ("UPDATE {shape} SET key_shape = '[]'",                             "forge the shape record"),
    ("DROP TABLE {inbox}",                                              "drop the inbox"),
    ("INSERT INTO {T} (uid, title, tenant_id) VALUES ('x', 'ghost', 'k')", "write a data table directly"),
    ("PRAGMA foreign_keys = OFF",                                       "switch a semantic pragma"),
    ("WITH x AS (SELECT 1) DELETE FROM _zebridge_outbox",              "hide DML behind a CTE"),
    ("SELECT 1; DELETE FROM _zebridge_outbox",                         "smuggle a second statement"),
    ("ATTACH DATABASE '/tmp/zb-evil.sqlite3' AS evil",                 "attach another file"),
]


def teardown():
    for sql in (f"DROP TABLE IF EXISTS public.{T}", f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'",
                f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'"):
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
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), title text, {COMMON})")
    fresh_sqlite(PY_DB); fresh_sqlite(NODE_DB)
    py = nd = None
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE})")
            check(f"zebridge_enable({T}) live: {out.strip()[:50]}…", "error" not in out.lower())
            py = Lib(PY_DB, [T], "py-outbox-break")
            nd = Node(NODE_DB, "/tmp/zb_outbox_break_node.log")
            tenant = py.tenant
            dt = both(py, nd, lambda c: c.q(f"SELECT count(*) FROM {T}")[0][0] == 0, 150)
            check(f"§0 both clients up on an empty table ({dt} s)", dt is not None)

        # ── bridge DOWN: one write queued in each outbox ──
        u_py, u_nd = str(uuid.uuid4()), str(uuid.uuid4())
        py.mutate(T, "INSERT", {"uid": u_py}, {"uid": u_py, "title": "queued-py", "tenant_id": tenant}); f = py.flush(2000)
        nd.mutate(T, "INSERT", {"uid": u_nd}, {"uid": u_nd, "title": "queued-node", "tenant_id": tenant})
        time.sleep(1)
        outbox = lambda c: c.q("SELECT count(*) FROM _zebridge_outbox")[0][0]
        check(f"§1 one write queued in each outbox, no verdict (bridge down): py {outbox(py)}, node {outbox(nd)}", outbox(py) == 1 and outbox(nd) == 1)

        names = {"py": {"seq": "_zbz_stream_seq", "shape": "_zbz_shape", "inbox": "_zbz_inbox"},
                 "node": {"seq": "_zebridge_stream_seq", "shape": "_zebridge_shape", "inbox": "_zebridge_inbox"}}
        for who, c in (("py", py), ("node", nd)):
            refused, landed = [], []
            for sql, what in ATTACKS:
                stmt = sql.format(T=T, **names[who])
                try:
                    c.q(stmt)
                    landed.append(what)
                except Exception as e:
                    refused.append((what, str(e)[:60]))
            check(f"§2 {who}: {len(refused)}/{len(ATTACKS)} attacks refused" + (f"; LANDED: {landed}" if landed else "") + f"; e.g. '{refused[0][0]}' → {refused[0][1]}" if refused else f"§2 {who}: nothing refused",
                  not landed)
        check(f"§2 the outboxes survived every attempt: py {outbox(py)}, node {outbox(nd)}; positions kept: py {py.q('SELECT count(*) FROM _zbz_stream_seq')[0][0]}, node {nd.q('SELECT count(*) FROM _zebridge_stream_seq')[0][0]}",
              outbox(py) == 1 and outbox(nd) == 1 and py.q("SELECT count(*) FROM _zbz_stream_seq")[0][0] > 0 and nd.q("SELECT count(*) FROM _zebridge_stream_seq")[0][0] > 0)
        reads_ok = True
        for c in (py, nd):
            try:
                c.q(f"SELECT count(*) FROM {T}"); c.q(f"PRAGMA table_info('{T}')"); c.q("PRAGMA foreign_key_check")
                c.q(f"WITH x AS (SELECT count(*) AS n FROM {T}) SELECT n FROM x"); c.q("SELECT 1 WHERE 'delete me' = 'delete me'")
            except Exception as e:
                reads_ok = False; print(f"  read refused: {e}")
        check("§3 reads still answer on both: SELECT, PRAGMA table_info, PRAGMA foreign_key_check, a CTE, a literal containing a write word", reads_ok)

        # ── bridge back: the queued writes land ──
        with zb.Bridge(LOG2, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge2:
            if not bridge2.wait_for_log("Generation producer started", timeout=30):
                zb.bad("restarted bridge never started its producer"); print(bridge2.text()[-1500:]); return 1
            dt = both(py, nd, lambda c: outbox(c) == 0, 60)
            pg = zb.psql(f"SELECT string_agg(title, ',' ORDER BY title) FROM {T}").strip()
            check(f"§4 both queued writes reached PostgreSQL once the bridge returned ({dt} s): {pg}", pg == "queued-node,queued-py")
            dt = both(py, nd, lambda c: c.q(f"SELECT count(*) FROM {T}")[0][0] == 2, 60)
            check(f"§4 and both replicas hold both rows ({dt} s)", dt is not None)
            py.close(); nd.close(); py = nd = None
            teardown()
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

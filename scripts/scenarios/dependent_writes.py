"""A rejected write and the writes queued after it.

Each mutation is judged alone, in its own PostgreSQL transaction: a `rejected` verdict
costs that write and nothing else. A write that depended on it is not cancelled by the
client; it is sent and judged on its own. Here: a parent INSERT refused by a server-side
trigger (SQLSTATE 23514, which no replica can foresee), then a child INSERT referencing
that parent, then an unrelated parent and its child.

Both clients (libzb, zb-client-ts in Node):

  §1  the refused parent → `rejected` (23514), not in PostgreSQL
  §2  its child → `rejected` by the foreign key (23503), not in PostgreSQL
  §3  the unrelated parent and child, written after both → applied
  §4  the replica: the refused parent and its child are gone, the others there, outbox empty
  §5  a rejected INSERT, then a sparse UPDATE of that row (`row_deleted`), leave no optimistic trace
  §6  physical local CASCADE, with parent/child SQL gated independently
  §7  missing accepted baseline recovers after restart, with no further writes
"""
import json, os, re, shlex, sqlite3, subprocess, sys, time, uuid
import zb
from datetime import datetime
from clients import Lib, Node, both, fresh_sqlite

LOG = "/tmp/zb_dependent_writes_bridge.log"
NODE_LOG = "/tmp/zb_dependent_writes_node.log"
P, C = "dw_parent", "dw_child"
PUB = zb.publication()
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")
REFUSED = "refuse-me"
CASCADE_NOTE = "hold-cascade-child"
LOCK_CLASS = 1_800_000 + os.getpid()


def teardown():
    zb.forget_table(C, P)
    for sql in (f"DROP TABLE IF EXISTS public.{C}", f"DROP TABLE IF EXISTS public.{P}",
                "DROP FUNCTION IF EXISTS public.dw_refuse()", "DROP FUNCTION IF EXISTS public.dw_pause_child()",
                f"DELETE FROM public.zebridge_catalogue WHERE tbl IN ('{P}','{C}')",
                f"DELETE FROM public.zebridge_generations WHERE tbl IN ('{P}','{C}')"):
        zb.psql(sql, quiet=True)


def node_verdicts():
    out = []
    for line in open(NODE_LOG):
        m = re.search(r"\[VERDICT-API\] (\{.*\})", line)
        if m:
            try: out.append(json.loads(m.group(1)))
            except json.JSONDecodeError: pass
    return out


class VerdictGate:
    """Hold parent and child SQL independently, rather than race their verdicts."""
    def __enter__(self):
        self.p = subprocess.Popen(shlex.split(zb.PSQL) + ["-X", "-q", "-v", "ON_ERROR_STOP=1"],
                                  stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        self.p.stdin.write(f"SELECT pg_advisory_lock({LOCK_CLASS}, 1); SELECT pg_advisory_lock({LOCK_CLASS}, 2);\n")
        self.p.stdin.flush()
        until = time.monotonic() + 10
        while time.monotonic() < until:
            if self.p.poll() is not None: break
            held = zb.psql(f"SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid={LOCK_CLASS} AND objsubid=2 AND objid IN (1,2) AND granted")
            if held == "2": return self
            time.sleep(0.05)
        self.__exit__(None, None, None)
        raise RuntimeError("could not establish both verdict gates")
    def wait_for_parent(self):
        # Prove the parent batch is already executing before the child is published.
        # Otherwise both could share a batch, whose child gate would delay both verdicts.
        until = time.monotonic() + 10
        while time.monotonic() < until:
            waiting = zb.psql(f"SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid={LOCK_CLASS} AND objsubid=2 AND objid=1 AND NOT granted")
            if waiting.isdigit() and int(waiting) > 0: return
            time.sleep(0.05)
        raise RuntimeError("parent SQL did not enter its gate before the child was queued")
    def release(self, which):
        self.p.stdin.write(f"SELECT pg_advisory_unlock({LOCK_CLASS}, {which});\n"); self.p.stdin.flush()
    def __exit__(self, *_):
        if self.p.poll() is None:
            self.p.stdin.write("\\q\n"); self.p.stdin.flush()
            try: self.p.wait(timeout=5)
            except subprocess.TimeoutExpired: self.p.kill(); self.p.wait()
        self.p.stdin.close()


def local_cascade(path):
    # Descriptors use NO ACTION. Install the defensive physical-CASCADE fixture while
    # idle in one SQLite transaction; this is not a second synchronization client.
    with sqlite3.connect(path, timeout=10) as db:
        db.execute("PRAGMA foreign_keys=OFF")
        ddl = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (C,)).fetchone()[0]
        ddl, n = re.subn(rf'(REFERENCES\s+"?{P}"?\s*\([^)]*\))', r'\1 ON DELETE CASCADE', ddl, flags=re.I)
        if n != 1: raise RuntimeError("expected one local parent FK")
        ddl, n = re.subn(rf'^CREATE TABLE\s+"?{C}"?', 'CREATE TABLE "dw_child_cascade_stage"', ddl, count=1, flags=re.I)
        if n != 1: raise RuntimeError("could not name the local CASCADE fixture")
        view = db.execute("SELECT sql FROM sqlite_master WHERE type='view' AND name=?", (C + '_view',)).fetchone()
        db.execute("BEGIN IMMEDIATE")
        if view: db.execute(f'DROP VIEW "{C}_view"')
        db.execute(ddl)
        db.execute(f'INSERT INTO dw_child_cascade_stage SELECT * FROM "{C}"')
        db.execute(f'DROP TABLE "{C}"')
        db.execute(f'ALTER TABLE dw_child_cascade_stage RENAME TO "{C}"')
        if view: db.execute(view[0])
        db.commit()
        if not any(r[6] == 'CASCADE' for r in db.execute(f'PRAGMA foreign_key_list("{C}")')):
            raise RuntimeError("the local CASCADE fixture was not installed")


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
    zb.psql(f"CREATE TABLE public.{P} (uid uuid PRIMARY KEY, title text, {COMMON})")
    zb.psql(f"CREATE TABLE public.{C} (uid uuid PRIMARY KEY, parent_id uuid NOT NULL REFERENCES public.{P}(uid), note text, {COMMON})")
    # Server-side only: a trigger is not part of any replica's schema, so the client
    # applies the row optimistically and only PostgreSQL refuses it.
    zb.psql(f"""CREATE FUNCTION public.dw_refuse() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.title = '{REFUSED}' THEN
        PERFORM pg_advisory_xact_lock({LOCK_CLASS}, 1);
        RAISE EXCEPTION 'dw_refuse: this title is refused' USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$""")
    zb.psql(f"CREATE TRIGGER dw_refuse_t BEFORE INSERT OR UPDATE ON public.{P} FOR EACH ROW EXECUTE FUNCTION public.dw_refuse()")
    zb.psql(f"""CREATE FUNCTION public.dw_pause_child() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.note = '{CASCADE_NOTE}' THEN PERFORM pg_advisory_xact_lock({LOCK_CLASS}, 2); END IF;
    RETURN NEW;
END $$""")
    zb.psql(f"CREATE TRIGGER dw_pause_child_t BEFORE INSERT ON public.{C} FOR EACH ROW EXECUTE FUNCTION public.dw_pause_child()")
    for db in ("/tmp/zb-dw-py.sqlite3", "/tmp/zb-dw-node.sqlite3"): fresh_sqlite(db)
    py = nd = None
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            for t in (P, C):
                out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{t}', {ENABLE})")
                check(f"zebridge_enable({t}): {out.strip()[:50]}…", "error" not in out.lower())
            py = Lib("/tmp/zb-dw-py.sqlite3", [P, C], "py-dependent-writes")
            nd = Node("/tmp/zb-dw-node.sqlite3", NODE_LOG)
            pg_has = lambda t, u: zb.psql(f"SELECT count(*) FROM {t} WHERE uid = '{u}'").strip() == "1"
            # Write only once both tables are seeded: a full that lands later would replace the rows under test.
            anchors = lambda c: c.q(f"SELECT count(*) FROM {'_zbz_generations' if c is py else '_zebridge_generations'} "
                                    f"WHERE tbl IN ('{P}', '{C}')")[0][0] == 2
            dt = both(py, nd, anchors, 90)
            check(f"both clients seeded {P} and {C} ({dt} s)", dt is not None)
            if dt is None: return failed

            for name, cl in (("libzb", py), ("zb-client-ts", nd)):
                bad_p, bad_c, ok_p, ok_c = (str(uuid.uuid4()) for _ in range(4))
                tenant = py.tenant
                # Four writes back to back, in this order, before any verdict.
                writes = [
                    (P, {"uid": bad_p}, {"uid": bad_p, "title": REFUSED, "tenant_id": tenant}),
                    (C, {"uid": bad_c}, {"uid": bad_c, "parent_id": bad_p, "note": "child of the refused parent", "tenant_id": tenant}),
                    (P, {"uid": ok_p}, {"uid": ok_p, "title": "fine", "tenant_id": tenant}),
                    (C, {"uid": ok_c}, {"uid": ok_c, "parent_id": ok_p, "note": "child of the fine parent", "tenant_id": tenant}),
                ]
                ids = []
                for t, key, vals in writes:
                    r = cl.mutate(t, "INSERT", key, vals)
                    ids.append(r.get("msgId") if isinstance(r, dict) else None)
                local_before = [cl.q(f"SELECT count(*) FROM {t} WHERE uid = ?", [k["uid"]])[0][0] for t, k, _ in writes]
                check(f"{name}: all four rows applied optimistically on the replica first: {local_before}", local_before == [1, 1, 1, 1])

                # Wait until every write has its outcome.
                t0 = time.monotonic()
                outcomes = {}
                while time.monotonic() - t0 < 30:
                    if cl is py:
                        py.flush(500); py.poll()
                        outcomes = {i: py.outcome(i) for i in ids if py.outcome(i)}
                    else:
                        mine = {k["uid"] for _, k, _ in writes}
                        outcomes = {}
                        for v in node_verdicts():
                            uid = (v.get("data") or {}).get("uid") or (v.get("key") or {}).get("uid")
                            if uid in mine: outcomes[uid] = v
                        time.sleep(0.3)
                    if len(outcomes) == 4: break
                by_uid = {}
                for (t, k, _), i in zip(writes, ids):
                    by_uid[k["uid"]] = outcomes.get(i) if cl is py else outcomes.get(k["uid"])
                print(f"   {name} outcomes:")
                for (t, k, _) in writes:
                    o = by_uid[k["uid"]] or {}
                    print(f"     {t:10} {k['uid'][:8]}  {o.get('outcome')}  sqlstate={o.get('sqlstate')}  reason={o.get('reason')}  detail={str(o.get('detail'))[:90]}")
                ob, oc, op_, oc2 = (by_uid[k["uid"]] or {} for _, k, _ in writes)
                check(f"§1 {name}: the refused parent → {ob.get('outcome')} ({ob.get('sqlstate')}), not in PostgreSQL",
                      ob.get("outcome") == "rejected" and ob.get("sqlstate") == "23514" and not pg_has(P, bad_p))
                check(f"§2 {name}: its child → {oc.get('outcome')} ({oc.get('sqlstate')}), not in PostgreSQL",
                      oc.get("outcome") == "rejected" and oc.get("sqlstate") == "23503" and not pg_has(C, bad_c))
                check(f"§3 {name}: the unrelated parent and child, sent after → {op_.get('outcome')}, {oc2.get('outcome')}; in PostgreSQL",
                      op_.get("outcome") == "applied" and oc2.get("outcome") == "applied" and pg_has(P, ok_p) and pg_has(C, ok_c))

                # The replica settles: reverts done, the echo of the applied rows in.
                t0 = time.monotonic()
                expect = [0, 0, 1, 1]
                local = None
                while time.monotonic() - t0 < 20:
                    if cl is py: py.poll()
                    local = [cl.q(f"SELECT count(*) FROM {t} WHERE uid = ?", [k["uid"]])[0][0] for t, k, _ in writes]
                    if local == expect: break
                    time.sleep(0.3)
                pending = cl.q("SELECT count(*) FROM _zebridge_outbox")[0][0]
                # An empty outbox is not a settled replica: a refused write's undo may still be owed.
                owed = cl.q(f"SELECT count(*) FROM {'_zbz_undo' if cl is py else '_zebridge_undo'}")[0][0]
                check(f"§4 {name}: replica [refused parent, its child, fine parent, fine child] = {local} (want {expect}); outbox {pending}, undos owed {owed}",
                      local == expect and pending == 0 and owed == 0)

                # §5 Two writes on ONE row: the refused INSERT, then a sparse UPDATE of it. The
                # INSERT's SQL is held so both optimistic writes are seen before any verdict.
                # Server verdicts: `rejected` (23514), then `row_deleted` (no row to update),
                # both reported as outcomes. Two `rejected`, and both orders, are unit tests.
                x = str(uuid.uuid4())
                undo = '_zbz_undo' if cl is py else '_zebridge_undo'
                with VerdictGate() as gate:
                    r = cl.mutate(P, "INSERT", {"uid": x}, {"uid": x, "title": REFUSED, "tenant_id": tenant})
                    ins_mid = r.get("msgId") if isinstance(r, dict) else None
                    gate.wait_for_parent()
                    r = cl.mutate(P, "UPDATE", {"uid": x}, {"title": "edited after"})
                    upd_mid = r.get("msgId") if isinstance(r, dict) else None
                    seen = cl.q(f"SELECT title FROM {P} WHERE uid = ?", [x])
                    check(f"§5 {name}: the INSERT and its UPDATE applied optimistically, INSERT held: {seen}",
                          seen == [["edited after"]] and not pg_has(P, x))
                    gate.release(1)
                    # Outcomes by write: libzb by msgId; the Node log by key and columns (the UPDATE sends only `title`).
                    t0 = time.monotonic(); ins = upd = None
                    while time.monotonic() - t0 < 30 and not (ins and upd):
                        if cl is py:
                            py.flush(500); py.poll(); ins, upd = py.outcome(ins_mid), py.outcome(upd_mid)
                        else:
                            for v in node_verdicts():
                                if (v.get("key") or {}).get("uid") != x: continue
                                if v.get("columns") == ["title"]: upd = v
                                else: ins = v
                            time.sleep(0.3)
                ins, upd = ins or {}, upd or {}
                check(f"§5 {name}: INSERT → {ins.get('outcome')} ({ins.get('sqlstate')}), UPDATE → {upd.get('outcome')}",
                      ins.get("outcome") == "rejected" and ins.get("sqlstate") == "23514" and upd.get("outcome") == "deleted")
                t0 = time.monotonic()
                while time.monotonic() - t0 < 30:
                    if cl is py: py.flush(500); py.poll()
                    else: time.sleep(0.3)
                    pending = cl.q("SELECT count(*) FROM _zebridge_outbox")[0][0]
                    owed = cl.q(f"SELECT count(*) FROM {undo}")[0][0]
                    here = cl.q(f"SELECT count(*) FROM {P} WHERE uid = ?", [x])[0][0]
                    if pending == 0 and owed == 0 and here == 0: break
                check(f"§5 {name}: no row left (replica {here}, PostgreSQL {int(pg_has(P, x))}); outbox {pending}, undos owed {owed}",
                      here == 0 and not pg_has(P, x) and pending == 0 and owed == 0)

            # §6 Force the parent verdict while the child's SQL still waits. A CASCADE
            # undo used to remove both rows here, before the child's independent verdict.
            for name, cl, path in (("libzb", py, "/tmp/zb-dw-py.sqlite3"), ("zb-client-ts", nd, "/tmp/zb-dw-node.sqlite3")):
                local_cascade(path)
                parent, child = str(uuid.uuid4()), str(uuid.uuid4())
                undo = '_zbz_undo' if cl is py else '_zebridge_undo'
                rows = lambda: [cl.q(f'SELECT count(*) FROM "{t}" WHERE uid=?', [uid])[0][0] for t, uid in ((P,parent),(C,child))]
                with VerdictGate() as gate:
                    sent = cl.mutate(P, "INSERT", {"uid": parent}, {"uid": parent, "title": REFUSED, "tenant_id": py.tenant})
                    parent_mid = sent.get('msgId') if isinstance(sent, dict) else None
                    gate.wait_for_parent()
                    cl.mutate(C, "INSERT", {"uid": child}, {"uid": child, "parent_id": parent, "note": CASCADE_NOTE, "tenant_id": py.tenant})
                    check(f"§6 {name}: CASCADE family applied before either verdict", rows() == [1,1])
                    gate.release(1)
                    t0 = time.monotonic(); outcome = None
                    while time.monotonic() - t0 < 30:
                        if cl is py:
                            py.poll(); outcome = py.outcome(parent_mid)
                        else:
                            outcome = next((v for v in node_verdicts() if ((v.get('data') or {}).get('uid') or (v.get('key') or {}).get('uid')) == parent), None)
                        if outcome: break
                        time.sleep(0.05)
                    local = rows()
                    owed = cl.q(f"SELECT count(*) FROM {undo} WHERE mode <> 'accepted'")[0][0]
                    left = cl.q("SELECT count(*) FROM _zebridge_outbox")[0][0]
                    check(f"§6 {name}: parent rejected, child still undecided: rows {local}, outbox {left}, undo {owed}",
                          bool(outcome) and outcome.get('outcome') == 'rejected' and local == [1,1] and left == 1 and owed == 1)
                    gate.release(2)
                t0 = time.monotonic()
                while time.monotonic() - t0 < 30:
                    if cl is py: py.poll()
                    else: time.sleep(0.1)
                    local = rows()
                    owed = cl.q(f"SELECT count(*) FROM {undo} WHERE mode <> 'accepted'")[0][0]
                    left = cl.q("SELECT count(*) FROM _zebridge_outbox")[0][0]
                    if local == [0,0] and owed == 0 and left == 0: break
                check(f"§6 {name}: both verdicts settle independently: rows {local}, outbox {left}, undo {owed}",
                      local == [0,0] and left == 0 and owed == 0 and not pg_has(P,parent) and not pg_has(C,child))

            # No later mutation or CDC can rescue this client by accident: establish
            # and drain the server row first, close both owners, then stage the durable
            # ledger produced by A refused / B accepted / C refused on an absent base.
            recovery_id = str(uuid.uuid4())
            zb.psql(f"INSERT INTO {P}(uid,title,tenant_id) VALUES ('{recovery_id}','server baseline','{py.tenant}')")
            ready = both(py, nd, lambda c: c.q(f"SELECT title FROM {P} WHERE uid='{recovery_id}'") == [['server baseline']], 30)
            check(f"§7 both clients saw the authoritative row before stopping ({ready} s)", ready is not None)
            tenant = py.tenant
            truth = json.loads(zb.psql(f"SELECT row_to_json(t) FROM {P} t WHERE uid='{recovery_id}'"))
            columns = ['uid','title','tenant_id','last_writer','inserted_at','updated_at','deleted_at']
            def normalized(values):
                return [datetime.fromisoformat(str(v).replace('Z','+00:00')) if v is not None and col.endswith('_at') else v
                        for col,v in zip(columns,values)]
            expected = normalized([truth[col] for col in columns])
            py.close(); nd.close(); py = nd = None
            for path, native in (("/tmp/zb-dw-py.sqlite3", True), ("/tmp/zb-dw-node.sqlite3", False)):
                undo = '_zbz_undo' if native else '_zebridge_undo'
                base = '_zbz_base' if native else '_zebridge_base'
                with sqlite3.connect(path, timeout=10) as db:
                    db.execute(f"UPDATE {P} SET title='C' WHERE uid=?", (recovery_id,))
                    for order, label, mode, title in ((1,'a','restore','A'), (2,'b','accepted','B'), (3,'c','restore','C')):
                        payload = json.dumps({'operation':'INSERT' if order == 1 else 'UPDATE', 'key':{'uid':recovery_id}, 'data':{'title':title}})
                        optimistic = json.dumps({'operation':'INSERT' if order == 1 else 'UPDATE', 'data':{'uid':recovery_id,'title':title}})
                        cols = 'msg_id,tbl,row_id,payload,before,optimistic,created_at,authoritative,mode,since'
                        vals = [f'baseline-{label}',P,recovery_id,payload,None,optimistic,order,1 if order == 1 else 0,mode,int(time.time()*1000)]
                        if native: cols += ',tenant'; vals.append(tenant)
                        db.execute(f"INSERT INTO {undo} ({cols}) VALUES ({','.join('?' for _ in vals)})", vals)
                    # A known absence is not the same thing as an unknown baseline.
                    if db.execute("SELECT 1 FROM sqlite_master WHERE name=?", (base,)).fetchone():
                        db.execute(f"INSERT INTO {base}(tbl,row_id,at,data) VALUES (?,?,1,NULL) ON CONFLICT(tbl,row_id) DO UPDATE SET at=1,data=NULL", (P,recovery_id))
                    check(f"§7 {'libzb' if native else 'TS'} staged: two undos, accepted B, empty outbox", db.execute("SELECT count(*) FROM _zebridge_outbox").fetchone()[0] == 0 and db.execute(f"SELECT count(*) FROM {undo} WHERE row_id=? AND mode <> 'accepted'", (recovery_id,)).fetchone()[0] == 2)
            # Reopen only. There are deliberately no server writes after the staging.
            py = Lib("/tmp/zb-dw-py.sqlite3", [P,C], "py-dependent-writes")
            nd = Node("/tmp/zb-dw-node.sqlite3", NODE_LOG + '.baseline-restart')
            def recovered(c):
                undo = '_zbz_undo' if c is py else '_zebridge_undo'
                reconcile = '_zbz_reconcile' if c is py else '_zebridge_reconcile'
                generations = '_zbz_generations' if c is py else '_zebridge_generations'
                return (c.q(f"SELECT title,tenant_id FROM {P} WHERE uid='{recovery_id}'") == [['server baseline',tenant]]
                        and c.q(f"SELECT count(*) FROM {undo} WHERE row_id='{recovery_id}'")[0][0] == 0
                        and c.q(f"SELECT count(*) FROM {reconcile}")[0][0] == 0
                        and c.q("SELECT count(*) FROM _zebridge_outbox")[0][0] == 0
                        and c.q(f"SELECT count(*) FROM {generations} WHERE tbl='{P}'")[0][0] > 0)
            dt = both(py, nd, recovered, 90)
            check(f"§7 restart with no further writes: both exact, anchored, no undo/recovery/outbox ({dt} s)", dt is not None)
            for cl, name in ((py,'libzb'), (nd,'TS')):
                rows = cl.q(f"SELECT {','.join(columns)} FROM {P} WHERE uid='{recovery_id}'")
                check(f"§7 {name}: every server column restored, including generated timestamps", bool(rows) and normalized(rows[0]) == expected)
            check("§7 server value unchanged throughout recovery", zb.psql(f"SELECT title FROM {P} WHERE uid='{recovery_id}'").strip() == 'server baseline')

            py.close(); nd.close(); py = nd = None
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

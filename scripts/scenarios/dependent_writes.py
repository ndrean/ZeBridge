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
"""
import json, re, sys, time, uuid
import zb
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


def teardown():
    zb.forget_table(C, P)
    for sql in (f"DROP TABLE IF EXISTS public.{C}", f"DROP TABLE IF EXISTS public.{P}",
                "DROP FUNCTION IF EXISTS public.dw_refuse()",
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
        RAISE EXCEPTION 'dw_refuse: this title is refused' USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$""")
    zb.psql(f"CREATE TRIGGER dw_refuse_t BEFORE INSERT OR UPDATE ON public.{P} FOR EACH ROW EXECUTE FUNCTION public.dw_refuse()")
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
                check(f"§4 {name}: replica [refused parent, its child, fine parent, fine child] = {local} (want {expect}); outbox {pending}",
                      local == expect and pending == 0)

            py.close(); nd.close(); py = nd = None
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

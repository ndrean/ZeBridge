"""Ordering modes per write: tail (default), after(writeId), independent.

A parent P is held in PostgreSQL by an application trigger. Then, from the same client:
B and C, each `after(P)`, which queue behind P on its lane (routed by client); and,
once they are observably queued there and another lane is empty, I, `independent`,
which has no predecessor and goes to that other lane.

  §1  I is accepted while P is still held, and B, C have no rows
  §2  P released: P, B and C are accepted, each exactly once (one receipt each)
  §3  the replica's outbox is empty, and no write of the sequence was refused

Both clients (libzb through its C ABI, zb-client-ts in Node), one after the other, on a
probe bridge with two ingress lanes. The promise tested is that `independent` has no
predecessor, under a known routing condition; it is not a latency bound.
"""
import json, os, re, shlex, subprocess, sys, time, urllib.request, uuid
import zb
from clients import Lib, Node, both, fresh_sqlite

LOG = "/tmp/zb_ordering_modes_bridge.log"
NODE_LOG = "/tmp/zb_ordering_modes_node.log"
T = "om_rows"
PUB = zb.publication()
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"generations => true, publication => '{PUB}', dry_run => false")
HOLD = "hold-parent"
LOCK_CLASS = 1_900_000 + os.getpid()


def teardown():
    zb.forget_table(T)
    for sql in (f"DROP TABLE IF EXISTS public.{T}", "DROP FUNCTION IF EXISTS public.om_hold()",
                f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'",
                f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'"):
        zb.psql(sql, quiet=True)


class Gate:
    """A session holding an advisory lock that the trigger waits on for the parent's row."""
    def __enter__(self):
        self.p = subprocess.Popen(shlex.split(zb.PSQL) + ["-X", "-q", "-v", "ON_ERROR_STOP=1"],
                                  stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        self.p.stdin.write(f"SELECT pg_advisory_lock({LOCK_CLASS}, 1);\n"); self.p.stdin.flush()
        if not until(lambda: zb.psql(f"SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid={LOCK_CLASS} AND objid=1 AND granted") == "1", 10):
            self.__exit__(); raise RuntimeError("could not take the parent's gate")
        return self
    def parent_waiting(self):
        return zb.psql(f"SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid={LOCK_CLASS} AND objid=1 AND NOT granted") == "1"
    def release(self):
        if self.p.poll() is None:
            self.p.stdin.write(f"SELECT pg_advisory_unlock({LOCK_CLASS}, 1);\n"); self.p.stdin.flush()
    def __exit__(self, *_):
        if self.p.poll() is None:
            self.p.stdin.write("\\q\n"); self.p.stdin.flush()
            try: self.p.wait(timeout=5)
            except subprocess.TimeoutExpired: self.p.kill(); self.p.wait()
        self.p.stdin.close()


def until(pred, budget):
    end = time.monotonic() + budget
    while time.monotonic() < end:
        if pred(): return True
        time.sleep(0.05)
    return False


def queued():
    """`bridge_ingress_queued{lane="n"}` of the probe bridge: lane → depth."""
    text = urllib.request.urlopen(f"http://127.0.0.1:{zb.bridge_port()}/metrics", timeout=5).read().decode()
    return {int(m.group(1)): int(float(m.group(2))) for m in re.finditer(r'^bridge_ingress_queued\{lane="(\d+)"\} (\S+)$', text, re.M)}


def main():
    failed = 0
    def check(label, cond):
        nonlocal failed
        (zb.ok if cond else zb.bad)(label)
        if not cond: failed += 1
        return cond

    teardown()
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY, title text, tenant_id varchar(255) NOT NULL, "
            "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)")
    zb.psql(f"""CREATE FUNCTION public.om_hold() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.title = '{HOLD}' THEN PERFORM pg_advisory_xact_lock({LOCK_CLASS}, 1); END IF;
    RETURN NEW;
END $$""")
    zb.psql(f"CREATE TRIGGER om_hold_t BEFORE INSERT ON public.{T} FOR EACH ROW EXECUTE FUNCTION public.om_hold()")
    for db in ("/tmp/zb-om-py.sqlite3", "/tmp/zb-om-node.sqlite3"): fresh_sqlite(db)
    py = nd = None
    try:
        with zb.Bridge(LOG, ZB_INGRESS_LANES="2", GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE})")
            check(f"zebridge_enable({T}): {out.strip()[:50]}…", "error" not in out.lower())
            py = Lib("/tmp/zb-om-py.sqlite3", [T], "py-ordering-modes")
            nd = Node("/tmp/zb-om-node.sqlite3", NODE_LOG)
            seeded = lambda c: c.q(f"SELECT count(*) FROM {'_zbz_generations' if c is py else '_zebridge_generations'} WHERE tbl = '{T}'")[0][0] == 1
            dt = both(py, nd, seeded, 90)
            if not check(f"both clients seeded {T} ({dt} s)", dt is not None): return failed
            pg_rows = lambda *uids: int(zb.psql(f"SELECT count(*) FROM public.{T} WHERE uid IN ({','.join(repr(u) for u in uids)})"))
            receipts = lambda mid: zb.psql(f"SELECT count(*) || ':' || coalesce(min(outcome->>'status'), '') FROM zebridge_private.receipts WHERE message_id = '{mid}'")

            for name, cl in (("libzb", py), ("zb-client-ts", nd)):
                tenant = py.tenant
                p, b, c, i = (str(uuid.uuid4()) for _ in range(4))
                row = lambda uid, title: ({"uid": uid}, {"uid": uid, "title": title, "tenant_id": tenant})
                write = (lambda uid, title, **kw: cl.mutate(T, "INSERT", *row(uid, title), **kw)["msgId"]) if cl is py else \
                        (lambda uid, title, **kw: cl.mutate(T, "INSERT", *row(uid, title), with_id=True, **kw)["writeId"])
                with Gate() as gate:
                    try:
                        P = write(p, HOLD)
                        if cl is py: py.flush(0)
                        check(f"{name}: the parent is held in its trigger", until(gate.parent_waiting, 20))
                        B = write(b, "child b", after=P)
                        C = write(c, "child c", after=P)
                        if cl is py: py.flush(0)
                        # The routing condition: B and C queued behind P on its lane, another lane empty.
                        lanes = {}
                        def condition():
                            nonlocal lanes
                            lanes = queued()
                            return any(d >= 2 for d in lanes.values()) and any(d == 0 for d in lanes.values())
                        held = until(condition, 20)
                        if not check(f"{name}: B and C queued behind P, another lane empty (queued per lane: {lanes})", held):
                            continue
                        write(i, "independent", independent=True)
                        if cl is py: py.flush(0)
                        check(f"{name}: §1 the independent write is accepted while P is held", until(lambda: pg_rows(i) == 1, 20))
                        check(f"{name}: §1 B and C have no rows yet", pg_rows(p, b, c) == 0)
                    finally:
                        gate.release()
                check(f"{name}: §2 P released: P, B and C applied", until(lambda: pg_rows(p, b, c) == 3, 30))
                for label, mid in (("P", P), ("B", B), ("C", C)):
                    check(f"{name}: §2 {label} has exactly one receipt, accepted ({receipts(mid)})", receipts(mid) == "1:accepted")
                def drained():
                    if cl is py: py.flush(0); py.poll(200)
                    return cl.q("SELECT count(*) FROM _zebridge_outbox")[0][0] == 0
                check(f"{name}: §3 the replica's outbox is empty", until(drained, 30))
            return failed
    finally:
        for cl in (py, nd):
            try:
                if cl: cl.close()
            except Exception: pass
        teardown()


if __name__ == "__main__":
    sys.exit(main())

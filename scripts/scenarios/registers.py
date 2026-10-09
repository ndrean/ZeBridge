"""Register columns merged by PostgreSQL (NOTES §10lz).

A jsonb column of registers {v, t, w} (COOPERATIVE_EDITING.md), two editors. Carol goes
offline; Alice, online, raises `rating`; Carol, still offline, changes `access` — her
write is built on the doc she saw before going offline, and stamped AFTER Alice's. On
reconnect her write is the newest, so the row's last-writer-wins accepts it whole.

  A. without the merge (zebridge_enable re-run without register_cols): Carol's doc replaces the row's, and
     Alice's rating goes back to Carol's stale copy — accepted, no verdict says so;
  B. with `register_cols => ARRAY['doc']` (zebridge_enable): PostgreSQL merges the write
     register by register — Carol's access, Alice's rating, both kept.

Alice is libzb, Carol is zb-client-ts (she has the socket switch). Before both, the 8 `mergeRegisters` fixtures run through zebridge_merge_registers. What must hold, in B:
PostgreSQL and both replicas at {access: Carol's, rating: Alice's}, Carol's write
reported `applied`, both outboxes empty. A is the control: it shows what B prevents.
"""
import json, sys, time, uuid
from datetime import datetime, timezone
import zb
from clients import Lib, Node, both, fresh_sqlite

LOG = "/tmp/zb_registers_bridge.log"
NODE_LOG = "/tmp/zb_registers_node.log"
T = "mig_reg"
PUB = zb.publication()
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
REGS = "register_cols => ARRAY['doc']::name[], "
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', {REGS}generations => true, "
          f"publication => '{PUB}', dry_run => false")
PY_DB, NODE_DB = "/tmp/zb-registers-py.sqlite3", "/tmp/zb-registers-node.sqlite3"


def stamp():
    """A register stamp: RFC 3339, UTC, six fractional digits (COOPERATIVE_EDITING.md)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def reg(v, w):
    return {"v": v, "t": stamp(), "w": w}


def merge(a, b):
    """mergeRegisters, for building each editor's write the way a client does."""
    out = dict(a)
    for k, r in b.items():
        cur = out.get(k)
        if cur is None or (r.get("t", ""), r.get("w", "")) > (cur.get("t", ""), cur.get("w", "")):
            out[k] = r
    return out


def teardown():
    zb.forget_table(T)
    for sql in (f"DROP TABLE IF EXISTS public.{T}", f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'",
                f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'"):
        zb.psql(sql, quiet=True)


def merge_on():
    return zb.psql(f"SELECT count(*) FROM pg_trigger WHERE tgrelid = 'public.{T}'::regclass "
                   "AND tgname = 'zebridge_merge_registers_t'").strip() == "1"


def values(doc):
    return {k: r.get("v") for k, r in sorted(doc.items())}


def pg_doc(uid):
    out = zb.psql(f"SELECT doc::text FROM {T} WHERE uid = '{uid}'").strip()
    return json.loads(out) if out else {}


def local_doc(c, uid):
    r = c.q(f"SELECT doc FROM {T} WHERE uid = ?", [uid])
    if not r: return None
    d = r[0][0]
    return json.loads(d) if isinstance(d, str) else d


def wait_pg(uid, want, budget=30):
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        if values(pg_doc(uid)) == want: return round(time.monotonic() - t0, 2)
        time.sleep(0.2)
    return None


def verdict(version, timeout=30):
    end = time.time() + timeout
    while time.time() < end:
        for line in open(NODE_LOG):
            if line.startswith("[VERDICT-API] "):
                v = json.loads(line[len("[VERDICT-API] "):])
                if v.get("version") == version: return v
        time.sleep(0.2)
    return None


def main():
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")
    failed = 0

    def check(label, ok):
        nonlocal failed
        (zb.ok if ok else zb.bad)(label)
        if not ok: failed += 1

    # The SQL merge is mergeRegisters: the same fixtures the two client libraries answer.
    fx = json.load(open(zb.ROOT / "zb-client-ts" / "fixtures" / "core-fixtures.json"))["mergeRegisters"]
    for c in fx:
        q = lambda d: json.dumps(d).replace("'", "''")
        got = zb.psql(f"SELECT public.zebridge_merge_registers('{q(c['a'])}'::jsonb, '{q(c['b'])}'::jsonb) = '{q(c['want'])}'::jsonb").strip()
        check(f"fixture mergeRegisters, in SQL: {c['name']}", got == "t")

    teardown()
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), doc jsonb NOT NULL DEFAULT '{{}}'::jsonb, {COMMON})")
    fresh_sqlite(PY_DB); fresh_sqlite(NODE_DB)
    alice = carol = None
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE})")
            check(f"zebridge_enable({T}, register_cols => doc): {out.strip()[:90]}…", "registers:done" in out and "error" not in out.lower())
            alice = Lib(PY_DB, [T], "py-registers")
            carol = Node(NODE_DB, NODE_LOG)
            tenant = alice.tenant

            def run(label, uid):
                """The sequence, on a fresh row: both see {access 4424, rating 3}; Carol
                goes offline; Alice sets rating 4; Carol sets access 4425 from her copy."""
                seed = {"access": reg("4424", "alice"), "rating": reg(3, "alice")}
                zb.psql(f"INSERT INTO {T} (uid, doc, tenant_id) VALUES ('{uid}', '{json.dumps(seed)}'::jsonb, '{tenant}')")
                dt = both(alice, carol, lambda c: values(local_doc(c, uid) or {}) == {"access": "4424", "rating": 3}, 120)
                check(f"{label} both replicas hold {{access 4424, rating 3}} ({dt} s)", dt is not None)
                carol.disconnect()
                carols_copy = local_doc(carol, uid)
                time.sleep(0.05)
                alice.mutate(T, "UPDATE", {"uid": uid}, {"doc": merge(local_doc(alice, uid), {"rating": reg(4, "alice")})})
                alice.flush(5000)
                dt = wait_pg(uid, {"access": "4424", "rating": 4})
                check(f"{label} Alice's rating 4 landed while Carol is offline ({dt} s)", dt is not None)
                time.sleep(0.05)
                v = carol.mutate(T, "UPDATE", {"uid": uid}, {"doc": merge(carols_copy, {"access": reg("4425", "carol")})})["version"]
                carol.connect()
                return v

            # ── A. the control: no merge, the row's last-writer-wins alone ──
            out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE.replace(REGS, '')})")
            check(f"zebridge_enable({T}) without register_cols turns the merge off: {out.strip()[:90]}…", "registers:done" in out and "error" not in out.lower() and not merge_on())
            uid_a = str(uuid.uuid4())
            v = run("§A", uid_a)
            dt = wait_pg(uid_a, {"access": "4425", "rating": 3})
            check(f"§A without the merge, Carol's whole doc wins: PostgreSQL {values(pg_doc(uid_a))} — Alice's rating 4 rolled back ({dt} s)", dt is not None)
            vd = verdict(v)
            check(f"§A ...and Carol's write is reported applied, so nobody is told: {vd and vd['outcome']}", vd is not None and vd["outcome"] == "applied")

            # ── B. the merge, as zebridge_enable installs it ──
            out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE})")
            check(f"zebridge_enable({T}, register_cols => doc) turns it back on", "registers:done" in out and merge_on())
            uid_b = str(uuid.uuid4())
            v = run("§B", uid_b)
            want = {"access": "4425", "rating": 4}
            dt = wait_pg(uid_b, want)
            check(f"§B with the merge: PostgreSQL {values(pg_doc(uid_b))} — Carol's access AND Alice's rating ({dt} s)", dt is not None)
            doc = pg_doc(uid_b)
            check(f"§B each register keeps its writer: access by {doc.get('access', {}).get('w')}, rating by {doc.get('rating', {}).get('w')}",
                  doc.get("access", {}).get("w") == "carol" and doc.get("rating", {}).get("w") == "alice")
            vd = verdict(v)
            check(f"§B Carol's write reported applied: {vd and vd['outcome']}", vd is not None and vd["outcome"] == "applied")
            dt = both(alice, carol, lambda c: values(local_doc(c, uid_b) or {}) == want, 60)
            check(f"§B both replicas at {want} ({dt} s): alice {values(local_doc(alice, uid_b) or {})}, carol {values(local_doc(carol, uid_b) or {})}", dt is not None)
            check(f"§B Carol's outbox empty: pending() = {carol.pending()}", carol.pending() == 0)

            # ── C. a document that is not an object: refused, not silently ignored ──
            def safe(d):
                return values(d) if isinstance(d, dict) else d
            before = pg_doc(uid_b)
            v = carol.mutate(T, "UPDATE", {"uid": uid_b}, {"doc": ["not", "registers"]})["version"]
            vd = verdict(v)
            check(f"§C zb-client-ts: an array as doc is rejected, with PostgreSQL's code: {vd and (vd['outcome'], vd.get('sqlstate'))}",
                  vd is not None and vd["outcome"] == "rejected" and vd.get("sqlstate") == "22023")
            check(f"§C zb-client-ts: ...and its message in detail: {vd and vd.get('detail', '')[:70]}",
                  vd is not None and "JSON object of registers" in vd.get("detail", ""))
            check(f"§C PostgreSQL's doc unchanged: {values(pg_doc(uid_b))}", pg_doc(uid_b) == before)
            dt = both(alice, carol, lambda c: safe(local_doc(c, uid_b) or {}) == want, 30)
            check(f"§C Carol's optimistic copy put back ({dt} s): {safe(local_doc(carol, uid_b) or {})}", dt is not None)
            mid = alice.mutate(T, "UPDATE", {"uid": uid_b}, {"doc": "not registers"})["msgId"]
            end = time.monotonic() + 30
            while alice.outcome(mid) is None and time.monotonic() < end:
                alice.flush(500); alice.poll(300)
            o = alice.outcome(mid) or {}
            check(f"§C libzb: a string as doc is rejected, sqlstate and detail in the poll report's outcome: {(o.get('outcome'), o.get('sqlstate'))}",
                  o.get("outcome") == "rejected" and o.get("sqlstate") == "22023" and "JSON object of registers" in o.get("detail", ""))
            check(f"§C PostgreSQL's doc still unchanged: {values(pg_doc(uid_b))}", pg_doc(uid_b) == before)
            ins = zb.psql(f"INSERT INTO {T} (uid, doc, tenant_id) VALUES ('{uuid.uuid4()}', '\"text\"'::jsonb, '{tenant}')", quiet=True)
            check("§C an INSERT with a non-object doc is refused too", "INSERT" not in ins)
            alice.close(); carol.close(); alice = carol = None
            teardown()
    finally:
        if alice: alice.close()
        if carol: carol.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(1 if main() else 0)

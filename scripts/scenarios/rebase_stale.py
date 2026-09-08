"""Rebase on stale (NOTES §10do).

Two clients edit the same row. B's edit is stamped BEFORE A's lands but reaches
PostgreSQL AFTER it: the LWW function judges it `stale`. Until §10do the client
dropped that edit. Now it is kept aside until the winning row is here, and:

  A. B changed `title`, A changed `status` — disjoint columns: B's edit is
     resubmitted with a fresh stamp and everyone ends at {New, Done}. libzb is B,
     stamped a millisecond after the seed row's version — a slow clock (§7.2) —
     and sent after Node's edit landed, before the host polled the echo in: the
     verdict arrives BEFORE the winning row.
  B. both changed `status` — the same column: B's edit is dropped and surfaced,
     the winner stands.
  C. the roles swapped: Node is B, with a stamp 5 s old. The winning row is
     already here when the write is made, so the rebase fires the moment the
     verdict lands and is stamped above the winner by the HLC floor.
  D. Node OFFLINE: the socket closed, an edit queued in the outbox, libzb edits the
     other column meanwhile, Node reconnects. The catch-up delivers the winner's row
     on the queued write's key BEFORE the flush — which used to "confirm" the queued
     write as its own echo and drop it unsent (§10dt). The echo must carry our stamp.

What must hold: PostgreSQL and both replicas equal, both outboxes empty, and each
client's log names what it did (rebased / lost).
"""
import os, sys, time, uuid
from datetime import datetime, timedelta, timezone
import zb
from clients import Lib, Node, both, fresh_sqlite

LOG = "/tmp/zb_rebase_stale_bridge.log"
PY_ERR, NODE_LOG = "/tmp/zb_rebase_stale_py.log", "/tmp/zb_rebase_stale_node.log"
T = "mig_rb"
PUB = zb.publication()
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")
PY_DB, NODE_DB = "/tmp/zb-rebase-py.sqlite3", "/tmp/zb-rebase-node.sqlite3"


def teardown():
    for sql in (f"DROP TABLE IF EXISTS public.{T}", f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'",
                f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'"):
        zb.psql(sql, quiet=True)


def pg_row(uid):
    return tuple(zb.psql(f"SELECT title || '|' || status FROM {T} WHERE uid = '{uid}'").strip().split("|"))


def pg_version(uid):
    return zb.psql(f"SELECT to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') FROM {T} WHERE uid = '{uid}'").strip()


def just_after(version):
    """One millisecond past a wire version: below anything written from now on."""
    t = datetime.strptime(version, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(milliseconds=1)
    return t.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def wait_pg(uid, want, budget=30):
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        if pg_row(uid) == want: return round(time.monotonic() - t0, 2)
        time.sleep(0.2)
    return None


def outbox_left(c, tbl_like):
    names = [r[0] for r in c.q("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ?", [tbl_like])]
    return sum(c.q(f"SELECT count(*) FROM {n}")[0][0] for n in names) if names else -1


def local(c, uid):
    r = c.q(f"SELECT title, status FROM {T} WHERE uid = ?", [uid])
    return tuple(r[0]) if r else None


def turn(py, budget, done):
    """Drive libzb (poll + flush, the host's loop) until `done()` or the budget runs out."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        py.poll(); py.flush(300)
        if done(): return round(time.monotonic() - t0, 2)
        time.sleep(0.2)
    return None


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
    # libzb prints its decisions on stderr: keep them, the scenario reads them back
    err = open(PY_ERR, "w"); os.dup2(err.fileno(), 2)
    said = lambda path, s: s in open(path).read()

    teardown()
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), title text, status text, {COMMON})")
    fresh_sqlite(PY_DB); fresh_sqlite(NODE_DB)
    py = nd = None
    uid = str(uuid.uuid4())
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE})")
            check(f"zebridge_enable({T}) live: {out.strip()[:50]}…", "error" not in out.lower())
            py = Lib(PY_DB, [T], "py-rebase")
            nd = Node(NODE_DB, NODE_LOG)
            tenant = py.tenant
            zb.psql(f"INSERT INTO {T} (uid, title, status, tenant_id) VALUES ('{uid}', 'Old', 'Pending', '{tenant}')")
            dt = both(py, nd, lambda c: local(c, uid) == ("Old", "Pending"), 150)
            check(f"§0 both replicas hold the row {{Old, Pending}} ({dt} s)", dt is not None)

            # ── A. disjoint columns, libzb late, verdict before echo ──
            old = just_after(pg_version(uid))                                   # a slow clock: past the seed, below Node's
            nd.mutate(T, "UPDATE", {"uid": uid}, {"status": "Done"})
            dt = wait_pg(uid, ("Old", "Done"))
            check(f"§A Node's edit landed: PostgreSQL {pg_row(uid)} ({dt} s)", dt is not None)
            py.mutate(T, "UPDATE", {"uid": uid}, {"title": "New"}, version=old)  # sent at once, stamped below Node's
            f = py.flush(5000)
            check(f"§A libzb's slow-clock edit is judged on arrival: settled={f.get('settled')}, PostgreSQL still {pg_row(uid)}",
                  f.get("settled") >= 1 and pg_row(uid) == ("Old", "Done"))
            check("§A libzb held the edit for a rebase (no 'rebased' yet: the winning row has not been polled in)",
                  not said(PY_ERR, "libzb: rebased"))
            dt = turn(py, 30, lambda: pg_row(uid) == ("New", "Done"))
            check(f"§A polled the winner in, rebased and resent: PostgreSQL {pg_row(uid)} ({dt} s)", dt is not None)
            check("§A libzb said so: 'rebased title of mig_rb onto the newer row'", said(PY_ERR, f"libzb: rebased title of {T} onto the newer row"))
            dt = both(py, nd, lambda c: local(c, uid) == ("New", "Done"), 60)
            check(f"§A both replicas at {{New, Done}} ({dt} s): py {local(py, uid)}, node {local(nd, uid)}", dt is not None)
            check(f"§A outboxes empty: py {outbox_left(py, '%outbox%')}, node {outbox_left(nd, '_zebridge_outbox')}",
                  outbox_left(py, '%outbox%') == 0 and outbox_left(nd, '_zebridge_outbox') == 0)

            # ── B. the same column: dropped and surfaced ──
            old = just_after(pg_version(uid))
            nd.mutate(T, "UPDATE", {"uid": uid}, {"status": "Final"})
            dt = wait_pg(uid, ("New", "Final"))
            check(f"§B Node's edit landed: PostgreSQL {pg_row(uid)} ({dt} s)", dt is not None)
            py.mutate(T, "UPDATE", {"uid": uid}, {"status": "Late"}, version=old)
            f = py.flush(5000)
            turn(py, 5, lambda: False)
            check(f"§B libzb's slow-clock edit judged stale (settled={f.get('settled')}), NOT resent: PostgreSQL {pg_row(uid)}", f.get("settled") >= 1 and pg_row(uid) == ("New", "Final"))
            check("§B libzb surfaced the loss: 'edit LOST to a newer version on the same column(s) status'",
                  said(PY_ERR, "edit LOST to a newer version on the same column(s) status"))
            dt = both(py, nd, lambda c: local(c, uid) == ("New", "Final"), 60)
            check(f"§B both replicas at the winner {{New, Final}} ({dt} s): py {local(py, uid)}, node {local(nd, uid)}", dt is not None)
            check(f"§B outboxes empty: py {outbox_left(py, '%outbox%')}, node {outbox_left(nd, '_zebridge_outbox')}",
                  outbox_left(py, '%outbox%') == 0 and outbox_left(nd, '_zebridge_outbox') == 0)

            # ── C. Node late with a slow clock; the winner is already here ──
            py.mutate(T, "UPDATE", {"uid": uid}, {"status": "Again"}); py.flush(5000)
            dt = wait_pg(uid, ("New", "Again"))
            check(f"§C libzb's edit landed: PostgreSQL {pg_row(uid)} ({dt} s)", dt is not None)
            dt = both(py, nd, lambda c: local(c, uid) == ("New", "Again"), 60)
            check(f"§C both replicas hold it before Node writes ({dt} s)", dt is not None)
            old = (datetime.now(timezone.utc) - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
            nd.mutate(T, "UPDATE", {"uid": uid}, {"title": "Newer"}, version=old)   # a clock 5 s slow
            dt = wait_pg(uid, ("Newer", "Again"))
            check(f"§C Node's slow-clock edit judged stale, rebased at once, landed: PostgreSQL {pg_row(uid)} ({dt} s)", dt is not None)
            check("§C Node said so: 'rebased title onto the newer row'", said(NODE_LOG, "rebased title onto the newer row"))
            check(f"§C the resent stamp is above the winner, not the slow clock's: {pg_version(uid)} > {old}", pg_version(uid) > old)
            dt = both(py, nd, lambda c: local(c, uid) == ("Newer", "Again"), 60)
            check(f"§C both replicas at {{Newer, Again}} ({dt} s): py {local(py, uid)}, node {local(nd, uid)}", dt is not None)
            turn(py, 2, lambda: False)
            check(f"§C outboxes empty: py {outbox_left(py, '%outbox%')}, node {outbox_left(nd, '_zebridge_outbox')}",
                  outbox_left(py, '%outbox%') == 0 and outbox_left(nd, '_zebridge_outbox') == 0)

            # ── D. Node offline: a queued edit meets the winner's row in the catch-up ──
            nd.disconnect()
            nd.mutate(T, "UPDATE", {"uid": uid}, {"status": "Queued"})          # stamped now, sent on connect
            check(f"§D Node hung up and queued an edit: outbox {outbox_left(nd, '_zebridge_outbox')}", outbox_left(nd, '_zebridge_outbox') == 1)
            time.sleep(0.5)
            py.mutate(T, "UPDATE", {"uid": uid}, {"title": "Meanwhile"}); py.flush(5000)
            dt = wait_pg(uid, ("Meanwhile", "Again"))
            check(f"§D libzb's edit landed meanwhile: PostgreSQL {pg_row(uid)} ({dt} s)", dt is not None)
            nd.connect()
            dt = wait_pg(uid, ("Meanwhile", "Queued"))
            check(f"§D reconnected: the queued edit was judged stale, rebased and landed: PostgreSQL {pg_row(uid)} ({dt} s)", dt is not None)
            check("§D Node said so: 'rebased status onto the newer row'", said(NODE_LOG, "rebased status onto the newer row"))
            check("§D the winner's row was NOT taken for the queued write's echo", "confirmed by CDC echo" not in open(NODE_LOG).read().split("replaying 1 unconfirmed")[-1].split("rebased status")[0])
            dt = both(py, nd, lambda c: local(c, uid) == ("Meanwhile", "Queued"), 60)
            check(f"§D both replicas at {{Meanwhile, Queued}} ({dt} s): py {local(py, uid)}, node {local(nd, uid)}", dt is not None)
            turn(py, 2, lambda: False)
            check(f"§D outboxes empty: py {outbox_left(py, '%outbox%')}, node {outbox_left(nd, '_zebridge_outbox')}",
                  outbox_left(py, '%outbox%') == 0 and outbox_left(nd, '_zebridge_outbox') == 0)
            py.close(); nd.close(); py = nd = None
            teardown()
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

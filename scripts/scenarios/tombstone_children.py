"""Tombstones across a foreign key (NOTES §10dn).

A tombstone is a DELETE on every replica, and a replica cannot delete a parent whose
children are still there — its foreign keys have no cascade. So the rule lives where the
truth does: PostgreSQL refuses to tombstone a row while a live child references it,
and the client gets `rejected` naming the child table. Children first, then the parent.
And the other contradiction, a physical cascade into a table that keeps tombstones, is
refused twice: at `zebridge_enable` and by the DDL guard when the constraint comes later.

Both clients (libzb, zb-client-ts in Node), the DBA's psql path, and the sweeper:

  §1  a client deletes a parent with live children → rejected, reverted, nothing held
  §2  the DBA's DELETE on that parent → the same refusal, from the soft-delete trigger
  §3  the children first, then the parent → applied on both, equal to PostgreSQL
  §4  enabling a tombstone table under a cascade → preflight ERROR
  §5  adding a cascade to an enabled tombstone table → the migration is rejected
  §6  the sweeper reaps the family, children then parent; no replica hears about it
"""
import os, subprocess, sys, time, uuid
import zb
from clients import Lib, Node, both, fresh_sqlite

LOG = "/tmp/zb_tombstone_children_bridge.log"
P, C, BAD = "mig_tp", "mig_tc", "mig_tbad"
PUB = zb.publication()
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")


def teardown():
    for sql in (f"DROP TABLE IF EXISTS public.{BAD}", f"DROP TABLE IF EXISTS public.{C}", f"DROP TABLE IF EXISTS public.{P}",
                f"DELETE FROM public.zebridge_catalogue WHERE tbl IN ('{P}','{C}','{BAD}')",
                f"DELETE FROM public.zebridge_generations WHERE tbl IN ('{P}','{C}','{BAD}')"):
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
    zb.psql(f"CREATE TABLE public.{P} (uid uuid PRIMARY KEY, title text, {COMMON})")
    zb.psql(f"CREATE TABLE public.{C} (uid uuid PRIMARY KEY, parent_id uuid NOT NULL REFERENCES public.{P}(uid), note text, {COMMON})")
    for db in ("/tmp/zb-tc-py.sqlite3", "/tmp/zb-tc-node.sqlite3"): fresh_sqlite(db)
    py = nd = None
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            for t in (P, C):
                out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{t}', {ENABLE})")
                check(f"zebridge_enable({t}) live: {out.strip()[:50]}…", "error" not in out.lower())
            py = Lib("/tmp/zb-tc-py.sqlite3", [P, C], "py-tombstone-children")
            nd = Node("/tmp/zb-tc-node.sqlite3", "/tmp/zb_tombstone_children_node.log")
            tenant = py.tenant
            p1, p2 = str(uuid.uuid4()), str(uuid.uuid4())
            c1a, c1b, c2 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
            zb.psql(f"INSERT INTO {P} (uid, title, tenant_id) VALUES ('{p1}', 'p1', '{tenant}'), ('{p2}', 'p2', '{tenant}')")
            zb.psql(f"INSERT INTO {C} (uid, parent_id, note, tenant_id) VALUES ('{c1a}', '{p1}', 'c1a', '{tenant}'), ('{c1b}', '{p1}', 'c1b', '{tenant}'), ('{c2}', '{p2}', 'c2', '{tenant}')")
            live = lambda c, t: c.q(f"SELECT count(*) FROM {t} WHERE deleted_at IS NULL")[0][0]
            dt = both(py, nd, lambda c: live(c, P) == 2 and live(c, C) == 3, 150)
            check(f"§0 both replicas hold 2 parents and 3 children ({dt} s)", dt is not None)
            pg_live = lambda t: zb.psql(f"SELECT count(*) FROM {t} WHERE deleted_at IS NULL").strip()

            # ── 1. a client deletes a parent with live children ──
            py.mutate(P, "DELETE", {"uid": p1}); f = py.flush(5000)
            nd.mutate(P, "DELETE", {"uid": p2})
            t0 = time.monotonic()
            while time.monotonic() - t0 < 20 and (py.q("SELECT count(*) FROM _zebridge_outbox")[0][0] or nd.q("SELECT count(*) FROM _zebridge_outbox")[0][0]):
                py.poll(); time.sleep(0.3)
            heard = open("/tmp/zb_tombstone_children_node.log").read()
            check(f"§1 both parent deletes were REJECTED naming the child table (PostgreSQL still has {pg_live(P)} live parents; outboxes py {py.q('SELECT count(*) FROM _zebridge_outbox')[0][0]}, node {nd.q('SELECT count(*) FROM _zebridge_outbox')[0][0]}; Node heard: {'live row(s) in ' + C in heard})",
                  pg_live(P) == "2" and py.q("SELECT count(*) FROM _zebridge_outbox")[0][0] == 0 and nd.q("SELECT count(*) FROM _zebridge_outbox")[0][0] == 0 and f"live row(s) in {C}" in heard)
            for _ in range(3): py.poll()
            check(f"§1 the replicas still hold both parents, nothing held: py {live(py, P)}/{py.q('SELECT count(*) FROM _zbz_inbox')[0][0]} held, node {live(nd, P)}/{nd.q('SELECT count(*) FROM _zebridge_inbox')[0][0]} held",
                  live(py, P) == 2 and live(nd, P) == 2 and py.q("SELECT count(*) FROM _zbz_inbox")[0][0] == 0 and nd.q("SELECT count(*) FROM _zebridge_inbox")[0][0] == 0)

            # ── 2. the DBA's DELETE takes the same door ──
            r = subprocess.run(zb.PSQL.split() + ["-tA", "-c", f"DELETE FROM {P} WHERE uid = '{p1}'"], capture_output=True, text=True)
            check(f"§2 a psql DELETE on the parent is refused by the same guard: {r.stderr.strip()[:110]!r}", r.returncode != 0 and "delete the children first" in r.stderr)

            # ── 3. children first, then the parent ──
            py.mutate(C, "DELETE", {"uid": c1a}); nd.mutate(C, "DELETE", {"uid": c1b})
            py.flush(5000)
            dt = both(py, nd, lambda c: live(c, C) == 1, 60)
            check(f"§3 both children of p1 deleted from both clients, tombstoned upstream ({dt} s; PostgreSQL live children {pg_live(C)})", dt is not None and pg_live(C) == "1")
            py.mutate(P, "DELETE", {"uid": p1}); py.flush(5000)
            dt = both(py, nd, lambda c: live(c, P) == 1 and c.q(f"SELECT count(*) FROM {P} WHERE uid = ?", [p1])[0][0] == 0, 60)
            check(f"§3 then the parent: accepted, gone from both replicas, tombstoned upstream ({dt} s; PostgreSQL live parents {pg_live(P)})", dt is not None and pg_live(P) == "1")

            # ── 4. a cascade into a tombstone table is refused at enable ──
            zb.psql(f"CREATE TABLE public.{BAD} (uid uuid PRIMARY KEY, parent_id uuid NOT NULL REFERENCES public.{P}(uid) ON DELETE CASCADE, {COMMON})")
            out = zb.psql(f"SELECT step || ':' || status || ' ' || coalesce(detail, '') FROM zebridge_enable('public.{BAD}', {ENABLE})")
            check(f"§4 zebridge_enable of a tombstone table under ON DELETE CASCADE: {out.strip()[:90]}…", "preflight:ERROR" in out and "cascades" in out)

            # ── 5. a cascade added later is rejected as a migration ──
            r = subprocess.run(zb.PSQL.split() + ["-tA", "-c", f"ALTER TABLE {C} ADD CONSTRAINT {C}_cascade FOREIGN KEY (parent_id) REFERENCES {P}(uid) ON DELETE CASCADE"], capture_output=True, text=True)
            check(f"§5 ALTER TABLE adding a cascade to the enabled tombstone table is rejected: {r.stderr.strip()[:100]!r}", r.returncode != 0 and "migration rejected" in r.stderr)

            # ── 6. the reap: children then parent, in bounded batches, and no replica hears it ──
            # 2,500 more tombstoned children of p2's family, tombstoned in PostgreSQL directly
            # (a batch of soft deletes an application might do) — then the sweeper must reap
            # them in batches of 1000, three batches, one pass.
            zb.psql(f"INSERT INTO {C} (uid, parent_id, note, tenant_id, deleted_at) SELECT gen_random_uuid(), '{p2}', 'bulk' || g, '{tenant}', now() FROM generate_series(1, 2500) g")
            before = (live(py, P), live(py, C), live(nd, P), live(nd, C))
            # the sweeper refuses a sub-minute window unless told it is meant (a guard of its own)
            env = dict(os.environ, GC_THRESHOLD_MS="1000", GC_INTERVAL_MS="999999999", GC_ALLOW_SHORT_THRESHOLD="1", GC_BATCH_ROWS="1000")
            time.sleep(2)
            passes = 0; remaining = None; sweep_out = ""
            for i in range(3):
                try:
                    r = subprocess.run([str(zb.SWEEPER)], env=env, capture_output=True, text=True, timeout=12)
                    sweep_out += (r.stdout or "") + (r.stderr or "")
                except subprocess.TimeoutExpired as e:
                    sweep_out += (e.stdout or b"").decode(errors="replace") + (e.stderr or b"").decode(errors="replace")
                passes += 1
                remaining = zb.psql(f"SELECT (SELECT count(*) FROM {P} WHERE deleted_at IS NOT NULL) || '/' || (SELECT count(*) FROM {C} WHERE deleted_at IS NOT NULL)").strip()
                if remaining == "0/0": break
            time.sleep(3); py.poll()
            after = (live(py, P), live(py, C), live(nd, P), live(nd, C))
            batch_lines = [l for l in sweep_out.splitlines() if "batch(es)" in l]
            check(f"§6 the sweeper reaped the tombstoned family in {passes} pass(es), children first, in bounded batches (tombstones left parent/child: {remaining}; {'; '.join(l.split('GC: ')[-1] for l in batch_lines)[:160]}); the replicas did not move: {before} → {after}",
                  remaining == "0/0" and before == after and any("3 batch(es)" in l for l in batch_lines))
            if remaining != "0/0":
                print("  · sweeper said: " + " | ".join(l for l in sweep_out.splitlines() if "GC" in l or "ERROR" in l)[-600:])
            py.close(); nd.close(); py = nd = None
            teardown()
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

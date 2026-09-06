"""A migration grows a table past MAX_COLUMNS (NOTES §10dh).

MAX_COLUMNS is sized ONCE, at boot: the widest published table, doubled. A migration
that outgrows it suspends the table (`too_many_columns`) instead of taking the bridge
down — and every row written while it is suspended is DROPPED, not deferred. This
scenario measures the whole arc under both clients (libzb polling, zb-client-ts in
Node):

  1. both replicas follow the table; columns are added in batches until the bridge
     suspends it — the bridge stays up, the other tables keep flowing;
  2. rows are inserted, updated and soft-deleted WHILE it is suspended;
  3. the bridge restarts (MAX_COLUMNS re-detects) — the boot sees the suspension it
     inherited, bumps the table's seed epoch, lifts it; the producer builds a full;
     both replicas grow the columns, drop their watermark and re-seed;
  4. both replicas equal PostgreSQL, the suspended-window writes included.
"""
import sys, time, urllib.request, uuid
import zb
from clients import Lib, Node, both, fresh_sqlite

LOG = "/tmp/zb_column_flood_bridge.log"
LOG2 = "/tmp/zb_column_flood_bridge2.log"
T = "zb_colflood"
PUB = zb.publication()
ENABLE = (f"tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")


def teardown():
    zb.psql(f"DROP TABLE IF EXISTS public.{T}", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'", quiet=True)


def suspended_reason():
    raw = zb.kv_get("schemas", T)
    return raw.split('"reason":"')[1].split('"')[0] if '"suspended":true' in raw else None


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
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), label text, tenant_id varchar(255) NOT NULL, "
            f"last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)")
    for db in ("/tmp/zb-colflood-py.sqlite3", "/tmp/zb-colflood-node.sqlite3"):
        fresh_sqlite(db)
    py = nd = None
    try:
        # BASE_BUF=14: a table's descriptor rides the event buffer (~150 bytes per
        # column), and the 4 KiB dev default cannot carry the ~40-column table this
        # grows — measured: "Row too large for the event buffer … column 'schema'".
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5", BASE_BUF="14") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE})")
            check(f"zebridge_enable({T}) live: {out.strip()[:60]}…", "error" not in out.lower())
            py = Lib("/tmp/zb-colflood-py.sqlite3", [T], "py-colflood")
            nd = Node("/tmp/zb-colflood-node.sqlite3", "/tmp/zb_column_flood_node.log")
            tenant = py.tenant
            zb.psql(f"INSERT INTO {T} (label, tenant_id) SELECT 'r' || g, '{tenant}' FROM generate_series(1,3) g")
            # 150 s: the Node client's connect waits 90 s for any followed table without a
            # chain (the dev database's suspended probe tables — CLIENTS.md divergence 3).
            dt = both(py, nd, lambda c: c.q(f"SELECT count(*) FROM {T}")[0][0] == 3, 150)
            check(f"§0 both replicas hold the 3 rows ({dt} s)", dt is not None)

            # ── 1. grow past MAX_COLUMNS, ten columns at a time ──
            added = 0
            for batch in range(10):
                cols = ", ".join(f"ADD COLUMN c{added + i} integer" for i in range(10))
                zb.psql(f"ALTER TABLE {T} {cols}")
                added += 10
                t0 = time.monotonic()
                while time.monotonic() - t0 < 5 and suspended_reason() is None:
                    py.poll(); time.sleep(0.2)
                if suspended_reason() is not None: break
            reason = suspended_reason()
            cap = bridge.text().split("more than the ")[1].split(" one CDC event")[0] if "more than the " in bridge.text() else "?"
            check(f"§1 after +{added} columns the descriptor is a suspension ({reason}); one event carries {cap}", reason == "too_many_columns")
            check("§1 the bridge is still up, its other tables still served", bridge.proc.poll() is None and "Generation producer started" in bridge.text())
            zb.psql(f"INSERT INTO test_types (uid, some_text, tenant_id, inserted_at, updated_at) VALUES ('{uuid.uuid4()}', 'colflood-canary', '{tenant}', now(), now())")
            dt = both(py, nd, lambda c: True, 1)  # a couple of polls
            check(f"§1 both replicas kept the table and its 3 rows: py {py.q(f'SELECT count(*) FROM {T}')[0][0]}, node {nd.q(f'SELECT count(*) FROM {T}')[0][0]}",
                  py.q(f"SELECT count(*) FROM {T}")[0][0] == 3 and nd.q(f"SELECT count(*) FROM {T}")[0][0] == 3)

            # ── 2. writes while suspended: dropped by the bridge, by design ──
            zb.psql(f"INSERT INTO {T} (label, tenant_id, c0) SELECT 'while-suspended-' || g, '{tenant}', g FROM generate_series(1,2) g")
            zb.psql(f"UPDATE {T} SET label = 'r1-touched', c1 = 11, updated_at = now() WHERE label = 'r1'")
            zb.psql(f"UPDATE {T} SET deleted_at = now(), updated_at = now() WHERE label = 'r2'")
            time.sleep(2); py.poll()
            metrics = urllib.request.urlopen(zb.http_base(probe=True) + "/metrics", timeout=5).read().decode()
            dropped = next((int(l.split()[1]) for l in metrics.splitlines() if l.startswith("bridge_refused_events_dropped_total ")), -1)
            check(f"§2 rows written while suspended did not reach the replicas (py {py.q(f'SELECT count(*) FROM {T}')[0][0]} rows, node {nd.q(f'SELECT count(*) FROM {T}')[0][0]}); bridge_refused_events_dropped_total = {dropped}",
                  py.q(f"SELECT count(*) FROM {T}")[0][0] == 3 and nd.q(f"SELECT count(*) FROM {T}")[0][0] == 3 and dropped >= 4)
            e0 = int(zb.psql(f"SELECT seed_epoch FROM zebridge_catalogue WHERE tbl = '{T}'") or 0)

        # ── 3. restart: MAX_COLUMNS re-detects; the inherited suspension becomes a re-seed ──
        with zb.Bridge(LOG2, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5", BASE_BUF="14") as bridge2:
            if not bridge2.wait_for_log("Generation producer started", timeout=30):
                zb.bad("restarted bridge never started its producer"); print(bridge2.text()[-1500:]); return 1
            e1 = int(zb.psql(f"SELECT seed_epoch FROM zebridge_catalogue WHERE tbl = '{T}'") or 0)
            check(f"§3 the boot saw the inherited suspension and bumped the seed epoch ({e0} → {e1})", e1 == e0 + 1 and "asking for a re-seed" in bridge2.text())
            t0 = time.monotonic()
            while time.monotonic() - t0 < 20 and suspended_reason() is not None:
                py.poll(); time.sleep(0.2)
            check(f"§3 the descriptor is unsuspended after the restart ({round(time.monotonic() - t0, 1)} s)", suspended_reason() is None)
            # LIVE rows only: a replica reaps tombstoned rows on seed (PROTOCOL §7.5), so
            # the soft-deleted r2 is absent there and NULL-stamped in PostgreSQL.
            pg = zb.psql(f"SELECT string_agg(label || ':' || coalesce(c1::text, '-'), ',' ORDER BY label) FROM {T} WHERE deleted_at IS NULL")
            def converged(c):
                rows = c.q(f"SELECT label, c1 FROM {T} WHERE deleted_at IS NULL ORDER BY label")
                return ",".join(f"{r[0]}:{'-' if r[1] is None else r[1]}" for r in rows) == pg
            dt = both(py, nd, converged, 120)
            check(f"§4 both replicas equal PostgreSQL, the suspended-window writes included ({dt} s): {pg}", dt is not None)
            check(f"§4 both replicas grew the columns: py {len(py.cols(T))}, node {len(nd.cols(T))}", len(py.cols(T)) == 7 + added and len(nd.cols(T)) == 7 + added)
            py.close(); nd.close(); py = nd = None
            teardown()
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

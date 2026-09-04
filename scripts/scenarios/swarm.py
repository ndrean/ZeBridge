#!/usr/bin/env python3
"""The 100-client hour: whole-replica equality at scale, under faults (NOTES §10cp).

100 services — 50 Node/better-sqlite3, 49 Python/libzb-sqlite, 1 Node/PGlite (a
client whose replica IS PostgreSQL, in-process) — each running the same 5 s CRUD
cycle at one mutation per second: three test_types INSERTs, an UPDATE of the last,
a soft DELETE of the first, with an orders INSERT/UPDATE/physical-DELETE folded
onto the first three ticks. Aggregate: ~160 mutations/s, test_types grows +2 live
rows per cycle per client — ~144,000 live rows over a full hour at 100 clients.

Into that steady roar, the fault schedule (fractions of the duration, so a smoke
run exercises the same shape): NATS bounces at 20/45/75%, PostgreSQL fast
stop/starts at 33/66%, one bridge restart at 50%.

The verdict is the one this suite has owed since the WAL-head resume bug (memory:
"whole-replica equality against Postgres — still unautomated"): after a drain and
settle window, EVERY client reports (count, md5-of-sorted-uids) for its tenant's
live test_types slice and for the public orders table, and every digest must equal
PostgreSQL's. Fairness under LWW is clockskew.py's business; this asserts the only
thing that matters at scale — nobody diverged, nothing was lost, through every
fault.

Manual group: an hour of machine by default. ZB_SOAK_S=120 for a smoke run.

Usage:  python scripts/scenarios/swarm.py
Env:    ZB_SOAK_S (3600) ZB_SWARM_NODE (50) ZB_SWARM_PY (49) ZB_SETTLE_S (240)
"""
import hashlib
import json
import os
import pathlib
import shutil
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import zb  # noqa: E402
from matrix import Nats, pg_ctl, pg_start, pg_up, ensure_pg_up  # noqa: E402

ROOT = zb.ROOT
SOAK_S = float(os.environ.get("ZB_SOAK_S", "3600"))
N_NODE = int(os.environ.get("ZB_SWARM_NODE", "50"))     # worker 0 runs on PGlite
N_PY = int(os.environ.get("ZB_SWARM_PY", "49"))
SETTLE_S = float(os.environ.get("ZB_SETTLE_S", "240"))
PRINCIPALS = ["alice", "bob", "mary", "nina", "omar"]
SCRATCH = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb-swarm"
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_swarm_bridge.log"
NODE_DIR = ROOT / "examples" / "04-node-consumer"


def psql(sql):
    return zb.psql(sql)


def pg_digest(sql):
    raw = psql(f"SELECT count(*) || '|' || coalesce(md5(string_agg(uid::text, ',' ORDER BY uid)), 'empty') FROM ({sql}) t").strip()
    n, m = raw.split("|")
    return {"count": int(n), "md5": m if m != "empty" else hashlib.md5(b"").hexdigest()}


def bridge_metrics():
    try:
        import urllib.request
        return urllib.request.urlopen(f"http://127.0.0.1:{zb.bridge_port()}/metrics", timeout=3).read().decode()
    except Exception:  # noqa: BLE001
        return ""


def gauge(m, name):
    for line in m.splitlines():
        if line.startswith(name + " ") or line.startswith(name + "{"):
            try:
                return float(line.rsplit(" ", 1)[1])
            except ValueError:
                pass
    return -1


def main():
    if zb.another_bridge_running():
        sys.exit("another bridge is running — the swarm starts (and RESTARTS) its own; stop yours first")
    ensure_pg_up()
    n = Nats()
    if SCRATCH.exists():
        shutil.rmtree(SCRATCH)
    SCRATCH.mkdir(parents=True)

    tenants = {p: psql(f"SELECT tenant_id FROM zebridge_user_tenants WHERE principal='{p}' LIMIT 1").strip()
               for p in PRINCIPALS}
    if not all(tenants.values()):
        sys.exit(f"principal→tenant mapping incomplete: {tenants}")

    # users seed: orders.user_id references users.id (DB-allocated, so clients never
    # insert users — the fixture does, through PostgreSQL, and CDC fans it out).
    psql("INSERT INTO users (name, email, inserted_at, updated_at) "
         "SELECT 'swarm-user-' || i, 'swarm' || i || '@x.test', now(), now() "
         "FROM generate_series(1, 20) i "
         "ON CONFLICT DO NOTHING")
    user_ids = ",".join(x for x in psql("SELECT id FROM users ORDER BY id DESC LIMIT 20").split() if x)
    if not user_ids:
        sys.exit("no users to reference — the orders dance needs parents")

    # Defensive pre-clean: a previous run leaves producer MEMORY behind
    # (zebridge_generations rows) and stale/DEL-marked manifests. The boot tick then
    # compares the recreated EMPTY tables against that memory and can skip them as
    # "unchanged" — no manifest until the +300s cadence tick, and clients exclude
    # the tables (measured, §10cq). Clean slate, then create.
    psql("DELETE FROM zebridge_generations WHERE tbl IN ('sw_orders','sw_clients','sw_suppliers')")
    for t in ("sw_orders", "sw_clients", "sw_suppliers"):
        subprocess.run(["nats", "--server", "nats://127.0.0.1:4222",
                        "--creds", str(ROOT / "scripts" / "native" / "creds" / "bridge.creds"),
                        "kv", "purge", "generations", f"_default.{t}", "-f"], capture_output=True)

    # ── the FK web (§10cq): sw_suppliers (COMPOSITE pk country+name) ← sw_orders
    # (two FKs) → sw_clients. No scenario had ever used a composite primary key;
    # the whole point is to force mutationKeyId's pk-join, composite-key deletes,
    # and FK-change updates that must move two columns atomically. All public, all
    # writable, deletes physical (the cascade IS the test).
    psql("""
        CREATE TABLE IF NOT EXISTS sw_suppliers (
            country varchar(40) NOT NULL, name varchar(60) NOT NULL,
            rating int, inserted_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
            PRIMARY KEY (country, name));
        CREATE TABLE IF NOT EXISTS sw_clients (
            uid uuid PRIMARY KEY, label varchar(80) NOT NULL,
            inserted_at timestamptz NOT NULL, updated_at timestamptz NOT NULL);
        CREATE TABLE IF NOT EXISTS sw_orders (
            uid uuid PRIMARY KEY,
            client_id uuid NOT NULL REFERENCES sw_clients(uid) ON DELETE CASCADE,
            supplier_country varchar(40) NOT NULL, supplier_name varchar(60) NOT NULL,
            label varchar(80) NOT NULL,
            inserted_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
            FOREIGN KEY (supplier_country, supplier_name)
                REFERENCES sw_suppliers(country, name) ON DELETE CASCADE);
    """)
    for t in ("sw_suppliers", "sw_clients", "sw_orders"):
        out = psql(f"SELECT step || ':' || status FROM zebridge_enable('public.{t}', "
                   "writable => true, version_col => 'updated_at', "
                   "allow_physical_deletes => true, "
                   "public_reason => 'swarm FK web - shared reference data', "
                   f"publication => '{zb.publication()}', dry_run => false)")
        if "error" in out.lower():
            sys.exit(f"enable {t} failed: {out}")
        psql(f"SELECT zebridge_grant_edge_writes('public.{t}')")
    psql("INSERT INTO sw_suppliers (country, name, rating, inserted_at, updated_at) "
         "SELECT c, 'sup-' || c || '-' || i, i, now(), now() "
         "FROM unnest(ARRAY['fr','de','jp','br','ca']) c, generate_series(1, 2) i "
         "ON CONFLICT DO NOTHING")
    suppliers = ",".join(x for x in psql(
        "SELECT country || '|' || name FROM sw_suppliers ORDER BY country, name").split() if x)

    # orders boots outbound-only (the writer has no INSERT on it — the bridge preflight
    # names the fix). Open it for the soak; restore the boot state at the end. There is
    # no revoke function, so the restore is the raw inverse of what grant_edge_writes did.
    writer_role = psql("SELECT grantee FROM information_schema.role_table_grants "
                       "WHERE table_name='test_types' AND privilege_type='INSERT' "
                       "AND grantee <> 'postgres' LIMIT 1").strip()
    orders_was_writable = bool(psql(
        "SELECT 1 FROM information_schema.role_table_grants WHERE table_name='orders' "
        f"AND privilege_type='INSERT' AND grantee='{writer_role}'").strip())
    psql("SELECT zebridge_grant_edge_writes('public.orders')")

    procs, reports = [], {}

    # Node clients are grouped several to a process: each keeps its own NATS
    # connection, consumers, outbox and replica file — only the V8 baseline is
    # shared, which is what makes 100 logical clients fit a 16 GB machine.
    per_proc = int(os.environ.get("ZB_NODE_PER_PROC", "5"))

    def spawn_node_group(ids):
        spec = []
        for i in ids:
            wid = f"n{i}"
            p = PRINCIPALS[i % len(PRINCIPALS)]
            spec.append({"wid": wid, "principal": p, "tenant": tenants[p],
                         "engine": "pglite" if i == 0 else "sqlite",
                         "db": str(SCRATCH / (f"pglite-{wid}" if i == 0 else f"{wid}.sqlite3")),
                         "report": str(SCRATCH / f"report-{wid}.json")})
        env = dict(os.environ)
        env.update({"ZB_CLIENTS_SPEC": json.dumps(spec), "ZB_SUPPLIERS": suppliers,
                    "ZB_DURATION_S": str(SOAK_S), "ZB_SETTLE_S": str(SETTLE_S),
                    "ZB_USER_IDS": user_ids})
        gname = f"n{ids[0]}-{ids[-1]}"
        lf = open(SCRATCH / f"{gname}.log", "w")
        wids = [c["wid"] for c in spec]
        return wids, subprocess.Popen(
            ["node", "--experimental-strip-types", "swarm-worker.ts"],
            cwd=NODE_DIR, env=env, stdout=lf, stderr=subprocess.STDOUT), lf

    def spawn_py(i):
        wid = [f"p{i}"]
        p = PRINCIPALS[(N_NODE + i) % len(PRINCIPALS)]
        env = dict(os.environ)
        env.update({"ZB_PRINCIPAL": p, "ZB_WORKER_ID": wid[0],
                    "ZB_DB": str(SCRATCH / f"{wid[0]}.sqlite3"),
                    "ZB_DURATION_S": str(SOAK_S), "ZB_SETTLE_S": str(SETTLE_S),
                    "ZB_REPORT": str(SCRATCH / f"report-{wid[0]}.json"), "ZB_USER_IDS": user_ids,
                    "ZB_SUPPLIERS": suppliers})
        lf = open(SCRATCH / f"{wid[0]}.log", "w")
        return wid, subprocess.Popen(
            [sys.executable, str(ROOT / "scripts" / "scenarios" / "swarm_worker.py")],
            env=env, stdout=lf, stderr=subprocess.STDOUT), lf

    def all_wids():
        return [w for wids, _, _ in procs for w in wids]

    failed = 0
    br = zb.Bridge(LOG)
    br.__enter__()
    try:
        if not br.wait_for_log("Replication started successfully", timeout=90):
            zb.bad("bridge did not start")
            return 1

        # The FK-web tables were born THIS run: their first generation manifests
        # arrive with the producer's boot pass, and a client whose 90s patience
        # loses that race EXCLUDES the table for its whole life (§10cq — the
        # product-side question is recorded there; the scenario simply refuses to
        # start clients against a feed that is not ready yet).
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            r = subprocess.run(["nats", "--server", "nats://127.0.0.1:4222",
                                "--creds", str(ROOT / "scripts" / "native" / "creds" / "bridge.creds"),
                                "kv", "get", "generations", "_default.sw_orders", "--raw"],
                               capture_output=True, text=True)
            if r.returncode == 0 and r.stdout.strip().startswith("{"):
                break
            time.sleep(2)
        else:
            zb.bad("the producer never built the FK-web chains — clients would exclude the tables")
            return 1
        zb.ok(f"FK-web manifests ready {time.monotonic() - deadline + 240:.0f}s after boot")

        for lo in range(0, N_NODE, per_proc):
            procs.append(spawn_node_group(list(range(lo, min(lo + per_proc, N_NODE)))))
            time.sleep(0.5)
        for i in range(N_PY):
            procs.append(spawn_py(i))
            time.sleep(0.3)
        n_clients = N_NODE + N_PY
        zb.ok(f"{n_clients} clients launched in {len(procs)} processes "
              f"({N_NODE} node incl. 1 PGlite at {per_proc}/proc, {N_PY} python), "
              f"{SOAK_S:.0f}s of ~{n_clients * 8 / 5:.0f} mutations/s ahead")

        # ZB_FAULTS picks which fault kinds run (csv of nats,pg,bridge; default all) —
        # the isolation lever when a divergence needs bisecting.
        kinds = set((os.environ.get("ZB_FAULTS") or "nats,pg,bridge").split(","))
        faults = sorted([f for f in [
            (0.20 * SOAK_S, "nats"), (0.45 * SOAK_S, "nats"), (0.75 * SOAK_S, "nats"),
            (0.33 * SOAK_S, "pg"), (0.66 * SOAK_S, "pg"),
            (0.50 * SOAK_S, "bridge"),
            (0.70 * SOAK_S, "cascade"),
        ] if f[1] in kinds or f[1] == "cascade"])
        t0 = time.monotonic()
        next_status = 60.0
        while time.monotonic() - t0 < SOAK_S + 30:
            el = time.monotonic() - t0
            if faults and el >= faults[0][0]:
                _, kind = faults.pop(0)
                if kind == "nats":
                    print(f"  ⚡ {el:5.0f}s NATS bounce", flush=True)
                    n.stop(); time.sleep(1.5); n.start()
                elif kind == "pg":
                    print(f"  ⚡ {el:5.0f}s PostgreSQL fast stop/start", flush=True)
                    r = pg_ctl("stop", "-m", "fast")
                    if r.returncode != 0:
                        zb.bad("fast stop hung — the §10cd step-aside regressed"); failed += 1
                    time.sleep(1)
                    pg_start()
                    deadline = time.monotonic() + 60
                    while time.monotonic() < deadline and not pg_up():
                        time.sleep(0.5)
                elif kind == "cascade":
                    # Delete one seeded supplier the workers actively reference:
                    # PostgreSQL cascades its sw_orders, logical replication emits
                    # every cascaded DELETE, and each replica must apply them —
                    # including replicas whose local FK also cascades (idempotent).
                    gone = psql("DELETE FROM sw_suppliers WHERE country='fr' AND name='sup-fr-1' "
                                "RETURNING country || '|' || name").splitlines()
                    print(f"  ⚡ {el:5.0f}s CASCADE: supplier deleted ({gone[0] if gone else 'already gone'}) "
                          "— its orders die with it, everywhere", flush=True)
                else:
                    print(f"  ⚡ {el:5.0f}s bridge restart", flush=True)
                    br.__exit__(None, None, None)
                    br = zb.Bridge(LOG.with_suffix(".2.log"))
                    br.__enter__()
                    if not br.wait_for_log("Replication started successfully", timeout=90):
                        zb.bad("bridge did not come back from its restart"); failed += 1
            if el >= next_status:
                m = bridge_metrics()
                alive = sum(1 for _, pr, _ in procs if pr.poll() is None)
                live = psql("SELECT count(*) FROM test_types WHERE deleted_at IS NULL").strip() if pg_up() else "?"
                print(f"  … {el:5.0f}s workers {alive}/{len(procs)} | PG live test_types {live} | "
                      f"bridge RSS {gauge(m, 'bridge_max_rss_bytes') / 1048576:.0f}MB "
                      f"queue {gauge(m, 'bridge_queue_usage_percent'):.0f}%", flush=True)
                next_status += 60.0
            if all(pr.poll() is not None for _, pr, _ in procs):
                break
            time.sleep(1)

        # workers self-stop at DURATION, then settle and report; wait for them all
        deadline = time.monotonic() + SETTLE_S + 300
        while time.monotonic() < deadline:
            if all(pr.poll() is not None for _, pr, _ in procs):
                break
            time.sleep(2)
        for wids, pr, lf in procs:
            if pr.poll() is None:
                pr.terminate()
            lf.close()
            for w in wids:
                rp = SCRATCH / f"report-{w}.json"
                if rp.exists():
                    try:
                        reports[w] = json.loads(rp.read_text())
                    except Exception:  # noqa: BLE001
                        pass

        missing = [w for w in all_wids() if w not in reports]
        if missing:
            zb.bad(f"{len(missing)} worker(s) never reported: {missing[:10]}{'…' if len(missing) > 10 else ''}")
            failed += 1
        else:
            zb.ok(f"all {len(all_wids())} clients reported")

        fatal = {w: r["fatal"] for w, r in reports.items() if "fatal" in r}
        if fatal:
            zb.bad(f"fatal workers: {fatal}")
            failed += 1

        stuck = {w: r.get("outboxLeft") for w, r in reports.items() if r.get("outboxLeft") not in (0, None)}
        if stuck:
            zb.bad(f"{len(stuck)} worker(s) ended with a non-empty outbox: {dict(list(stuck.items())[:5])}")
            failed += 1
        else:
            zb.ok("every outbox drained to zero — all writes definitively acked")

        # the audit: whole-replica equality, per tenant and on the public tables
        truth_web = {
            "sw_orders": pg_digest("SELECT uid FROM sw_orders"),
            "sw_clients": pg_digest("SELECT uid FROM sw_clients"),
            "sw_suppliers": pg_digest("SELECT country || '|' || name AS uid FROM sw_suppliers"),
        }
        truth_orders = pg_digest("SELECT uid FROM orders")
        truth_tt = {t: pg_digest("SELECT uid FROM test_types WHERE deleted_at IS NULL "
                                 f"AND tenant_id = '{t}'")
                    for t in sorted(set(tenants.values()))}
        bad_tt, bad_ord, bad_web = [], [], []
        for w, r in sorted(reports.items()):
            if "fatal" in r:
                continue
            want = truth_tt.get(r["tenant"])
            if want and r.get("test_types") != want:
                bad_tt.append((w, r["kind"], r["tenant"], r.get("test_types"), want))
            if r.get("orders") != truth_orders:
                bad_ord.append((w, r["kind"], r.get("orders"), truth_orders))
            for t, want_w in truth_web.items():
                if r.get(t) != want_w:
                    bad_web.append((w, r["kind"], t, r.get(t), want_w))
            if r.get("orphans", 0) != 0:
                bad_web.append((w, r["kind"], "orphans", r.get("orphans"), 0))
        sent = sum(r.get("sent", 0) for r in reports.values())
        errs = sum(r.get("sendErrors", 0) for r in reports.values())
        if bad_tt:
            zb.bad(f"test_types DIVERGED on {len(bad_tt)} replica(s); first: {bad_tt[0]}")
            failed += 1
        else:
            counts = {t: d["count"] for t, d in truth_tt.items()}
            zb.ok(f"test_types: every replica equals PostgreSQL, per tenant {counts}")
        if bad_ord:
            zb.bad(f"orders DIVERGED on {len(bad_ord)} replica(s); first: {bad_ord[0]}")
            failed += 1
        else:
            zb.ok(f"orders: every replica equals PostgreSQL ({truth_orders['count']} rows)")
        if bad_web:
            zb.bad(f"the FK web DIVERGED in {len(bad_web)} place(s); first: {bad_web[0]}")
            failed += 1
        else:
            zb.ok("the FK web holds on every replica: suppliers (composite pk, "
                  f"{truth_web['sw_suppliers']['count']}) / clients ({truth_web['sw_clients']['count']}) / "
                  f"orders ({truth_web['sw_orders']['count']}), cascade included, zero orphans")
        zb.ok(f"{sent} mutations sent by the swarm, {errs} local send errors")
    finally:
        for _, pr, _ in procs:
            if pr.poll() is None:
                pr.kill()
        # The funeral happens WHILE THE BRIDGE WATCHES (§10cq): catalogue rows out,
        # tables dropped, and the bridge's own drop prune consumed from the WAL
        # before it stops. Dropped after the stop, the funeral waits in the WAL and
        # the NEXT boot replays it onto freshly recreated tables — purging their
        # newborn chains the same second the producer builds them (measured: a
        # PURGE revision stamped 17:18:57 against a g1 built 17:18:57.2).
        psql("DELETE FROM zebridge_catalogue WHERE tbl IN ('sw_orders','sw_clients','sw_suppliers')")
        psql("DROP TABLE IF EXISTS sw_orders; DROP TABLE IF EXISTS sw_clients; "
             "DROP TABLE IF EXISTS sw_suppliers")
        if br.proc is not None and br.proc.poll() is None:
            br.wait_for_log("drop prune for 'sw_suppliers'", timeout=30)
        br.__exit__(None, None, None)
        if not orders_was_writable and writer_role:
            psql(f"REVOKE INSERT, UPDATE, DELETE ON public.orders FROM {writer_role}")
        # FK-web teardown, the §10cp-amended hygiene: catalogue rows FIRST (the live
        # bridge reconciles), then the tables, then the one-shot schema keys purged —
        # ghosts cost every future fresh client a 90s chain wait.
        psql("DELETE FROM zebridge_generations WHERE tbl IN ('sw_orders','sw_clients','sw_suppliers')")
        for t in ("sw_orders", "sw_clients", "sw_suppliers"):
            for bucket, key in (("schemas", t), ("generations", f"_default.{t}")):
                subprocess.run(["nats", "--server", "nats://127.0.0.1:4222",
                                "--creds", str(ROOT / "scripts" / "native" / "creds" / "bridge.creds"),
                                "kv", "purge", bucket, key, "-f"],
                               capture_output=True)
        if not failed:
            shutil.rmtree(SCRATCH, ignore_errors=True)
        else:
            print(f"  replica files and logs kept for the postmortem: {SCRATCH}")

    print("\nPASS" if not failed else f"\nFAIL ({failed})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

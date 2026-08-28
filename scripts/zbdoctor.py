#!/usr/bin/env python3
"""zbdoctor — one command, one verdict: is this deployment actually wired?

The post-boot gate the README's roadmap asked for. `check.py` compares what is
DECLARED against what is LIVE; the `zebridge_audit_*()` functions report the
PostgreSQL posture; neither answers the operator's real question after
`bridge` starts: *can a fresh client actually connect, seed, and follow?*

This runs five gates and prints ONE verdict:

    A  the bridge is alive          /health, /status
    B  PostgreSQL is wired          audit_publications / write_guards / sweeper, slots
    C  NATS carries the topology    CDC streams, KV buckets, retired leftovers
    D  the client contract holds    schemas published, tenant map, seedable chains
    E  no declared-vs-actual drift  delegates to scripts/scenarios/check.py

⚠️ Deliberately stdlib-only and dependency-free: an operator runs this on a
box that has `psql`, `nats` and python3 — not a venv with msgpack and nats-py.
Gate E is the one part that needs the scenario suite's deps, and it degrades
to SKIP (never to red) when they are absent.

    python3 scripts/zbdoctor.py            # human output, exit 0 green / 1 red
    python3 scripts/zbdoctor.py --json     # machine verdict for CI

Environment (all optional, sane defaults):
    ZB_PSQL / DATABASE_READER_URL   how to reach Postgres  (default: docker exec)
    NATS_URL                 how to reach NATS      (default: 127.0.0.1:4222)
    NATS_CREDS               a .creds file (operator/JWT mode) — wins if both are set
    NATS_NKEY_SEED           the seed itself (nkey mode), as the bridge takes it

⚠️ Gates C and D need ADMIN-SHAPED reach: they enumerate streams and read the
KV/object stores. A tenant client's own creds authenticate fine and then show
an empty world — grants are per-principal by design. Use the dedicated
auditor minted by scripts/native/jwt-bootstrap.sh:

    NATS_CREDS=scripts/native/creds/zbdoctor.creds

It is read-only by construction (NOTES §10y): stream names/info, per-key
DIRECT.GET, its own inbox — and no CONSUMER.CREATE, so it cannot read a
stream's data even though it can see that the stream exists. Postgres needs
no more than the read role either: `bridge_reader` runs every gate.
    BRIDGE_URL               the bridge's HTTP      (default: http://127.0.0.1:9090)
"""

import json
import os
import re
import shlex
import atexit
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GRAMMAR = json.loads((ROOT / "grammar.json").read_text())
OPEN_TENANT = GRAMMAR.get("open_tenant", "_default")
CDC_PREFIX = GRAMMAR.get("cdc_streams", {}).get("tenant_prefix", "CDC_")
CDC_PUBLIC = GRAMMAR.get("cdc_streams", {}).get("public", "CDC_PUBLIC")
KV_SCHEMAS = GRAMMAR.get("kv", {}).get("schemas", "schemas")
KV_TENANTS = GRAMMAR.get("kv", {}).get("tenants", "tenants")
KV_GENERATIONS = GRAMMAR.get("generations", {}).get("kv", "generations")
GEN_PREFIX = GRAMMAR.get("generations", {}).get("bucket_prefix", "gen-")

BRIDGE_URL = os.environ.get("BRIDGE_URL", "http://127.0.0.1:9090").rstrip("/")
NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
NATS_CREDS = os.environ.get("NATS_CREDS")

if os.environ.get("ZB_PSQL"):
    PSQL = shlex.split(os.environ["ZB_PSQL"])
elif os.environ.get("DATABASE_READER_URL"):
    PSQL = ["psql", os.environ["DATABASE_READER_URL"]]
else:
    PSQL = shlex.split("docker exec -i postgres-primary psql -U postgres")

ANSI = re.compile(r"\x1b\[[0-9;]*m")
GREEN, RED, YELLOW, DIM, BOLD, OFF = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[1m", "\033[0m"

findings: list[dict] = []   # red: the deployment is not wired
warnings: list[dict] = []   # amber: works today, wants attention
skipped: list[str] = []


def red(gate: str, msg: str, fix: str = ""):
    findings.append({"gate": gate, "message": msg, "fix": fix})
    print(f"  {RED}✗{OFF} {msg}")
    if fix:
        print(f"    {DIM}→ {fix}{OFF}")


def amber(gate: str, msg: str, fix: str = ""):
    warnings.append({"gate": gate, "message": msg, "fix": fix})
    print(f"  {YELLOW}!{OFF} {msg}")
    if fix:
        print(f"    {DIM}→ {fix}{OFF}")


def ok(msg: str):
    print(f"  {GREEN}✓{OFF} {msg}")


def gate(title: str):
    print(f"\n{BOLD}{title}{OFF}")


# ─── the two shells this tool speaks through ────────────────────────────────

def psql(sql: str) -> list[list[str]]:
    """Rows as lists of column strings. `-tAF|` keeps parsing trivial."""
    r = subprocess.run(PSQL + ["-tAF|", "-c", sql], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip().splitlines()[-1] if r.stderr.strip() else "psql failed")
    return [line.split("|") for line in r.stdout.strip().splitlines() if line]


def truthy(v: str) -> bool:
    """⚠️ PostgreSQL renders a boolean two ways through psql -tA: a RAW column
    prints `t`/`f`, while `col::text` prints `true`/`false`. Comparing against
    one form silently mis-reads the other — which is how this tool first
    reported an active slot as INACTIVE *and* skipped every chain check by
    reading `generations::text` as false. Accept both, always."""
    return v.strip().lower() in ("t", "true", "y", "yes", "1")


_seed_path: str | None = None


def nkey_file() -> str | None:
    """The `nats` CLI takes `--nkey` as a FILE, so a seed handed through the
    environment has to touch disk for the length of this run. Written 0600 and
    unlinked at exit: a seed travels in env/CLI and must not outlive the
    process in a file (SECURITY.md's rule, which is why the bridge takes
    NATS_NKEY_SEED the same way)."""
    global _seed_path
    seed = (os.environ.get("NATS_NKEY_SEED") or "").strip()
    if not seed:
        return None
    if _seed_path is None:
        fd, path = tempfile.mkstemp(prefix="zbdoctor-", suffix=".nk")
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as fh:
            fh.write(seed + "\n")
        _seed_path = path
        atexit.register(lambda: os.path.exists(path) and os.unlink(path))
    return _seed_path


def nats(*args: str) -> subprocess.CompletedProcess:
    """Creds win over a seed — the same precedence the bridge resolves."""
    server, auth = NATS_URL, []
    if NATS_CREDS:
        auth = ["--creds", NATS_CREDS]
    elif (seed_file := nkey_file()):
        auth = ["--nkey", seed_file]
        # ⚠️ Strip `user:pass@` from the URL when the seed IS the credential.
        # Inline credentials win, and every administrative command is then denied
        # opaquely — a denied JetStream API publish never answers, so it surfaces
        # as "context deadline exceeded", not as an auth error (zb.py, §1.8).
        scheme, _, rest = NATS_URL.partition("://")
        address = rest.rsplit("@", 1)[-1] if "@" in rest else rest
        server = f"{scheme}://{address}" if scheme else address
    return subprocess.run(["nats", "--server", server, *auth, *args],
                          capture_output=True, text=True)


def http_json(path: str):
    with urllib.request.urlopen(BRIDGE_URL + path, timeout=5) as r:
        return json.loads(r.read().decode())


# ─── Gate A: the bridge is alive ────────────────────────────────────────────

def gate_bridge() -> dict | None:
    gate("A. the bridge is alive")
    try:
        with urllib.request.urlopen(BRIDGE_URL + "/health", timeout=5) as r:
            if r.status != 200:
                red("A", f"/health answered {r.status}")
                return None
    except (urllib.error.URLError, OSError) as e:
        red("A", f"the bridge is not answering on {BRIDGE_URL} ({e})",
            "start it, or point BRIDGE_URL at the right host:port — every gate below "
            "describes a deployment nothing is currently driving")
        return None
    ok(f"/health is green at {BRIDGE_URL}")

    try:
        st = http_json("/status")
    except Exception as e:  # noqa: BLE001
        amber("A", f"/status unreadable ({e}) — skipping the runtime posture checks")
        return None

    if not st.get("is_connected"):
        red("A", "the bridge is not connected to NATS", "check NATS_URL and its credentials")
    else:
        ok("connected to NATS")

    if not st.get("slot_active"):
        # ⚠️ /status reports the bridge's OWN VIEW, refreshed by the WAL monitor on a
        # timer — so for the first seconds after a restart it reads false while
        # PostgreSQL already shows the slot held. That is exactly when a POST-BOOT
        # checker runs, so ask PostgreSQL (the authority) before calling it red.
        try:
            rows = psql("SELECT active FROM pg_replication_slots WHERE active")
            pg_holds = len(rows) > 0
        except RuntimeError:
            pg_holds = False
        if pg_holds:
            amber("A", "the bridge reports slot_active=false while PostgreSQL shows a slot held",
                  "normal for a few seconds after a restart — the metric is monitor-sampled; "
                  "if it persists, the WAL monitor is stuck")
        else:
            red("A", "the replication slot is INACTIVE — no CDC is flowing",
                "a previous bridge may still hold it (kill -9 leaves a walsender for up to "
                "wal_sender_timeout); check pg_replication_slots")
    else:
        ok(f"replication slot active, WAL lag {st.get('wal_lag_mb', '?')} MB")

    # Refusals are per-table quarantine, not a wiring fault — but they are the one
    # runtime state that silently freezes a client's copy of a table.
    if st.get("refused_tables", 0):
        amber("A", f"{st['refused_tables']} table(s) SUSPENDED upstream "
                   f"({st.get('refused_events_dropped', 0)} events dropped)",
              "read $KV.schemas.<table> for the reason; clients of those tables are frozen")
    if st.get("pg_reconnect_count") or st.get("nats_reconnect_count"):
        amber("A", f"reconnects since boot: pg={st.get('pg_reconnect_count')} "
                   f"nats={st.get('nats_reconnect_count')}", "not a fault, but worth a glance at the logs")
    return st


# ─── Gate B: PostgreSQL is wired ────────────────────────────────────────────

def gate_postgres(catalogue: dict) -> None:
    gate("B. PostgreSQL is wired")

    # audit_publications, read THROUGH the catalogue: an unscoped publication is a
    # finding only for a TENANT-SCOPED table. A catalogue-public table is unscoped
    # BY DECLARATION (its public_reason is recorded), so flagging it would train
    # the operator to ignore this gate.
    try:
        rows = psql("SELECT publication, tbl, verdict FROM zebridge_audit_publications()")
    except RuntimeError as e:
        amber("B", f"zebridge_audit_publications() unavailable ({e})")
    else:
        # ⚠️ The audit function reports the SINGLE-BRIDGE-PER-TENANT posture, where a
        # publication row filter is the boundary. This deployment shape uses the other
        # supported one: CDC routes per tenant into per-tenant STREAMS, and the stream
        # is the ACL boundary (SECURITY.md; crosstenant.py measures it). So an unscoped
        # publication with RLS ON — the function's 'CDC UNSCOPED' — is EXPECTED here,
        # and flagging it would train the operator to ignore this gate. What is never
        # expected: no RLS at all on a tenant-scoped table (nothing bounds WRITES, so a
        # principal can forge another tenant's rows), which is PASS-THROUGH or
        # 'WRITES UNSCOPED'. Internal zebridge_* tables are the bridge's own plumbing.
        unscoped = 0
        for pub, tbl, verdict in ((r[0], r[1], r[2]) for r in rows if len(r) >= 3):
            short = tbl.split(".")[-1]
            if short.startswith("zebridge_") or verdict.startswith(("scoped", "internal")):
                continue
            if short in catalogue and not catalogue[short]["tenant_col"]:
                continue  # catalogue-public: unscoped on purpose, with a recorded reason
            if verdict.startswith("CDC UNSCOPED"):
                continue  # RLS bounds writes; the per-tenant stream bounds reads
            unscoped += 1
            red("B", f"'{short}' in publication '{pub}': {verdict}",
                "it is tenant-scoped but NOTHING bounds writes — run "
                "zebridge_scope_writes_by_tenant('public.%s') (RLS + the zb_tenant_write "
                "policy), or a publication row filter for a single-tenant bridge" % short)
        if not unscoped:
            ok(f"every published table is bounded or declared public ({len(rows)} audited)")

    # audit_write_guards vs what the catalogue DECLARES the table needs: a
    # tombstone column with no soft-delete trigger means physical deletes, which
    # chains cannot express (§10k's tombstone gate, checked from the other end).
    # ⚠️ Guards are demanded ONLY of edge-writable tables, and "writable" means what
    # SECURITY.md says it means: the writer role holds INSERT. An outbound-only table
    # (users) legitimately declares a version column — chains and CDC compare on it —
    # while needing no triggers, because nothing writes through the bridge.
    writer = os.environ.get("POSTGRES_WRITER_USER", "bridge_writer")
    try:
        guards = {r[0]: (truthy(r[1]), truthy(r[2])) for r in
                  psql("SELECT tbl, version_guard, delete_guard FROM zebridge_audit_write_guards()")
                  if len(r) >= 3}
        writable = {r[0] for r in psql(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='public' AND c.relkind='r' AND EXISTS (SELECT 1 FROM pg_roles "
            f"WHERE rolname='{writer}') AND has_table_privilege('{writer}', c.oid, 'INSERT')")
            if r[0]}
    except RuntimeError as e:
        amber("B", f"write-guard audit unavailable ({e})")
    else:
        missing = 0
        for tbl in sorted(set(catalogue) & writable):
            meta = catalogue[tbl]
            has_version, has_delete = guards.get(tbl, (False, False))
            if meta["version_col"] and not has_version:
                missing += 1
                red("B", f"'{tbl}' is edge-writable with version_col={meta['version_col']}, "
                         "but the bump-version trigger is MISSING",
                    f"a psql write that omits the column is unversioned and loses every LWW "
                    f"comparison — re-run SELECT zebridge_enable('public.{tbl}', ... writable => true)")
            if meta["tombstone_col"] and not has_delete:
                missing += 1
                red("B", f"'{tbl}' is edge-writable with tombstone_col={meta['tombstone_col']}, "
                         "but the soft-delete trigger is MISSING",
                    "a psql DELETE removes the row PHYSICALLY; an upsert-only chain cannot "
                    "express that, so the row RESURRECTS on every fresh seed (§10i). The "
                    "ingress path soft-deletes on its own — this is the other door")
        if not missing:
            ok(f"every edge-writable table's declared guards are attached "
               f"({len(set(catalogue) & writable)} writable of {len(catalogue)})")

    # audit_sweeper: a tenant the sweeper cannot reach keeps its tombstones
    # forever, which is the offline-window promise quietly breaking.
    try:
        sweep = psql("SELECT tbl, tenant_id, verdict FROM zebridge_audit_sweeper()")
    except RuntimeError as e:
        amber("B", f"zebridge_audit_sweeper() unavailable ({e})")
    else:
        unreachable = [r for r in sweep if len(r) >= 3 and not r[2].lower().startswith(("ok", "reachable"))]
        for tbl, tenant, verdict in ((r[0], r[1], r[2]) for r in unreachable):
            amber("B", f"sweeper on '{tbl}' / tenant '{tenant}': {verdict}",
                  "tombstones there are never reaped — GC_THRESHOLD_MS stops bounding the offline window")
        if not unreachable:
            ok(f"the sweeper reaches every tenant it must ({len(sweep)} pairs)")

    # Several bridges over one table: the width guard bakes MIN(max_row_bytes)
    # across the instances whose publication carries it, because a row must fit
    # the NARROWEST carrier — a row that fits only the wider one would suspend
    # the table on the other bridge. Correct, and silent, so say it out loud.
    try:
        budgets = psql("SELECT l.slot, l.max_row_bytes, count(DISTINCT pt.tablename) "
                       "FROM zebridge_limits l LEFT JOIN pg_publication_tables pt "
                       "  ON pt.pubname = l.publication GROUP BY 1, 2 ORDER BY 1")
    except RuntimeError:
        budgets = []
    # ⚠️ A publication that EXISTS but carries no tables is the quietest failure in
    # the system: the bridge finds it, verifies it, logs one warning at boot and
    # then streams nothing, forever, while every health check stays green. The
    # registered publication comes from zebridge_limits — what the RUNNING bridge
    # told the database it replicates, not what an env file claims.
    for slot, _budget, ntables in ((r[0], r[1], int(r[2] or 0)) for r in budgets if len(r) >= 3):
        if ntables == 0:
            red("B", f"instance '{slot}' replicates a publication that carries NO tables",
                "the bridge boots, verifies it and delivers nothing — add tables with "
                "SELECT zebridge_enable('public.<table>', ...), which is the only way in "
                "(a bare ALTER PUBLICATION is refused by the publication guard)")

    if len({r[1] for r in budgets if len(r) >= 2}) > 1:
        detail = ", ".join(f"{r[0]}={r[1]}B" for r in budgets if len(r) >= 2)
        amber("B", f"instances disagree on the row-width budget: {detail}",
              "every shared table is guarded at the MINIMUM — raise BASE_BUF on the "
              "narrow instance, or keep their publications disjoint")
    elif budgets:
        ok(f"{len(budgets)} bridge instance(s) registered, one row-width budget "
           f"({budgets[0][1]} bytes)")

    # Slots: an orphan retains WAL for EVERYONE, which is the one PG-side fault
    # that degrades the whole cluster rather than one table.
    try:
        slots = psql("SELECT slot_name, active::text, coalesce(wal_status,'?'), "
                     "pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) "
                     "FROM pg_replication_slots")
    except RuntimeError as e:
        amber("B", f"pg_replication_slots unreadable ({e})")
    else:
        for name, active, wal_status, retained in ((r[0], r[1], r[2], r[3]) for r in slots if len(r) >= 4):
            if wal_status not in ("reserved", "extended"):
                red("B", f"slot '{name}' wal_status={wal_status} (retaining {retained})",
                    "an invalidated slot cannot resume — its consumer must re-seed")
            elif not truthy(active):
                amber("B", f"slot '{name}' is INACTIVE, retaining {retained}",
                      "if nothing will resume it, drop it: SELECT pg_drop_replication_slot('%s')" % name)
        if slots:
            ok(f"{len(slots)} replication slot(s) inspected")


# ─── Gate C: NATS carries the topology ──────────────────────────────────────

def gate_nats(tenants: list[str]) -> set[str]:
    gate("C. NATS carries the topology")
    r = nats("stream", "ls", "-n")
    if r.returncode != 0:
        detail = r.stderr.strip().splitlines()[-1] if r.stderr.strip() else "failed"
        how = ("NATS_CREDS is set" if NATS_CREDS else
               "NATS_NKEY_SEED is set" if os.environ.get("NATS_NKEY_SEED") else
               "NO credential is set")
        red("C", f"cannot list NATS streams ({detail}) — {how}",
            "gates C and D need an ADMIN credential: export NATS_CREDS=<.creds file> "
            "or NATS_NKEY_SEED=<seed>. A client principal's own credential "
            "authenticates and then sees nothing — the grants are per-principal")
        return set()
    streams = {s.strip() for s in r.stdout.splitlines() if s.strip()}

    wanted = {CDC_PUBLIC} | {f"{CDC_PREFIX}{t}" for t in tenants}
    for s in sorted(wanted):
        if s not in streams:
            red("C", f"stream {s} is missing",
                "the bridge creates CDC streams at boot from the catalogue and "
                "zebridge_user_tenants — a tenant born after boot needs its stream provisioned")
    if wanted <= streams:
        ok(f"every CDC stream exists ({len(wanted)}: public + {len(tenants)} tenant(s))")

    for bucket in (KV_SCHEMAS, KV_TENANTS, KV_GENERATIONS):
        if f"KV_{bucket}" not in streams:
            red("C", f"KV bucket '{bucket}' is missing",
                "nats-init creates the KV buckets; without it clients cannot read "
                f"{'schemas' if bucket == KV_SCHEMAS else 'their tenant / chain manifests'}")
    if all(f"KV_{b}" in streams for b in (KV_SCHEMAS, KV_TENANTS, KV_GENERATIONS)):
        ok("every KV bucket exists (schemas, tenants, generations)")

    # Retired by §10p — present means an old deployment was upgraded in place.
    retired = sorted(s for s in streams if s.startswith("INIT_") or s in ("INIT", "REQUESTS", "KV_snapshots"))
    if retired:
        amber("C", f"retired snapshot-era resources still present: {', '.join(retired)}",
              "snapshot-on-demand is gone (NOTES §10p); these hold storage and grants for nothing — "
              "`nats stream rm <name>` when convenient")
    return streams


# ─── Gate D: the client contract holds ──────────────────────────────────────

def gate_client(catalogue: dict, tenants: list[str], streams: set[str]) -> None:
    gate("D. a fresh client can connect, seed and follow")
    if not streams:
        skipped.append("D (NATS unreachable)")
        print(f"  {DIM}skipped — NATS is unreachable{OFF}")
        return

    # ⚠️ Every question below is "does key X exist?", never "what keys exist?" —
    # so each is a per-key GET, not a `kv ls`. Listing keys requires consumer
    # CREATION rights, and a credential that can create a consumer on a stream
    # can READ that stream: an auditor would need the very grant it exists to
    # verify nobody has. Per-key gets need only DIRECT.GET, which is the same
    # least-privilege shape the tenant clients use (§10y).
    def kv_has(bucket: str, key: str) -> bool:
        return nats("kv", "get", bucket, key, "--raw").returncode == 0

    # 1. every catalogue table must have a published schema, or a client cannot
    #    even build the local table.
    missing = [t for t in catalogue if not kv_has(KV_SCHEMAS, t)]
    if missing:
        red("D", f"no published schema for: {', '.join(sorted(missing))}",
            "the bridge publishes every catalogue table's schema at boot — a table enabled "
            "after boot needs a restart (the catalogue governs it → migration + restart)")
    else:
        ok(f"every catalogue table has a published schema ({len(catalogue)})")

    # 2. every mapped principal must resolve its tenant (PROTOCOL Step 0).
    try:
        principals = [row[0] for row in psql("SELECT DISTINCT principal FROM zebridge_user_tenants") if row[0]]
    except RuntimeError:
        principals = []
    lacking = [p for p in principals if not kv_has(KV_TENANTS, p)]
    if lacking:
        red("D", f"principal(s) with no $KV.{KV_TENANTS} entry: {', '.join(sorted(lacking))}",
            "Step 0 fails for them — the client cannot resolve its tenant and reads nothing")
    elif principals:
        ok(f"every mapped principal resolves its tenant ({len(principals)})")

    # 3. THE seeding question: for every (tenant, table) a client will ask for,
    #    is there a chain manifest, and does the object it names actually exist?
    #    The second half is the two-sided-cleanup failure (NOTES §10r): a manifest
    #    whose objects were purged leaves the producer building deltas over a void.
    expected: list[tuple[str, str]] = []
    for tbl, meta in catalogue.items():
        if not meta["generations"]:
            continue
        if meta["tenant_col"]:
            try:
                ts = [row[0] for row in psql(f"SELECT * FROM zebridge_tenants_of('{tbl}')") if row and row[0]]
            except RuntimeError:
                ts = tenants
            expected += [(t, tbl) for t in ts]
        else:
            expected.append((OPEN_TENANT, tbl))

    no_chain, dangling, checked = [], [], 0
    for tenant, tbl in expected:
        key = f"{tenant}.{tbl}"
        got = nats("kv", "get", KV_GENERATIONS, key, "--raw")
        if got.returncode != 0:
            no_chain.append(key)
            continue
        try:
            man = json.loads(got.stdout)
            obj = man["full"]["object"]
            bucket = man.get("bucket", f"{GEN_PREFIX}{tenant}")
        except (json.JSONDecodeError, KeyError):
            dangling.append(f"{key} (unreadable manifest)")
            continue
        if nats("object", "info", bucket, obj).returncode != 0:
            dangling.append(f"{key} → {bucket}/{obj}")
        checked += 1

    if no_chain:
        red("D", f"no seedable chain for: {', '.join(sorted(no_chain))}",
            "a fresh client CANNOT seed these — check GENERATIONS_ENABLED and wait one "
            "GENERATION_CADENCE_SECONDS; a table enabled seconds ago is legitimately here")
    if dangling:
        red("D", f"manifest points at a missing object: {', '.join(sorted(dangling))}",
            "the chain was half-cleaned (objects purged, PG bookkeeping kept) — the producer is "
            "building deltas over a void. Clear BOTH sides: "
            "DELETE FROM zebridge_generations WHERE tbl='<t>' and let the next tick rebuild")
    if not no_chain and not dangling and checked:
        ok(f"every (tenant, table) has a chain whose full object is present ({checked})")


# ─── Gate E: declared vs actual drift ───────────────────────────────────────

def gate_drift() -> None:
    gate("E. declared vs actual (scripts/scenarios/check.py)")
    script = ROOT / "scripts" / "scenarios" / "check.py"
    if not script.exists():
        skipped.append("E (check.py absent)")
        print(f"  {DIM}skipped — {script} not found{OFF}")
        return
    env = dict(os.environ)
    env.setdefault("ZB_PSQL", " ".join(shlex.quote(p) for p in PSQL))
    # This tool runs on bare python3 by design, but check.py needs msgpack/nats-py.
    # Try the interpreter we are, then the scenario suite's own venv, then skip.
    candidates = [sys.executable, str(ROOT / "scripts" / "scenarios" / ".venv" / "bin" / "python")]
    r = None
    for py in candidates:
        if not Path(py).exists():
            continue
        r = subprocess.run([py, str(script)], capture_output=True, text=True, env=env)
        if "ModuleNotFoundError" not in r.stderr:
            break
    if r is None or "ModuleNotFoundError" in r.stderr:
        missing = r.stderr.strip().splitlines()[-1] if r is not None else "no interpreter"
        skipped.append("E (scenario deps missing)")
        print(f"  {DIM}skipped — the scenario suite's deps are absent ({missing}){OFF}")
        print(f"  {DIM}  → python3 -m venv scripts/scenarios/.venv && "
              f"scripts/scenarios/.venv/bin/pip install msgpack nats-py nkeys{OFF}")
        return
    if r.returncode == 0:
        ok("no drift between what is declared and what is live")
        return
    # Surface check.py's own ✗ lines rather than a bare exit code.
    for line in r.stdout.splitlines():
        if "✗" in line:
            # check.py colours its own output; strip the escapes so the finding
            # reads as one line here (and so --json carries clean text).
            clean = ANSI.sub("", line).strip().lstrip("✗").strip()
            findings.append({"gate": "E", "message": clean, "fix": ""})
            print(f"  {RED}✗{OFF} {clean}")
    if not any(f["gate"] == "E" for f in findings):
        red("E", f"check.py exited {r.returncode}", "run it directly for the detail")


# ─── the verdict ────────────────────────────────────────────────────────────

def main() -> int:
    as_json = "--json" in sys.argv
    if as_json:
        sys.stdout = open(os.devnull, "w")  # gates print; only the verdict escapes

    try:
        cat_rows = psql("SELECT tbl, coalesce(tenant_col,''), coalesce(version_col,''), "
                        "coalesce(tombstone_col,''), generations::text FROM zebridge_catalogue ORDER BY tbl")
    except RuntimeError as e:
        print(f"{RED}cannot read zebridge_catalogue: {e}{OFF}", file=sys.stderr)
        print("the catalogue IS the configuration — without it there is nothing to check "
              "(set ZB_PSQL or DATABASE_READER_URL)", file=sys.stderr)
        return 2
    catalogue = {r[0]: {"tenant_col": r[1] or None, "version_col": r[2] or None,
                        "tombstone_col": r[3] or None, "generations": truthy(r[4])}
                 for r in cat_rows if len(r) >= 5}
    try:
        tenants = [r[0] for r in psql("SELECT DISTINCT tenant_id FROM zebridge_user_tenants ORDER BY 1") if r[0]]
    except RuntimeError:
        tenants = []

    print(f"{BOLD}zbdoctor{OFF} — {len(catalogue)} catalogue table(s), {len(tenants)} tenant(s)")
    gate_bridge()
    gate_postgres(catalogue)
    streams = gate_nats(tenants)
    gate_client(catalogue, tenants, streams)
    gate_drift()

    if as_json:
        sys.stdout = sys.__stdout__
        print(json.dumps({"verdict": "red" if findings else "green",
                          "findings": findings, "warnings": warnings, "skipped": skipped}, indent=2))
        return 1 if findings else 0

    print()
    if findings:
        print(f"{RED}{BOLD}RED{OFF} — {len(findings)} finding(s); this deployment is not fully wired")
    else:
        print(f"{GREEN}{BOLD}GREEN{OFF} — wired: the bridge is live, the topology exists, "
              f"and a fresh client can seed and follow")
    if warnings:
        print(f"{YELLOW}{len(warnings)} warning(s){OFF} — working today, worth attention")
    if skipped:
        print(f"{DIM}skipped: {', '.join(skipped)}{OFF}")
    print()
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())

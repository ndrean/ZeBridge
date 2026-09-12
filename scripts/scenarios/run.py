#!/usr/bin/env python3
"""The scenario battery — one command, one exit code.

    scripts/scenarios/run.py offline            # no stack needed
    scripts/scenarios/run.py live               # against the running native stack + bridge
    scripts/scenarios/run.py owns               # each starts its OWN bridge: stop yours first
    scripts/scenarios/run.py all                # offline, then live, then owns
    scripts/scenarios/run.py live -k mutate -k replies    # a subset by name
    scripts/scenarios/run.py --list

Groups, because the scenarios differ in what they need and what they break:

  offline   pure SQL / files / a scratch package — no bridge, no NATS, seconds each.
  live      need the long-running bridge (:9090) and act as a client principal
            (`NATS_CREDS`, default omar) or as the bridge (`bridge.creds`).
  owns      start a probe bridge (`--slot zb_probe --port 9096`) and refuse to run
            beside another — serialized here, never in a parallel lane.
  manual    benchmarks and soaks that report rather than assert (speed, burst,
            leaksoak, objstore_race, bench_poll): listed, never run by the battery.

Every scenario's exit code is its verdict (`zb.run`); this only sequences them, keeps
the per-scenario log, and fails if any failed. Env: `.env.bridge` sourced,
`BRIDGE_CDC_PUBLICATION`, `NATS_CREDS` for the client role (see below), `ZB_PSQL`
only when the database is not the native one.
"""
import argparse
import os
import pathlib
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[1]
PY = sys.executable
CREDS = ROOT / "scripts" / "native" / "creds"

# name → (role, note). role: "client" connects as NATS_CREDS's principal (omar unless
# set); "bridge" connects with bridge.creds; "none" needs no NATS at all.
GROUPS = {
    "offline": {
        "render":        ("none",   "envsubst on both templates, applied to a scratch DB"),
        "tzguard":       ("none",   "naive timestamp columns refused at DDL time"),
        "tenant_writes": ("none",   "RLS + tenant stamping on the write path"),
        "guards":        ("none",   "version/delete write guards"),
        "generations":   ("none",   "zebridge_generations contract"),
        "envcheck":      ("none",   ".env.bridge vs .env.admin"),
        "pubname":       ("none",   "the publication is named, never defaulted"),
    },
    "live": {
        "check":         ("bridge", "declared vs actual drift"),
        "diagnose":      ("none",   "bridge --diagnose: the pre-run doctor says everything, changes nothing"),
        "revoke":        ("bridge", "deleting a principal's mapping purges its $KV.tenants key live"),
        "init_nats":     ("none",   "bridge --init-nats: the whole NATS stack generated, then BOOTED and proven"),
        "revoke_full":   ("none",   "the hard kill: --revoke --conf amends the account JWT; reload kicks the live session"),
        "grammar_served": ("bridge", "the embedded grammar served at /grammar and /enroll; a file-free libzb client syncs"),
        "telemetry":     ("none",   "HTTP surface"),
        "writable":      ("client", "grants vs published write contract"),
        "mutate":        ("client", "LWW round trip"),
        "replies":       ("client", "every write gets a verdict"),
        "offline":       ("client", "outbox replay by version"),
        "tiebreak":      ("client", "equal versions resolved by the tiebreak column"),
        "clamp":         ("client", "future versions clamped"),
        "clockskew":     ("client", "LWW under a lying clock: theft bounded, loss audible, replicas convergent"),
        "crdt":          ("client", "map-of-registers on jsonb: blind replace loses intents, merge-on-stale loses none"),
        "widthguard":    ("client", "row width guard, psql and edge"),
        "rowsize":       ("client", "oversized row → verdict, not suspension"),
        "probe":         ("client", "read the schema, denied the write, once"),
        "reaps":         ("bridge", "sweeper reaps never reach clients"),
        "tenant_kv":     ("bridge", "$KV.tenants exact-key grants"),
        "crosstenant":   ("client", "cross-tenant reach (expected to find the known hole)"),
        "dyntenant":     ("bridge", "tenant born at runtime"),
        "invalidate":    ("bridge", "the caches that must notice DDL"),
        "keys":          ("client", "database-allocated keys refused on the write path"),
        "connbudget":    ("bridge", "connection limits"),
        "sweeper":       ("bridge", "tombstone GC boundary"),
        "client_gap":    ("bridge", "a client returns after the tail is gone: gap → re-seed → converge"),
        "shared_gap":    ("bridge", "a CDC_PUBLIC gap re-seeds tenant-scoped tables too — their shared rows ride it"),
    },
    "owns": {
        "sizing":        ("bridge", "BASE_BUF / ring sizing refusals"),
        "endpoint":      ("bridge", "one NATS address"),
        "credentials":   ("bridge", "no admin fallback; principal enforced"),
        "downtime":      ("bridge", "slot resume across kills"),
        "decode_integrity": ("bridge", "zero-copy decode never aliases"),
        "genproducer":   ("bridge", "chains: full, delta, prune"),
        "chain_kill":    ("bridge", "kill -9 mid-chain-build: the manifest never names a missing object"),
        "txn_kill":      ("bridge", "kill -9 mid-transaction: the unacked transaction replays whole, no loss"),
        "stream_full":   ("bridge", "a full stream refuses publishes: retry budget, deliberate stop, lossless restart"),
        "shrink":        ("bridge", "BASE_BUF lowered under stored data: the shrink-gated scan warns at boot"),
        "legacybait":    ("bridge", "pre-guard oversized rows"),
        "suspension_lift": ("bridge", "a row_too_large suspension lifts live once its cause is gone"),
        "livebirth":     ("bridge", "a table born and enabled while running"),
        "fleet":         ("bridge", "clients' heartbeats → per-client lag on /metrics; every slot inventoried"),
        "race":          ("bridge", "24 writers against ingress"),
        "adversarial":   ("bridge", "fuzz the two untrusted entry points"),
        "chaos":         ("bridge", "broker kill, backend kill, socket exhaustion"),
        "nats_outage":   ("bridge", "broker down past the retry budget: bridge stops, resumes, nothing lost"),
        "slot_loss":     ("bridge", "slot invalidated: refused at boot, recovered as a new feed, clients re-seed"),
        "slot_contest":  ("bridge", "two bridges, one slot: the loser refuses cleanly and leaks nothing"),
        "jwt_expiry":    ("bridge", "enroll with a tiny TTL: full invite-code bootstrap, then the read door closes audibly"),
        "churn":         ("bridge", "65 reconnect cycles (50 NATS, 15 PG, wasp swarms): RSS/fd/threads flat, counters honest, one client converges"),
        "stream_wipe":   ("bridge", "a CDC stream deleted wholesale: bridge stops, boot recreates, client resets and converges"),
        "pg_restart":    ("bridge", "PostgreSQL stopped and restarted: patient retry, resume from the slot, no loss"),
        "matrix":        ("bridge", "PG and NATS failing and returning in every order; the 3am case on top"),
        "sweeper_restart": ("bridge", "PostgreSQL restarts under a running sweeper: reconnect re-arms statements, principal, UTC"),
        "cascade":       ("bridge", "NATS gone under a steady feed: ring fills, bridge halts, WAL dams behind the slot, then drains"),
        "client_kill":   ("bridge", "kill -9 the client's HOST mid-seed: the SQLite file survives, the re-seed converges"),
        "migrate_both":  ("bridge", "every migration shape (add/rename/drop/volatile default/re-key/drop table) under libzb AND zb-client-ts at once"),
        "column_flood":  ("bridge", "a migration grows a table past MAX_COLUMNS: suspended not crashed, writes meanwhile dropped, the restart re-detects and re-seeds both clients"),
        "rekey_two_parents": ("bridge", "two parents re-keyed in one transaction, a child referencing both: one epoch bump each, both clients converge"),
        "rekey_offline": ("bridge", "both clients OFFLINE across a re-key and the writes after it: reopen on the same files, meet the new key and epoch at once, converge"),
        "cascade_held":  ("bridge", "a two-level cascade delete while the middle rows are HELD in the inbox: no ghost, no orphan, both replicas equal PostgreSQL"),
        "tombstone_children": ("bridge", "a parent's tombstone is refused while a live child references it; children first, then the parent; a cascade into a tombstone table refused at enable and at DDL; the reap"),
        "revoke_midseed": ("bridge", "a principal revoked while its re-seed is in flight: the mapping rung (seed completes, writes die) and the full hammer (session kicked mid-download, no partial table, cause named); a witness untouched"),
        "suspension_reasons": ("bridge", "every refusal reason: the registry's row and the published descriptor say the same thing, live and at boot; each lifts when fixed"),
        "rebuild_kill":  ("bridge", "a client killed at every point of a local rebuild and of a re-key: each leftover state converges on reopen, both clients"),
        "outbox_break":  ("bridge", "the application tries to damage the client's bookkeeping through query(): every write refused on both clients, the queued write survives and lands"),
        "write_stale":   ("bridge", "writes queued while the bridge is down, delivered after a DROP COLUMN, a new NOT NULL column and a re-key: verdicts, reverts, no ghost rows, both replicas equal PostgreSQL"),
        "rebase_stale":  ("bridge", "an UPDATE judged stale is rebased when its columns are disjoint from the winner's (both clients, verdict before and after the echo, a slow clock, a queued offline write), dropped and surfaced when they overlap"),
    },
    "manual": {
        "speed":         ("bridge", "2M-row benchmark — hours of machine, not a verdict"),
        "arrays":        ("live",   "arrays as JSON text on the wire: a SQLite replica reads them with json_extract, a PGlite replica holds native arrays from the same wire, writes with arrays from both clients land as native arrays and echo in each engine's shape, the audit agrees (§10ey)"),
        "collist":       ("live",   "publication column lists: a STORED tsvector and a column the caller leaves out never travel — the publication, the descriptor, a libzb replica, CDC, a client write and the audit agree; zebridge_enable refreshes the list after ADD COLUMN (§10ff)"),
        "vectors":       ("live",   "pgvector and bit(n) on the wire: vector/halfvec/sparsevec/bit as BLOBs sqlite-vec reads as they are (vec_distance_L2, vec_bit), varbit as text, a client's bytes back in pgvector's text form, the audit, a PostgreSQL replica with pgvector's own types; needs pgvector on both databases and sqlite-vec in the venv (§10fg)"),
        "pgreplica":     ("live",   "a PostgreSQL replica: libzb with dbUrl — the table from the descriptor's pg block, the seed, CDC, a client write with arrays, bytes and a point, all native in the replica; needs the zb_replica database with PostGIS (§10fd)"),
        "blobs":         ("live",   "bytes end to end: a bytea tile and a PostGIS point — BLOB in the replica, seed and CDC byte-exact, a client's write lands as the same bytea and point, the Node client and the audit agree; needs PostGIS (§10ex)"),
        "cdc_wall":      ("live",   "the wall: a client whose position fell off a pruned CDC stream — the count valve with the chain inside the window (re-seed, agree), the age under the 2 × cadence floor (doctor, boot and fleet monitor say so; no resume past the hole), the bridge down longer than the age; runs its own bridge on the live slot, stop yours first (§10eg)"),
        "drip":          ("live",   "the wasp: a create/update/delete trio on test_types every 200 ms for hours against the running stack, invariants checked every minute (PG == both replicas, outboxes empty, no refused verdict, RSS flat, tombstones bounded), the sweeper run hourly"),
        "swarm":         ("bridge", "the 100-client hour: 50 node + 49 python + 1 PGlite, ~160 mut/s, faults, whole-replica equality"),
        "stamp":         ("bridge", "the capacity stamp: saturated, fault-free, flat applied-rate for 3 minutes"),
        "burst":         ("none",   "throughput driver, leaves rows behind"),
        "leaksoak":      ("bridge", "macOS leaks soak"),
        "objstore_race": ("bridge", "40 MB get/put race"),
        "tls":           ("none",   "TLS against the vendored nats.zig — optional for the colocated topology"),
    },
}


def derived_env() -> dict:
    """What `scripts/zb-derive-env.py` prints (`export K=V` lines): the POSTGRES_*
    role names/passwords and OPEN_TENANT the SQL templates interpolate. Without them a
    rendered template carries `CREATE USER  WITH PASSWORD ''` (measured: pubname)."""
    out = {}
    try:
        text = subprocess.run([PY, str(ROOT / "scripts" / "zb-derive-env.py")], capture_output=True, text=True, cwd=ROOT).stdout
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("export "):
            line = line[7:]
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip("'\"")
    return out


DERIVED = None


def env_for(role: str) -> dict:
    """⚠️ The role OVERRIDES `NATS_CREDS`, it never inherits it: `.env.bridge` carries
    `bridge.creds`, and a client scenario that inherited it ran as the bridge — every
    confinement check passed while proving nothing, and crosstenant found the whole
    world reachable (measured 2026-08-29). `ZB_CLIENT_CREDS` picks the client."""
    global DERIVED
    if DERIVED is None:
        DERIVED = derived_env()
    env = dict(os.environ)
    for k, v in DERIVED.items():
        env.setdefault(k, v)
    # ⚠️ `info`, overriding whatever .env.bridge carries. At debug the bridge warns that
    # per-event hot-path logging costs ~4x CPU (so any timing a scenario measures is
    # invalid), and the client dumps raw payloads — a 5.4 MiB compressed chain object
    # landed in a scenario log as binary and made it ungreppable. ZB_LOG_LEVEL overrides.
    env["LOG_LEVEL"] = os.environ.get("ZB_LOG_LEVEL", "info")
    if role == "bridge":
        env["NATS_CREDS"] = str(CREDS / "bridge.creds")
        env.pop("ZB_PRINCIPAL", None)
    elif role == "client":
        env["NATS_CREDS"] = os.environ.get("ZB_CLIENT_CREDS", str(CREDS / "omar.creds"))
    return env


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("group", nargs="?", choices=[*GROUPS, "all"], default="offline")
    ap.add_argument("-k", action="append", default=[], help="only scenarios whose name contains this")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--logs", default=os.environ.get("TMPDIR", "/tmp"), help="where per-scenario logs go")
    a = ap.parse_args()
    if a.list:
        for g, items in GROUPS.items():
            print(f"{g}:")
            for n, (role, note) in items.items():
                print(f"  {n:18s} {role:7s} {note}")
        return 0
    groups = ["offline", "live", "owns"] if a.group == "all" else [a.group]
    if a.group == "manual":
        print("manual scenarios are listed, not run: see --list"); return 0
    results = []
    for g in groups:
        for name, (role, note) in GROUPS[g].items():
            if a.k and not any(k in name for k in a.k):
                continue
            log = pathlib.Path(a.logs) / f"zb-scenario-{name}.log"
            t0 = time.monotonic()
            with open(log, "w") as f:
                rc = subprocess.run([PY, str(HERE / f"{name}.py")], env=env_for(role), stdout=f, stderr=subprocess.STDOUT, cwd=ROOT).returncode
            dt = time.monotonic() - t0
            results.append((g, name, rc, dt, log))
            print(f"  {'✓' if rc == 0 else '✗'} {g}/{name:18s} rc={rc:<3d} {dt:6.1f}s  {log}", flush=True)
    failed = [r for r in results if r[2] != 0]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed" + (f"; failed: {', '.join(r[1] for r in failed)}" if failed else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""One fresh SQLite replica: direct Postgres SELECT/import versus libzb/NATS.

See README.md beside this file. No concurrency; every sample gets its own process
and a new database. The private fixture is never written during measurements.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[3]
TABLE = "seed_items"
TENANT = "seedbench"
PRINCIPAL = "seed-reader"
COLUMNS = "id, tenant_id, note, quantity, updated_at, deleted_at"


def sqlite_value(value):
    if isinstance(value, datetime.datetime):
        return value.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return value


def fingerprint(rows):
    """Unambiguous, typed framing: NULL, empty strings and newlines stay distinct."""
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        payload = json.dumps([sqlite_value(v) for v in row], ensure_ascii=False,
                             separators=(",", ":")).encode()
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
        count += 1
    return {"rows": count, "sha256": digest.hexdigest()}


def verify(path, expected):
    with sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True) as db:
        actual = fingerprint(db.execute(f"SELECT {COLUMNS} FROM {TABLE} ORDER BY id"))
        if actual != expected:
            raise RuntimeError(f"local contents differ: expected {expected}, got {actual}")
        return actual


def storage_settings(db):
    # Match libzb/src/storage.zig:217-233. Never disable journaling/fsync for the
    # direct path; both endpoints mean committed and queryable, not power-loss safe.
    db.execute("PRAGMA page_size = 16384")
    db.execute("PRAGMA journal_mode = WAL")
    db.execute("PRAGMA synchronous = NORMAL")
    db.execute("PRAGMA wal_autocheckpoint = 10000")
    db.execute("PRAGMA foreign_keys = ON")
    db.execute("PRAGMA cache_size = -131072")


def direct(config, path):
    import psycopg

    started = time.perf_counter()
    pg = psycopg.connect(config["pg_url"], connect_timeout=10)
    db = None
    try:
        connected = time.perf_counter()
        db = sqlite3.connect(path)
        storage_settings(db)
        for ddl in config["ddl"]:
            db.execute(ddl)
        db.commit()
        created = time.perf_counter()
        # A named cursor bounds memory and transfers batches over one connection.
        # The fixed fixture's tenant is selected explicitly on both paths. The
        # direct connection is also the bridge reader role, under the same RLS.
        pg.execute("SET TRANSACTION READ ONLY")
        pg.execute("SELECT set_config('zb.tenant', %s, true)", (TENANT,))
        with pg.cursor(name="seed") as cursor:
            cursor.execute(f"SELECT {COLUMNS} FROM public.{TABLE} WHERE tenant_id = %s ORDER BY id", (TENANT,))
            while rows := cursor.fetchmany(config["batch_rows"]):
                db.executemany(f"INSERT INTO {TABLE} VALUES (?, ?, ?, ?, ?, ?)",
                               (tuple(sqlite_value(v) for v in row) for row in rows))
                db.commit()
        loaded = time.perf_counter()
        count = db.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
        ready = time.perf_counter()
        if count != config["expected"]["rows"]:
            raise RuntimeError(f"direct import: expected {config['expected']['rows']} rows, got {count}")
        return {"seconds": ready - started, "engine_sqlite_version": sqlite3.sqlite_version,
                "phases_seconds": {
            "connect": connected - started, "create": created - connected,
            "select_and_insert": loaded - created, "ready_query": ready - loaded}}
    finally:
        if db is not None:
            db.close()
        pg.close()


def libzb(config, path, *, enroll=False):
    # Use the C ABI directly so no Python background poll thread or sleep interval
    # becomes part of the result (zb-python's constructor also connects AND syncs).
    sys.path.insert(0, str(ROOT / "zb-python/src"))
    from zebridge import _native as native

    def take(ptr):
        result = native.take(ptr)
        if result.get("error"):
            raise RuntimeError(f"libzb: {result}")
        return result

    options = {"natsUrl": config["nats_url"], "credsPath": config["creds"],
               "principal": PRINCIPAL, "jsDomain": "hub", "dbPath": str(path),
               "clientId": path.stem, "tables": [TABLE], "heartbeatMs": 0,
               "seedChunkRows": config["batch_rows"]}
    identity_path = Path(str(path) + ".identity")
    if enroll:
        if identity_path.exists():
            raise RuntimeError("enrollment requires a new identity")
        # The endpoint supplies NATS URLs, domain and principal. No pre-minted
        # credentials or endpoint override may bypass the real invitation path.
        options = {key: value for key, value in options.items()
                   if key not in ("natsUrl", "credsPath", "principal", "jsDomain")}
        options.update(bridgeUrl=config["bridge_url"], invite=config["invite"],
                       identityPath=str(identity_path))
    encoded = json.dumps(options).encode()
    query = (f"SELECT count(*) FROM {TABLE} WHERE 1 = "
             f"(SELECT count(*) FROM _zbz_generations WHERE tbl = '{TABLE}' "
             f"AND tenant = '{TENANT}' AND watermark IS NOT NULL)").encode()
    started = time.perf_counter()
    handle = native.lib.zb_client_connect(encoded)
    if not handle:
        raise RuntimeError(native.last_error() or "libzb connect failed")
    connected = time.perf_counter()
    try:
        take(native.lib.zb_client_sync(handle))
        while True:
            result = take(native.lib.zb_client_query(handle, query, b"[]"))
            if result["rows"][0][0] == config["expected"]["rows"]:
                break
            take(native.lib.zb_client_poll(handle, 10))
        elapsed = time.perf_counter() - started
        version = take(native.lib.zb_client_query(handle, b"SELECT sqlite_version()", b"[]"))["rows"][0][0]
        result = {"seconds": elapsed, "engine_sqlite_version": version}
        if enroll:
            identity = json.loads(identity_path.read_text())
            if identity.get("principal") != PRINCIPAL or not identity.get("creds"):
                raise RuntimeError("enrollment did not persist the expected identity")
            result.update(identity_saved=True, phases_seconds={
                "connect_with_enrollment": connected - started,
                "sync_to_ready": elapsed - (connected - started)})
        return result
    finally:
        native.lib.zb_client_close(handle)


def worker(config_path, mode, path):
    config = json.loads(config_path.read_text())
    if path.exists():
        raise RuntimeError(f"refusing to reuse a replica: {path}")
    result = direct(config, path) if mode == "direct" else libzb(config, path, enroll=mode == "enrolled")
    # Both timed handles have closed. A fresh SQLite reader proves that the rows
    # survived close; hashing and teardown are deliberately outside the timer.
    result.update(verify(path, config["expected"]))
    result.update(mode=mode, verification_sqlite_version=sqlite3.sqlite_version)
    print(json.dumps(result))


def enrollment_duration(stderr):
    # capi.zig times the real enrollment/save path, inside zb_client_connect.
    # Fail if an older build cannot report it; never invent a phase by subtraction.
    matches = re.findall(r"zebridge: enrolled in (\d+) ms \(the bridge's /enroll, the identity saved\)", stderr)
    if len(matches) != 1:
        raise RuntimeError("expected exactly one libzb enrollment timing; use a current libzb build")
    return int(matches[0]) / 1000


def sample(config_path, mode, path, timeout):
    completed = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--worker", mode,
         "--config", str(config_path), "--db", str(path)],
        capture_output=True, text=True, timeout=timeout,
        env=dict(os.environ, ZB_LOG_LEVEL="info") if mode == "enrolled" else None)
    if completed.returncode:
        raise RuntimeError(f"{mode} worker failed:\n{completed.stderr[-3000:]}")
    result = json.loads(completed.stdout)
    if mode == "enrolled":
        result["phases_seconds"]["enrollment"] = enrollment_duration(completed.stderr)
    return result


def start_bridge(stack, *, enrollment):
    sys.path.insert(0, str(ROOT / "scripts/scenarios"))
    from hub_fallback_stage2 import port

    http_port = port()
    env = dict(stack.env, ZB_FEED_RESTART="1")
    if enrollment:
        # Generated operator config, same signing key trusted by this private NATS.
        identity_dir = stack.tmp / f"{stack.name}-id"
        values = dict(line.split("=", 1) for line in (identity_dir / ".env.nats").read_text().splitlines()
                      if "=" in line and not line.startswith("#"))
        env.update(ZB_SIGNING_SEED=values["ZB_SIGNING_SEED"],
                   ZB_ACCOUNT_PUB=values["ZB_ACCOUNT_PUB"], ENROLL_NATS_URL=stack.url)
    stack.spawn("bridge1", [str(stack.bridge_bin), "--pub", "rr_pub", "--slot", stack.name + "_slot",
                            "--port", str(http_port)], env)
    stack.wait_log("bridge1", "Replication started successfully")
    stack.wait_log("bridge1", f"g1 for '{TENANT}'/'{TABLE}': full")
    if enrollment:
        stack.wait_log("bridge1", "enrollment endpoint armed")
    return f"http://127.0.0.1:{http_port}"


async def wait_seed_metadata(stack, timeout):
    import nats
    from nats.js.errors import NotFoundError, KeyNotFoundError

    nc = await nats.connect(stack.url, user_credentials=stack.bridge_creds)
    try:
        js = nc.jetstream(domain="hub")
        deadline = time.monotonic() + timeout
        # Boot log lines can precede visibility through the direct KV read API.
        # Do not charge a fresh client for waiting on a partially prepared service.
        while time.monotonic() < deadline:
            try:
                for bucket, key in (("schemas", TABLE), ("tenants", PRINCIPAL),
                                    ("generations", f"{TENANT}.{TABLE}")):
                    kv = await js.key_value(bucket)
                    entry = await kv.get(key)
                    if not entry.value:
                        raise RuntimeError(f"empty seed metadata in {bucket}")
                return
            except (NotFoundError, KeyNotFoundError):
                await asyncio.sleep(0.05)
        raise TimeoutError("seed schema, tenant mapping and generation did not become readable")
    finally:
        await nc.close()


def enrollment_samples(stack, tmp, config, args):
    results = []
    for round_no in range(1, args.rounds + 1):
        # Invitation creation is backend setup, excluded from the device's timer.
        code = "seed-" + uuid.uuid4().hex
        stack.sql("INSERT INTO zebridge_invites (code, principal, tenant_id) "
                  f"VALUES ('{code}', '{PRINCIPAL}', '{TENANT}')")
        config_path = tmp / f"enroll-{round_no}.json"
        config_path.write_text(json.dumps(dict(config, invite=code)))
        config_path.chmod(0o600)
        result = sample(config_path, "enrolled", tmp / f"enrolled-{round_no}.sqlite3", args.timeout)
        if stack.sql(f"SELECT used_at IS NOT NULL FROM zebridge_invites WHERE code = '{code}'") != "t":
            raise RuntimeError("enrollment did not redeem its fresh invitation")
        result.update(round=round_no, invite_redeemed=True)
        results.append(result)
        phases = result["phases_seconds"]
        print(f"Enrollment {round_no}: {phases['enrollment']:.3f} s; "
              f"connect+seed {result['seconds']:.6f} s; verified", file=sys.stderr, flush=True)
    return {"transport": "HTTP over loopback", "bridge_running_during_measurement": True,
            "enrollment_resolution_seconds": 0.001, "invitation_creation_timed": False,
            "fresh_identity_per_sample": True,
            "median_seconds": {
                **{phase: statistics.median(r["phases_seconds"][phase] for r in results)
                   for phase in ("enrollment", "connect_with_enrollment", "sync_to_ready")},
                "total": statistics.median(r["seconds"] for r in results)}, "samples": results}


def run(args):
    # Reuse the existing scenario's private DB, roles, authenticated NATS, and
    # bridge lifecycle; do not touch a developer's configured tables or streams.
    sys.path.insert(0, str(ROOT / "scripts/scenarios"))
    import psycopg
    import recreate_recovery as rr

    with tempfile.TemporaryDirectory(prefix="zb-seed-compare-") as directory:
        tmp = Path(directory)
        stack = rr.Stack(tmp, "seed-" + tmp.name, 600,
                         {"CDC_MAX_AGE_SECONDS": "86400"}, pg_port=args.pg_port)
        try:
            print("Preparing isolated fixture and generation (untimed)…", file=sys.stderr, flush=True)
            stack.start({PRINCIPAL: TENANT})
            stack.sql(f"""
                CREATE TABLE public.{TABLE} (
                  id text PRIMARY KEY, tenant_id text NOT NULL, note text,
                  quantity integer NOT NULL,
                  updated_at timestamptz NOT NULL DEFAULT '2026-01-01T00:00:00.123456Z',
                  deleted_at timestamptz);
                SELECT count(*) FROM zebridge_enable('public.{TABLE}'::regclass,
                  writable => false, tenant_col => 'tenant_id', version_col => 'updated_at',
                  tombstone_col => 'deleted_at', publication => 'rr_pub', dry_run => false);
                INSERT INTO public.{TABLE} (id, tenant_id, note, quantity)
                SELECT 'row-' || lpad(i::text, 12, '0'), '{TENANT}',
                  CASE WHEN i % 17 = 0 THEN NULL ELSE
                    left(repeat(md5(i::text), {max(1, (args.payload_bytes + 31) // 32)}), {args.payload_bytes}) END,
                  i FROM generate_series(1, {args.rows}) AS i;
                ANALYZE public.{TABLE};
            """)
            bridge_url = start_bridge(stack, enrollment=args.enrollment)
            asyncio.run(wait_seed_metadata(stack, args.timeout))
            pg_url = stack.env["DATABASE_READER_URL"]
            with psycopg.connect(pg_url) as pg:
                pg.execute("SELECT set_config('zb.tenant', %s, true)", (TENANT,))
                expected = fingerprint(pg.execute(
                    f"SELECT {COLUMNS} FROM {TABLE} WHERE tenant_id = %s ORDER BY id", (TENANT,)))
                pg_version = pg.info.server_version
            if expected["rows"] != args.rows:
                raise RuntimeError("reader role cannot see the entire fixture")
            config = {"pg_url": pg_url, "nats_url": stack.url, "creds": stack.creds[PRINCIPAL],
                      "batch_rows": args.batch_rows, "expected": expected, "bridge_url": bridge_url}
            config_path = tmp / "config.json"
            config_path.write_text(json.dumps(config))
            config_path.chmod(0o600)
            # Warm preparation proves a complete *generation* exists, rather than
            # measuring a CDC replay of inserts or waiting for generation creation.
            probe = tmp / "prepared.sqlite3"
            sample(config_path, "libzb", probe, args.timeout)
            with sqlite3.connect(probe) as db:
                config["ddl"] = [row[0] for row in db.execute(
                    "SELECT sql FROM sqlite_master WHERE tbl_name = ? AND sql IS NOT NULL "
                    "AND type IN ('table', 'index') ORDER BY type DESC, name", (TABLE,))]
                local_columns = [row[1] for row in db.execute(f"PRAGMA table_info({TABLE})")]
                libzb_sqlite = db.execute("PRAGMA page_size").fetchone()[0]
            if local_columns != COLUMNS.split(", "):
                raise RuntimeError(f"unexpected replica columns: {local_columns}")
            config_path.write_text(json.dumps(config))

            enrollment = enrollment_samples(stack, tmp, config, args) if args.enrollment else None

            # With a static source, NATS can serve the prepared objects and retained
            # stream without a running bridge. Remove producer work from BOTH timers.
            for label in list(stack.procs):
                if label.startswith("bridge"):
                    stack.bridge_down(label)
            results = []
            for round_no in range(1, args.rounds + 1):
                order = ("direct", "libzb") if round_no % 2 else ("libzb", "direct")
                for mode in order:
                    result = sample(config_path, mode, tmp / f"{round_no}-{mode}.sqlite3", args.timeout)
                    result["round"] = round_no
                    results.append(result)
                    print(f"Round {round_no}: {mode} {result['seconds']:.6f} s; verified", file=sys.stderr, flush=True)
            medians = {mode: statistics.median(r["seconds"] for r in results if r["mode"] == mode)
                       for mode in ("direct", "libzb")}
            return {"benchmark": "postgres-select-vs-libzb-seed", "verified": True,
                    "rows": args.rows, "payload_bytes": args.payload_bytes,
                    "batch_rows": args.batch_rows, "rounds": args.rounds,
                    "concurrent_clients": 1, "generation_prepared": True,
                    "bridge_running_during_measurement": False,
                    "enrollment_timed": False, "schema": config["ddl"],
                    "sqlite_page_size": libzb_sqlite,
                    "postgres_version": pg_version, "python_version": platform.python_version(),
                    "psycopg_version": psycopg.__version__,
                    "platform": platform.platform(),
                    "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                    "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "bridge_sha256": hashlib.sha256(rr.BRIDGE.read_bytes()).hexdigest(),
                    "libzb_sha256": hashlib.sha256(Path(os.environ.get("ZB_LIB", rr.LIBRARY)).read_bytes()).hexdigest(),
                    "nats_version": subprocess.check_output(["nats-server", "--version"], text=True).strip(),
                    "median_seconds": medians,
                    "libzb_over_direct": medians["libzb"] / medians["direct"],
                    "enrollment": enrollment,
                    "samples": results}
        finally:
            stack.stop()


def positive(value):
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=positive, default=50000)
    parser.add_argument("--payload-bytes", type=positive, default=192)
    parser.add_argument("--rounds", type=positive, default=5)
    parser.add_argument("--batch-rows", type=positive, default=50000)
    parser.add_argument("--pg-port", type=positive, default=5432)
    parser.add_argument("--timeout", type=positive, default=180, help="hard timeout per worker, seconds")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--enrollment", action="store_true", help="also measure fresh invitation enrollment and seeding")
    parser.add_argument("--worker", choices=("direct", "libzb", "enrolled"), help=argparse.SUPPRESS)
    parser.add_argument("--config", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--db", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(args.config, args.worker, args.db)
        return
    result = run(args)
    output = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(output)
    print(output, end="")


if __name__ == "__main__":
    main()

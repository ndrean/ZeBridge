#!/usr/bin/env python3
"""Read-only ZeBridge cold-seed runner paired with seed_electric.mjs.

Prepare identities before timing:
  PYTHONPATH=zb-python/src python3 scripts/compare/electric/seed_zebridge.py prepare 10
Then run cold seeds after the fixture and bridge are ready:
  PYTHONPATH=zb-python/src python3 scripts/compare/electric/seed_zebridge.py seed 10
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
import pathlib
import sqlite3
import statistics
import tempfile
import time

from zebridge import ZeBridge, ZeBridgeError

NATS_URL = os.environ.get("ZB_COMPARE_NATS_URL", "nats://127.0.0.1:14223")
BRIDGE_URL = os.environ.get("ZB_COMPARE_BRIDGE_URL", "http://127.0.0.1:27435")
TABLE = "storm_items"
COUNT = int(os.environ.get("COMPARE_ROWS", "50000"))
STATE = pathlib.Path(os.environ.get(
    "ZB_COMPARE_STATE", str(pathlib.Path(tempfile.gettempdir()) / "zebridge-electric-compare")
))


def identity_path(i: int) -> pathlib.Path:
    return STATE / "identities" / f"compare-reader-{i:03d}.identity"


def db_path(i: int) -> pathlib.Path:
    return STATE / "zebridge" / f"client-{i:03d}.sqlite3"


def client_options(i: int, *, enroll: bool) -> dict:
    principal = f"compare-reader-{i:03d}"
    opts = {
        "bridge_url": BRIDGE_URL if enroll else "",
        "nats_url": NATS_URL,
        "identity_path": str(identity_path(i)),
        "db_path": str(db_path(i)),
        "client_id": f"electric-read-compare-{i:03d}",
        "tables": [TABLE],
        "heartbeat_ms": 0,
    }
    if enroll:
        opts["invite"] = f"zb-read-compare-{i:03d}"
    return opts


def remove_replica(i: int) -> None:
    p = db_path(i)
    for suffix in ("", "-wal", "-shm", ".lock"):
        try:
            pathlib.Path(str(p) + suffix).unlink()
        except FileNotFoundError:
            pass


def prepare_one(i: int) -> str:
    identity_path(i).parent.mkdir(parents=True, exist_ok=True)
    db_path(i).parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(30):
        try:
            with ZeBridge(client_options(i, enroll=True), poll_ms=250) as client:
                if "cmp" not in client.tenants:
                    raise RuntimeError(f"client {i} did not receive tenant cmp: {client.tenants}")
            remove_replica(i)
            return str(identity_path(i))
        except ZeBridgeError as error:
            transient = any(word in str(error).lower() for word in
                            ("503", "streamnotfound", "unreachable", "temporarily unavailable", "busy"))
            if not transient or attempt == 29:
                raise
            time.sleep(0.5 + attempt * 0.2)
    raise RuntimeError(f"client {i}: enrollment did not complete")


def expected_digest() -> str:
    h = hashlib.sha256()
    for i in range(1, COUNT + 1):
        row_id = f"row-{i:05d}"
        note = hashlib.md5(str(i).encode()).hexdigest() * 6
        h.update(f"{row_id}\0cmp\0{note}\n".encode())
    return h.hexdigest()
EXPECTED_DIGEST = expected_digest()


def seed_one(i: int, timeout: float) -> dict:
    p = db_path(i)
    p.parent.mkdir(parents=True, exist_ok=True)
    remove_replica(i)
    started = time.perf_counter()
    with ZeBridge(client_options(i, enroll=False), poll_ms=250) as client:
        deadline = started + timeout
        while time.perf_counter() < deadline:
            try:
                n = client.query(f"SELECT count(*) AS n FROM {TABLE}")[0]["n"]
                if n == COUNT:
                    seconds = time.perf_counter() - started
                    break
            except ZeBridgeError:
                pass  # Local schema/data have not arrived yet.
            time.sleep(0.05)
        else:
            raise TimeoutError(f"client {i}: did not apply {COUNT} rows in {timeout:g} s")
    return {"client": i, "seconds": seconds, "db_path": str(p)}


def verify_one(result: dict) -> dict:
    conn = sqlite3.connect(f"file:{result['db_path']}?mode=ro", uri=True)
    try:
        h = hashlib.sha256()
        n = 0
        for row_id, tenant_id, note in conn.execute(
                f"SELECT id, tenant_id, note FROM {TABLE} ORDER BY id"):
            h.update(f"{row_id}\0{tenant_id}\0{note}\n".encode())
            n += 1
        digest = h.hexdigest()
        if n != COUNT or digest != EXPECTED_DIGEST:
            raise RuntimeError(f"client {result['client']}: local verification mismatch, rows={n}, sha256={digest}")
        return {"client": result["client"], "seconds": result["seconds"], "rows": n, "sha256": digest}
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="phase", required=True)
    for phase in ("prepare", "seed"):
        p = sub.add_parser(phase)
        p.add_argument("clients", type=int)
        if phase == "seed":
            p.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    if not 1 <= args.clients <= 100:
        parser.error("clients must be between 1 and 100 (the fixture creates 100 invites)")

    STATE.mkdir(parents=True, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(args.clients, 4) if args.phase == "prepare" else args.clients) as pool:
        if args.phase == "prepare":
            results = list(pool.map(prepare_one, range(args.clients)))
            print(json.dumps({"system": "zebridge", "phase": "prepare", "identities": len(results),
                              "state_dir": str(STATE)}))
            return
        results = list(pool.map(lambda i: seed_one(i, args.timeout), range(args.clients)))

    # Verify only after every timed client has stopped, so checksum work cannot slow
    # another client's seed and inflate its result.
    results = [verify_one(r) for r in results]
    times = sorted(r["seconds"] for r in results)
    output = {
        "system": "zebridge", "phase": "cold-seed", "clients": args.clients,
        "rows_per_client": COUNT, "median_seconds": round(statistics.median(times), 3),
        "slowest_seconds": round(max(times), 3), "verified": len({r["sha256"] for r in results}) == 1,
        "row_sha256": results[0]["sha256"], "clients_result": results,
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()

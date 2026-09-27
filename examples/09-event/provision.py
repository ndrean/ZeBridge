#!/usr/bin/env python3
"""09-event: the table the simulated sensors write into.

    scripts/scenarios/.venv/bin/python examples/09-event/provision.py            # create + enable, wait for the chain
    scripts/scenarios/.venv/bin/python examples/09-event/provision.py teardown   # drop it

`sensor_events` follows TimescaleDB's events-uuidv7 walkthrough: the key IS the time.
`event_id` is a UUIDv7 minted by the sensor, so its first 48 bits are the reading's
millisecond; PostgreSQL 18 (`uuid_extract_timestamp`) and DuckDB read it back, and there is
no separate timestamp column. Key order is time order, which is also the cheap case for a
replica: every insert lands at the right edge of the index.

Writable from the edge (sensors publish mutations as `bob`, tenant globex), with a
tombstone so a retention job's deletes travel as ordinary deltas. Run with .env.admin
loaded (ADMIN_DATABASE_URL) and the dev bridge running.
"""
import json, os, pathlib, subprocess, sys, time

ROOT = pathlib.Path(__file__).resolve().parents[2]
PSQL = os.environ.get("PSQL", "/opt/homebrew/opt/postgresql@18/bin/psql")
ADMIN_URL = os.environ.get("ADMIN_DATABASE_URL", "postgres://postgres@127.0.0.1:5432/postgres")
TABLE, TENANT = "sensor_events", "globex"
SERVICE = "events"  # the responder principal of event_service.py
NATS = ["nats", "--creds", str(ROOT / "scripts/native/creds/bridge.creds"), "--inbox-prefix", "_INBOX.bridge",
        "-s", os.environ.get("NATS_URL", "nats://127.0.0.1:4222")]
BRIDGE_LOG = ROOT / "scripts/native/bridge.log"

DDL = f"""CREATE TABLE public.{TABLE} (
    event_id   uuid PRIMARY KEY DEFAULT uuidv7(),  -- the reading's time, to the millisecond
    tenant_id  text NOT NULL,
    sensor_id  integer NOT NULL,
    kind       text NOT NULL,                      -- temperature | humidity | pressure
    value      double precision NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(), -- the version column (last-write-wins)
    deleted_at timestamptz                         -- the tombstone: retention deletes travel as deltas
)"""


def psql(sql: str) -> str:
    r = subprocess.run([PSQL, ADMIN_URL, "-XtAq", "-v", "ON_ERROR_STOP=1", "-c", sql], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"psql: {r.stderr.strip()[:400]}")
    return r.stdout.strip()


def kv(bucket: str, key: str) -> dict:
    r = subprocess.run(NATS + ["kv", "get", bucket, key, "--raw"], capture_output=True, text=True, timeout=10)
    return json.loads(r.stdout) if r.returncode == 0 and r.stdout.strip() else {}


def setup():
    psql(f"DROP TABLE IF EXISTS public.{TABLE}")
    time.sleep(3)  # the bridge prunes the dropped table before it comes back
    psql(DDL)
    mark = BRIDGE_LOG.stat().st_size if BRIDGE_LOG.exists() else 0
    out = psql(f"SELECT step || ': ' || detail FROM zebridge_enable('public.{TABLE}'::regclass, tenant_col => 'tenant_id', "
               f"writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
               f"publication => 'my_pub', dry_run => false) WHERE status = 'ERROR'")
    if out:
        sys.exit(f"zebridge_enable refused: {out}")
    # The service's principal needs its tenant, like any reader of a tenant-scoped table:
    # NATS lets `events` in (its credential's tag), this row tells libzb which tenant it
    # follows (it reaches the `tenants` KV bucket through the WAL).
    psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) VALUES ('{SERVICE}', '{TENANT}') ON CONFLICT DO NOTHING")
    print(f"{TABLE}: created on {TENANT}, writable; waiting for its first chain (up to one cadence)…", flush=True)
    # Ready = a chain whose seed epoch equals the table descriptor's, unchanged for 15 s:
    # the table's own DDL events can still move the epoch just after the first cut.
    stable, prev, t0 = 0, None, time.time()
    while time.time() - t0 < 900:
        d, m = kv("schemas", TABLE), kv("generations", f"{TENANT}.{TABLE}")
        pair = (d.get("seed_epoch"), m.get("seed_epoch"), m.get("gen")) if d and m else None
        ok = pair is not None and pair[0] == pair[1]
        stable = stable + 1 if ok and pair == prev else 0
        prev = pair
        if stable >= 15:
            print(f"ready in {time.time() - t0:.0f} s: generation {pair[2]}, seed epoch {pair[0]}", flush=True)
            return
        time.sleep(1)
    tail = ""
    if BRIDGE_LOG.exists():
        with BRIDGE_LOG.open(errors="replace") as f:
            f.seek(mark)
            tail = "\n".join(l for l in f.read().splitlines() if TABLE in l)[-600:]
    sys.exit(f"no stable chain for {TENANT}/{TABLE} after 15 minutes — is the dev bridge running?\n{tail}")


def teardown():
    psql(f"DROP TABLE IF EXISTS public.{TABLE}")
    psql(f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{SERVICE}' AND tenant_id = '{TENANT}'")
    print(f"{TABLE} dropped; the bridge prunes its catalogue row and chain", flush=True)


if __name__ == "__main__":
    teardown() if sys.argv[1:] == ["teardown"] else setup()

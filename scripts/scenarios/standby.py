#!/usr/bin/env python3
"""The bridge reads from a standby and writes to the primary.

The README allows DATABASE_READER_URL to point at a hot standby: the CDC slot and every
read stay on the standby, and the few writes the bridge makes (its bookkeeping, and the
clients' writes) go to the primary over DATABASE_WRITER_URL. This builds that setup and
checks it:

  0. a standby of the dev database (pg_basebackup, a physical slot, port 15433,
     hot_standby_feedback=on), and a probe bridge on its own NATS (port 14225) with
     DATABASE_READER_URL on the standby and DATABASE_WRITER_URL on the primary;
  1. the bridge says it reads from a standby, and does not warn about feedback;
  2. its logical slot is on the standby, not on the primary;
  3. its snapshot bookkeeping lands on the primary;
  4. a client enrolled at this bridge seeds the table, receives a change made on the
     primary (decoded on the standby), and its own write lands on the primary.

Needs PostgreSQL 16 or later (logical decoding on a standby) and about the dev
database's size in free disk for the copy. The dev bridge may keep running.

Usage:  set -a; . ./.env.bridge; set +a; python scripts/scenarios/standby.py
"""
import json
import os
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import uuid

import zb
from multi_bridge import BRIDGE, NATS_URL, lib, start_stack, take, wait_log

PRIMARY = "postgres://postgres@127.0.0.1:5432/postgres"
SB_PORT = 15433
PHYS_SLOT = "zb_standby_phys"
T, PUB, SLOT, PORT = "sb_t", "zb_sb", "zb_sb", 9099
RUN = uuid.uuid4().hex[:8]


def on_standby(sql: str) -> str:
    out = subprocess.run(["psql", "-h", "127.0.0.1", "-p", str(SB_PORT), "-U", "postgres", "-d", zb_db(), "-Atc", sql],
                         capture_output=True, text=True)
    return out.stdout.strip()


def zb_db() -> str:
    return urllib.parse.urlparse(os.environ["DATABASE_READER_URL"]).path.lstrip("/")


def with_port(url: str, port: int) -> str:
    u = urllib.parse.urlparse(url)
    host = u.netloc.rsplit("@", 1)
    hostport = host[-1].rsplit(":", 1)[0] + f":{port}"
    netloc = (host[0] + "@" + hostport) if len(host) == 2 else hostport
    return urllib.parse.urlunparse(u._replace(netloc=netloc))


def setup_db():
    zb.psql(f"SELECT * FROM zebridge_create_publication('{PUB}')")
    zb.psql(f"CREATE TABLE IF NOT EXISTS public.{T} (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), v int, "
            "updated_at timestamptz NOT NULL DEFAULT now(), last_writer text)")
    out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', "
                  f"public_reason => 'standby test', writable => true, version_col => 'updated_at', "
                  f"tiebreak_col => 'last_writer', allow_physical_deletes => true, "
                  f"publication => '{PUB}', dry_run => false)")
    if "ERROR" in out:
        raise SystemExit(f"zebridge_enable {T}: {out}")
    zb.psql(f"INSERT INTO public.{T} (v) VALUES (1), (2)")


def teardown_db(principal: str):
    zb.psql(f"DROP TABLE IF EXISTS public.{T}", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'", quiet=True)
    zb.psql(f"DROP PUBLICATION IF EXISTS {PUB}", quiet=True)
    for t in ("zebridge_principal_keys", "zebridge_invites", "zebridge_user_tenants"):
        zb.psql(f"DELETE FROM public.{t} WHERE principal = '{principal}'", quiet=True)


def primary_admin(sql: str) -> str:
    return subprocess.run(["psql", PRIMARY, "-Atc", sql], capture_output=True, text=True).stdout.strip()


def start_standby(datadir: str, log: str) -> bool:
    primary_admin(f"SELECT pg_drop_replication_slot('{PHYS_SLOT}') WHERE EXISTS "
                  f"(SELECT 1 FROM pg_replication_slots WHERE slot_name = '{PHYS_SLOT}')")
    subprocess.run(["pg_basebackup", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-D", datadir,
                    "-R", "-C", "-S", PHYS_SLOT, "-X", "stream", "-c", "fast"], check=True, capture_output=True)
    # wal_level comes from the primary's command line in dev, not from a file the copy takes.
    opts = f"-p {SB_PORT} -c wal_level=logical -c hot_standby=on -c hot_standby_feedback=on -c archive_mode=off"
    subprocess.run(["pg_ctl", "-D", datadir, "-o", opts, "-l", log, "-w", "start"], check=True, capture_output=True)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if on_standby("SELECT pg_is_in_recovery()") == "t":
            return True
        time.sleep(0.5)
    return False


def stop_standby(datadir: str):
    subprocess.run(["pg_ctl", "-D", datadir, "-m", "fast", "-w", "stop"], capture_output=True)
    primary_admin(f"SELECT pg_drop_replication_slot('{PHYS_SLOT}') WHERE EXISTS "
                  f"(SELECT 1 FROM pg_replication_slots WHERE slot_name = '{PHYS_SLOT}')")


def main() -> int:
    failed = 0

    def check(label: str, cond: bool, detail: str):
        nonlocal failed
        (zb.ok if cond else zb.bad)(f"{label}: {detail}")
        failed += 0 if cond else 1

    principal = f"sb_{RUN}"
    tmp = tempfile.mkdtemp(prefix="zb-standby-")
    datadir = os.path.join(tmp, "pg")
    blog = os.path.join(tmp, "bridge.log")
    ns = bridge = None
    h = 0
    standby_up = False
    try:
        teardown_db(principal)
        setup_db()

        # ── 0. the standby, the NATS stack, the bridge ──────────────────────────
        t0 = time.monotonic()
        standby_up = start_standby(datadir, os.path.join(tmp, "standby.log"))
        check("0. standby", standby_up, f"a copy of the dev database in recovery on port {SB_PORT} "
              f"({time.monotonic() - t0:.0f} s to copy and start)")
        if not standby_up:
            return 1
        ns, genv = start_stack(tmp)
        env = dict(os.environ, NATS_URL=NATS_URL, NATS_CREDS=genv["NATS_CREDS"], NATS_TLS_CA="", NATS_JS_DOMAIN="",
                   ZB_SIGNING_SEED=genv["ZB_SIGNING_SEED"], ZB_ACCOUNT_PUB=genv["ZB_ACCOUNT_PUB"],
                   DATABASE_READER_URL=with_port(os.environ["DATABASE_READER_URL"], SB_PORT),
                   GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="10", LOG_LEVEL="info")
        bridge = subprocess.Popen([str(BRIDGE), "--pub", PUB, "--slot", SLOT, "--port", str(PORT)],
                                  env=env, stdout=subprocess.DEVNULL, stderr=open(blog, "w"))

        # ── 1. it knows it reads from a standby ─────────────────────────────────
        said = wait_log(blog, "DATABASE_READER_URL is a STANDBY", 30)
        check("1. standby seen", said, "the bridge says the slot and every read stay on the standby")
        # A logical slot on a standby waits for the primary to log a snapshot of running
        # transactions; asking for one now saves up to 15 s.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and not wait_log(blog, f"deriving from publication '{PUB}'", 1):
            primary_admin("SELECT pg_log_standby_snapshot()")
        text = open(blog, errors="replace").read()
        check("1. feedback", "hot_standby_feedback=off" not in text, "no warning about hot_standby_feedback")

        # ── 2. the slot is on the standby ───────────────────────────────────────
        sb_slot = on_standby(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{SLOT}'")
        pr_slot = zb.psql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{SLOT}'")
        check("2. slot", sb_slot == "1" and pr_slot == "0",
              f"logical slot {SLOT} on the standby ({sb_slot}), not on the primary ({pr_slot})")

        # ── 3. bookkeeping on the primary ───────────────────────────────────────
        wait_log(blog, f"g1 for '_default'/'{T}'", 30)
        gens = zb.psql(f"SELECT count(*) FROM public.zebridge_generations WHERE tbl = '{T}'")
        check("3. bookkeeping", gens not in ("", "0"), f"the snapshot's row ({gens}) is on the primary")

        # ── 4. a client: seed, live change, write ───────────────────────────────
        code = f"standby-{uuid.uuid4().hex}"
        zb.psql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id) VALUES ('{code}', '{principal}', 'acme')")
        h = lib.zb_client_connect(json.dumps({
            "bridgeUrl": f"http://127.0.0.1:{PORT}", "invite": code, "dbPath": os.path.join(tmp, "client.sqlite3"),
            "natsUrl": NATS_URL, "tables": [T], "heartbeatMs": 0, "clientId": f"sb-{RUN}"}).encode())
        r = take(lib.zb_client_sync(h)) if h else {"error": "open failed"}
        check("4. seed", h != 0 and not r.get("error") and not r.get("unseeded"),
              f"enrolled at the bridge, seeded {T} ({r.get('error') or r.get('unseeded') or 'ok'})")

        def rows() -> int:
            return take(lib.zb_client_query(h, f"SELECT count(*) FROM {T}".encode(), b"[]")).get("rows", [[0]])[0][0]

        zb.psql(f"INSERT INTO public.{T} (v) VALUES (3)")
        want = int(zb.psql(f"SELECT count(*) FROM public.{T}"))
        deadline = time.monotonic() + 20
        while rows() < want and time.monotonic() < deadline:
            take(lib.zb_client_poll(h, 300))
        check("4. live", rows() == want, f"a row inserted on the primary reached the client ({rows()} of {want})")

        key = str(uuid.uuid4())
        take(lib.zb_client_mutate(h, T.encode(), b"INSERT", json.dumps({"id": key}).encode(), json.dumps({"v": 42}).encode()))
        deadline = time.monotonic() + 20
        landed = False
        while not landed and time.monotonic() < deadline:
            take(lib.zb_client_poll(h, 300))
            landed = zb.psql(f"SELECT count(*) FROM public.{T} WHERE id = '{key}'") == "1"
        check("4. write", landed, "the client's INSERT is in the primary")
    finally:
        if h:
            lib.zb_client_close(h)
        if bridge:
            bridge.terminate()
            try:
                bridge.wait(timeout=15)
            except subprocess.TimeoutExpired:
                bridge.kill()
        if ns:
            ns.terminate()
            ns.wait(timeout=10)
        if standby_up or os.path.exists(datadir):
            stop_standby(datadir)
        teardown_db(principal)
        if os.path.exists(blog):
            shutil.copy(blog, "/tmp/zb_standby_bridge.log")
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if failed:
        zb.bad(f"{failed} check(s) failed")
        return 1
    zb.ok("a bridge reading from a standby and writing to the primary serves clients both ways")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

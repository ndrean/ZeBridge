#!/usr/bin/env python3
"""Two bridges side by side, one publication each, on one database and one NATS (§10kp).

The README allows several bridges, each with its own publication, slot and port, to
split slow tables from busy ones. This runs two:

  bridge A  publication zb_mb_a (table mb_a), slot zb_mb_a, port 9097
  bridge B  publication zb_mb_b (table mb_b), slot zb_mb_b, port 9098

on their own NATS (a generated `--init-nats operator` config on port 14225), so the dev
broker is never touched. What must hold:

  1. both boot, each deriving its snapshots from its own publication;
  2. a change to mb_a is published once, by A; a change to mb_b once, by B;
  3. each builds snapshots for its own table only;
  4. one client, enrolled at A, follows both tables: it seeds both, receives a change
     to each, and its writes to both land (either bridge may apply a write);
  5. a schema change is published once, though both bridges see the DDL event;
  6. reported, not judged: both bridges serve the fleet series on /metrics, so a
     dashboard that sums them counts every client twice.

Not exercised: `ZB_FEED_RESTART=1` on one bridge's new slot deletes the CDC streams the
other still feeds. That is by design, and the README warns about it.

Usage:  set -a; . ./.env.bridge; set +a; BRIDGE_CDC_PUBLICATION=my_pub \\
        python scripts/scenarios/multi_bridge.py
"""
import asyncio
import ctypes
import json
import os
import subprocess
import tempfile
import time
import urllib.request
import uuid

import nats
import zb

ADMIN_URL = "postgres://postgres@127.0.0.1:5432/postgres"
BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
LIBZB = zb.ROOT / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if os.uname().sysname == "Darwin" else "libzbcore.so")
NATS_PORT = 14225
NATS_URL = f"nats://127.0.0.1:{NATS_PORT}"
SIDES = {"a": 9097, "b": 9098}
RUN = uuid.uuid4().hex[:8]

lib = ctypes.CDLL(str(LIBZB))
lib.zb_free.argtypes = [ctypes.c_void_p]
lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
for name, extra in (("sync", []), ("poll", [ctypes.c_uint64]),
                    ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                    ("mutate", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p])):
    f = getattr(lib, f"zb_client_{name}")
    f.restype, f.argtypes = ctypes.c_void_p, [ctypes.c_uint64] + extra
lib.zb_client_close.argtypes = [ctypes.c_uint64]


def take(p) -> dict:
    try:
        return json.loads(ctypes.string_at(p).decode())
    finally:
        lib.zb_free(p)


def psql(sql: str) -> str:
    return zb.psql(sql)


def setup_db():
    for s in SIDES:
        psql(f"SELECT * FROM zebridge_create_publication('zb_mb_{s}')")
        psql(f"CREATE TABLE IF NOT EXISTS public.mb_{s} (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), v int, "
             "updated_at timestamptz NOT NULL DEFAULT now(), last_writer text)")
        out = psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.mb_{s}', "
                   f"public_reason => 'multi_bridge test', writable => true, version_col => 'updated_at', "
                   f"tiebreak_col => 'last_writer', allow_physical_deletes => true, "
                   f"publication => 'zb_mb_{s}', dry_run => false)")
        if "ERROR" in out:
            raise SystemExit(f"zebridge_enable mb_{s}: {out}")
        psql(f"INSERT INTO public.mb_{s} (v) VALUES (1)")


def teardown_db(principal: str):
    for s in SIDES:
        subprocess.run([str(BRIDGE), "--drop-slot", f"zb_mb_{s}"], env=dict(os.environ, ADMIN_DATABASE_URL=ADMIN_URL),
                       capture_output=True)
        zb.psql(f"DROP TABLE IF EXISTS public.mb_{s}", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = 'mb_{s}'", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_generations WHERE tbl = 'mb_{s}'", quiet=True)
        zb.psql(f"DROP PUBLICATION IF EXISTS zb_mb_{s}", quiet=True)
    for t in ("zebridge_principal_keys", "zebridge_invites", "zebridge_user_tenants"):
        zb.psql(f"DELETE FROM public.{t} WHERE principal = '{principal}'", quiet=True)


def start_stack(tmp: str) -> tuple[subprocess.Popen, dict]:
    subprocess.run([str(BRIDGE), "--init-nats", "operator"], cwd=tmp, capture_output=True, check=True)
    gen = os.path.join(tmp, "zb-nats")
    conf = os.path.join(gen, "nats-server.conf")
    text = open(conf).read().replace("port: 4222", f"port: {NATS_PORT}") \
        .replace("port: 8222", "port: 18225").replace("port: 8080", "port: 18083")
    open(conf, "w").write(text)
    genv = dict(line.split("=", 1) for line in open(os.path.join(gen, "." + "env.bridge")).read().splitlines()
                if "=" in line and not line.startswith("#"))
    ns = subprocess.Popen(["nats-server", "-c", conf], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and subprocess.run(["nc", "-z", "127.0.0.1", str(NATS_PORT)],
                                                         capture_output=True).returncode:
        time.sleep(0.2)
    return ns, genv


def start_bridge(tmp: str, side: str, genv: dict) -> tuple[subprocess.Popen, str]:
    log = os.path.join(tmp, f"bridge_{side}.log")
    env = dict(os.environ, NATS_URL=NATS_URL, NATS_CREDS=genv["NATS_CREDS"], NATS_TLS_CA="", NATS_JS_DOMAIN="",
               ZB_SIGNING_SEED=genv["ZB_SIGNING_SEED"], ZB_ACCOUNT_PUB=genv["ZB_ACCOUNT_PUB"],
               GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="10", LOG_LEVEL="info")
    p = subprocess.Popen([str(BRIDGE), "--pub", f"zb_mb_{side}", "--slot", f"zb_mb_{side}", "--port", str(SIDES[side])],
                         env=env, stdout=subprocess.DEVNULL, stderr=open(log, "w"))
    return p, log


def wait_log(path: str, needle: str, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if needle in open(path, errors="replace").read():
            return True
        time.sleep(0.5)
    return False


def count_lines(path: str, needle: str) -> int:
    return open(path, errors="replace").read().count(needle)


async def count_published(creds: str, subject: str, action, wait: float = 4.0) -> int:
    """How many messages reach `subject` on the broker while `action` runs."""
    nc = await nats.connect(NATS_URL, user_credentials=creds)
    got = 0

    async def cb(_msg):
        nonlocal got
        got += 1

    await nc.subscribe(subject, cb=cb)
    await nc.flush()
    action()
    await asyncio.sleep(wait)
    await nc.close()
    return got


async def stored_revisions(creds: str, key: str) -> int:
    """How many revisions of `$KV.schemas.<key>` JetStream KEPT (duplicates it discarded
    by message id are not counted — a core subscription would see them)."""
    nc = await nats.connect(NATS_URL, user_credentials=creds)
    js = nc.jetstream()
    try:
        info = await js.stream_info("KV_schemas", subjects_filter=f"$KV.schemas.{key}")
        return int((info.state.subjects or {}).get(f"$KV.schemas.{key}", 0))
    finally:
        await nc.close()


def main() -> int:
    failed = 0

    def check(label: str, cond: bool, detail: str):
        nonlocal failed
        (zb.ok if cond else zb.bad)(f"{label}: {detail}")
        failed += 0 if cond else 1

    principal = f"mb_{RUN}"
    tmp = tempfile.mkdtemp(prefix="zb-multi-")
    ns = None
    bridges: dict[str, tuple[subprocess.Popen, str]] = {}
    h = 0
    try:
        teardown_db(principal)
        setup_db()
        ns, genv = start_stack(tmp)
        creds = genv["NATS_CREDS"]
        for s in SIDES:
            bridges[s] = start_bridge(tmp, s, genv)

        # ── 1. both boot, each on its own publication ───────────────────────────
        for s, (_, log) in bridges.items():
            ok = wait_log(log, f"deriving from publication 'zb_mb_{s}'", 60)
            check(f"1. bridge {s.upper()}", ok, f"booted on slot zb_mb_{s}, snapshots derived from publication zb_mb_{s}")
        for s, (_, log) in bridges.items():
            wait_log(log, "enrollment endpoint armed", 30)

        # ── 2. each change published once, by its own bridge ───────────────────
        for s in SIDES:
            n = asyncio.run(count_published(creds, f"cdc.mb_{s}.>",
                                            lambda s=s: zb.psql(f"INSERT INTO public.mb_{s} (v) VALUES (2)")))
            check(f"2. mb_{s}", n == 1, f"one INSERT reached NATS {n} time(s)")
        other = {"a": "b", "b": "a"}
        for s, (_, log) in bridges.items():
            leaked = count_lines(log, f"'mb_{other[s]}'")
            check(f"2. bridge {s.upper()} stays in its lane", "mb_" + other[s] not in
                  "".join(l for l in open(log, errors="replace") if "published" in l.lower() and "cdc" in l.lower()),
                  f"its log publishes no change of mb_{other[s]} ({leaked} mention(s) overall)")

        # ── 3. each builds snapshots for its own table only ─────────────────────
        time.sleep(15)
        for s, (_, log) in bridges.items():
            text = open(log, errors="replace").read()
            own = f"'mb_{s}'" in text and "🧬" in text
            foreign = any(f"mb_{other[s]}" in l for l in text.splitlines() if "🧬" in l)
            check(f"3. bridge {s.upper()} snapshots", own and not foreign,
                  f"builds mb_{s}'s chain, never mb_{other[s]}'s")
            # Any other bridge on this database (the dev one included) must leave the
            # chain alone: a swept chain shows here as a manifest with no bookkeeping.
            orphans = text.count("no bookkeeping behind it")
            check(f"3. bridge {s.upper()} chain kept", orphans == 0,
                  f"no other bridge swept mb_{s}'s chain ({orphans} rebuild(s))")

        # ── 4. one client across both bridges ───────────────────────────────────
        code = f"multi-{uuid.uuid4().hex}"
        zb.psql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id) VALUES ('{code}', '{principal}', 'acme')")
        db = os.path.join(tmp, "client.sqlite3")
        h = lib.zb_client_connect(json.dumps({
            "bridgeUrl": f"http://127.0.0.1:{SIDES['a']}", "invite": code, "dbPath": db, "natsUrl": NATS_URL,
            "tables": ["mb_a", "mb_b"], "heartbeatMs": 0, "clientId": f"mb-{RUN}"}).encode())
        r = take(lib.zb_client_sync(h)) if h else {"error": "open failed"}
        check("4. sync", h != 0 and not r.get("error") and not r.get("unseeded"),
              f"enrolled at bridge A, seeded both tables ({r.get('error') or r.get('unseeded') or 'ok'})")

        def rows(t: str) -> int:
            out = take(lib.zb_client_query(h, f"SELECT count(*) FROM {t}".encode(), b"[]"))
            return out.get("rows", [[0]])[0][0]

        for s in SIDES:
            want = int(zb.psql(f"SELECT count(*) FROM public.mb_{s}"))
            zb.psql(f"INSERT INTO public.mb_{s} (v) VALUES (3)")
            want += 1
            deadline = time.monotonic() + 15
            while rows(f"mb_{s}") < want and time.monotonic() < deadline:
                take(lib.zb_client_poll(h, 300))
            check(f"4. live mb_{s}", rows(f"mb_{s}") == want, f"the client holds {rows(f'mb_{s}')} of {want} rows")

        for s in SIDES:
            key = str(uuid.uuid4())
            take(lib.zb_client_mutate(h, f"mb_{s}".encode(), b"INSERT", json.dumps({"id": key}).encode(),
                                      json.dumps({"v": 42}).encode()))
            deadline = time.monotonic() + 15
            landed = False
            while not landed and time.monotonic() < deadline:
                take(lib.zb_client_poll(h, 300))
                landed = zb.psql(f"SELECT count(*) FROM public.mb_{s} WHERE id = '{key}'") == "1"
            check(f"4. write mb_{s}", landed, "the client's INSERT is in PostgreSQL")

        # ── 5. a schema change is published once ────────────────────────────────
        before = asyncio.run(stored_revisions(creds, "mb_a"))
        zb.psql("ALTER TABLE public.mb_a ADD COLUMN note text")
        time.sleep(6)
        after = asyncio.run(stored_revisions(creds, "mb_a"))
        check("5. DDL once", after - before == 1, f"one ALTER on mb_a stored {after - before} new description(s)")

        # ── 6. reported: the fleet series on both /metrics ──────────────────────
        for s, port in SIDES.items():
            try:
                body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
                n = sum(1 for l in body.splitlines() if l.startswith("bridge_fleet_"))
                print(f"  ⓘ bridge {s.upper()} serves {n} bridge_fleet_* line(s): a dashboard summing both counts twice")
            except Exception as e:  # noqa: BLE001
                print(f"  ⓘ bridge {s.upper()} /metrics unreadable: {e}")
    finally:
        if h:
            lib.zb_client_close(h)
        for p, _ in bridges.values():
            p.terminate()
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()
        if ns:
            ns.terminate()
            ns.wait(timeout=10)
        teardown_db(principal)
        for s in SIDES:
            src = os.path.join(tmp, f"bridge_{s}.log")
            if os.path.exists(src):
                subprocess.run(["cp", src, f"/tmp/zb_multi_bridge_{s}.log"])
        subprocess.run(["rm", "-rf", tmp])

    print()
    if failed:
        zb.bad(f"{failed} check(s) failed")
        return 1
    zb.ok("two bridges, one publication each, share a database and a NATS without stepping on each other")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

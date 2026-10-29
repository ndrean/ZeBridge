#!/usr/bin/env python3
"""The JWT renews itself, and a wrong device clock never locks a device out.

A probe bridge mints 40 s JWTs, on its own NATS (port 14225), publication and slot. A
libzb device and a zb-client-ts device (Node) enroll, then:

  0. extreme and malformed renewal timestamps are refused before authentication,
     without killing the bridge; a signed renewal then succeeds on the same process;
  A. both stay connected for 100 s, two and a half JWT lifetimes: their identity carries
     at least three JWTs (two renewals), and a row inserted at the end reaches both;
  B. both close; their stored `clock_offset` is pushed 2 h AHEAD (a device clock that
     moved forward since its last JWT). On reopen the renewal is due at once and its
     stamp is 2 h off: the bridge refuses it with its own time, the device re-stamps
     and renews, and the connect succeeds with the offset corrected;
  C. both close; their offset is pushed 2 h BEHIND, and their JWT is left to expire.
     On reopen the device thinks nothing is due, NATS refuses the expired JWT, the
     device renews anyway (and corrects its stamp the same way), and connects.
  D. the client signing key is rotated (`--init-nats --rotate-client-key`, as after a
     leaked ZB_SIGNING_SEED), NATS reloads, the bridge restarts with the new seed. Both
     devices reopen with a JWT that is still valid in time but signed by the old key:
     NATS refuses it, the device renews, and its new JWT is signed by the new key. No
     invite, nothing done on the device.

The devices run on this machine, so their true offset is about 0: what B and C break is
the device's stored estimate, the same state a real clock change leaves.

Usage:  set -a; . ./.env.bridge; set +a; python scripts/scenarios/jwt_renew.py
"""
import base64
import ctypes
import json
import os
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import nkeys
import zb
from multi_bridge import BRIDGE, NATS_URL, lib, start_stack, take, wait_log

T, PUB, SLOT, PORT = "jr_t", "zb_jr", "zb_jr", 9096
BRIDGE_URL = f"http://127.0.0.1:{PORT}"
TTL = 40
RUN = uuid.uuid4().hex[:8]
NODE = str(zb.ROOT / "scripts" / "scenarios" / "renew_client.mts")
lib.zb_last_error.restype = ctypes.c_char_p


def setup_db():
    zb.psql(f"SELECT * FROM zebridge_create_publication('{PUB}')")
    zb.psql(f"CREATE TABLE IF NOT EXISTS public.{T} (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), v int, "
            "updated_at timestamptz NOT NULL DEFAULT now())")
    out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', "
                  f"public_reason => 'jwt_renew test', writable => true, version_col => 'updated_at', "
                  f"allow_physical_deletes => true, publication => '{PUB}', dry_run => false)")
    if "ERROR" in out:
        raise SystemExit(f"zebridge_enable {T}: {out}")
    zb.psql(f"INSERT INTO public.{T} (v) VALUES (1)")


def teardown_db(principals):
    subprocess.run([str(BRIDGE), "--drop-slot", SLOT],
                   env=dict(os.environ, ADMIN_DATABASE_URL="postgres://postgres@127.0.0.1:5432/postgres"),
                   capture_output=True)
    zb.psql(f"DROP TABLE IF EXISTS public.{T}", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'", quiet=True)
    zb.psql(f"DROP PUBLICATION IF EXISTS {PUB}", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_limits WHERE slot = '{SLOT}'", quiet=True)   # the instance's registration too
    for p in principals:
        for t in ("zebridge_principal_keys", "zebridge_invites", "zebridge_user_tenants"):
            zb.psql(f"DELETE FROM public.{t} WHERE principal = '{p}'", quiet=True)


def invite(principal: str) -> str:
    code = f"renew-{uuid.uuid4().hex}"
    zb.psql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id) VALUES ('{code}', '{principal}', 'acme')")
    return code


def http_json(path: str, params: dict) -> tuple[int, dict]:
    url = f"{BRIDGE_URL}{path}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        with e:
            return e.code, json.loads(e.read())


def identity(db: str) -> dict:
    return json.load(open(db + ".identity"))


def jwt_of(db: str) -> str:
    return identity(db)["creds"].split("\n")[1]


def claims_of(db: str) -> dict:
    payload = jwt_of(db).split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))


def exp_of(db: str) -> int:
    return claims_of(db)["exp"]


def shift_offset(db: str, by: int):
    """Push the stored estimate of the bridge's time, as a device clock change would."""
    i = identity(db)
    i["clock_offset"] = int(i.get("clock_offset", 0)) + by
    with open(db + ".identity", "w") as f:
        json.dump(i, f)


def lib_open(db: str, code: str | None) -> int:
    opts = {"bridgeUrl": BRIDGE_URL, "dbPath": db, "natsUrl": NATS_URL, "tables": [T],
            "heartbeatMs": 0, "clientId": f"renew-{RUN}"}
    if code:
        opts["invite"] = code
    return lib.zb_client_connect(json.dumps(opts).encode())


def node(db: str, code: str | None, seconds: int) -> dict:
    out = subprocess.run(["node", "--experimental-strip-types", NODE, BRIDGE_URL, code or "-", db, NATS_URL, T, str(seconds)],
                         capture_output=True, text=True, timeout=seconds + 120)
    lines = [l for l in out.stdout.splitlines() if l.startswith("{")]
    return json.loads(lines[-1]) if lines else {"error": (out.stderr or out.stdout)[-300:]}


def lib_rows(h: int) -> int:
    lib.zb_client_query.restype, lib.zb_client_query.argtypes = ctypes.c_void_p, [ctypes.c_uint64, ctypes.c_char_p, ctypes.c_char_p]
    return take(lib.zb_client_query(h, f"SELECT count(*) FROM {T}".encode(), b"[]")).get("rows", [[0]])[0][0]


def main() -> int:
    failed = 0

    def check(label: str, cond: bool, detail: str):
        nonlocal failed
        (zb.ok if cond else zb.bad)(f"{label}: {detail}")
        failed += 0 if cond else 1

    principals = [f"jr_lib_{RUN}", f"jr_ts_{RUN}", f"jr_http_{RUN}"]
    tmp = tempfile.mkdtemp(prefix="zb-renew-")
    blog = os.path.join(tmp, "bridge.log")
    lib_db, ts_db = os.path.join(tmp, "lib.sqlite3"), os.path.join(tmp, "ts.sqlite3")
    ns = bridge = admission = None
    h = 0
    try:
        teardown_db(principals)
        setup_db()
        ns, genv = start_stack(tmp)
        env = dict(os.environ, NATS_URL=NATS_URL, NATS_CREDS=genv["NATS_CREDS"], NATS_TLS_CA="", NATS_JS_DOMAIN="",
                   ZB_SIGNING_SEED=genv["ZB_SIGNING_SEED"], ZB_ACCOUNT_PUB=genv["ZB_ACCOUNT_PUB"],
                   ENROLL_JWT_TTL_SECONDS=str(TTL), GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="10",
                   LOG_LEVEL="info", NATS_ACCOUNT_CONF=os.path.join(tmp, "zb-nats/nats-server.conf"))
        if os.environ.get("MUTATION_ADMISSION") == "1":
            admission_log = os.path.join(tmp, "admission.log")
            with open(admission_log, "w") as output:
                admission = subprocess.Popen([str(BRIDGE), "--mutation-ingress"], env=env,
                                             stdout=subprocess.DEVNULL, stderr=output)
            if not wait_log(admission_log, "mutation admission ready", 30):
                zb.bad("the admission service did not start")
                return 1
        bridge = subprocess.Popen([str(BRIDGE), "--pub", PUB, "--slot", SLOT, "--port", str(PORT)],
                                  env=env, stdout=subprocess.DEVNULL, stderr=open(blog, "w"))
        if not wait_log(blog, f"deriving from publication '{PUB}'", 60):
            zb.bad("the probe bridge did not start")
            return 1
        wait_log(blog, "enrollment endpoint armed", 30)

        def check_feed(label):
            with urllib.request.urlopen(f"{BRIDGE_URL}/nats-account?fresh={uuid.uuid4().hex}", timeout=10) as response:
                payload = json.loads(response.read())
                expected = re.search(r"(?m)^\s*" + re.escape(genv["ZB_ACCOUNT_PUB"]) + r':\s*"?([A-Za-z0-9_.-]+)',
                                     open(env["NATS_ACCOUNT_CONF"]).read()).group(1)
                check(label, set(payload) == {"account", "jwt"} and payload["account"] == genv["ZB_ACCOUNT_PUB"]
                      and payload["jwt"] == expected and response.headers.get("Cache-Control") == "no-store",
                      "only the current public account JWT is served, without secrets or caching")
        check_feed("0. public trust feed")

        # ── 0. hostile timestamps must not terminate the renewal server ─────────
        kp = nkeys.from_seed(nkeys.encode_seed(os.urandom(32), nkeys.PREFIX_BYTE_USER))
        pub = kp.public_key.decode()
        status, enrolled = http_json("/enroll", {"code": invite(principals[2]), "user_pubkey": pub})
        check("0. HTTP enrolled", status == 200 and bool(enrolled.get("jwt")), f"HTTP {status}")
        if status != 200:
            return 1
        # A correctly sized but invalid signature reaches the timestamp check, which
        # runs before signature verification. These require no authenticated caller.
        dummy_sig = base64.urlsafe_b64encode(bytes(64)).decode().rstrip("=")
        for stamp, expected in ((str(-(1 << 63)), 401), (str((1 << 63) - 1), 401),
                                (str(-(1 << 63) - 1), 400), (str(1 << 63), 400),
                                ("not-a-timestamp", 400), ("", 400)):
            status, body = http_json("/renew", {"user_pubkey": pub, "ts": stamp, "sig": dummy_sig})
            skew_response = (body.get("error") == "stale or future timestamp"
                             and isinstance(body.get("server_time"), int))
            check(f"0. timestamp {stamp!r}", status == expected and (expected != 401 or skew_response),
                  f"HTTP {status}, {body.get('error')}")
        with urllib.request.urlopen(f"{BRIDGE_URL}/health", timeout=10) as r:
            check("0. bridge survived", bridge.poll() is None and r.status == 200,
                  "the original process still serves /health")
        stamp = int(time.time())
        sig = base64.urlsafe_b64encode(kp.sign(f"zebridge-renew:{pub}:{stamp}".encode())).decode().rstrip("=")
        status, renewed = http_json("/renew", {"user_pubkey": pub, "ts": stamp, "sig": sig})
        check("0. valid renewal after hostile requests", status == 200 and bool(renewed.get("jwt"))
              and renewed.get("principal") == principals[2] and bridge.poll() is None,
              f"HTTP {status}, the same process renews the enrolled identity")

        # ── A. two and a half lifetimes, connected ──────────────────────────────
        stay = int(TTL * 2.5)
        ts_result: dict = {}
        ts_thread = threading.Thread(target=lambda: ts_result.update(node(ts_db, invite(principals[1]), stay)))
        ts_thread.start()
        h = lib_open(lib_db, invite(principals[0]))
        check("A. libzb enrolled", h != 0 and not take(lib.zb_client_sync(h)).get("error"),
              f"a {TTL} s JWT, synced ({(lib.zb_last_error() or b'').decode() or 'ok'})")
        row_id = str(uuid.uuid4())
        mutation = take(lib.zb_client_mutate(h, T.encode(), b"INSERT", json.dumps({"id": row_id}).encode(), b'{"v":3}'))
        deadline = time.monotonic() + 15
        landed = False
        while h and time.monotonic() < deadline:
            take(lib.zb_client_poll(h, 200))
            landed = zb.psql(f"SELECT count(*) FROM public.{T} WHERE id = '{row_id}'") == "1"
            if landed:
                break
        check("A. client mutation reaches PostgreSQL", "error" not in mutation and landed,
              "the existing libzb API writes through the configured admission path")
        jwts, reconnects = {jwt_of(lib_db)}, 0
        t0 = time.monotonic()
        inserted = False
        while time.monotonic() - t0 < stay:
            r = take(lib.zb_client_poll(h, 500))
            if r.get("error"):
                # What a host does when the server ended the session: open again.
                reconnects += 1
                lib.zb_client_close(h)
                h = lib_open(lib_db, None)
                if not h:
                    break
            jwts.add(jwt_of(lib_db))
            if not inserted and time.monotonic() - t0 > stay - 15:
                zb.psql(f"INSERT INTO public.{T} (v) VALUES (2)")
                inserted = True
        want = int(zb.psql(f"SELECT count(*) FROM public.{T}"))
        deadline = time.monotonic() + 10
        while h and lib_rows(h) < want and time.monotonic() < deadline:
            take(lib.zb_client_poll(h, 300))
        got = lib_rows(h) if h else 0
        check("A. libzb renewed", len(jwts) >= 3, f"{len(jwts)} JWTs in {stay} s ({reconnects} reopen(s) after the server ended a session)")
        check("A. libzb still works", got == want, f"after {stay} s it holds {got} of {want} rows, the last inserted at the end")
        ts_thread.join()
        check("A. zb-client-ts renewed", ts_result.get("jwts", 0) >= 3, f"{ts_result.get('jwts')} JWTs in {stay} s ({ts_result.get('error') or 'ok'})")
        check("A. zb-client-ts still works", ts_result.get("rows") == want, f"after {stay} s it holds {ts_result.get('rows')} of {want} rows")
        lib.zb_client_close(h)
        h = 0

        # ── B. the estimate 2 h ahead: due at once, stamp refused, corrected ───
        refusals0 = open(blog, errors="replace").read().count("off the bridge's clock")
        for db in (lib_db, ts_db):
            shift_offset(db, +7200)
        before = (jwt_of(lib_db), jwt_of(ts_db))
        h = lib_open(lib_db, None)
        check("B. libzb", h != 0 and jwt_of(lib_db) != before[0] and abs(identity(lib_db)["clock_offset"]) <= 5,
              f"renewed and connected, offset back to {identity(lib_db).get('clock_offset')} s "
              f"({(lib.zb_last_error() or b'').decode() or 'ok'})")
        lib.zb_client_close(h)
        h = 0
        r = node(ts_db, None, 1)
        check("B. zb-client-ts", r.get("connected") is True and jwt_of(ts_db) != before[1] and abs(identity(ts_db).get("clock_offset", 99)) <= 5,
              f"renewed and connected, offset back to {identity(ts_db).get('clock_offset')} s "
              f"(connected={r.get('connected')}, jwt_changed={jwt_of(ts_db) != before[1]}, {r.get('error') or 'ok'})")
        refusals = open(blog, errors="replace").read().count("off the bridge's clock") - refusals0
        check("B. bridge", refusals >= 2, f"refused {refusals} stamp(s) 2 h off and answered with its time")

        # ── C. the estimate 2 h behind, the JWT expired: NATS refuses, renew anyway
        for db in (lib_db, ts_db):
            shift_offset(db, -7200)
        last_exp = max(exp_of(lib_db), exp_of(ts_db))
        while time.time() < last_exp + 3:
            time.sleep(1)
        before = (jwt_of(lib_db), jwt_of(ts_db))
        h = lib_open(lib_db, None)
        check("C. libzb", h != 0 and jwt_of(lib_db) != before[0],
              f"an expired JWT and a clock that says not yet: renewed on NATS's refusal, connected "
              f"({(lib.zb_last_error() or b'').decode() or 'ok'})")
        if h:
            lib.zb_client_close(h)
            h = 0
        r = node(ts_db, None, 1)
        check("C. zb-client-ts", r.get("connected") is True and jwt_of(ts_db) != before[1],
              f"the same, renewed and connected ({r.get('error') or 'ok'})")

        # ── D. the client signing key rotated: the devices renew onto the new key ──
        gen = os.path.join(tmp, "zb-nats")
        r = subprocess.run([str(BRIDGE), "--init-nats", "--rotate-client-key", "--dir", gen],
                           capture_output=True, text=True)
        new_seed = next((l.split("=", 1)[1] for l in open(os.path.join(gen, "." + "env.nats")).read().splitlines()
                         if l.startswith("ZB_SIGNING_SEED=")), "")
        new_key = nkeys.from_seed(new_seed.encode()).public_key.decode() if new_seed else ""
        old_key = claims_of(lib_db)["iss"]
        check("D. rotation", r.returncode == 0 and new_seed != genv["ZB_SIGNING_SEED"] and new_key != old_key,
              f"a new client signing key ({(r.stderr.strip().splitlines() or ['?'])[0]})")
        ns.send_signal(signal.SIGHUP)
        bridge.terminate()
        bridge.wait(timeout=15)
        check("D. bridge shutdown", bridge.returncode == 0, f"exit {bridge.returncode}")
        blog2 = os.path.join(tmp, "bridge2.log")
        env["ZB_SIGNING_SEED"] = new_seed
        bridge = subprocess.Popen([str(BRIDGE), "--pub", PUB, "--slot", SLOT, "--port", str(PORT)],
                                  env=env, stdout=subprocess.DEVNULL, stderr=open(blog2, "w"))
        if not wait_log(blog2, "enrollment endpoint armed", 60):
            zb.bad("the probe bridge did not restart")
            return 1
        check_feed("D. refreshed public trust feed")
        before = (jwt_of(lib_db), jwt_of(ts_db))
        h = lib_open(lib_db, None)
        check("D. libzb", h != 0 and jwt_of(lib_db) != before[0] and claims_of(lib_db)["iss"] == new_key,
              f"its JWT from the old key refused, renewed, connected on a JWT from the new key "
              f"({(lib.zb_last_error() or b'').decode() or 'ok'})")
        if h:
            lib.zb_client_close(h)
            h = 0
        r = node(ts_db, None, 1)
        check("D. zb-client-ts", r.get("connected") is True and jwt_of(ts_db) != before[1] and claims_of(ts_db)["iss"] == new_key,
              f"the same, on the new key ({r.get('error') or 'ok'})")
    finally:
        if h:
            lib.zb_client_close(h)
        if bridge:
            bridge.terminate()
            try:
                bridge.wait(timeout=15)
            except subprocess.TimeoutExpired:
                bridge.kill()
                bridge.wait(timeout=5)
            check("final bridge shutdown", bridge.returncode == 0, f"exit {bridge.returncode}")
        if admission:
            admission.terminate()
            admission.wait(timeout=15)
            check("admission shutdown", admission.returncode == 0, f"exit {admission.returncode}")
        if ns:
            ns.terminate()
            ns.wait(timeout=10)
        teardown_db(principals)
        for path in (blog, os.path.join(tmp, "bridge2.log"), os.path.join(tmp, "admission.log")):
            if os.path.exists(path):
                shutdown_log = open(path, errors="replace").read()
                check(f"shutdown {os.path.basename(path)}", "error(DebugAllocator)" not in shutdown_log
                      and "panic:" not in shutdown_log, "no allocator leaks or panics")
        if os.path.exists(blog):
            shutil.copy(blog, "/tmp/zb_jwt_renew_bridge.log")
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if failed:
        zb.bad(f"{failed} check(s) failed")
        return 1
    zb.ok("the JWT renews itself, and a device clock off by hours neither blocks nor loops")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

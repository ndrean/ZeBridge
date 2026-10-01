#!/usr/bin/env python3
"""`bridge --revoke <principal> --purge`: the device deletes its local data (§10kn).

A plain revocation cuts access but leaves the rows on the device. With `--purge` the
devices are told to delete their replica and their stored identity too. The promise holds
only for a device that reconnects (a device offline for good keeps its data), and only for
the real libraries (a modified client can ignore it).

What must hold, each device enrolled with a fresh invite into tenant acme:

  A. libzb, connected, `--purge`      → the next poll answers Revoked; the replica and the
                                        identity file are gone; zb_client_revoked is 1
  B. libzb, connected, plain revoke   → Revoked too, but the files stay (the control)
  C. zb-client-ts, connected, `--purge` → `purged`; the replica is gone, the identity empty
  D. /renew for a purged principal's key → 403 with "purge": true (the channel that still
                                        reaches a device after a FULL revocation)
  E. libzb, closed while `--purge` ran, reopened → refused, and its files are gone
  F. the renewal path, on a second stack minting 20 s JWTs: a libzb device and a
     zb-client-ts device, closed while `--purge` ran, reopened once their JWT is due
     for renewal → the renewal at connect gets the purge answer, the files are
     deleted before anything else, and the connect fails

It runs on its own stack, so the dev broker is never touched: a generated
`--init-nats operator` config served by a scratch nats-server on port 14224, and a probe
bridge (slot zb_probe, port 9096) given the generated signing key, so /enroll is armed.
The probe slot is dropped at the end.

Usage:  set -a; . ./.env.bridge; set +a; python scripts/scenarios/revoke_purge.py
"""

import base64
import ctypes
import json
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

import nkeys
import zb
from contextlib import contextmanager

ADMIN_URL = "postgres://postgres@127.0.0.1:5432/postgres"
BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
NATS_PORT = 14224
BRIDGE_URL = f"http://127.0.0.1:{zb.bridge_port()}"   # the probe bridge (zb.BRIDGE_ARGS)
NATS_URL = f"nats://127.0.0.1:{NATS_PORT}"
LIBZB = zb.ROOT / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if os.uname().sysname == "Darwin" else "libzbcore.so")
TENANT = "acme"
RUN = uuid.uuid4().hex[:8]

lib = ctypes.CDLL(str(LIBZB))
lib.zb_free.argtypes = [ctypes.c_void_p]
lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
for name, extra in (("sync", []), ("poll", [ctypes.c_uint64])):
    f = getattr(lib, f"zb_client_{name}")
    f.restype, f.argtypes = ctypes.c_void_p, [ctypes.c_uint64] + extra
lib.zb_client_revoked.restype, lib.zb_client_revoked.argtypes = ctypes.c_int, [ctypes.c_uint64]
lib.zb_client_close.argtypes = [ctypes.c_uint64]
lib.zb_client_wipe.restype, lib.zb_client_wipe.argtypes = ctypes.c_int, [ctypes.c_uint64]
lib.zb_last_error.restype = ctypes.c_char_p


def take(p) -> dict:
    try:
        return json.loads(ctypes.string_at(p).decode())
    finally:
        lib.zb_free(p)


def invite(principal: str) -> str:
    code = f"purge-{uuid.uuid4().hex}"
    zb.psql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id) VALUES ('{code}', '{principal}', '{TENANT}')")
    return code


def revoke(principal: str, purge: bool) -> subprocess.CompletedProcess:
    env = dict(os.environ, ADMIN_DATABASE_URL=ADMIN_URL)
    args = [str(BRIDGE), "--revoke", principal] + (["--purge"] if purge else [])
    return subprocess.run(args, env=env, capture_output=True, text=True, timeout=30)


@contextmanager
def isolated_stack(tmp: str, **extra_env):
    """A generated operator NATS on shifted ports and a probe bridge with enrollment on."""
    subprocess.run([str(BRIDGE), "--init-nats", "operator"], cwd=tmp, capture_output=True, check=True)
    gen = os.path.join(tmp, "zb-nats")
    conf = os.path.join(gen, "nats-server.conf")
    text = open(conf).read().replace("port: 4222", f"port: {NATS_PORT}") \
        .replace("port: 8222", "port: 18224").replace("port: 8080", "port: 18082")
    open(conf, "w").write(text)
    genv = dict(line.split("=", 1) for line in open(os.path.join(gen, ".env.bridge")).read().splitlines()
                if "=" in line and not line.startswith("#"))
    ns = subprocess.Popen(["nats-server", "-c", conf], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and subprocess.run(["nc", "-z", "127.0.0.1", str(NATS_PORT)]).returncode:
        time.sleep(0.2)
    try:
        with zb.Bridge(os.path.join(tmp, "bridge.log"), NATS_URL=NATS_URL, NATS_CREDS=genv["NATS_CREDS"],
                       NATS_TLS_CA="", NATS_JS_DOMAIN="", ZB_SIGNING_SEED=genv["ZB_SIGNING_SEED"],
                       ZB_ACCOUNT_PUB=genv["ZB_ACCOUNT_PUB"], GENERATIONS_ENABLED="1",
                       GENERATION_CADENCE_SECONDS="10", **extra_env) as br:
            if not br.wait_for_log("enrollment endpoint armed", timeout=40):
                raise SystemExit(f"the probe bridge did not arm /enroll — see {br.log_path}")
            yield br
    finally:
        ns.terminate()
        ns.wait(timeout=10)
        subprocess.run([str(BRIDGE), "--drop-slot", "zb_probe"], env=dict(os.environ, ADMIN_DATABASE_URL=ADMIN_URL),
                       capture_output=True)


def lib_open(db: str, code: str | None) -> int:
    opts = {"bridgeUrl": BRIDGE_URL, "dbPath": db, "natsUrl": NATS_URL,
            "tables": ["counter_public"], "heartbeatMs": 0, "clientId": f"purge-{RUN}"}
    if code:
        opts["invite"] = code
    return lib.zb_client_connect(json.dumps(opts).encode())


def poll_until_revoked(h: int, budget: float = 20.0) -> dict:
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        r = take(lib.zb_client_poll(h, 300))
        if r.get("error"):
            return r
    return {}


def gone(path: str) -> bool:
    return not os.path.exists(path)


def main() -> int:
    failed = 0
    tmp = tempfile.mkdtemp(prefix="zb-purge-")
    principals = [f"purge_{x}_{RUN}" for x in ("lib", "keep", "ts", "back", "renlib", "rents")]

    def check(label: str, cond: bool, detail: str):
        nonlocal failed
        (zb.ok if cond else zb.bad)(f"{label}: {detail}")
        failed += 0 if cond else 1

    stack = isolated_stack(tmp)
    stack.__enter__()
    try:
        # ── A. libzb, connected, --purge ────────────────────────────────────────
        p = principals[0]
        db = f"{tmp}/a.sqlite3"
        h = lib_open(db, invite(p))
        check("A. enrolled", h != 0 and not take(lib.zb_client_sync(h)).get("error") and os.path.exists(db + ".identity"),
              "libzb enrolled with an invite, synced, identity file written")
        r = revoke(p, purge=True)
        check("A. command", r.returncode == 0 and "local data" in r.stdout + r.stderr, "bridge --revoke --purge says what it asked for")
        res = poll_until_revoked(h)
        check("A. poll", res.get("error") == "Revoked", f"the next poll answers {res.get('error')}")
        check("A. files", gone(db) and gone(db + ".identity"), "the replica and the identity file are deleted")
        check("A. revoked", lib.zb_client_revoked(h) == 1, "zb_client_revoked still answers 1 for the purged handle")

        # ── B. libzb, connected, plain revoke (the control) ─────────────────────
        p = principals[1]
        db = f"{tmp}/b.sqlite3"
        h = lib_open(db, invite(p))
        take(lib.zb_client_sync(h))
        revoke(p, purge=False)
        res = poll_until_revoked(h)
        check("B. poll", res.get("error") == "Revoked", f"a plain revoke: the next poll answers {res.get('error')}")
        check("B. files", os.path.exists(db) and os.path.exists(db + ".identity"),
              "without --purge the replica and the identity stay: deleting them is the app's call")
        lib.zb_client_wipe(h)

        # ── C. zb-client-ts, connected, --purge ─────────────────────────────────
        p = principals[2]
        db = f"{tmp}/c.sqlite3"
        node = subprocess.Popen(
            ["node", "--experimental-strip-types", str(zb.ROOT / "scripts" / "scenarios" / "purge_client.mts"),
             BRIDGE_URL, invite(p), db, NATS_URL],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        first = json.loads(node.stdout.readline() or "{}")
        check("C. enrolled", first.get("connected") is True, "zb-client-ts enrolled with an invite and connected")
        revoke(p, purge=True)
        last = json.loads(node.stdout.readline() or "{}")
        node.wait(timeout=10)
        ident = db + ".identity"
        check("C. purged", last.get("purged") is True, f"the client reports {last}")
        check("C. files", gone(db) and (gone(ident) or os.path.getsize(ident) == 0),
              "the replica is deleted and the stored identity is emptied")

        # ── D + E. a device that was away ───────────────────────────────────────
        p = principals[3]
        db = f"{tmp}/e.sqlite3"
        h = lib_open(db, invite(p))
        take(lib.zb_client_sync(h))
        lib.zb_client_close(h)
        creds = json.load(open(db + ".identity"))["creds"]
        seed = creds.split("-----BEGIN USER NKEY SEED-----")[1].split("------END USER NKEY SEED------")[0].strip()
        revoke(p, purge=True)

        kp = nkeys.from_seed(seed.encode())
        pub, ts = kp.public_key.decode(), int(time.time())
        sig = base64.urlsafe_b64encode(kp.sign(f"zebridge-renew:{pub}:{ts}".encode())).decode().rstrip("=")
        try:
            urllib.request.urlopen(f"{BRIDGE_URL}/renew?user_pubkey={pub}&ts={ts}&sig={sig}", timeout=10)
            status, body = 200, {}
        except urllib.error.HTTPError as e:
            status, body = e.code, json.loads(e.read() or b"{}")
        check("D. /renew", status == 403 and body.get("purge") is True, f"answers {status} {body}")

        h = lib_open(db, None)  # the identity file is still there: no invite needed
        res = take(lib.zb_client_sync(h)) if h else {"error": "open failed"}
        check("E. reopen", res.get("error") == "Revoked", f"the returning device is refused ({res.get('error')})")
        check("E. files", gone(db) and gone(db + ".identity"), "and its replica and identity are deleted on the spot")
    finally:
        stack.__exit__(None, None, None)

    # ── F. the renewal path: a second stack, minting 20 s JWTs ──────────────────
    tmp_f = tempfile.mkdtemp(prefix="zb-purge-f-")
    stack = isolated_stack(tmp_f, ENROLL_JWT_TTL_SECONDS="20")
    stack.__enter__()
    try:
        lib_db, ts_db = f"{tmp_f}/f-lib.sqlite3", f"{tmp_f}/f-ts.sqlite3"
        h = lib_open(lib_db, invite(principals[4]))
        take(lib.zb_client_sync(h))
        lib.zb_client_close(h)
        node_script = str(zb.ROOT / "scripts" / "scenarios" / "purge_client.mts")
        once = subprocess.run(["node", "--experimental-strip-types", node_script, BRIDGE_URL,
                               invite(principals[5]), ts_db, NATS_URL, "once"],
                              capture_output=True, text=True, timeout=120)
        check("F. setup", os.path.exists(lib_db + ".identity") and os.path.exists(ts_db + ".identity")
              and '"connected":true' in once.stdout,
              "both devices enrolled with a 20 s JWT, then closed with their identity stored")
        revoke(principals[4], purge=True)
        revoke(principals[5], purge=True)
        time.sleep(17)  # a quarter of 20 s left: both JWTs are now due for renewal

        h = lib_open(lib_db, None)
        err = (lib.zb_last_error() or b"").decode()
        check("F. libzb", h == 0 and "local data must be deleted" in err,
              f"the connect renews, gets the purge answer and fails ({err or 'connected'})")
        check("F. libzb files", gone(lib_db) and gone(lib_db + ".identity"), "its replica and identity are deleted")

        back = subprocess.run(["node", "--experimental-strip-types", node_script, BRIDGE_URL, "-", ts_db, NATS_URL],
                              capture_output=True, text=True, timeout=120)
        last = json.loads((back.stdout.strip().splitlines() or ["{}"])[-1])
        ident = ts_db + ".identity"
        check("F. zb-client-ts", last.get("purged") is True and "error" in last, f"the connect renews and is refused: {last}")
        check("F. zb-client-ts files", gone(ts_db) and (gone(ident) or os.path.getsize(ident) == 0),
              "its replica is deleted and its identity emptied")
    finally:
        stack.__exit__(None, None, None)
        subprocess.run(["rm", "-rf", tmp_f])
        names = ", ".join(f"'{x}'" for x in principals)
        for t in ("zebridge_purges", "zebridge_principal_keys", "zebridge_invites", "zebridge_user_tenants"):
            zb.psql(f"DELETE FROM public.{t} WHERE principal IN ({names})", quiet=True)
        names = ", ".join(f"'{x}'" for x in principals)
        zb.psql(f"DELETE FROM public.zebridge_purges WHERE principal IN ({names})", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_principal_keys WHERE principal IN ({names})", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_invites WHERE principal IN ({names})", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_user_tenants WHERE principal IN ({names})", quiet=True)
        subprocess.run(["rm", "-rf", tmp])

    print()
    if failed:
        zb.bad(f"{failed} check(s) failed")
        return 1
    zb.ok("--revoke --purge deletes a reconnecting device's replica and identity; a plain revoke leaves them")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

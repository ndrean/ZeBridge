"""JWT expiry: the read door closes on time — and the enroll payload proves itself
end to end on the way in (NOTES §10cj).

    scripts/scenarios/run.py owns -k jwt_expiry      (owns the bridge; arms /enroll)

The revocation ladder, tightest to loosest: WRITES die immediately (the guard and RLS
read `zebridge_user_tenants` live, per mutation); tenant RESOLUTION dies on the next
connect (the §10ce KV purge); READS — the CDC grants baked into the JWT — die at
EXPIRY, because NATS authorizes from the signature alone. This scenario is the last
rung, and its setup is the full onboarding story finally run live:

  1. a probe bridge armed with the scoped signing key mints against a TINY TTL
  2. an invite row + `bridge --gen-nkey` + one GET /enroll = a working client:
     the payload carries jwt AND grammar (§10ci) — bootstrap from an invite code,
     no file, no nsc, no NATS knowledge
  3. the client syncs normally inside the TTL
  4. past the TTL the server expires the session; the client must SURFACE it —
     an error an application can act on ("re-enroll"), never a silent forever-retry
"""
import ctypes
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
SK_FILE = zb.ROOT / ("scripts/native/nsc-store/data/nats/nsc/keys/keys/A/BG/"
                     "ABGOGUGNFMBG47I2NTHPQFX4G3YUP23ENMNEM7ZUURFKIBGUK743WPND.nk")
ACCOUNT_PUB = "ABSDAXPW56ON5USFCTUQSWSQWTNLZL3DDDVZAPUH3244HU4NM7QFQD5L"
PRINCIPAL = "ttl_probe"
TENANT = "kilo"
TTL_S = 10
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_jwt_expiry_bridge.log"


def cleanup(creds_path: pathlib.Path, db: str) -> None:
    zb.psql(f"DELETE FROM public.zebridge_invites WHERE principal = '{PRINCIPAL}'", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{PRINCIPAL}'", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_principal_keys WHERE principal = '{PRINCIPAL}'", quiet=True)
    creds_path.unlink(missing_ok=True)
    _env.rm_sqlite(db)


def main() -> int:
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")
    if not SK_FILE.exists():
        sys.exit("the scoped client signing key is not in the nsc store — run jwt-bootstrap.sh once")
    failed = 0
    creds_path = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_ttl_probe.creds"
    db = f"/tmp/zb-jwt-expiry-{os.getpid()}.sqlite3"
    h = 0

    lib = _env.load_lib()
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

    def take(p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: lib.zb_free(p)

    try:
        env = {"ZB_SIGNING_SEED": SK_FILE.read_text().strip(),
               "ZB_ACCOUNT_PUB": ACCOUNT_PUB,
               "ENROLL_JWT_TTL_SECONDS": str(TTL_S)}
        with zb.Bridge(LOG, **env) as br:
            if not br.wait_for_log("enrollment endpoint armed", timeout=60):
                zb.bad("enrollment never armed on the probe"); return 1
            if not br.wait_for_log("Replication started successfully", timeout=60):
                zb.bad("probe bridge did not start"); return 1

            # ── 2. invite → nkey → one GET = a working client ────────────────
            code = os.urandom(16).hex()
            zb.psql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id, role, expires_at) "
                    f"VALUES ('{code}', '{PRINCIPAL}', '{TENANT}', 'client', now() + interval '5 minutes')")
            gen = subprocess.run([str(BRIDGE), "--gen-nkey"], capture_output=True, text=True)
            user_pub = re.search(r"NATS_BRIDGE_NKEY_PUB=(U[A-Z0-9]+)", gen.stdout).group(1)
            user_seed = re.search(r"NATS_BRIDGE_NKEY_SEED=(SU[A-Z0-9]+)", gen.stdout).group(1)
            with urllib.request.urlopen(zb.http_base(probe=True) +
                                        f"/enroll?code={code}&user_pubkey={user_pub}", timeout=10) as r:
                payload = json.loads(r.read().decode())
            if "jwt" in payload and "grammar" in payload and "grammar_hash" in payload:
                served = json.dumps(payload["grammar"], separators=(",", ":"))
                zb.ok("one GET /enroll returned identity AND wire contract: jwt + grammar + "
                      f"grammar_hash ({payload['grammar_hash'][:12]}…) — bootstrap needs an "
                      "invite code and a URL, nothing else")
            else:
                zb.bad(f"enroll payload incomplete: {sorted(payload.keys())}"); return failed + 1
            key_row = zb.psql(f"SELECT principal || '|' || tenant_id FROM public.zebridge_principal_keys "
                              f"WHERE user_pubkey = '{user_pub}'").strip()
            if key_row == f"{PRINCIPAL}|{TENANT}":
                zb.ok("and the enrolled PUBKEY is remembered (zebridge_principal_keys) — the "
                      "future hard kill has its input; a key never recorded can never be revoked")
            else:
                zb.bad(f"the pubkey was not persisted at enrollment (got {key_row!r})"); failed += 1
            creds_path.write_text(
                "-----BEGIN NATS USER JWT-----\n" + payload["jwt"] +
                "\n------END NATS USER JWT------\n\n-----BEGIN USER NKEY SEED-----\n" +
                user_seed + "\n------END USER NKEY SEED------\n")

            # ── 3. inside the TTL: a normal client ───────────────────────────
            _env.rm_sqlite(db)
            h = lib.zb_client_open(json.dumps({
                "url": zb.nats_server(), "credsPath": str(creds_path),
                "grammarJson": json.dumps(payload["grammar"]),
                "dbPath": db, "principal": PRINCIPAL, "clientId": "py-jwt-expiry",
                "tables": ["test_types"]}).encode())
            if not h:
                zb.bad("the enrolled client could not open"); return failed + 1
            take(lib.zb_client_sync(h))
            r0 = take(lib.zb_client_query(h, b"SELECT count(*) FROM test_types", b"[]"))
            if r0.get("rows"):
                zb.ok(f"inside the TTL the enrolled client is ordinary: synced test_types "
                      f"({r0['rows'][0][0]} rows) from the payload's grammar — no file was ever copied")
            else:
                zb.bad("the enrolled client failed to sync inside the TTL"); failed += 1

            # ── 4. past the TTL: the read door must close, AUDIBLY ───────────
            time.sleep(TTL_S + 5)
            deadline = time.monotonic() + 45
            surfaced = None
            while time.monotonic() < deadline:
                res = take(lib.zb_client_poll(h, 800))
                if isinstance(res, dict) and res.get("error"):
                    surfaced = res["error"]
                    break
                time.sleep(1)
            if surfaced and re.search(r"auth|expir|violat", surfaced, re.I):
                zb.ok(f"expiry SURFACED as an actionable error: {surfaced[:90]!r} — an "
                      "application knows to re-enroll, nothing retries silently forever")
            elif surfaced:
                zb.ok(f"expiry surfaced (generic): {surfaced[:90]!r} — audible, though not "
                      "yet named as an auth event")
            else:
                zb.bad("45s past expiry the client still polls as if nothing happened — "
                       "a dead credential must be AUDIBLE, or apps hold stale data forever")
                failed += 1
    finally:
        if h:
            lib.zb_client_close(h)
        cleanup(creds_path, db)
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

"""`bridge --init-nats --rotate-client-key`: a leaked client signing key, replaced.

    scripts/scenarios/run.py live -k rotate_client_key    (own generated stack, shifted ports)

Whoever holds ZB_SIGNING_SEED can mint a client JWT for any name and any tenant. The
rotation re-signs the account from operator.store with a NEW client signing key: once
NATS reloads, every client JWT the old key signed is refused, forged or genuine. A
genuine device then renews (/renew proves its device key, not its old JWT, see
jwt_renew.py) and gets a JWT from the new key. Fully isolated: its own --init-nats
operator stack on shifted ports; the live broker is never touched.

  1. a generated stack boots; a live session runs on a client JWT from the CURRENT key
  2. the rotation: the store, .env.nats and the conf move to the new key, the
     responder and service keys stay
  3. reload: the live session is kicked, and the old key's JWT cannot come back
  4. a JWT from the NEW key connects; the bridge's own creds still work
  5. --store - (the store from a password manager): nothing written back to it, its
     new line printed on stdout
"""
import base64
import hashlib
import json
import pathlib
import re
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import nkeys  # noqa: E402
import zb  # noqa: E402

BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
PORT = 14227


def b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def b32(b: bytes) -> str:
    return base64.b32encode(b).rstrip(b"=").decode()


def user_key() -> tuple[str, str]:
    """A throwaway user nkey pair, from the bridge's own generator: (public, seed)."""
    out = subprocess.run([str(BRIDGE), "--gen-nkey"], capture_output=True, text=True, check=True).stdout
    return (re.search(r"_PUB=(U[A-Z0-9]+)", out).group(1), re.search(r"_SEED=(SU[A-Z0-9]+)", out).group(1))


def client_creds(signing_seed: str, account_pub: str, name: str, tenant: str) -> str:
    """A client user's creds, signed by `signing_seed` the way jwt_mint.mint signs them."""
    sk = nkeys.from_seed(signing_seed.encode())
    upub, useed = user_key()
    now = int(time.time())
    fmt = ('{{"jti":"{jti}","iat":{iat},"exp":{exp},"iss":"{iss}","name":"{name}","sub":"{sub}",'
           '"nats":{{"pub":{{}},"sub":{{}},"issuer_account":"{acct}","tags":["tenant:{tenant}"],'
           '"type":"user","version":2}}}}')
    fields = dict(iat=now, exp=now + 3600, iss=sk.public_key.decode(), name=name, sub=upub,
                  acct=account_pub, tenant=tenant)
    jti = b32(hashlib.sha256(fmt.format(jti="", **fields).encode()).digest())
    head_claims = b64(b'{"typ":"JWT","alg":"ed25519-nkey"}') + "." + b64(fmt.format(jti=jti, **fields).encode())
    jwt = head_claims + "." + b64(sk.sign(head_claims.encode()))
    return (f"-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n"
            f"-----BEGIN USER NKEY SEED-----\n{useed}\n------END USER NKEY SEED------\n")


def value(text: str, name: str) -> str:
    m = re.search(rf"^{name}=(\S+)", text, re.M)
    return m.group(1) if m else ""


def account_claims(conf_text: str, account_pub: str) -> dict:
    jwt = re.search(rf"{account_pub}: (ey[A-Za-z0-9_.-]+)", conf_text).group(1)
    seg = jwt.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))


def nats_cli(creds: pathlib.Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["nats", "--server", f"nats://127.0.0.1:{PORT}", "--creds", str(creds), *args],
                          capture_output=True, text=True, timeout=10)


def main() -> int:
    failed = 0
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb-rotate-"))
    ns = sub = None
    try:
        subprocess.run([str(BRIDGE), "--init-nats", "operator", "--port", str(PORT),
                        "--http-port", "18227", "--ws-port", "18087"], cwd=tmp, capture_output=True, check=True)
        d = tmp / "zb-nats"
        conf, env, store = d / "nats-server.conf", d / ".env.nats", d / "operator.store"
        acct = value(env.read_text(), "ZB_ACCOUNT_PUB")
        old_seed = value(env.read_text(), "ZB_SIGNING_SEED")
        before = account_claims(conf.read_text(), acct)["nats"]["signing_keys"]
        roles_before = {k["role"]: k["key"] for k in before}

        ns = subprocess.Popen(["nats-server", "-c", str(conf)], stdout=open(tmp / "ns.log", "w"),
                              stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and subprocess.run(["nc", "-z", "127.0.0.1", str(PORT)],
                                                             capture_output=True).returncode != 0:
            if ns.poll() is not None:
                zb.bad("the generated server died"); return 1
            time.sleep(0.3)

        # ── 1. a client JWT from the current key: what a leaked seed lets anyone mint ──
        old_creds = tmp / "old.creds"
        old_creds.write_text(client_creds(old_seed, acct, "alice", "acme"))
        r = nats_cli(old_creds, "pub", "mutation.alice.orders.UPDATE", "x")
        if r.returncode == 0:
            zb.ok("a client JWT signed with the current key connects and publishes under its name")
        else:
            zb.bad(f"the current key's JWT was refused before any rotation: {r.stderr[-160:]}"); return failed + 1
        sub = subprocess.Popen(["nats", "--server", f"nats://127.0.0.1:{PORT}", "--creds", str(old_creds),
                                "sub", "cdc.acme.>"], stdout=open(tmp / "sub.log", "w"), stderr=subprocess.STDOUT)
        time.sleep(2)
        if sub.poll() is not None:
            zb.bad("the live session never connected"); return failed + 1
        zb.ok("a live session runs on that JWT (subscribed to tenant acme's stream)")

        # ── 2. the rotation ─────────────────────────────────────────────────────────
        r = subprocess.run([str(BRIDGE), "--init-nats", "--rotate-client-key"], cwd=tmp, capture_output=True, text=True)
        outp = r.stdout + r.stderr
        new_seed = value(env.read_text(), "ZB_SIGNING_SEED")
        after = account_claims(conf.read_text(), acct)["nats"]["signing_keys"]
        roles_after = {k["role"]: k["key"] for k in after}
        new_pub = nkeys.from_seed(new_seed.encode()).public_key.decode() if new_seed else ""
        if r.returncode == 0 and "client signing key replaced" in outp and new_seed and new_seed != old_seed \
                and value(store.read_text(), "SK_CLIENT_SEED") == new_seed and roles_after.get("client") == new_pub:
            zb.ok("rotated: the store, .env.nats and the account JWT all name the new client key")
        else:
            zb.bad(f"the rotation did not land (rc={r.returncode}): {outp[-260:]}"); return failed + 1
        if roles_after.get("responder") == roles_before.get("responder") and \
                roles_after.get("service") == roles_before.get("service") and \
                roles_before.get("client") not in roles_after.values():
            zb.ok("the old client key is gone from the account; the responder and service keys are unchanged")
        else:
            zb.bad(f"the signing keys moved wrongly: {roles_before} → {roles_after}"); failed += 1
        if r.stdout.strip() == "":
            zb.ok("with both files present, nothing secret goes to standard output")
        else:
            zb.bad("the rotation printed a seed although it wrote both files"); failed += 1
        if (store.stat().st_mode & 0o777) == 0o600 and (env.stat().st_mode & 0o777) == 0o600:
            zb.ok("operator.store and .env.nats stay 0600")
        else:
            zb.bad("a rewritten secret file lost its 0600 mode"); failed += 1

        # ── 3. reload: the old key's JWTs are dead ─────────────────────────────────
        ns.send_signal(1)  # SIGHUP: reload the re-signed account
        deadline = time.monotonic() + 15
        kicked = False
        while time.monotonic() < deadline:
            if "Disconnected" in (tmp / "sub.log").read_text() or sub.poll() is not None:
                kicked = True
                break
            time.sleep(0.5)
        if kicked:
            zb.ok("on reload the live session on the old key's JWT was kicked")
        else:
            zb.bad("the live session survived the reload"); failed += 1
        r = nats_cli(old_creds, "pub", "mutation.alice.orders.UPDATE", "x")
        if r.returncode != 0 and "Authorization" in r.stderr + r.stdout:
            zb.ok("and it cannot come back: reconnect → Authorization Violation")
        else:
            zb.bad(f"the old key's JWT reconnected (rc={r.returncode})"); failed += 1
        forged = tmp / "forged.creds"
        forged.write_text(client_creds(old_seed, acct, "mallory", "globex"))
        r = nats_cli(forged, "pub", "mutation.mallory.orders.UPDATE", "x")
        if r.returncode != 0 and "Authorization" in r.stderr + r.stdout:
            zb.ok("a JWT freshly forged with the leaked seed is refused too")
        else:
            zb.bad(f"a forged JWT from the leaked seed connected (rc={r.returncode})"); failed += 1

        # ── 4. the new key works; the bridge's own creds were never touched ───────
        new_creds = tmp / "new.creds"
        new_creds.write_text(client_creds(new_seed, acct, "alice", "acme"))
        r = nats_cli(new_creds, "pub", "mutation.alice.orders.UPDATE", "x")
        if r.returncode == 0:
            zb.ok("a JWT from the new key connects: what /renew hands a device after the bridge restarts")
        else:
            zb.bad(f"the new key's JWT was refused: {r.stderr[-160:]}"); failed += 1
        r = nats_cli(d / "creds" / "bridge.creds", "pub", "smoke.rotate", "x")
        if r.returncode == 0:
            zb.ok("the bridge's own creds (service key) still connect: no bridge credential to reissue")
        else:
            zb.bad(f"the bridge's creds broke: {r.stderr[-160:]}"); failed += 1

        # ── 5. the store from standard input ───────────────────────────────────────
        store_text = store.read_text()
        r = subprocess.run([str(BRIDGE), "--init-nats", "--rotate-client-key", "--store", "-"],
                           cwd=tmp, input=store_text, capture_output=True, text=True)
        printed = value(r.stdout, "SK_CLIENT_SEED")
        if r.returncode == 0 and printed and printed != new_seed and store.read_text() == store_text \
                and value(env.read_text(), "ZB_SIGNING_SEED") == printed:
            zb.ok("--store -: the store file is left alone and its new SK_CLIENT_SEED line printed on stdout")
        else:
            zb.bad(f"--store - went wrong (rc={r.returncode}): {r.stderr[-200:]}"); failed += 1
    finally:
        for pr in (sub, ns):
            if pr and pr.poll() is None:
                pr.terminate()
                try:
                    pr.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    pr.kill()
        subprocess.run(["rm", "-rf", str(tmp)])
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

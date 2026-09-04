"""FULL revocation: the read door cut NOW, not at expiry (NOTES §10cm).

    scripts/scenarios/run.py live -k revoke_full    (own generated stack, shifted ports)

NATS has no per-JWT revoke command; the mechanism is the `revocations` map inside the
account JWT, operator-signed. `bridge --revoke <p> --conf <nats-server.conf>` with
OPERATOR_SEED escalates to it: the map is rebuilt from `zebridge_principal_keys`
(PostgreSQL is the source of truth — later revocations compose, never un-revoke), the
account JWT is re-signed and spliced in place, and on reload the live session is
KICKED and the dead token refused. Fully isolated here: its own --init-nats operator
stack on shifted ports; the live broker is never touched.

  1. generated stack boots; a live sub runs on the generated bridge.creds
  2. partial revoke (no OPERATOR_SEED): keys stamped, escalation hint printed
  3. full revoke (+seed +--conf): map written, conf amended
  4. HUP → the LIVE session is kicked; a reconnect gets Authorization Violation
"""
import base64
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402

BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
PORT = 14223
PRINCIPAL = "fullrev_probe"
ADMIN_URL = "postgres://postgres@127.0.0.1:5432/postgres"


def main() -> int:
    failed = 0
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb-revoke-full-"))
    ns = sub = None
    try:
        subprocess.run([str(BRIDGE), "--init-nats", "operator"], cwd=tmp, capture_output=True, check=True)
        conf = tmp / "zb-nats" / "nats-server.conf"
        c = conf.read_text().replace("port: 4222", f"port: {PORT}") \
                            .replace("port: 8222", "port: 18223").replace("port: 8080", "port: 18081")
        conf.write_text(c)
        env_text = (tmp / "zb-nats" / ".env.bridge").read_text()
        op_seed = re.search(r"ZB_OPERATOR_SEED=(SO[A-Z0-9]+)", env_text).group(1)
        acct = re.search(r"^ZB_ACCOUNT_PUB=(A[A-Z0-9]+)", env_text, re.M).group(1)
        creds = tmp / "zb-nats" / "creds" / "bridge.creds"
        jwt = re.search(r"JWT-----\n(ey[^\n]+)", creds.read_text()).group(1)
        seg = jwt.split(".")[1]
        upub = json.loads(base64.urlsafe_b64decode(seg + "==" * (-len(seg) % 4)))["sub"]

        ns = subprocess.Popen(["nats-server", "-c", str(conf)],
                              stdout=open(tmp / "ns.log", "w"), stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and subprocess.run(["nc", "-z", "127.0.0.1", str(PORT)],
                                                             capture_output=True).returncode != 0:
            if ns.poll() is not None:
                zb.bad("generated server died"); return 1
            time.sleep(0.3)

        zb.psql(f"DELETE FROM public.zebridge_principal_keys WHERE principal = '{PRINCIPAL}'", quiet=True)
        zb.psql(f"INSERT INTO public.zebridge_principal_keys (user_pubkey, principal, tenant_id) "
                f"VALUES ('{upub}', '{PRINCIPAL}', 'kilo')")

        sub = subprocess.Popen(["nats", "--server", f"nats://127.0.0.1:{PORT}", "--creds", str(creds),
                                "sub", ">"], stdout=open(tmp / "sub.log", "w"), stderr=subprocess.STDOUT)
        time.sleep(2)
        if sub.poll() is not None:
            zb.bad("the live sub never connected"); return 1
        zb.ok("a live session runs on the soon-to-be-revoked key (its pubkey is on record, §10cl)")

        env = dict(os.environ); env["ADMIN_DATABASE_URL"] = ADMIN_URL
        env.pop("OPERATOR_SEED", None)
        r = subprocess.run([str(BRIDGE), "--revoke", PRINCIPAL], env=env, capture_output=True, text=True)
        outp = r.stdout + r.stderr
        if "1 key(s) marked" in outp and "OPERATOR_SEED" in outp:
            zb.ok("partial (no operator seed): the key is STAMPED and the escalation path printed — "
                  "partial is a fallback the credentials dictate, never a choice")
        else:
            zb.bad(f"partial revoke wrong: {outp[-160:]}"); failed += 1

        env["OPERATOR_SEED"] = op_seed
        env["ZB_ACCOUNT_PUB"] = acct
        r = subprocess.run([str(BRIDGE), "--revoke", PRINCIPAL, "--conf", str(conf)],
                           env=env, capture_output=True, text=True)
        outp = r.stdout + r.stderr
        if r.returncode == 0 and "FULL revocation written" in outp:
            zb.ok("full: the revocations map rebuilt from PostgreSQL, the account JWT re-signed "
                  "by the operator and spliced into the conf in place")
        else:
            zb.bad(f"full revoke failed: {outp[-260:]}"); return failed + 1

        ns.send_signal(1)  # SIGHUP: reload the amended conf
        deadline = time.monotonic() + 15
        kicked = False
        while time.monotonic() < deadline:
            if "Disconnected" in (tmp / "sub.log").read_text() or sub.poll() is not None:
                kicked = True
                break
            time.sleep(0.5)
        if kicked:
            zb.ok("on reload the LIVE session was kicked — the read door closed in seconds, not at TTL")
        else:
            zb.bad("the live session survived the reload"); failed += 1

        bare = subprocess.run(["nats", "--server", f"nats://127.0.0.1:{PORT}", "--creds", str(creds),
                               "pub", "x", "y"], capture_output=True, text=True, timeout=10)
        if bare.returncode != 0 and "Authorization" in bare.stderr + bare.stdout:
            zb.ok("and the dead token cannot come back: reconnect → Authorization Violation")
        else:
            zb.bad(f"the revoked token reconnected (rc={bare.returncode})"); failed += 1
    finally:
        for pr in (sub, ns):
            if pr and pr.poll() is None:
                pr.terminate()
                try:
                    pr.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    pr.kill()
        zb.psql(f"DELETE FROM public.zebridge_principal_keys WHERE principal = '{PRINCIPAL}'", quiet=True)
        subprocess.run(["rm", "-rf", str(tmp)])
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

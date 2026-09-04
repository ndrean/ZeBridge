"""`bridge --init-nats`: the whole NATS stack generated — and PROVEN by booting it
(NOTES §10ch).

    scripts/scenarios/run.py live -k init_nats     (boots its own server on shifted ports)

Dev mode: an OPEN server conf and a matching .env.bridge, no JWT in sight — the
ten-second path. Operator mode: operator + SYS + ZEBRIDGE accounts, two scoped signing
keys, the bridge's creds, enrollment wired — all minted by the bridge itself, no nsc.
The proof is not "files exist": a real nats-server boots from the generated conf, the
generated creds pass a full JetStream round trip, and a credless connection is refused.
"""
import json
import pathlib
import re
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402

BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
PORT = 14222


def main() -> int:
    failed = 0
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb-init-nats-"))
    ns = None
    try:
        # ── dev mode: the ten-second path ────────────────────────────────────
        r = subprocess.run([str(BRIDGE), "--init-nats", "dev"], cwd=tmp, capture_output=True, text=True)
        conf = (tmp / "zb-nats" / "nats-server.conf").read_text()
        env = (tmp / "zb-nats" / ".env.bridge").read_text()
        # the block SYNTAX, not the word: the dev conf's comment explains there is no
        # authorization block, and the word in the comment matched the naive check
        if r.returncode == 0 and not re.search(r"^\s*authorization\s*\{", conf, re.M) and "jetstream" in conf:
            zb.ok("dev mode: an OPEN server conf (JetStream on, no auth block) — the 10s path")
        else:
            zb.bad(f"dev mode wrong (rc={r.returncode}, auth-block={'authorization' in conf})"); failed += 1
        if "ZB_SIGNING_SEED" not in env.replace("# ZB_SIGNING", "").split("ZB_SIGNING_SEED=")[0] and "ZB_SIGNING_SEED=" not in env:
            zb.ok("dev mode: no ZB_SIGNING_SEED — enrollment stays dark, as documented")
        else:
            zb.bad("dev mode leaked a signing seed"); failed += 1

        # ── refusal to overwrite ─────────────────────────────────────────────
        r = subprocess.run([str(BRIDGE), "--init-nats", "dev"], cwd=tmp, capture_output=True, text=True)
        if r.returncode != 0:
            zb.ok("a second run REFUSES to overwrite (seeds are credentials); --force overrides")
        else:
            zb.bad("a second run silently overwrote generated credentials"); failed += 1

        # ── operator mode: generate, then BOOT it ────────────────────────────
        r = subprocess.run([str(BRIDGE), "--init-nats", "operator", "--force"], cwd=tmp, capture_output=True, text=True)
        if r.returncode != 0:
            zb.bad(f"operator generation failed: {r.stdout[-200:]}{r.stderr[-200:]}"); return failed + 1
        conf_path = tmp / "zb-nats" / "nats-server.conf"
        c = conf_path.read_text()
        c = c.replace("port: 4222", f"port: {PORT}").replace("port: 8222", "port: 18222").replace("port: 8080", "port: 18080")
        conf_path.write_text(c)
        ns = subprocess.Popen(["nats-server", "-c", str(conf_path)],
                              stdout=open(tmp / "ns.log", "w"), stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 15
        up = False
        while time.monotonic() < deadline:
            if ns.poll() is not None:
                break
            if subprocess.run(["nc", "-z", "127.0.0.1", str(PORT)], capture_output=True).returncode == 0:
                up = True
                break
            time.sleep(0.3)
        if not up:
            zb.bad(f"the generated server did not boot: {(tmp / 'ns.log').read_text()[-200:]}")
            return failed + 1
        zb.ok("the generated operator conf BOOTS a real nats-server (operator + SYS + "
              "ZEBRIDGE preloaded, JetStream on)")

        creds = str(tmp / "zb-nats" / "creds" / "bridge.creds")
        N = ["nats", "--server", f"nats://127.0.0.1:{PORT}", "--creds", creds]
        add = subprocess.run(N + ["stream", "add", "SMOKE", "--subjects=smoke.>", "--storage=memory",
                                  "--retention=limits", "--replicas=1", "--defaults"],
                             capture_output=True, text=True)
        subprocess.run(N + ["pub", "smoke.proof", "generated"], capture_output=True)
        info = subprocess.run(N + ["stream", "info", "SMOKE", "-j"], capture_output=True, text=True)
        msgs = json.loads(info.stdout)["state"]["messages"] if info.returncode == 0 else -1
        subprocess.run(N + ["stream", "rm", "SMOKE", "-f"], capture_output=True)
        if add.returncode == 0 and msgs == 1:
            zb.ok("the generated bridge.creds pass a full JetStream round trip "
                  "(stream add → publish → 1 stored → rm) — the service scope is real")
        else:
            zb.bad(f"generated creds failed JetStream (add rc={add.returncode}, msgs={msgs})"); failed += 1

        bare = subprocess.run(["nats", "--server", f"nats://127.0.0.1:{PORT}", "pub", "x", "y"],
                              capture_output=True, text=True)
        if bare.returncode != 0 and "Authorization" in (bare.stderr + bare.stdout):
            zb.ok("and a CREDLESS connection is refused — this is operator mode, not the dev bypass")
        else:
            zb.bad("a credless connection was accepted under operator mode"); failed += 1

        env2 = (tmp / "zb-nats" / ".env.bridge").read_text()
        if re.search(r"^ZB_SIGNING_SEED=SA", env2, re.M) and re.search(r"^ZB_ACCOUNT_PUB=A", env2, re.M):
            zb.ok("enrollment wired: ZB_SIGNING_SEED (scoped client key) + ZB_ACCOUNT_PUB in .env.bridge")
        else:
            zb.bad("the enrollment seed/account are missing from the generated env"); failed += 1
    finally:
        if ns and ns.poll() is None:
            ns.terminate()
            try:
                ns.wait(timeout=10)
            except subprocess.TimeoutExpired:
                ns.kill()
        subprocess.run(["rm", "-rf", str(tmp)])
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

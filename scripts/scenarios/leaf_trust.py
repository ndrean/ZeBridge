#!/usr/bin/env python3
"""Signed trust refresh, revocation, key rotation and stale-feed recovery on private NATS servers."""
import asyncio
import base64
import copy
import http.server
import importlib.util
import json
import os
import pathlib
import re
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time

import nats
import nkeys
import zb
from rotate_client_key import client_creds as mint_client

BRIDGE = pathlib.Path(os.environ.get("ZB_BRIDGE_BIN", zb.ROOT / "zig-out/bin/bridge"))
WATCHER = zb.ROOT / "deploy/compose/leaf/leaf_trust.py"
spec = importlib.util.spec_from_file_location("leaf_trust_runtime", WATCHER)
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def signed(claims, seed):
    sk = nkeys.from_seed(seed.encode())
    def b64(data):
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()
    value = b64(b'{"typ":"JWT","alg":"ed25519-nkey"}') + "." + b64(json.dumps(claims, separators=(",", ":")).encode())
    return value + "." + b64(sk.sign(value.encode()))


async def main():
    failed = 0
    def check(ok, label):
        nonlocal failed
        (zb.ok if ok else zb.bad)(label)
        failed += not ok
    async def until(predicate, timeout=8):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if predicate():
                return True
            await asyncio.sleep(.1)
        return False

    with tempfile.TemporaryDirectory(prefix="zb-leaf-trust-") as folder:
        tmp = pathlib.Path(folder)
        generated = subprocess.run([str(BRIDGE), "--init-nats", "operator", "--dir", str(tmp / "hub"), "--js-domain", "hub"],
                                   capture_output=True, text=True)
        assert generated.returncode == 0, generated.stderr
        store_path = tmp / "hub/operator.store"
        def store():
            return dict(line.split("=", 1) for line in store_path.read_text().splitlines() if "=" in line and not line.startswith("#"))
        keys = store()
        original = (tmp / "hub/nats-server.conf").read_text()
        trust_text = original[:original.index("\nport:")]
        operator, account = runtime.pinned(trust_text)
        old_jwt = runtime.account_token(trust_text, account)[0]
        hub_port, leaf_port, link_port = port(), port(), port()
        conf = tmp / "hub/nats-server.conf"
        conf.write_text(trust_text + f'\nhost: "127.0.0.1"\nport: {hub_port}\nleafnodes {{ listen: "127.0.0.1:{link_port}" }}\njetstream {{ domain: hub, store_dir: "{tmp}/hub-data" }}\n')
        leaf = tmp / "leaf"
        leaf.mkdir()
        (leaf / "trust.conf").write_text(trust_text)
        link = subprocess.run([str(BRIDGE), "--mint-leaf", "--name", "lf-edge1", "--tenant", "acme", "--store", str(store_path)], capture_output=True, text=True)
        assert link.returncode == 0, link.stderr
        (leaf / "link.creds").write_text(link.stdout)
        (leaf / "nats-server.conf").write_text(f'include ./trust.conf\nhost: "127.0.0.1"\nport: {leaf_port}\nleafnodes {{ remotes: [{{url:"nats://127.0.0.1:{link_port}", credentials:"{leaf}/link.creds", account:"{account}"}}] }}\n')
        def mint(name):
            path = tmp / f"{name}.creds"
            path.write_text(mint_client(store()["SK_CLIENT_SEED"], account, name, "acme"))
            return path
        alice = mint("lf-edge1.alice")
        bob = mint("lf-edge1.bob")
        alice_key = runtime.claims(re.search(r'JWT-----\s*(\S+)', alice.read_text()).group(1))["sub"]
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
                        "-keyout", str(tmp / "https.key"), "-out", str(tmp / "https.crt")], capture_output=True, check=True)
        feed = {"available": True, "override": None, "delay": 0}
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                time.sleep(feed["delay"])
                if not feed["available"]:
                    self.send_error(503)
                    return
                token = feed["override"] or runtime.account_token(conf.read_text(), account)[0]
                payload = json.dumps({"account": account, "jwt": token}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                try:
                    self.wfile.write(payload)
                except (BrokenPipeError, ssl.SSLError):
                    pass
            def log_message(self, *_):
                pass
        httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(tmp / "https.crt"), str(tmp / "https.key"))
        httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        hub = supervisor = None
        clients = []
        async def connect(path):
            async def ignored(_error):
                pass
            return await nats.connect(f"nats://127.0.0.1:{leaf_port}", user_credentials=str(path),
                inbox_prefix=b"_INBOX.lf-edge1.alice", allow_reconnect=False, max_reconnect_attempts=0, error_cb=ignored)
        async def refused(path):
            try:
                connection = await asyncio.wait_for(connect(path), 3)
            except (nats.errors.Error, asyncio.TimeoutError):
                return True
            await connection.close()
            return False
        try:
            with open(tmp / "hub.log", "w") as output:
                hub = subprocess.Popen(["nats-server", "-c", str(conf)], stdout=output, stderr=output)
            with open(tmp / "supervisor.log", "w") as output:
                supervisor = subprocess.Popen([sys.executable, str(WATCHER), "--config", str(leaf / "nats-server.conf"),
                    "--pinned", str(leaf / "trust.conf"), "--state-dir", str(tmp / "active"),
                    "--feed", f"https://localhost:{httpd.server_port}/nats-account", "--ca", str(tmp / "https.crt"),
                    "--link-creds", str(leaf / "link.creds"), "--leaf-name", "lf-edge1", "--tenants", "acme",
                    "--interval", ".25", "--max-stale", "2"], stdout=output, stderr=output)
            assert await until(lambda: (tmp / "active/last-success.json").exists()), (tmp / "supervisor.log").read_text()
            async def ready():
                for _ in range(50):
                    try:
                        return await connect(alice)
                    except nats.errors.Error:
                        await asyncio.sleep(.1)
                raise RuntimeError((tmp / "supervisor.log").read_text())
            session = await ready()
            clients.append(session)
            check(not session.is_closed, "the leaf starts only after receiving verified account trust")
            revoked = subprocess.run([str(BRIDGE), "--revoke", "--key", alice_key, "--conf", str(conf)],
                env=dict(os.environ, OPERATOR_SEED=keys["OPERATOR_SEED"], ZB_ACCOUNT_PUB=account), capture_output=True, text=True)
            assert revoked.returncode == 0, revoked.stderr
            hub.send_signal(signal.SIGHUP)
            check(await until(lambda: session.is_closed), "a public signed account refresh kicks a revoked live leaf session")
            check(await refused(alice), "the revoked key cannot reconnect to the leaf")
            other = await connect(bob)
            clients.append(other)
            check(not other.is_closed, "an unrelated authorized device still connects")
            rotated = subprocess.run([str(BRIDGE), "--init-nats", "--rotate-client-key", "--dir", str(tmp / "hub")], capture_output=True, text=True)
            assert rotated.returncode == 0, rotated.stderr
            hub.send_signal(signal.SIGHUP)
            check(await until(lambda: other.is_closed), "client signing-key rotation propagates and closes sessions signed by the old key")
            carol = mint("lf-edge1.carol")
            # Wait until the supervisor has installed the exact current account claim.
            active = tmp / "active/trust.conf"
            current = runtime.account_token(conf.read_text(), account)[0]
            assert await until(lambda: runtime.account_token(active.read_text(), account)[0] == current)
            valid = await connect(carol)
            clients.append(valid)
            check(not valid.is_closed, "a device signed by the new key connects after automatic trust refresh")
            for token, label in ((old_jwt, "an older, correctly signed account cannot roll back trust"),
                                 (current[:-2] + ("A" if current[-2] != "A" else "B") + current[-1], "a modified account signature is rejected")):
                try:
                    runtime.updated_trust(active.read_text(), token, operator, account)
                    rejected = False
                except Exception:
                    rejected = True
                check(rejected and runtime.account_token(active.read_text(), account)[0] == current, label)
            decreased = copy.deepcopy(runtime.claims(current))
            decreased["iat"] += 1
            decreased["nats"]["revocations"] = {}
            try:
                runtime.updated_trust(active.read_text(), signed(decreased, keys["OPERATOR_SEED"]), operator, account)
                rejected = False
            except ValueError:
                rejected = True
            check(rejected, "even a newly signed revision cannot remove an existing revocation")
            # Isolate only trust refresh, leaving the data link and hub alive.
            # A removed signing key can forge fresh, long-lived client claims on
            # the stale leaf; the hub authenticates the leaf link, not that claim.
            stale_seed = store()["SK_CLIENT_SEED"]
            feed["override"] = current
            rotated_again = subprocess.run([str(BRIDGE), "--init-nats", "--rotate-client-key", "--dir", str(tmp / "hub")], capture_output=True, text=True)
            assert rotated_again.returncode == 0, rotated_again.stderr
            forged = tmp / "forged.creds"
            forged_text = mint_client(stale_seed, account, "lf-edge1.alice", "acme")
            original_token = re.search(r'JWT-----\s*(\S+)', forged_text).group(1)
            forged_claims = runtime.claims(original_token)
            forged_claims["exp"] = int(time.time()) + 10 * 365 * 86400
            forged.write_text(forged_text.replace(original_token, signed(forged_claims, stale_seed)))
            hub_client = await nats.connect(f"nats://127.0.0.1:{hub_port}", user_credentials=str(tmp / "hub/creds/bridge.creds"))
            clients.append(hub_client)
            observation = await hub_client.subscribe("mutation.lf-edge1.alice.probe.insert")
            await hub_client.flush()
            feed["available"] = False
            feed["override"] = None
            hub.send_signal(signal.SIGHUP)
            # Wait for the hub itself to reject the old signer, while the leaf
            # remains inside its explicitly configured short test lease.
            hub_rejected = False
            for _ in range(10):
                try:
                    probe = await nats.connect(f"nats://127.0.0.1:{hub_port}", user_credentials=str(forged),
                        allow_reconnect=False, max_reconnect_attempts=0, error_cb=lambda _: asyncio.sleep(0))
                    await probe.close()
                except nats.errors.Error:
                    hub_rejected = True
                    break
                await asyncio.sleep(.02)
            check(hub_rejected, "the live hub rejects credentials forged with its removed signing key")
            forged_session = await connect(forged)
            clients.append(forged_session)
            check(not forged_session.is_closed and forged_claims["exp"] > time.time() + 86400,
                  "stale leaf trust accepts a forged JWT beyond the bridge's normal client lifetime")
            await forged_session.publish("mutation.lf-edge1.alice.probe.insert", b"stale-trust-proof")
            await forged_session.flush()
            delivered = await observation.next_msg(timeout=1)
            check(delivered.data == b"stale-trust-proof",
                  "a stale leaf can relay a forged client's mutation to a live hub within its allowed scope")
            await observation.unsubscribe()
            check(await until(lambda: valid.is_closed, timeout=5), "an unavailable trust feed stops leaf NATS after the configured staleness limit")
            check(supervisor.poll() is None, "the trust supervisor remains alive to recover from the outage")
            feed["available"] = True
            carol = mint("lf-edge1.carol")
            assert await until(lambda: "leaf NATS exited" in (tmp / "supervisor.log").read_text() or (tmp / "active/last-success.json").stat().st_mtime > time.time() - 1)
            for _ in range(50):
                try:
                    recovered = await connect(carol)
                    clients.append(recovered)
                    break
                except nats.errors.Error:
                    await asyncio.sleep(.1)
            else:
                raise RuntimeError((tmp / "supervisor.log").read_text())
            check(not recovered.is_closed, "a fresh verified feed automatically restarts the leaf")
            check(await refused(alice), "recovery retains the revocation instead of restoring the original trust copy")
            feed["delay"] = 6
            check(await until(lambda: recovered.is_closed, timeout=3),
                  "a blocked HTTPS refresh cannot extend the trust lease or delay pausing NATS")
            feed["delay"] = 0

        except Exception:
            print((tmp / "supervisor.log").read_text(errors="replace")[-3000:])
            raise
        finally:
            for connection in clients:
                await connection.close()
            for process in (supervisor, hub):
                if process and process.poll() is None:
                    process.terminate()
                    process.wait(timeout=15)
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=3)
            if supervisor:
                check(supervisor.returncode == 0, "the supervisor and its NATS child shut down cleanly")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

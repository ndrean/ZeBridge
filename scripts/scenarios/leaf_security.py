#!/usr/bin/env python3
"""Leaf-link and mirror-keeper credentials, on disposable Docker NATS 2.15 servers.

No PostgreSQL or live VPS is touched. Docker gives the hub and leaf distinct network
addresses, so the keeper's loopback source restriction is tested across a real link.
Run with scripts/scenarios/.venv/bin/python scripts/scenarios/leaf_security.py.
"""
import asyncio
import ctypes
import json
import os
import pathlib
import re
import socket
import ssl
import subprocess
import tempfile
import time
import uuid

import zb
import nats
from rotate_client_key import client_creds as mint_client

BRIDGE = pathlib.Path(os.environ.get("ZB_BRIDGE_BIN", zb.ROOT / "zig-out/bin/bridge"))
SERVER = "nats:2.15.0-alpine"
SUPERVISED = "zebridge-leaf-nats:2.15.0-security"
CLIENT = "natsio/nats-box:0.20.0"


def run(*args, check=True, timeout=30):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{args[0]} failed: {result.stderr[-500:]}")
    return result


def main():
    ident = "zb-leaf-security-" + uuid.uuid4().hex[:10]
    hub, leaf, feed = ident + "-hub", ident + "-leaf", ident + "-feed"
    failures = 0
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        hub_port = sock.getsockname()[1]
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        leaf_port = sock.getsockname()[1]

    def check(condition, label):
        nonlocal failures
        (zb.ok if condition else zb.bad)(label)
        failures += not condition

    with tempfile.TemporaryDirectory(prefix="zb-leaf-security-") as folder:
        tmp = pathlib.Path(folder)
        run(str(BRIDGE), "--init-nats", "operator", "--dir", str(tmp / "hub"), "--js-domain", "hub")
        store = tmp / "hub/operator.store"
        for name, flag in (("link", "--mirror"), ("keeper", "--keeper")):
            result = run(str(BRIDGE), "--mint-leaf", "--name", "lf-leaf1", "--tenant", "acme", flag, "--store", str(store))
            (tmp / f"{name}.creds").write_text(result.stdout)
            os.chmod(tmp / f"{name}.creds", 0o600)
        missing = run(str(BRIDGE), "--mint-leaf", "--name", "lf-bad", "--mirror", "--store", str(store), check=False)
        check(missing.returncode != 0 and not missing.stdout, "an omitted tenant list fails closed")
        for mint, name, flags in (("--mint-leaf", "leaf1", ["--mirror"]),
                                  ("--mint-leaf", "leaf1", ["--keeper"]),
                                  ("--mint-responder", "lf-leaf1", []),
                                  ("--mint-responder", "leaf1.alice", [])):
            refused = run(str(BRIDGE), mint, "--name", name, "--tenant", "acme",
                          *flags, "--store", str(store), check=False)
            check(refused.returncode != 0 and not refused.stdout and "lf-" in refused.stderr,
                  f"{mint} refuses invalid namespace {name!r} without emitting credentials")
        original = (tmp / "hub/nats-server.conf").read_text()
        run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-subj", "/CN=leaf.test", "-addext", "subjectAltName=DNS:leaf.test,DNS:localhost,DNS:feed.test,IP:127.0.0.1",
            "-keyout", str(tmp / "leaf.key"), "-out", str(tmp / "leaf.crt"))
        trust = original[:original.index("\nport:")]
        account = re.search(r'Account "ZEBRIDGE"\s*\n\s*(A[A-Z2-7]{55}):', trust).group(1)
        (tmp / "hub.conf").write_text(trust + '''
host: "0.0.0.0"
port: 4222
max_control_line: 131072
http: "127.0.0.1:8222"
jetstream { domain: hub, store_dir: "/data" }
leafnodes { listen: "0.0.0.0:7422" }
''')
        (tmp / "trust.conf").write_text(trust)
        account_jwt = re.search(r"(?m)^\s*" + re.escape(account) + r':\s*"?([A-Za-z0-9_.-]+)', trust).group(1)
        (tmp / "account.json").write_text(json.dumps({"account": account, "jwt": account_jwt}))
        (tmp / "feed.py").write_text('import http.server, ssl\nserver=http.server.HTTPServer(("0.0.0.0",8443),http.server.SimpleHTTPRequestHandler)\ncontext=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)\ncontext.load_cert_chain("/fixture/leaf.crt","/fixture/leaf.key")\nserver.socket=context.wrap_socket(server.socket,server_side=True)\nserver.serve_forever()\n')
        (tmp / "leaf.conf").write_text("include ./trust.conf\n" + f'''
host: "0.0.0.0"
port: 4222
max_control_line: 131072
http: "127.0.0.1:8222"
feature_flags {{ js_api_reply_v2: true }}
jetstream {{ domain: leaf, store_dir: "/data" }}
tls {{ cert_file: "/fixture/leaf.crt", key_file: "/fixture/leaf.key" }}
leafnodes {{ remotes: [{{ url: "nats://{hub}:7422", credentials: "/fixture/link.creds", account: "{account}" }}] }}
''')

        def client(at, creds, *args, check=True):
            # A keeper next to NATS uses loopback; a connection from that namespace
            # to the hub has the leaf's network address, just like the VPS deployment.
            target = "leaf.test" if at == leaf else hub
            scheme = "tls" if at == leaf else "nats"
            tls = ["--tlsca", "/fixture/leaf.crt"] if at == leaf else []
            return run("docker", "run", "--rm", "--network", f"container:{leaf}",
                       "--mount", f"type=bind,src={tmp},dst=/fixture,readonly",
                       "--entrypoint", "nats", CLIENT, "--server", f"{scheme}://{target}:4222", *tls,
                       "--creds", "/fixture/" + creds, "--inbox-prefix", "$ZB.KEEPER.lf-leaf1",
                       "--timeout", "2s", *args, check=check)

        def request(at, creds, subject, body):
            return json.loads(client(at, creds, "request", "--raw", subject, json.dumps(body)).stdout)

        try:
            run("docker", "network", "create", ident)
            run("docker", "run", "-d", "--name", feed, "--network", ident, "--network-alias", "feed.test",
                "--mount", f"type=bind,src={tmp},dst=/fixture,readonly", "-w", "/fixture",
                "--entrypoint", "python3", SUPERVISED, "/fixture/feed.py")
            for name, conf in ((hub, "hub.conf"), (leaf, "leaf.conf")):
                hosts = ["--add-host", "leaf.test:127.0.0.1"] if name == leaf else []
                ports = ["-p", f"127.0.0.1:{hub_port if name == hub else leaf_port}:4222"]
                command = [SERVER, "-c", f"/fixture/{conf}"] if name == hub else [
                    "--entrypoint", "python3", SUPERVISED, "/usr/local/bin/zebridge-leaf-trust.py",
                    "--config", "/fixture/leaf.conf", "--pinned", "/fixture/trust.conf",
                    "--state-dir", "/data/trust", "--link-creds", "/fixture/link.creds", "--leaf-name", "lf-leaf1",
                    "--tenants", "acme", "--feed", "https://feed.test:8443/account.json", "--ca", "/fixture/leaf.crt",
                    "--interval", ".5", "--max-stale", "5"]
                run("docker", "run", "-d", "--name", name, "--network", ident, *ports,
                    *hosts, "--user", "0:0", "--tmpfs", "/data", "--mount",
                    f"type=bind,src={tmp},dst=/fixture,readonly", *command)
            linked = False
            for _ in range(50):
                result = run("docker", "exec", leaf, "wget", "-qO-", "http://127.0.0.1:8222/leafz", check=False)
                if result.returncode == 0 and json.loads(result.stdout).get("leafnodes", 0) > 0:
                    linked = True
                    break
                time.sleep(0.2)
            check(linked, "LEAFNODE-only identity establishes the leaf link")
            if not linked:
                return 1
            result = client(hub, "link.creds", "request", "--raw", "$JS.API.INFO", "{}", check=False)
            check(result.returncode != 0 and any(x in result.stderr.lower() for x in ("authorization", "authentication", "connection type")),
                  "link credentials cannot log in directly to the hub's client port")
            if not (result.returncode != 0 and any(x in result.stderr.lower() for x in ("authorization", "authentication", "connection type"))):
                print("link client denial diagnostic:", result.returncode, result.stderr, result.stdout[:160])
            result = client(hub, "keeper.creds", "request", "--raw", "$JS.API.INFO", "{}", check=False)
            check(result.returncode != 0 and "Authorization" in result.stderr,
                  "keeper credentials cannot log in to the hub from the leaf's network address")

            admin = "hub/creds/bridge.creds"
            cfg = {"name": "OBJ_gen-acme", "subjects": ["$O.gen-acme.>"],
                   "storage": "memory", "allow_direct": True}
            created = request(hub, admin, "$JS.API.STREAM.CREATE.OBJ_gen-acme", cfg)
            check("error" not in created, "hub snapshot stream created by the hub's service identity")
            client(hub, admin, "pub", "$O.gen-acme.C.proof", "seed-bytes")
            mirror = {"name": "OBJ_gen-acme", "storage": "memory", "allow_direct": True,
                      "mirror_direct": True, "mirror": {"name": "OBJ_gen-acme",
                      "external": {"api": "$JS.hub.API", "deliver": "$ZB.MIRROR.lf-leaf1"}}}
            created = request(leaf, "keeper.creds", "$JS.API.STREAM.CREATE.OBJ_gen-acme", mirror)
            check("error" not in created, "loopback keeper creates the local mirror")
            copied = False
            for _ in range(20):
                info = request(leaf, "keeper.creds", "$JS.API.STREAM.INFO.OBJ_gen-acme", {})
                if info.get("state", {}).get("messages") == 1:
                    copied = True
                    break
                time.sleep(0.2)
            check(copied, "snapshot replication continues without stream administration on the link")
            denied = client(leaf, "keeper.creds", "request", "--raw",
                            "$JS.hub.API.STREAM.DELETE.OBJ_gen-acme", "{}", check=False)
            # nats CLI can exit zero after an asynchronous permissions error. Judge
            # the operation's response and the actual upstream state, not its exit.
            try:
                delete_response = json.loads(denied.stdout or "{}")
            except json.JSONDecodeError:
                delete_response = {}
            upstream = request(hub, admin, "$JS.API.STREAM.INFO.OBJ_gen-acme", {})
            check(delete_response.get("success") is not True and upstream.get("state", {}).get("messages") == 1,
                  "keeper cannot delete a hub stream through the leaf link")
            check(upstream.get("state", {}).get("messages") == 1, "hub stream and its message remain intact")
            removed = request(leaf, "keeper.creds", "$JS.API.STREAM.DELETE.OBJ_gen-acme", {})
            check(removed.get("success") is True, "keeper can delete its local mirror")
            # A compromised leaf has an unrestricted local identity. The hub's
            # link ACL, not the downstream user's JWT, must contain its access.
            for tenant in ("other",):
                cfg = {"name": f"OBJ_gen-{tenant}", "subjects": [f"$O.gen-{tenant}.>"], "storage": "memory", "allow_direct": True}
                request(hub, admin, f"$JS.API.STREAM.CREATE.OBJ_gen-{tenant}", cfg)
                client(hub, admin, "pub", f"$O.gen-{tenant}.C.secret", "foreign-seed-bytes")
                response = client(leaf, admin, "request", "--raw",
                                  f"$JS.hub.API.DIRECT.GET.OBJ_gen-{tenant}.$O.gen-{tenant}.C.secret", "{}", check=False)
                check("foreign-seed-bytes" not in response.stdout,
                      "a compromised leaf cannot directly read another tenant's seed")
                response = client(leaf, admin, "request", "--raw",
                                  f"$JS.hub.API.CONSUMER.CREATE.OBJ_gen-{tenant}",
                                  json.dumps({"stream_name": f"OBJ_gen-{tenant}", "config": {"name": "stolen", "ack_policy": "none"}}), check=False)
                check('"num_pending"' not in response.stdout and '"created"' not in response.stdout,
                      "a compromised leaf cannot create a consumer on another tenant's seed stream")
            # Unscoped generations consumers would bypass every per-key grant.
            request(hub, admin, "$JS.API.STREAM.CREATE.KV_generations", {
                "name": "KV_generations", "subjects": ["$KV.generations.>"], "allow_direct": True,
                "storage": "memory", "max_msgs_per_subject": 1})
            client(hub, admin, "pub", "$KV.generations.other.table", "foreign-manifest")
            response = client(leaf, admin, "request", "--raw", "$JS.hub.API.CONSUMER.CREATE.KV_generations",
                              json.dumps({"stream_name": "KV_generations", "config": {"name": "all_manifests", "ack_policy": "none"}}), check=False)
            check('"num_pending"' not in response.stdout and '"created"' not in response.stdout,
                  "a compromised leaf cannot open an unfiltered consumer on shared manifests")
            client(hub, admin, "pub", "$KV.generations.acme.table", "allowed-manifest")
            client(hub, admin, "pub", "$KV.generations._default.table", "public-manifest")
            prepared = subprocess.run(
                [str(BRIDGE), "--mutation-ingress", "--prepare-projections"],
                env=dict(os.environ, NATS_URL=f"nats://127.0.0.1:{hub_port}", NATS_JS_DOMAIN="hub",
                         NATS_CREDS=str(tmp / "hub/creds/bridge.creds"), NATS_TLS_CA="", ZB_LEAF_TENANTS="acme"),
                capture_output=True, text=True, timeout=30)
            check(prepared.returncode == 0, "the hub creates tenant-filtered manifest projections")
            if prepared.returncode:
                raise RuntimeError(prepared.stderr[-1800:])
            sources = [{"name": f"KV_generations_leaf_{t}", "external": {
                        "api": "$JS.hub.API", "deliver": "$ZB.MIRROR.lf-leaf1"}} for t in ("acme", "_default")]
            merged = request(leaf, "keeper.creds", "$JS.API.STREAM.CREATE.KV_generations", {
                "name": "KV_generations", "subjects": ["$KV.generations.>"], "storage": "memory",
                "allow_direct": True, "max_bytes": 64 << 20, "max_msgs_per_subject": 1, "sources": sources})
            check("error" not in merged, "the leaf combines only its allowed and public manifest projections")
            info = {}
            for _ in range(30):
                info = request(leaf, "keeper.creds", "$JS.API.STREAM.INFO.KV_generations", {})
                if info.get("state", {}).get("messages") == 2:
                    break
                time.sleep(.2)
            check(info.get("state", {}).get("messages") == 2,
                  "local manifest storage contains allowed and public keys, with no foreign tenant keys")
            if info.get("state", {}).get("messages") != 2:
                print("local manifest diagnostics:", json.dumps(info))
                print("hub projection diagnostics:", json.dumps(request(hub, admin, "$JS.API.STREAM.INFO.KV_generations_leaf_acme", {})))
                print("leaf log:", run("docker", "logs", "--tail", "25", leaf, check=False).stderr)

            response = client(leaf, admin, "request", "--raw", "$JS.API.DIRECT.GET.KV_generations.$KV.generations.acme.table", "{}", check=False)
            check("allowed-manifest" in response.stdout, "clients can seed from their local manifest projection")
            response = client(leaf, admin, "request", "--raw", "$JS.API.DIRECT.GET.KV_generations.$KV.generations.other.table", "{}", check=False)
            check("foreign-manifest" not in response.stdout, "foreign manifests are absent even for the leaf's local administrator")
            async def boundary_probes():
                hub_nc = await nats.connect(f"nats://127.0.0.1:{hub_port}", user_credentials=str(tmp / admin))
                tls = ssl.create_default_context(cafile=str(tmp / "leaf.crt"))
                leaf_nc = await nats.connect(f"tls://127.0.0.1:{leaf_port}", tls=tls, tls_hostname="leaf.test",
                                             user_credentials=str(tmp / admin))
                scoped = None
                try:
                    ours = await leaf_nc.subscribe("_INBOX.lf-leaf1.alice.probe")
                    theirs = await leaf_nc.subscribe("_INBOX.lf-leaf2.bob.probe")
                    own_verdict = await leaf_nc.subscribe("mutation_ack.lf-leaf1.alice.probe")
                    other_verdict = await leaf_nc.subscribe("mutation_ack.lf-leaf2.bob.probe")
                    allowed_writes = await hub_nc.subscribe("mutation.lf-leaf1.*.*.*")
                    foreign_writes = await hub_nc.subscribe("mutation.lf-leaf2.*.*.*")
                    root_writes = await hub_nc.subscribe("mutation.bob.*.*")
                    await leaf_nc.flush()
                    await hub_nc.flush()
                    await asyncio.sleep(.3)
                    for topic in ("_INBOX.lf-leaf1.alice.probe", "_INBOX.lf-leaf2.bob.probe",
                                  "mutation_ack.lf-leaf1.alice.probe", "mutation_ack.lf-leaf2.bob.probe"):
                        await hub_nc.publish(topic, b"proof")
                    for topic in ("mutation.lf-leaf1.alice.table.insert", "mutation.lf-leaf2.bob.table.insert", "mutation.bob.table.insert"):
                        await leaf_nc.publish(topic, b"proof")
                    await hub_nc.flush()
                    await leaf_nc.flush()
                    await asyncio.sleep(.3)
                    check(ours.pending_msgs == 1 and theirs.pending_msgs == 0,
                          "a compromised leaf can receive its own namespace's inboxes but cannot sniff another leaf's replies")
                    check(own_verdict.pending_msgs == 1 and other_verdict.pending_msgs == 0,
                          "verdict delivery is restricted to the leaf's principal namespace")
                    check(allowed_writes.pending_msgs == 1 and foreign_writes.pending_msgs == 0 and root_writes.pending_msgs == 0,
                          "a compromised leaf cannot impersonate a hub principal or another leaf's principal")
                    async def answer(message):
                        await leaf_nc.publish(message.reply, b'{"ok":true}')
                    responder = await leaf_nc.subscribe("query.acme.probe", cb=answer)
                    forbidden = await leaf_nc.subscribe("query.other.probe", cb=answer)
                    await leaf_nc.flush()
                    await asyncio.sleep(.3)
                    async def query(subject, tenant):
                        inbox = f"_INBOX.hub_user.query.{tenant}.{uuid.uuid4().hex}"
                        sub = await hub_nc.subscribe(inbox)
                        try:
                            await hub_nc.publish(subject, b"request", reply=inbox)
                            message = await sub.next_msg(timeout=2)
                            if message.headers and message.headers.get("Status") == "503":
                                raise nats.errors.NoRespondersError
                            return message
                        finally:
                            await sub.unsubscribe()
                    reply = await query("query.acme.probe", "acme")
                    check(json.loads(reply.data).get("ok") is True, "tenant-specific reply inboxes let a leaf responder answer an authorized tenant's request")
                    try:
                        await query("query.other.probe", "other")
                        foreign_answer = True
                    except (nats.errors.NoRespondersError, nats.errors.TimeoutError):
                        foreign_answer = False
                    check(not foreign_answer, "the leaf cannot receive or answer another tenant's requests")
                    store_values = dict(line.split("=", 1) for line in store.read_text().splitlines()
                                        if "=" in line and not line.startswith("#"))
                    (tmp / "scoped.creds").write_text(mint_client(store_values["SK_CLIENT_SEED"], account, "lf-leaf1.alice", "acme"))
                    scoped = await nats.connect(f"tls://127.0.0.1:{leaf_port}", tls=tls, tls_hostname="leaf.test",
                        user_credentials=str(tmp / "scoped.creds"), inbox_prefix=b"_INBOX.lf-leaf1.alice")
                    reply = await scoped.request("$JS.hub.API.INFO", b"{}", timeout=2)
                    check("error" not in json.loads(reply.data), "a real scoped client JWT can authenticate and receive its own hub API replies")
                    reply = await scoped.request("$JS.leaf.API.DIRECT.GET.KV_generations.$KV.generations.acme.table", b"{}", timeout=2)
                    check(reply.data == b"allowed-manifest", "a scoped client reads its own manifest from the leaf JetStream domain")
                    # These clients use their shipped request APIs, with no per-call
                    # inbox override in the test. Native shims all share this C ABI.
                    native_path = pathlib.Path(os.environ.get("ZB_LIBZB", zb.ROOT / "libzb/zig-out/lib" / ("libzbcore.dylib" if os.uname().sysname == "Darwin" else "libzbcore.so")))
                    native = ctypes.CDLL(str(native_path))
                    native.zb_client_connect.argtypes = [ctypes.c_char_p]
                    native.zb_client_connect.restype = ctypes.c_uint64
                    native.zb_client_close.argtypes = [ctypes.c_uint64]
                    native.zb_client_request.argtypes = [ctypes.c_uint64, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint64]
                    native.zb_client_request.restype = ctypes.c_void_p
                    native.zb_free.argtypes = [ctypes.c_void_p]
                    def native_queries():
                        handle = native.zb_client_connect(json.dumps({"natsUrl": f"tls://localhost:{leaf_port}",
                            "caFile": str(tmp / "leaf.crt"), "credsPath": str(tmp / "scoped.creds"), "principal": "lf-leaf1.alice",
                            "jsDomain": "hub", "seedDomain": "leaf", "tables": [], "heartbeatMs": 0,
                            "dbPath": str(tmp / "native.sqlite3")}).encode())
                        if not handle:
                            return False
                        try:
                            for _ in range(10):
                                pointer = native.zb_client_request(handle, b"query.acme.probe", b"{}", 2000)
                                try:
                                    if not pointer or json.loads(ctypes.string_at(pointer)).get("ok") is not True:
                                        return False
                                finally:
                                    if pointer:
                                        native.zb_free(pointer)
                            return True
                        finally:
                            native.zb_client_close(handle)
                    check(await asyncio.to_thread(native_queries), "libzb's C ABI sends and completes repeated tenant-scoped query requests")
                    # TS connect also exercises scoped tenant resolution and schema watch.
                    for bucket in ("tenants", "schemas"):
                        request(hub, admin, f"$JS.API.STREAM.CREATE.KV_{bucket}", {"name": f"KV_{bucket}",
                            "subjects": [f"$KV.{bucket}.>"], "allow_direct": True, "storage": "memory", "max_msgs_per_subject": 1})
                    client(hub, admin, "pub", "$KV.tenants.lf-leaf1.alice", '["acme"]')
                    def ts_query():
                        return subprocess.run(["node", "--experimental-strip-types", str(zb.ROOT / "scripts/scenarios/leaf_query.mts"),
                            f"tls://localhost:{leaf_port}", str(tmp / "scoped.creds"), str(tmp / "ts.sqlite3")],
                            env=dict(os.environ, NODE_EXTRA_CA_CERTS=str(tmp / "leaf.crt")), capture_output=True, text=True, timeout=30)
                    ts = await asyncio.to_thread(ts_query)
                    check(ts.returncode == 0 and any('"ok":true' in line for line in ts.stdout.splitlines()),
                          "the TS client connects with a scoped identity and completes a tenant-scoped query request")
                    if ts.returncode:
                        print("TS query diagnostics:", ts.stderr[:1000], ts.stdout[-500:])
                    if os.environ.get("ZB_CHECK_LEAKS") == "1" and os.uname().sysname == "Darwin":
                        import gc
                        gc.collect()
                        def leak_report():
                            return subprocess.run(["leaks", "--nocontext", str(os.getpid())], capture_output=True, text=True, timeout=45)
                        report = await asyncio.to_thread(leak_report)
                        check(bool(re.search(r'\b0 leaks for 0 total leaked bytes', report.stdout)),
                              "macOS leaks: no leaked bytes after native query subscriptions close")
                    await responder.unsubscribe()
                    await forbidden.unsubscribe()
                finally:
                    if scoped:
                        await scoped.close()
                    await leaf_nc.close()
                    await hub_nc.close()
            asyncio.run(boundary_probes())
            (tmp / "mirrors.sh").write_text((zb.ROOT / "deploy/compose/leaf/mirrors.sh").read_text())
            def reconcile():
                args = ["docker", "run", "--rm", "--network", f"container:{leaf}",
                        "--mount", f"type=bind,src={tmp},dst=/fixture,readonly"]
                for key, value in {"LEAF_NAME": "lf-leaf1", "LEAF_HOST": "leaf.test", "LEAF_JS_DOMAIN": "leaf",
                                   "LEAF_TENANTS": "acme", "HUB_JS_DOMAIN": "hub", "ZB_MIRROR_ONCE": "1",
                                   "KEEPER_CREDS": "/fixture/keeper.creds", "NATS_TLS_CA": "/fixture/leaf.crt",
                                   "NATS_REQUEST_TIMEOUT": "500ms"}.items():
                    args += ["-e", f"{key}={value}"]
                return run(*args, "--entrypoint", "/bin/sh", CLIENT, "/fixture/mirrors.sh", timeout=30)
            result = reconcile()
            check("local seeding stream created: OBJ_gen-acme" in result.stdout,
                  "the shared Docker/Ansible keeper creates the expected local cache")
            for _ in range(20):
                cached = request(leaf, "keeper.creds", "$JS.API.STREAM.INFO.OBJ_gen-acme", {})
                if cached.get("state", {}).get("messages") == 1:
                    break
                time.sleep(.2)
            check(cached.get("state", {}).get("messages") == 1, "the managed object cache receives its seed data")
            request(hub, admin, "$JS.API.STREAM.DELETE.OBJ_gen-acme", {})
            reconcile()
            retained = request(leaf, "keeper.creds", "$JS.API.STREAM.INFO.OBJ_gen-acme", {})
            check(retained.get("state", {}).get("messages") == 1,
                  "an absent upstream stream never triggers deletion of a local cache")
            run("docker", "stop", "--time", "2", feed)
            stopped = False
            for _ in range(80):
                result = run("docker", "exec", leaf, "wget", "-qO-", "http://127.0.0.1:8222/varz", check=False)
                if result.returncode:
                    stopped = True
                    break
                time.sleep(.1)
            check(stopped and run("docker", "inspect", "-f", "{{.State.Running}}", leaf).stdout.strip() == "true",
                  "the packaged Docker supervisor pauses NATS when verified trust becomes stale")
            run("docker", "start", feed)
            recovered = False
            for _ in range(80):
                result = run("docker", "exec", leaf, "wget", "-qO-", "http://127.0.0.1:8222/leafz", check=False)
                if result.returncode == 0 and json.loads(result.stdout).get("leafnodes", 0) > 0:
                    recovered = True
                    break
                time.sleep(.1)
            check(recovered, "the packaged Docker supervisor restores NATS and its leaf link after trust recovers")
        finally:
            for name in (leaf, hub, feed):
                run("docker", "rm", "-f", name, check=False)
            run("docker", "network", "rm", ident, check=False)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Hub fallback stage 3: enrolled endpoint order and real TCP/WSS connection failures.

Disposable PostgreSQL, operator-mode hub and scoped leaf, admission and bridge. Leaf and
hub use different certificate names. C ABI and TS clients retain their identity, replica,
message IDs and bytes when both brokers disappear and only the hub comes back.
"""
import asyncio, ctypes, http.server, json, os, pathlib, signal, sqlite3, subprocess, tempfile, threading, time, urllib.parse, urllib.request, uuid

import zb
from hub_fallback_stage2 import BRIDGE, LIBRARY, Stack, port

PROBE = pathlib.Path(os.environ.get("ZB_ENDPOINT_PROBE", zb.ROOT / "libzb/zig-out/bin/zb-endpoint-probe"))
PRINCIPAL = "lf-edge1.alice"


class Fixture(Stack):
    async def start(self):
        await super().start()
        self.hub_port = urllib.parse.urlparse(self.url).port
        self.leaf_port, self.link_port = port(), port()
        self.hub_ws, self.leaf_ws, self.hub_http, self.leaf_http = (port() for _ in range(4))
        self.ca = self.tmp / "ca.pem"
        self.url, self.leaf_url = f"tls://localhost:{self.hub_port}", f"tls://127.0.0.1:{self.leaf_port}"
        self.ws_url, self.leaf_ws_url = f"wss://localhost:{self.hub_ws}", f"wss://127.0.0.1:{self.leaf_ws}"
        for label in ("bridge", "admission", "nats"):
            self.halt(label)
        self.certificates()
        ident = self.tmp / f"{self.name}-id"
        keys = dict(l.split("=", 1) for l in (ident / "operator.store").read_text().splitlines() if "=" in l and not l.startswith("#"))
        import nkeys
        account = nkeys.from_seed(keys["ACCOUNT_SEED"].encode()).public_key.decode()
        from rotate_client_key import client_creds
        self.probe_creds = self.tmp / "probe.creds"
        self.probe_creds.write_text(client_creds(keys["SK_CLIENT_SEED"], account, PRINCIPAL, "acme"))
        self.account = account
        result = subprocess.run([str(BRIDGE), "--mint-leaf", "--name", "lf-edge1", "--tenant", "acme", "--mirror", "--store", str(ident / "operator.store")], capture_output=True, text=True, check=True)
        (self.tmp / "link.creds").write_text(result.stdout)
        original = (ident / "nats-server.conf").read_text()
        self.trust = original[:original.index("\nport:")]
        self.hub_conf = self.tmp / "hub.conf"
        self.leaf_conf = self.tmp / "leaf.conf"
        self.hub_conf.write_text(self.conf("hub", self.hub_port, self.hub_ws, self.hub_http) + f'\nleafnodes {{ listen: "127.0.0.1:{self.link_port}", no_advertise: true }}\n')
        self.leaf_conf.write_text(self.conf("leaf", self.leaf_port, self.leaf_ws, self.leaf_http) + f'\nleafnodes {{ remotes: [{{ url: "nats://127.0.0.1:{self.link_port}", credentials: "{self.tmp}/link.creds", account: "{account}" }}] }}\n')
        self.sql(f"INSERT INTO zebridge_leaf_scopes(leaf_id,tenant_ids,nats_url,nats_ws_url) VALUES ('lf-edge1',ARRAY['acme'],'{self.leaf_url}','{self.leaf_ws_url}'); "
                 f"INSERT INTO zebridge_user_tenants VALUES ('{PRINCIPAL}','acme');")
        self.env.update(NATS_URL=self.url, NATS_TLS_CA=str(self.ca), ZB_LEAF_NAMES="lf-edge1", ZB_SIGNING_SEED=keys["SK_CLIENT_SEED"], ZB_ACCOUNT_PUB=account,
                        ENROLL_NATS_URL=self.url, ENROLL_NATS_WS_URL=self.ws_url, ENROLL_JWT_TTL_SECONDS="86400")
        await self.broker("nats", self.hub_conf)
        await self.broker("leaf", self.leaf_conf)
        self.start_admission()
        await self.wait_log("admission", "mutation admission ready")
        self.bridge_port = port()
        self.spawn("bridge", [str(BRIDGE), "--pub", "hf2_pub", "--slot", self.name + "_slot", "--port", str(self.bridge_port)], self.env)
        await self.wait_log("bridge", "Ready! Pulling mutations")
        await self.linked()
        self.requests = []
        fixture = self
        class Proxy(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_GET(self):
                fixture.requests.append(self.path.split("?", 1)[0])
                try:
                    result = urllib.request.urlopen(f"http://127.0.0.1:{fixture.bridge_port}{self.path}", timeout=10)
                except urllib.error.HTTPError as error:
                    result = error
                with result:
                    body = result.read()
                    self.send_response(result.status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
        self.Proxy = Proxy
        self.open_http()
        await asyncio.sleep(2)

    def open_http(self):
        self.proxy = http.server.ThreadingHTTPServer(("127.0.0.1", port()), self.Proxy)
        self.proxy_thread = threading.Thread(target=self.proxy.serve_forever, daemon=True)
        self.proxy_thread.start()
        self.bridge_url = f"http://127.0.0.1:{self.proxy.server_port}"

    def halt(self, label):
        proc = self.procs.get(label)
        if proc and proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=15)

    async def broker(self, label, conf):
        self.spawn(label, ["nats-server", "-c", str(conf)], dict(os.environ))
        await self.wait_log(label, "Server is ready")

    def certificates(self):
        def openssl(*args):
            subprocess.run(["openssl", *map(str, args)], capture_output=True, check=True)
        openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", self.tmp / "ca.key", "-out", self.ca, "-days", "2", "-subj", "/CN=ZeBridge endpoint fixture", "-addext", "basicConstraints=critical,CA:TRUE")
        # Native TLS currently matches DNS SANs; Node matches an IP URL to its IP
        # SAN. Carry both on the loopback leaf certificate. Their expected names
        # still differ: leaf 127.0.0.1, hub localhost.
        for name, san in (("hub", "DNS:localhost"), ("leaf", "DNS:127.0.0.1,IP:127.0.0.1"), ("bad", "DNS:wrong.invalid")):
            openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", self.tmp / f"{name}.key", "-out", self.tmp / f"{name}.csr", "-subj", f"/CN={name}")
            ext = self.tmp / f"{name}.ext"
            ext.write_text(f"subjectAltName={san}\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n")
            openssl("x509", "-req", "-in", self.tmp / f"{name}.csr", "-CA", self.ca, "-CAkey", self.tmp / "ca.key", "-CAcreateserial", "-out", self.tmp / f"{name}.pem", "-days", "2", "-extfile", ext)

    def conf(self, name, tcp, ws, monitor, cert=None):
        cert = cert or name
        tls = f'tls {{ cert_file: "{self.tmp}/{cert}.pem", key_file: "{self.tmp}/{cert}.key" }}'
        return (self.trust + f'\nhost: "127.0.0.1"\nport: {tcp}\nhttp: "127.0.0.1:{monitor}"\nmax_payload: 1048576\n'
                f'max_control_line: 131072\n{tls}\nwebsocket {{ host: "127.0.0.1", port: {ws}, {tls} }}\n'
                f'feature_flags {{ js_api_reply_v2: true }}\njetstream {{ domain: {name}, store_dir: "{self.tmp}/{name}-js" }}\n')

    def monitor(self, leaf=False, endpoint="connz"):
        return json.load(urllib.request.urlopen(f"http://127.0.0.1:{self.leaf_http if leaf else self.hub_http}/{endpoint}", timeout=3))

    async def cdc_consumers_since(self, since):
        """Tail consumers created on each CDC stream at or after `since`: what one client
        session opened. Replaced consumers are reaped by the server, not deleted (clients
        have no CONSUMER.DELETE grant), so a count over all consumers would include them.
        Tails are reaped after 120 s idle; libzb's catch-up drain at sync opens its own
        consumer reaped after 30 s, which is not a tail and is not counted."""
        import nats, ssl
        ctx = ssl.create_default_context(cafile=str(self.ca))
        ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT  # the fixture's test CA has no Authority Key Identifier
        nc = await nats.connect(self.url, user_credentials=self.bridge_creds, tls=ctx)
        try:
            js = nc.jetstream(domain="hub")
            out = {}
            for info in await js.streams_info():
                name = info.config.name
                if not name.startswith("CDC_"): continue
                n = sum(1 for c in await js.consumers_info(name)
                        if c.created and c.created >= since and (c.config.inactive_threshold or 0) >= 60)
                if n: out[name] = n
            return out
        finally:
            await nc.close()

    def devices_at_leaf(self):
        # Pending TCP/TLS handshakes are connections too; only count authenticated
        # clients, never a failed handshake still being closed by the server.
        return sum(bool(c.get("authorized_user") or c.get("user")) for c in self.monitor(True, "connz?auth=1")["connections"])

    async def linked(self):
        for _ in range(200):
            if self.monitor(True, "leafz").get("leafnodes", 0): return
            await asyncio.sleep(.1)
        raise RuntimeError("leaf did not connect to the hub")

    def invite(self):
        return self.sql(f"SELECT zebridge_invite('{PRINCIPAL}','acme')")

    def options(self, label):
        return dict(bridgeUrl=self.bridge_url, invite=self.invite(), dbPath=str(self.tmp / f"{label}.sqlite3"), identityPath=str(self.tmp / f"{label}.identity"),
                    caFile=str(self.ca), tables=["s2_row"], seedDomain="leaf", heartbeatMs=0)

    def close_http(self):
        if getattr(self, "proxy", None):
            self.proxy.shutdown()
            self.proxy.server_close()
            self.proxy_thread.join()
            self.proxy = None

    def stop(self):
        self.close_http()
        super().stop()


class Native:
    def __init__(self, lib, options):
        self.lib, self.options, self.h = lib, options, 0

    def take(self, ptr):
        if not ptr: raise RuntimeError("native call returned no result")
        try: return json.loads(ctypes.string_at(ptr))
        finally: self.lib.zb_free(ptr)

    async def open(self):
        def connect():
            handle = self.lib.zb_client_connect(json.dumps(self.options).encode())
            return handle, None if handle else self.lib.zb_last_error()
        self.h, error = await asyncio.to_thread(connect)
        if not self.h: raise RuntimeError(error.decode() if error else "native connect failed")
        result = await asyncio.to_thread(lambda: self.take(self.lib.zb_client_sync(self.h)))
        if "error" in result: raise RuntimeError(str(result))

    async def snapshot(self):
        def query(statement):
            result = self.take(self.lib.zb_client_query(self.h, statement.encode(), b"[]"))
            if "error" in result: raise RuntimeError(str(result))
            return result["rows"]
        return {"outbox": [{"msg_id": r[0], "payload": r[1]} for r in query("SELECT msg_id,hex(payload) FROM _zebridge_outbox ORDER BY msg_id")],
                "rows": [{"id": r[0], "v": r[1]} for r in query("SELECT id,v FROM s2_row ORDER BY id")]}

    async def write(self, key, value):
        await asyncio.to_thread(lambda: self.take(self.lib.zb_client_mutate(self.h, b"s2_row", b"INSERT", json.dumps({"id": key}).encode(), json.dumps({"v": value}).encode())))
        return await self.snapshot()

    async def pump(self):
        await asyncio.to_thread(lambda: self.take(self.lib.zb_client_poll(self.h, 100)))
        return await self.snapshot()

    async def close(self):
        if self.h:
            self.lib.zb_client_close(self.h)
            self.h = 0


class Typescript:
    def __init__(self, fixture, label, options, mode):
        self.fixture, self.label, self.options, self.mode = fixture, label, options, mode
        self.proc = None

    async def open(self):
        path = self.fixture.tmp / f"{self.label}.json"
        path.write_text(json.dumps(self.options))
        log = self.fixture.tmp / f"{self.label}-ts.log"
        self.proc = await asyncio.create_subprocess_exec("node", "--experimental-strip-types", str(zb.ROOT / "scripts/scenarios/hub_fallback_endpoints.mts"), str(path), self.mode, str(self.fixture.ca),
            env=dict(os.environ, NODE_EXTRA_CA_CERTS=str(self.fixture.ca)), stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=open(log, "a"))
        self.log = log
        return await self.receive()

    async def receive(self):
        line = await asyncio.wait_for(self.proc.stdout.readline(), timeout=60)
        if not line: raise RuntimeError(self.log.read_text(errors="replace")[-3000:])
        return json.loads(line)

    async def command(self, **command):
        self.proc.stdin.write(json.dumps(command).encode() + b"\n")
        await self.proc.stdin.drain()
        return await self.receive()

    async def snapshot(self): return await self.command(cmd="snapshot")
    async def write(self, key, value): return await self.command(cmd="write", id=key, v=value)
    async def pump(self): return await self.snapshot()
    async def close(self):
        if self.proc:
            if self.proc.returncode is None:
                self.proc.stdin.write(b'{"cmd":"close"}\n')
                try:
                    await self.proc.stdin.drain()
                    await asyncio.wait_for(self.proc.wait(), timeout=15)
                except (asyncio.TimeoutError, BrokenPipeError):
                    self.proc.kill()
                    await self.proc.wait()
            self.proc = None


class Probe:
    def __init__(self, fixture, urls, label="probe"):
        self.fixture, self.urls, self.label = fixture, urls, label
        self.proc = None

    async def open(self):
        self.log = self.fixture.tmp / f"{self.label}.log"
        self.proc = await asyncio.create_subprocess_exec(str(PROBE), self.urls, str(self.fixture.probe_creds), PRINCIPAL, str(self.fixture.ca),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=open(self.log, "w"))
        return await self.snapshot(first=True)

    async def snapshot(self, first=False):
        if not first:
            self.proc.stdin.write(b"s")
            await self.proc.stdin.drain()
        line = await asyncio.wait_for(self.proc.stdout.readline(), timeout=30)
        if not line: raise RuntimeError(self.log.read_text(errors="replace")[-2000:])
        return json.loads(line)

    async def close(self):
        if self.proc:
            if self.proc.returncode is None:
                try:
                    self.proc.stdin.write(b"q")
                    await self.proc.stdin.drain()
                    await asyncio.wait_for(self.proc.wait(), timeout=15)
                except (asyncio.TimeoutError, BrokenPipeError):
                    self.proc.kill()
                    await self.proc.wait()
            self.proc = None


async def main():
    if not PROBE.is_file():
        raise RuntimeError("Build the native pool probe first: cd libzb && zig build endpoint-probe (or set ZB_ENDPOINT_PROBE)")
    passed = failed = 0
    def check(ok, label):
        nonlocal passed, failed
        (zb.ok if ok else zb.bad)(label)
        passed += bool(ok)
        failed += not ok
    lib = ctypes.CDLL(str(LIBRARY))
    lib.zb_client_connect.argtypes, lib.zb_client_connect.restype = [ctypes.c_char_p], ctypes.c_uint64
    lib.zb_client_close.argtypes, lib.zb_free.argtypes = [ctypes.c_uint64], [ctypes.c_void_p]
    lib.zb_last_error.restype = ctypes.c_char_p
    for name, extra in (("sync", []), ("poll", [ctypes.c_uint64]), ("flush_outbox", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p]), ("mutate", [ctypes.c_char_p] * 4)):
        fn = getattr(lib, "zb_client_" + name)
        fn.argtypes, fn.restype = [ctypes.c_uint64] + extra, ctypes.c_void_p
    with tempfile.TemporaryDirectory(prefix="zb-hf-endpoints-") as folder:
        hub = Fixture(pathlib.Path(folder))
        clients = []
        enrolled_options = None
        try:
            await hub.start()
            def diagnose(env=None):
                result = subprocess.run([str(BRIDGE), '--diagnose', '--pub', 'hf2_pub', '--slot', hub.name+'_slot', '--port', str(hub.bridge_port)],
                                        env=env or hub.env, capture_output=True, text=True, timeout=45)
                return result.returncode, result.stdout + result.stderr
            _, report = diagnose()
            check('enrollment endpoint policies: hub and 1 leaf' in report, 'diagnose checks the enrolled hub and leaf policies before devices use them')
            try:
                hub.sql("UPDATE zebridge_leaf_scopes SET nats_url='nats://leaf.invalid:4222', nats_ws_url='ws://leaf.invalid' WHERE leaf_id='lf-edge1'")
                code, report = diagnose()
                check(code != 0 and 'lf-edge1 (native): MixedEnrollmentEndpointSecurity' in report and 'lf-edge1 (WebSocket): MixedEnrollmentEndpointSecurity' in report,
                      'diagnose refuses mixed leaf/hub security for native and WebSocket lists')
            finally:
                hub.sql(f"UPDATE zebridge_leaf_scopes SET nats_url='{hub.leaf_url}', nats_ws_url='{hub.leaf_ws_url}' WHERE leaf_id='lf-edge1'")
            code, report = diagnose(dict(hub.env, ENROLL_NATS_URL=hub.url+',nats://hub.invalid:4222'))
            check(code != 0 and 'hub (native): MixedEnrollmentEndpointSecurity' in report, 'diagnose catches a mixed hub list too')
            _, report = diagnose()
            check('enrollment endpoint policies: hub and 1 leaf' in report and 'MixedEnrollmentEndpointSecurity' not in report,
                  'correcting the policy clears the endpoint finding; diagnosis did not change the registry')
            # The pool is read from the same native Transport used by the C ABI.
            # A refused first endpoint must not move the last ahead of the second.
            refused = f"tls://127.0.0.1:{port()}"
            probe = Probe(hub, ",".join((refused, hub.leaf_url, hub.url)))
            clients.append(probe)
            state = await probe.open()
            expected_pool = {urllib.parse.urlparse(url).netloc for url in (refused, hub.leaf_url, hub.url)}
            check(state["server"] == urllib.parse.urlparse(hub.leaf_url).netloc and set(state["pool"]) == expected_pool and state["implicit"] == 0,
                  "native: refused first endpoint preserves second-before-third order and the explicit pool")
            # Authenticated NATS sends a second INFO after CONNECT/PONG, without
            # the handshake nonce. Observe that actual asynchronous frame.
            for _ in range(100):
                state = await probe.snapshot()
                if state["post_connect_info"]: break
                await asyncio.sleep(.1)
            check(state["post_connect_info"] and set(state["pool"]) == expected_pool and state["implicit"] == 0,
                  "native: a real asynchronous INFO preserves the explicit pool without unmanaged endpoints")
            hub.halt("leaf")
            for _ in range(100):
                state = await probe.snapshot()
                if state["status"] == "connected" and state["server"] == urllib.parse.urlparse(hub.url).netloc: break
                await asyncio.sleep(.1)
            check(state["status"] == "connected" and state["server"] == urllib.parse.urlparse(hub.url).netloc and set(state["pool"]) == expected_pool,
                  "native: reconnect preserves the pool and verifies the hub's distinct TLS name")
            await probe.close()
            await hub.broker("leaf", hub.leaf_conf)
            await hub.linked()
            for label, mode in (("native", None), ("ts-tcp", "tcp"), ("ts-ws", "ws")):
                if not hub.proxy: hub.open_http()
                opts = hub.options(label)
                if mode is None: enrolled_options = dict(opts)
                client = Native(lib, opts) if mode is None else Typescript(hub, label, opts, mode)
                clients.append(client)
                result = await client.open()
                identity = json.loads(pathlib.Path(opts["identityPath"]).read_text())
                check(identity["principal"] == PRINCIPAL and identity["nats_urls"] == [hub.leaf_url, hub.url] and identity["nats_ws_urls"] == [hub.leaf_ws_url, hub.ws_url],
                      f"{label}: enrollment persists leaf-first native and WSS lists under the same scoped identity")
                check(identity["nats_url"] == hub.leaf_url and identity["nats_ws_url"] == hub.leaf_ws_url,
                      f"{label}: singular fields retain the leaf for deployed clients")
                check(hub.devices_at_leaf() == 1, f"{label}: the real client initially connects to the assigned leaf")
                renewals = hub.requests.count("/renew")
                if mode:
                    await client.command(cmd="renew")
                else:
                    await client.close()
                    identity["clock_offset"] = 61  # native's normal stale-clock renewal path
                    pathlib.Path(opts["identityPath"]).write_text(json.dumps(identity))
                    await client.open()
                renewed = json.loads(pathlib.Path(opts["identityPath"]).read_text())
                check(hub.requests.count("/renew") > renewals and renewed["nats_urls"] == identity["nats_urls"] and renewed["nats_ws_urls"] == identity["nats_ws_urls"],
                      f"{label}: signed renewal refreshes and durably preserves both endpoint lists")
                if mode:
                    expected = [hub.leaf_ws_url, hub.ws_url] if mode == "ws" else [hub.leaf_url, hub.url]
                    check(result["dial"] == {"servers": expected, "noRandomize": True, "ignoreClusterUpdates": True}, f"{label}: the real dial uses exactly the ordered enrolled list")
                    before = result["pool"]
                    result = await client.command(cmd="info")
                    check(result["pool"] == before and result["implicit"] == 0, f"{label}: asynchronous INFO cannot add an advertised server")
                # The TCP connection actually dies. Outbox IDs survive the ensuing
                # period with both endpoints unavailable and a full client restart.
                hub.halt("leaf")
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    state = await client.pump()
                    if hub.monitor()["num_connections"] >= 3 and (not mode or state["connected"] and str(hub.hub_ws if mode == "ws" else hub.hub_port) in state["server"]): break
                    await asyncio.sleep(.1)
                key = str(uuid.uuid4())
                await client.write(key, 11)
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    state = await client.pump()
                    if not state["outbox"] and hub.executions("s2_row", key) == 1: break
                    await asyncio.sleep(.1)
                check(not state["outbox"] and hub.executions("s2_row", key) == 1, f"{label}: leaf loss reconnects to the hub and a write commits once")
                # Keep the same handle alive through a complete outage. Only the
                # normal host poll / TS event loop may replay the queued write.
                hub.halt("nats")
                recovery_key = str(uuid.uuid4())
                recovery_queued = (await client.write(recovery_key, 17))["outbox"]
                await hub.broker("nats", hub.hub_conf)
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    state = await client.pump()
                    if not state["outbox"] and hub.executions("s2_row", recovery_key) == 1: break
                    await asyncio.sleep(.1)
                recovery_id = recovery_queued[0]["msg_id"] if len(recovery_queued) == 1 else ""
                recorded = hub.sql(f"SELECT count(*) FROM zebridge_private.receipts WHERE principal='{PRINCIPAL}' AND message_id='{recovery_id}'")
                check(len(recovery_queued) == 1 and not state["outbox"] and recorded == "1" and hub.executions("s2_row", recovery_key) == 1,
                      f"{label}: the normal event loop replays the same ID once after both endpoints recover, without an explicit flush")
                hub.halt("nats")
                offline_key = str(uuid.uuid4())
                state = await client.write(offline_key, 22)
                queued = state["outbox"]
                check(len(queued) == 1 and queued[0]["msg_id"].startswith("zb1-t"), f"{label}: both endpoints down leaves the write durably queued")
                await client.close()
                with sqlite3.connect(opts["dbPath"]) as replica:
                    persisted = [{"msg_id": r[0], "payload": r[1]} for r in replica.execute("SELECT msg_id,hex(payload) FROM _zebridge_outbox ORDER BY msg_id")]
                check(persisted == queued, f"{label}: closing preserves the exact pending ID and payload bytes")
                await hub.broker("nats", hub.hub_conf)
                if getattr(hub, "proxy", None): await asyncio.to_thread(hub.close_http)
                requests = len(hub.requests)
                import datetime
                opened_at = datetime.datetime.now(datetime.timezone.utc)
                await client.open()
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    state = await client.pump()
                    if not state["outbox"] and hub.executions("s2_row", offline_key) == 1: break
                    await asyncio.sleep(.1)
                receipt = hub.sql(f"SELECT count(*) FROM zebridge_private.receipts WHERE principal='{PRINCIPAL}' AND message_id='{queued[0]['msg_id'] if queued else ''}'")
                check(not state["outbox"] and hub.executions("s2_row", offline_key) == 1 and receipt == "1" and len(hub.requests) == requests,
                      f"{label}: reopening with HTTP unavailable falls back to the hub and settles the same ID exactly once")
                check(any(r["id"] == offline_key and r["v"] == 22 for r in state["rows"]), f"{label}: the local replica converges after failover")
                await hub.broker("leaf", hub.leaf_conf)
                await hub.linked()
                await asyncio.sleep(.3)
                check(hub.devices_at_leaf() == 0, f"{label}: leaf recovery does not move a healthy hub connection")
                # s2_row is public, the principal's tenant is acme: one tail, on CDC_PUBLIC,
                # and none on CDC_acme (no followed table rides it).
                tails = await hub.cdc_consumers_since(opened_at)
                check(tails == {"CDC_PUBLIC": 1}, f"{label}: one tail consumer per followed stream through failover and leaf recovery ({tails})")
                await client.close()
                override_opts = dict(opts, natsUrl=hub.ws_url if mode == "ws" else hub.url)
                override = Native(lib, override_opts) if mode is None else Typescript(hub, label + "-override", override_opts, mode)
                clients.append(override)
                result = await override.open()
                check(hub.devices_at_leaf() == 0 and (not mode or len(result["pool"]) == 1),
                      f"{label}: an explicit application override replaces the enrolled list")
                await override.close()
            # A trusted CA is insufficient: each URL's own name must match its
            # certificate. The bad broker is otherwise a working hub-connected leaf.
            bad_tcp, bad_ws, bad_http = port(), port(), port()
            bad_conf = hub.tmp / "bad.conf"
            remote = f'\nleafnodes {{ remotes: [{{ url: "nats://127.0.0.1:{hub.link_port}", credentials: "{hub.tmp}/link.creds", account: "{hub.account}" }}] }}\n'
            bad_conf.write_text(hub.conf("bad", bad_tcp, bad_ws, bad_http) + remote)
            await hub.broker("bad", bad_conf)
            for _ in range(150):
                if json.load(urllib.request.urlopen(f"http://127.0.0.1:{bad_http}/leafz")).get("leafnodes", 0): break
                await asyncio.sleep(.1)
            check(json.load(urllib.request.urlopen(f"http://127.0.0.1:{bad_http}/leafz")).get("leafnodes", 0) == 1,
                  "wrong-certificate fixture has a working hub link")
            for label, mode in (("native", None), ("ts-tcp", "tcp"), ("ts-ws", "ws")):
                base = dict(enrolled_options)
                base.update(dbPath=str(hub.tmp / f"bad-{label}.sqlite3"), natsUrl=f"{'wss' if mode == 'ws' else 'tls'}://localhost:{bad_ws if mode == 'ws' else bad_tcp}")
                client = Native(lib, base) if mode is None else Typescript(hub, "bad-" + label, base, mode)
                clients.append(client)
                refused = False
                try: await client.open()
                except Exception: refused = True
                finally: await client.close()
                check(refused, f"{label}: a trusted certificate for the wrong URL name is refused")
            # Same broker, account, routes and ports; only its certificate changes.
            bad_conf.write_text(hub.conf("bad", bad_tcp, bad_ws, bad_http, cert="hub") + remote)
            hub.procs["bad"].send_signal(signal.SIGHUP)
            await asyncio.sleep(.3)
            for label, mode in (("native", None), ("ts-tcp", "tcp"), ("ts-ws", "ws")):
                base = dict(enrolled_options)
                base.update(dbPath=str(hub.tmp / f"good-{label}.sqlite3"), natsUrl=f"{'wss' if mode == 'ws' else 'tls'}://localhost:{bad_ws if mode == 'ws' else bad_tcp}")
                client = Native(lib, base) if mode is None else Typescript(hub, "good-" + label, base, mode)
                clients.append(client)
                await client.open()
                check(True, f"{label}: the same broker works once its certificate matches the URL")
                await client.close()
            for label, mode in (("native", None), ("ts-tcp", "tcp"), ("ts-ws", "ws")):
                base = dict(enrolled_options)
                base.update(dbPath=str(hub.tmp / f"ip-{label}.sqlite3"), natsUrl=f"{'wss' if mode == 'ws' else 'tls'}://127.0.0.1:{bad_ws if mode == 'ws' else bad_tcp}")
                client = Native(lib, base) if mode is None else Typescript(hub, "ip-" + label, base, mode)
                clients.append(client)
                refused = False
                try: await client.open()
                except Exception: refused = True
                finally: await client.close()
                check(refused, f"{label}: an IP URL refuses the localhost certificate even though it is valid for another endpoint")
            if os.environ.get("ZB_CHECK_LEAKS") == "1":
                report = subprocess.run(["leaks", "--nocontext", str(os.getpid())], capture_output=True, text=True, timeout=45)
                check(report.returncode == 0 and "0 leaks for 0 total leaked bytes" in report.stdout, "native clients: macOS leaks reports zero leaked bytes")
        except Exception:
            for label in hub.logs:
                print(label + ":\n" + hub.log(label)[-1800:])
            raise
        finally:
            for client in reversed(clients): await client.close()
            hub.stop()
    print(f"\nendpoint failover: {passed} passed, {failed} failed")
    return int(failed > 0)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

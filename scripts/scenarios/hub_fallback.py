#!/usr/bin/env python3
"""Hub fallback: mirrored seeds, durable retries and ordered endpoint failover.

Stage 1 uses disposable NATS; stages 2 and 3 also use their own PostgreSQL databases.
No shared dev database or VPS is touched.
"""
import asyncio
import copy
import json
import os
import pathlib
import socket
import subprocess
import tempfile
import time

import msgpack
import nats
import nkeys
from nats.js.api import ExternalStream, KeyValueConfig, ObjectMeta, ObjectMetaOptions, StreamConfig, StreamSource

import zb
from rotate_client_key import client_creds

BRIDGE = pathlib.Path(os.environ.get("ZB_BRIDGE_BIN", zb.ROOT / "zig-out/bin/bridge"))
GEN = zb.TOPOLOGY["generations"]["kv"]
BUCKET = zb.TOPOLOGY["generations"]["bucket_prefix"] + "acme"
TABLE, KEY = "seed_probe", "acme.seed_probe"


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def main():
    passed = failed = 0
    processes, connections, logs = [], [], []

    def check(condition, label):
        nonlocal passed, failed
        (zb.ok if condition else zb.bad)(label)
        passed += bool(condition)
        failed += not condition

    async def until(predicate, label):
        end = time.monotonic() + 10
        while time.monotonic() < end:
            if await predicate():
                return
            await asyncio.sleep(.05)
        raise RuntimeError(label)

    with tempfile.TemporaryDirectory(prefix="zb-hub-fallback-") as folder:
        tmp = pathlib.Path(folder)
        try:
            subprocess.run([str(BRIDGE), "--init-nats", "operator", "--dir", str(tmp / "identity"),
                            "--js-domain", "hub"], capture_output=True, text=True, check=True)
            store = tmp / "identity/operator.store"
            keys = dict(line.split("=", 1) for line in store.read_text().splitlines()
                        if "=" in line and not line.startswith("#"))
            account = nkeys.from_seed(keys["ACCOUNT_SEED"].encode()).public_key.decode()
            for name, flag in (("link", "--mirror"), ("keeper", "--keeper")):
                result = subprocess.run([str(BRIDGE), "--mint-leaf", "--name", "lf-edge1", "--tenant", "acme",
                                         flag, "--store", str(store)], capture_output=True, text=True, check=True)
                (tmp / f"{name}.creds").write_text(result.stdout)
                os.chmod(tmp / f"{name}.creds", 0o600)
            device = tmp / "device.creds"
            device.write_text(client_creds(keys["SK_CLIENT_SEED"], account, "lf-edge1.alice", "acme"))
            os.chmod(device, 0o600)
            original = (tmp / "identity/nats-server.conf").read_text()
            trust = original[:original.index("\nport:")]
            hub_port, leaf_port, link_port = port(), port(), port()
            urls = {"hub": f"nats://127.0.0.1:{hub_port}", "leaf": f"nats://127.0.0.1:{leaf_port}"}
            for name, tcp in (("hub", hub_port), ("leaf", leaf_port)):
                link = (f'leafnodes {{ listen: "127.0.0.1:{link_port}" }}' if name == "hub" else
                        f'leafnodes {{ remotes: [{{ url: "nats://127.0.0.1:{link_port}", '
                        f'credentials: "{tmp}/link.creds", account: "{account}" }}] }}')
                conf = tmp / f"{name}.conf"
                conf.write_text(trust + f'\nhost: "127.0.0.1"\nport: {tcp}\nmax_control_line: 131072\n'
                                'feature_flags { js_api_reply_v2: true }\n'
                                f'jetstream {{ domain: {name}, store_dir: "{tmp}/{name}-data" }}\n{link}\n')
                log = open(tmp / f"{name}.log", "w+")
                logs.append(log)
                proc = subprocess.Popen(["nats-server", "-c", str(conf)], stdout=log, stderr=log)
                processes.append(proc)
                for _ in range(100):
                    if proc.poll() is not None:
                        raise RuntimeError(conf.with_suffix(".log").read_text())
                    if "Server is ready" in pathlib.Path(log.name).read_text():
                        break
                    await asyncio.sleep(.05)
                else:
                    raise RuntimeError(f"{name} did not start")
            admin = tmp / "identity/creds/bridge.creds"
            hub = await nats.connect(urls["hub"], user_credentials=str(admin))
            leaf = await nats.connect(urls["leaf"], user_credentials=str(admin))
            keeper = await nats.connect(urls["leaf"], user_credentials=str(tmp / "keeper.creds"),
                                        inbox_prefix="$ZB.KEEPER.lf-edge1")
            connections.extend((hub, leaf, keeper))
            hj, lj, kj = hub.jetstream(domain="hub"), leaf.jetstream(domain="leaf"), keeper.jetstream(domain="leaf")
            for bucket in (zb.TOPOLOGY["kv"]["schemas"], zb.TOPOLOGY["kv"]["tenants"], GEN):
                await hj.create_key_value(KeyValueConfig(bucket=bucket, history=1, storage="memory", direct=True))
            roster = await hj.key_value(zb.TOPOLOGY["kv"]["tenants"])
            await roster.put("lf-edge1.alice", b'["acme"]')   # the roster is a JSON array, as the bridge writes it
            schemas = await hj.key_value(zb.TOPOLOGY["kv"]["schemas"])
            columns = [{"name": "uid", "type": "TEXT"}, {"name": "n", "type": "INTEGER"},
                       {"name": "body", "type": "TEXT"}, {"name": "updated_at", "type": "TEXT"},
                       {"name": "tenant_id", "type": "TEXT"}]
            await schemas.put(TABLE, json.dumps({"pk_columns": ["uid"], "tenant_column": "tenant_id",
                "version_column": "updated_at", "seed_epoch": 1, "lsn": 1,
                "sqlite": {"columns": columns}}).encode())
            await hj.add_stream(StreamConfig(name="CDC_PUBLIC", subjects=[f"cdc.{TABLE}.>", "cdc._default.>"], storage="memory"))
            await hj.add_stream(StreamConfig(name="VERDICTS", subjects=["mutation_ack.>"], storage="memory", allow_direct=True))
            # Real object-store metadata, digest and multiple chunks, then a real mirror.
            objects = await hj.create_object_store(bucket=BUCKET)
            rows = [[f"{i:04d}", i, "x" * 160, "2026-10-20T00:00:00.000000Z", "acme"] for i in range(2048)]
            document = {"columns": [c["name"] for c in columns], "rows": rows,
                        "version_column": "updated_at", "gen": 1, "kind": "full", "cutoff": "2026-10-20T00:00:00.000000Z"}
            obj = await objects.put("seed-full", msgpack.packb(document),
                meta=ObjectMeta(options=ObjectMetaOptions(max_chunk_size=65536)))
            check(obj.chunks > 1, "fixture full is a real object with multiple chunks and a digest")
            # Use the production projection preparation, then the keeper's scoped grants.
            prepared = await asyncio.create_subprocess_exec(str(BRIDGE), "--mutation-ingress", "--prepare-projections",
                env=dict(os.environ, NATS_URL=urls["hub"], NATS_CREDS=str(admin), NATS_JS_DOMAIN="hub",
                         NATS_TLS_CA="", ZB_LEAF_TENANTS="acme"), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                _, err = await asyncio.wait_for(prepared.communicate(), timeout=30)
            except asyncio.TimeoutError:
                prepared.kill()
                await prepared.communicate()
                raise RuntimeError("hub manifest projection preparation timed out")
            if prepared.returncode:
                raise RuntimeError(err.decode()[-2000:])
            for stream in ("OBJ_" + BUCKET,):
                await kj.add_stream(StreamConfig(name=stream, storage="memory", allow_direct=True, mirror_direct=True,
                    mirror=StreamSource(name=stream, external=ExternalStream(api="$JS.hub.API", deliver="$ZB.MIRROR.lf-edge1"))))
            await kj.add_stream(StreamConfig(name="KV_" + GEN, subjects=[f"$KV.{GEN}.>"], storage="memory",
                allow_direct=True, max_msgs_per_subject=1,
                sources=[StreamSource(name="KV_generations_leaf_" + tenant,
                    external=ExternalStream(api="$JS.hub.API", deliver="$ZB.MIRROR.lf-edge1")) for tenant in ("acme", "_default")]))
            async def copied():
                a, b = await hj.stream_info("OBJ_" + BUCKET), await lj.stream_info("OBJ_" + BUCKET)
                return a.state.messages == b.state.messages and b.state.messages > 1
            try:
                await until(copied, "object mirror did not catch up")
            except RuntimeError:
                print("object mirror:", (await lj.stream_info("OBJ_" + BUCKET)).as_dict())
                raise
            check(True, "loopback keeper's real object mirror catches up through the scoped leaf link")
            local_manifests, hub_manifests = await lj.key_value(GEN), await hj.key_value(GEN)
            base = {"gen": 1, "bucket": BUCKET, "seed_epoch": 1, "version_column": "updated_at",
                    "full": {"object": "seed-full", "cutoff": document["cutoff"], "gen": 1}, "deltas": [],
                    "cutoff_version": document["cutoff"], "cutoff_lsn": "0/1", "cdc_stream": "CDC_acme", "cutoff_seq": 1}

            async def cdc_ready(msg):
                data = {c["name"]: v for c, v in zip(columns, rows[0])}
                data.update(n=7, updated_at="2026-10-20T00:00:01.000000Z")
                await hj.publish(f"cdc.acme.{TABLE}.update", msgpack.packb({"table": TABLE, "operation": "UPDATE", "data": data, "lsn": 2}))
                await msg.respond(b'{"ok":true}')
            await hub.subscribe("query.acme.seed_probe_ready", cb=cdc_ready)
            await hub.flush()

            cases = [("local-buffered", "leaf", "buffer", "healthy"),
                     ("local-streaming", "leaf", "stream", "healthy"),
                     ("ordinary-hub", "-", "buffer", "healthy"),
                     ("same-domain", "hub", "buffer", "healthy"),
                     ("missing-domain", "absent", "buffer", "healthy"),
                     ("missing-manifest", "leaf", "buffer", "missing"),
                     ("stale-epoch", "leaf", "buffer", "epoch"),
                     ("stale-cut", "leaf", "stream", "cut"),
                     ("missing-object-buffered", "leaf", "buffer", "object"),
                     ("missing-object-streaming", "leaf", "stream", "object"),
                     ("partial-chain-buffered", "leaf", "buffer", "delta"),
                     ("partial-chain-streaming", "leaf", "stream", "delta")]
            for label, domain, mode, damage in cases:
                try:
                    await hj.delete_stream("CDC_acme")
                except nats.js.errors.NotFoundError:
                    pass
                await hj.add_stream(StreamConfig(name="CDC_acme", subjects=["cdc.acme.>"], storage="memory"))
                # Keep sequence 1 at the seed boundary, with no row outside the full.
                await hj.publish("cdc.acme.unfollowed.insert", msgpack.packb({"operation": "INSERT", "table": "unfollowed", "data": {}, "lsn": 1}))
                manifest = copy.deepcopy(base)
                # A unique revision lets us wait for this iteration's projection.
                manifest["probe"] = label
                await hub_manifests.put(KEY, json.dumps(manifest).encode())
                async def manifest_copied():
                    try:
                        return json.loads((await local_manifests.get(KEY)).value).get("probe") == label
                    except nats.js.errors.KeyNotFoundError:
                        return False
                await until(manifest_copied, "manifest projection did not catch up")
                if damage == "missing":
                    await local_manifests.delete(KEY)
                elif damage != "healthy":
                    broken = copy.deepcopy(manifest)
                    if damage == "epoch":
                        broken["seed_epoch"] = 0
                    elif damage == "cut":
                        # Make the local cut unspliceable while the hub's remains valid.
                        broken["cutoff_seq"] = 0
                        # Prune only the boundary message; next seq remains 2.
                        await hj.delete_msg("CDC_acme", 1)
                        # Hub cut 1 is valid when first_seq is 2.
                    elif damage == "object":
                        broken["full"]["object"] = "not-mirrored-yet"
                    elif damage == "delta":
                        broken["deltas"] = [{"gen": 2, "object": "not-mirrored-yet",
                            "prev_cutoff": document["cutoff"], "cutoff": "2026-10-20T00:00:01.000000Z"}]
                    await local_manifests.put(KEY, json.dumps(broken).encode())
                child = await asyncio.create_subprocess_exec("node", "--experimental-strip-types",
                    str(zb.ROOT / "scripts/scenarios/hub_fallback.mts"), urls["leaf"], str(device),
                    str(tmp / (label + ".sqlite3")), domain, mode,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                try:
                    out, err = await asyncio.wait_for(child.communicate(), timeout=60)
                except asyncio.TimeoutError:
                    child.kill()
                    await child.communicate()
                    raise RuntimeError(f"{label}: TS client timed out")
                if child.returncode:
                    raise RuntimeError(f"{label}: {err.decode()[-2500:]}\n{out.decode()[-1500:]}")
                result = json.loads(out.decode().splitlines()[-1])
                calls = result["calls"]
                local = [c for c in calls if c["domain"] == "leaf"]
                seed_kinds = lambda d: {(c["kind"], c["name"]) for c in calls if c["domain"] == d}
                check(result["seeded"] == 2048 and result["cdc"] == 7, f"{label}: exact seed content, persisted gate and live hub CDC")
                check(result["streamedBeforeComplete"] == (mode == "stream"), f"{label}: {'streamed rows before complete download' if mode == 'stream' else 'buffered seed path'}")
                if domain == "leaf" and damage == "healthy":
                    check({("kv", GEN), ("object", BUCKET), ("consumer", "OBJ_" + BUCKET)} <= seed_kinds("leaf")
                          and ("object", BUCKET) not in seed_kinds("hub"), f"{label}: manifest, metadata and chunks use the leaf without hub seed reads")
                else:
                    check({("kv", GEN), ("object", BUCKET), ("consumer", "OBJ_" + BUCKET)} <= seed_kinds("hub"),
                          f"{label}: usable hub chain supplies manifest, metadata and chunks")
                check(all(c["kind"] != "manager" and (c["kind"], c["name"]) in {
                    ("kv", GEN), ("object", BUCKET), ("consumer", "OBJ_" + BUCKET)} for c in local),
                    f"{label}: seedDomain does not redirect tenant membership, schemas or CDC checks")
                check({("kv", zb.TOPOLOGY["kv"]["tenants"]), ("consumer", "KV_" + zb.TOPOLOGY["kv"]["schemas"]),
                       ("consumer", "CDC_acme"), ("consumer", "CDC_PUBLIC"), ("manager", "")} <= seed_kinds("hub"),
                      f"{label}: hub domain serves membership, schema watch, CDC consumers and splice checks")
        except Exception:
            for log in logs:
                print(pathlib.Path(log.name).name + ":\n" + pathlib.Path(log.name).read_text()[-1800:])
            raise
        finally:
            for conn in reversed(connections):
                await conn.close()
            for proc in reversed(processes):
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
            for log in logs:
                log.close()
    print(f"\n{passed} passed, {failed} failed")
    return int(failed > 0)


if __name__ == "__main__":
    # Each stage owns and tears down its fixtures, independently of the others.
    stages = os.environ.get("ZB_HUB_FALLBACK_STAGES", "1 2 3").split()
    code = asyncio.run(main()) if "1" in stages else 0
    if "2" in stages:
        import hub_fallback_stage2
        code |= asyncio.run(hub_fallback_stage2.main())
    if "3" in stages:
        import hub_fallback_endpoints
        code |= asyncio.run(hub_fallback_endpoints.main())
    raise SystemExit(code)

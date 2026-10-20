#!/usr/bin/env python3
"""Admission quotas and durable PubAcks on a disposable local NATS, without PostgreSQL.

Run with ZB_CHECK_LEAKS=1 on macOS to sample the gateway's malloc allocations too.
"""
import asyncio
import importlib.util
import json
import os
import pathlib
import re
import socket
import subprocess
import tempfile
import time

import nats
import nkeys
import zb
from rotate_client_key import client_creds as mint_client

BRIDGE = pathlib.Path(os.environ.get("ZB_BRIDGE_BIN", zb.ROOT / "zig-out/bin/bridge"))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def client_creds(folder, who, store):
    # The real scoped client signer and template, as used by the rotation scenario.
    account = nkeys.from_seed(store["ACCOUNT_SEED"].encode())
    path = folder / f"{who}.creds"
    path.write_text(mint_client(store["SK_CLIENT_SEED"], account.public_key.decode(), who, "acme"))
    path.chmod(0o600)
    return path


async def main():
    failed = 0
    def check(ok, label):
        nonlocal failed
        (zb.ok if ok else zb.bad)(label)
        failed += not ok

    with tempfile.TemporaryDirectory(prefix="zb-admission-") as folder:
        tmp = pathlib.Path(folder)
        ports = [free_port() for _ in range(3)]
        generated = subprocess.run([str(BRIDGE), "--init-nats", "operator", "--dir", str(tmp / "hub"),
                                   "--port", str(ports[0]), "--http-port", str(ports[1]),
                                   "--ws-port", str(ports[2])], capture_output=True, text=True)
        assert generated.returncode == 0, generated.stderr
        store = dict(line.split("=", 1) for line in (tmp / "hub/operator.store").read_text().splitlines()
                     if "=" in line and not line.startswith("#"))
        creds = {p: client_creds(tmp, p, store) for p in ("alice", "bob")}
        url = f"nats://127.0.0.1:{ports[0]}"
        env = dict(os.environ, NATS_URL=url, NATS_CREDS=str(tmp / "hub/creds/bridge.creds"),
                   NATS_JS_DOMAIN="", NATS_TLS_CA="", MUTATION_MAX_MESSAGE_BYTES="4096",
                   MUTATION_BACKLOG_PER_PRINCIPAL="2", MUTATION_BACKLOG_BYTES_PER_PRINCIPAL="9216",
                   MUTATION_STREAM_MAX_BYTES=str(1 << 20))
        server = gateway = None
        clients = []
        logs = []
        def start_gateway():
            path = tmp / f"gateway{len(logs)}.log"
            logs.append(path)
            output = open(path, "w")
            try:
                process = subprocess.Popen([str(BRIDGE), "--mutation-ingress"], env=env,
                                           stdout=subprocess.DEVNULL, stderr=output)
            finally:
                output.close()
            return process, path

        async def ready(process, path):
            for _ in range(100):
                if "mutation admission ready" in path.read_text():
                    return
                if process.poll() is not None:
                    raise RuntimeError(path.read_text()[-1500:])
                await asyncio.sleep(.1)
            raise RuntimeError("admission startup timed out")

        try:
            config = tmp / "hub/nats-server.conf"
            with config.open("a") as f:
                f.write('\nhost: "127.0.0.1"\n')
            with open(tmp / "nats.log", "w") as output:
                server = subprocess.Popen(["nats-server", "-c", str(config)], stdout=output, stderr=output)
            for _ in range(100):
                try:
                    reader, writer = await asyncio.open_connection("127.0.0.1", ports[0])
                    writer.close()
                    await writer.wait_closed()
                    break
                except OSError:
                    await asyncio.sleep(.1)
            gateway, path = start_gateway()
            await ready(gateway, path)
            admin = await nats.connect(url, user_credentials=env["NATS_CREDS"])
            clients.append(admin)
            js = admin.jetstream()
            nc = {}
            errors = []
            async def on_error(error):
                errors.append(str(error))
            for who in creds:
                nc[who] = await nats.connect(url, user_credentials=str(creds[who]),
                                           inbox_prefix=f"_INBOX.{who}".encode(), error_cb=on_error)
                clients.append(nc[who])
                await nc[who].flush()
            alice, bob = nc["alice"].jetstream(), nc["bob"].jetstream()
            a = await alice.publish("mutation.alice.one.insert", b"{}", headers={"Nats-Msg-Id": "same-id",
                                    "ZB-Original-Subject": "mutation.bob.secrets.delete", "ZB-Leaf-Tenants": '["other"]'})
            await alice.publish("mutation.alice.two.delete", b"{}", headers={"Nats-Msg-Id": "second"})
            try:
                await alice.publish("mutation.alice.arbitrary.update", b"{}", headers={"Nats-Msg-Id": "third"})
                rejected = False
            except nats.js.errors.APIError:
                rejected = True
            check(rejected and (await js.stream_info("MUTATIONS")).state.messages == 2,
                  "changing table or operation cannot bypass a principal's aggregate backlog cap")
            stored = await js.get_msg("MUTATIONS", seq=a.seq)
            check(stored.subject == "$ZB.mutation.alice" and
                  stored.headers.get("ZB-Original-Subject") == "mutation.alice.one.insert" and
                  "ZB-Leaf-Tenants" not in stored.headers,
                  "admission rebuilds the trusted route and discards forged scope headers")
            b = await bob.publish("mutation.bob.one.insert", b"{}", headers={"Nats-Msg-Id": "same-id"})
            check(not b.duplicate and b.seq != a.seq, "another principal retains capacity and has its own deduplication namespace")
            retry = await alice.publish("mutation.alice.one.insert", b"{}", headers={"Nats-Msg-Id": "same-id"})
            check(retry.duplicate and retry.seq == a.seq, "a stored write's retry is acknowledged even when its principal's queue is full")
            await nc["alice"].publish("$ZB.mutation.bob", b"{}")
            await nc["alice"].flush()
            await asyncio.sleep(.2)
            check(any("permissions violation" in e.lower() for e in errors) and
                  (await js.stream_info("MUTATIONS")).state.messages == 3,
                  "an ordinary client cannot publish directly to the private queue")
            for i in range(100):
                await nc["alice"].publish(f"mutation.alice.bad.insert.extra{i}", b"{}")
            await nc["alice"].flush()
            await asyncio.sleep(.2)
            check((await js.stream_info("MUTATIONS")).state.messages == 3,
                  "malformed public subjects never occupy durable queue capacity")
            if os.environ.get("ZB_CHECK_LEAKS") == "1":
                report = subprocess.run(["leaks", "--nocontext", str(gateway.pid)], capture_output=True, text=True, timeout=45)
                clean = re.search(r'\b0 leaks for 0 total leaked bytes', report.stdout)
                check(bool(clean), "macOS leaks: zero leaked bytes after admission traffic")
            gateway.terminate()
            gateway.wait(timeout=15)
            check(gateway.returncode == 0, "admission service shuts down cleanly")
            gateway, path = start_gateway()
            await ready(gateway, path)
            retry = await alice.publish("mutation.alice.one.insert", b"{}", headers={"Nats-Msg-Id": "same-id"})
            check(retry.duplicate and retry.seq == a.seq, "restart preserves the durable queue and idempotent retry acknowledgement")
            await js.delete_stream("MUTATIONS")  # Disposable fixture: simulate missing storage.
            try:
                await bob.publish("mutation.bob.one.insert", b"{}", headers={"Nats-Msg-Id": "missing-store"}, timeout=1)
                false_ack = True
            except (nats.errors.NoRespondersError, nats.errors.TimeoutError):
                false_ack = False
            check(not false_ack, "loss of the storage stream never produces a successful PubAck")
            gateway.terminate()
            gateway.wait(timeout=15)
            await js.add_stream(name="MUTATIONS", subjects=["mutation.>"], retention="workqueue",
                                discard="new", max_bytes=1 << 20, max_msgs_per_subject=5000)
            gateway, path = start_gateway()
            gateway.wait(timeout=15)
            check(gateway.returncode != 0 and "LegacyMutationStreamNeedsMigration" in path.read_text(),
                  "a legacy public stream is refused without modifying or deleting it")
            spec = importlib.util.spec_from_file_location("migrate_mutations", zb.ROOT / "deploy/migrate-mutations.py")
            migration = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(migration)
            plan = subprocess.run([str(BRIDGE), "--mutation-ingress", "--print-config"], env=env,
                                  capture_output=True, text=True, check=True)
            wanted = json.loads(plan.stdout)
            before = (await js.stream_info("MUTATIONS")).config.as_dict()
            await migration.migrate(admin, wanted)
            check((await js.stream_info("MUTATIONS")).config.as_dict() == before,
                  "migration plan makes no stream changes")
            await js.publish("mutation.alice.one.insert", b"{}")
            try:
                await migration.migrate(admin, wanted, apply=True)
                refused_migration = False
            except RuntimeError:
                refused_migration = True
            check(refused_migration and (await js.stream_info("MUTATIONS")).state.messages == 1 and
                  (await js.stream_info("MUTATIONS")).config.as_dict() == before,
                  "non-empty legacy queue is refused without altering configuration or accepted writes")
            consumer = await js.pull_subscribe("mutation.>", durable="bridge_mutations_worker", stream="MUTATIONS")
            for message in await consumer.fetch(1, timeout=2):
                await message.ack_sync()
            await migration.migrate(admin, wanted, apply=True)
            after = await js.stream_info("MUTATIONS")
            check(after.state.messages == 0 and after.config.subjects == ["$ZB.mutation.>"] and
                  after.config.max_msgs_per_subject == 2,
                  "empty-queue cutover keeps the stream and installs the reviewed private quota")
            await migration.migrate(admin, wanted, apply=True)
            check((await js.stream_info("MUTATIONS")).config.max_age == 0,
                  "accepted writes have no age expiry; bounded capacity refuses new writes instead")
            backup_spec = importlib.util.spec_from_file_location("hub_backup", zb.ROOT / "deploy/backup-hub.py")
            backup = importlib.util.module_from_spec(backup_spec)
            backup_spec.loader.exec_module(backup)
            original_capture = (await js.stream_info("MUTATIONS")).config.as_dict()
            never_dump = lambda _path: (_ for _ in ()).throw(AssertionError("dump should not run"))
            await backup.checkpoint(admin, tmp / "plan", config, env["NATS_CREDS"], never_dump, never_dump)
            check(not (tmp / "plan").exists() and (await js.stream_info("MUTATIONS")).config.as_dict() == original_capture,
                  "the recovery-checkpoint plan changes neither storage nor files")
            await js.publish("$ZB.mutation.alice", b"accepted-before-checkpoint")
            worker = await js.pull_subscribe("$ZB.mutation.>", durable="backup_guard", stream="MUTATIONS")
            try:
                await backup.checkpoint(admin, tmp / "not-drained", config, env["NATS_CREDS"], never_dump, never_dump, apply=True, timeout=.2)
                drain_refused = False
            except RuntimeError:
                drain_refused = True
            check(drain_refused and (await js.stream_info("MUTATIONS")).state.messages == 1 and
                  (await js.stream_info("MUTATIONS")).config.as_dict() == original_capture,
                  "an undrained recovery checkpoint preserves accepted writes and resumes capture")
            for message in await worker.fetch(1, timeout=2):
                await message.ack_sync()
            def failed_dump(_path):
                raise RuntimeError("injected dump failure")
            try:
                await backup.checkpoint(admin, tmp / "failed-dump", config, env["NATS_CREDS"], failed_dump, never_dump, apply=True)
                dump_failed = False
            except RuntimeError:
                dump_failed = True
            check(dump_failed and (await js.stream_info("MUTATIONS")).config.as_dict() == original_capture and
                  not (tmp / "failed-dump/manifest.json").exists(),
                  "a failed PostgreSQL dump resumes capture without labelling a checkpoint complete")
        except Exception:
            print((tmp / "nats.log").read_text(errors="replace")[-1500:])
            raise
        finally:
            for client in clients:
                await client.close()
            if gateway and gateway.poll() is None:
                gateway.terminate()
                gateway.wait(timeout=15)
            if server:
                server.terminate()
                server.wait(timeout=15)
            for path in logs:
                text = path.read_text(errors="replace")
                check("error(DebugAllocator)" not in text and "panic:" not in text,
                      f"{path.name}: no allocator leaks or panics")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

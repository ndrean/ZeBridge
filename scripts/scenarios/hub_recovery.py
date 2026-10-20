#!/usr/bin/env python3
"""Real PostgreSQL checkpoint/restore and fresh-hub reseeding, on disposable local fixtures.

Creates two scratch databases and private NATS servers. A native client writes
through admission, a checkpoint drains it, and a replacement hub rebuilds from
pg_restore under the same NATS identity. The old replica must converge too.
"""
import asyncio
import ctypes
import importlib.util
import json
import os
import pathlib
import re
import socket
import subprocess
import tempfile
import time
import uuid
import urllib.parse
import urllib.request

import nats
import nkeys
import zb
from rotate_client_key import client_creds as mint_client

ROOT = zb.ROOT
BRIDGE = pathlib.Path(os.environ.get("ZB_BRIDGE_BIN", ROOT / "zig-out/bin/bridge"))
LIBRARY = pathlib.Path(os.environ.get("ZB_LIBZB", ROOT / "libzb/zig-out/lib" / ("libzbcore.dylib" if os.uname().sysname == "Darwin" else "libzbcore.so")))
spec = importlib.util.spec_from_file_location("checkpoint", ROOT / "deploy/backup-hub.py")
backup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backup)


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def main():
    ident = "zb_dr_" + uuid.uuid4().hex[:8]
    source, restored = ident, ident + "_restored"
    reader, writer = ident + "_r", ident + "_w"
    slots = [ident + "_source", ident + "_new"]
    psql = [zb._psql_binary(), "-X", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-Atq"]
    pgbin = pathlib.Path(zb._psql_binary()).parent
    failed = 0
    def sql(statement, db=source):
        r = subprocess.run([*psql, "-d", db], input=statement, capture_output=True, text=True)
        if r.returncode:
            raise RuntimeError(r.stderr[-1800:])
        return r.stdout.strip()
    def check(ok, label):
        nonlocal failed
        (zb.ok if ok else zb.bad)(label)
        failed += not ok
    def pg_env(database):
        return dict(os.environ, DATABASE_READER_URL=f"postgres://{reader}:fixture@127.0.0.1:5432/{database}",
                    DATABASE_WRITER_URL=f"postgres://{writer}:fixture@127.0.0.1:5432/{database}", TARGET_DB=database,
                    BRIDGE_CDC_PUBLICATION="recovery_pub")
    def render(database):
        return subprocess.run([str(BRIDGE), "--init-sql"], env=pg_env(database), capture_output=True, text=True, check=True).stdout
    lib = ctypes.CDLL(str(LIBRARY))
    lib.zb_client_connect.argtypes, lib.zb_client_connect.restype = [ctypes.c_char_p], ctypes.c_uint64
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    lib.zb_free.argtypes = [ctypes.c_void_p]
    for name, extra in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                        ("mutate", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + name)
        f.argtypes, f.restype = [ctypes.c_uint64] + extra, ctypes.c_void_p
    def take(pointer):
        try:
            return json.loads(ctypes.string_at(pointer))
        finally:
            lib.zb_free(pointer)
    def rows(handle):
        return take(lib.zb_client_query(handle, b"SELECT v FROM recovery_row ORDER BY v", b"[]")).get("rows", [])
    def pending(handle):
        return take(lib.zb_client_query(handle, b"SELECT count(*) FROM _zebridge_outbox", b"[]")).get("rows", [[-1]])[0][0]
    processes, logs, connections = [], [], []
    h = 0
    made = []
    with tempfile.TemporaryDirectory(prefix="zb-recovery-") as folder:
        tmp = pathlib.Path(folder)
        def spawn(args, env, name):
            log = tmp / (name + ".log")
            logs.append(log)
            with open(log, "w") as output:
                p = subprocess.Popen(args, env=env, stdout=output, stderr=output)
            processes.append(p)
            return p, log
        async def wait_text(process, log, text, timeout=40):
            end = time.monotonic() + timeout
            while time.monotonic() < end:
                if text in log.read_text():
                    return
                if process.poll() is not None:
                    raise RuntimeError(log.read_text()[-2000:])
                await asyncio.sleep(.1)
            raise RuntimeError(log.read_text()[-2000:])
        try:
            sql(f'CREATE DATABASE "{source}"', "postgres")
            made.append(source)
            sql(render(source))
            sql("CREATE TABLE recovery_row (id uuid PRIMARY KEY, v int NOT NULL, updated_at timestamptz NOT NULL DEFAULT now()); "
                "SELECT * FROM zebridge_enable('public.recovery_row', public_reason => 'recovery fixture', writable => true, "
                "version_col => 'updated_at', allow_physical_deletes => true, publication => 'recovery_pub', dry_run => false); "
                "INSERT INTO recovery_row VALUES ('00000000-0000-0000-0000-000000000001',10,now()); "
                "INSERT INTO zebridge_leaf_scopes(leaf_id,tenant_ids,nats_url,nats_ws_url) VALUES ('lf-edge1',ARRAY['_default'],'tls://leaf.example.org:4222','wss://leaf.example.org'); "
                "INSERT INTO zebridge_user_tenants VALUES ('lf-edge1.recovery_device','_default');")
            generated = subprocess.run([str(BRIDGE), "--init-nats", "operator", "--dir", str(tmp / "identity"), "--js-domain", "hub"], capture_output=True, text=True)
            assert generated.returncode == 0, generated.stderr
            keys = dict(line.split("=", 1) for line in (tmp / "identity/operator.store").read_text().splitlines() if "=" in line and not line.startswith("#"))
            account = nkeys.from_seed(keys["ACCOUNT_SEED"].encode()).public_key.decode()
            original = (tmp / "identity/nats-server.conf").read_text()
            trust = original[:original.index("\nport:")]
            creds = tmp / "identity/creds/bridge.creds"
            device = tmp / "device.creds"
            device.write_text(mint_client(keys["SK_CLIENT_SEED"], account, "lf-edge1.recovery_device", "_default"))
            async def start_hub(database, index, public_trust, credentials):
                tcp, http = port(), port()
                config = tmp / f"nats-{index}.conf"
                config.write_text(public_trust + f'\nhost: "127.0.0.1"\nport: {tcp}\njetstream {{ domain: hub, store_dir: "{tmp}/nats-data-{index}" }}\n')
                server, slog = spawn(["nats-server", "-c", str(config)], os.environ.copy(), f"nats-{index}")
                await wait_text(server, slog, "Server is ready")
                url = f"nats://127.0.0.1:{tcp}"
                env = dict(pg_env(database), NATS_URL=url, NATS_CREDS=str(credentials), NATS_JS_DOMAIN="hub", NATS_TLS_CA="",
                    MUTATION_ADMISSION="1", ZB_LEAF_NAMES="lf-edge1", GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="1", BASE_BUF="12",
                    ZB_SIGNING_SEED=keys["SK_CLIENT_SEED"], ZB_ACCOUNT_PUB=account, ZB_FEED_RESTART="1", NATS_ACCOUNT_CONF=str(config))
                gate, glog = spawn([str(BRIDGE), "--mutation-ingress"], env, f"admission-{index}")
                await wait_text(gate, glog, "mutation admission ready")
                bridge, blog = spawn([str(BRIDGE), "--pub", "recovery_pub", "--slot", slots[index], "--port", str(http)], env, f"bridge-{index}")
                nc = await nats.connect(url, user_credentials=str(credentials))
                connections.append(nc)
                js = nc.jetstream(domain="hub")
                end = time.monotonic() + 60
                while time.monotonic() < end:
                    try:
                        await js.get_msg("KV_generations", subject="$KV.generations._default.recovery_row", direct=True)
                        break
                    except (nats.js.errors.APIError, nats.errors.NoRespondersError, nats.errors.TimeoutError):
                        await asyncio.sleep(.2)
                    if bridge.poll() is not None:
                        raise RuntimeError(blog.read_text()[-2000:])
                else:
                    raise RuntimeError(blog.read_text()[-2000:])
                await wait_text(bridge, blog, "Ready! Pulling mutations", timeout=40)
                for bucket, key in (("schemas", "recovery_row"), ("tenants", "lf-edge1.recovery_device")):
                    for _ in range(100):
                        try:
                            await js.get_msg("KV_" + bucket, subject=f"$KV.{bucket}.{key}", direct=True)
                            break
                        except nats.js.errors.APIError:
                            await asyncio.sleep(.1)
                    else:
                        raise RuntimeError("initial metadata did not flush: " + bucket)
                code = f"recovery-enroll-{index}"
                sql(f"INSERT INTO zebridge_invites(code,principal,tenant_id) VALUES ('{code}','lf-edge1.enrolled_device','_default')", database)
                keypair = nkeys.from_seed(nkeys.encode_seed(os.urandom(32), nkeys.PREFIX_BYTE_USER))
                enrollment_url = f"http://127.0.0.1:{http}/enroll?" + urllib.parse.urlencode({"code":code,"user_pubkey":keypair.public_key.decode()})
                with urllib.request.urlopen(enrollment_url, timeout=10) as response:
                    identity = json.loads(response.read())
                check(identity.get("principal") == "lf-edge1.enrolled_device" and identity.get("nats_url") == "tls://leaf.example.org:4222"
                      and identity.get("nats_ws_url") == "wss://leaf.example.org",
                      "scoped enrollment returns the registered regional TLS and WSS URLs")
                return url, config, env, nc, (bridge, gate, server)
            def client(url, db):
                handle = lib.zb_client_connect(json.dumps({"natsUrl": url, "credsPath": str(device), "principal": "lf-edge1.recovery_device",
                    "jsDomain": "hub", "tables": ["recovery_row"], "dbPath": str(db), "heartbeatMs": 0}).encode())
                if not handle:
                    raise RuntimeError("native client could not open")
                synced = take(lib.zb_client_sync(handle))
                if "error" in synced:
                    lib.zb_client_close(handle)
                    raise RuntimeError(str(synced))
                return handle
            url, config, env, nc, old_processes = await start_hub(source, 0, trust, creds)
            replica = tmp / "replica.sqlite3"
            h = await asyncio.to_thread(client, url, replica)
            check(rows(h) == [[10]], "the source hub seeds a native replica from PostgreSQL")
            mutation = take(lib.zb_client_mutate(h, b"recovery_row", b"INSERT",
                b'{"id":"00000000-0000-0000-0000-000000000002"}', b'{"v":20}'))
            assert "error" not in mutation, mutation
            # Longer than the 128-byte tenant-token bound the verdict check once used, within
            # the 512-byte ID rule: clearing the outbox below proves such an ID gets its verdict.
            id_bytes = len(mutation["msgId"].encode("utf-8"))
            check(128 < id_bytes <= 512, f"the native write's escaped zb1 ID is longer than 128 bytes ({id_bytes})")
            for _ in range(100):
                await asyncio.to_thread(lambda: take(lib.zb_client_poll(h, 20)))
                if rows(h) == [[10], [20]] and pending(h) == 0:
                    break
                await asyncio.sleep(.05)
            check(rows(h) == [[10], [20]] and pending(h) == 0,
                  f"a write crosses admission, commits, echoes and clears its original client outbox (rows {rows(h)}, pending {pending(h)})")
            # A decided receipt in monthly partitions, as a running bridge leaves them.
            receipt_id = f"zb1-t{int(time.time() * 1000)}-recovery-1"
            receipt_args = (f"'lf-edge1.recovery_device', ARRAY['{receipt_id}']::text[], "
                            "ARRAY[convert_to('mutation.lf-edge1.recovery_device.recovery_row.insert','UTF8')]::bytea[], "
                            "ARRAY[convert_to('{}','UTF8')]::bytea[]")
            sql("SELECT count(*) FROM zebridge_private.maintain_receipts(); BEGIN; "
                f"SELECT count(*) FROM zebridge_private.claim_receipts({receipt_args}); "
                f"SELECT zebridge_private.finish_receipts('lf-edge1.recovery_device', ARRAY['{receipt_id}']::text[], "
                "ARRAY['{\"committed\":true}'::jsonb]); COMMIT;")
            policy_sql = "SELECT retention_days || '|' || min_accepted_ms FROM zebridge_private.receipt_policy"
            source_policy = sql(policy_sql)
            checkpoint = tmp / "checkpoint"
            def dump(path):
                dump_env = backup.postgres_environment(f"postgres://postgres@127.0.0.1:5432/{source}")
                subprocess.run([str(pgbin / "pg_dump"), "-Fc", "-f", str(path)], env=dump_env, capture_output=True, check=True)
            def bootstrap(path):
                path.write_text(render(source))
            await backup.checkpoint(nc, checkpoint, config, creds, dump, bootstrap, domain="hub", apply=True)
            check((checkpoint / "manifest.json").exists(), "the coordinated checkpoint produced a real PostgreSQL dump and recovery manifest")
            sql("UPDATE recovery_row SET v=999 WHERE v=10")
            for _ in range(100):
                await asyncio.to_thread(lambda: take(lib.zb_client_poll(h, 20)))
                if rows(h) == [[20], [999]]:
                    break
                await asyncio.sleep(.03)
            check(rows(h) == [[20], [999]], "the old replica observes a post-checkpoint change before the simulated loss")
            lib.zb_client_close(h)
            h = 0
            await nc.close()
            for process in old_processes:
                process.terminate()
                process.wait(timeout=15)
            sql(f'CREATE DATABASE "{restored}"', "postgres")
            made.append(restored)
            sql(render(restored), restored)
            restored_env = backup.postgres_environment(f"postgres://postgres@127.0.0.1:5432/{restored}")
            restore = subprocess.run([str(pgbin / "pg_restore"), "--clean", "--if-exists", "--no-owner", "-d", restored,
                                      str(checkpoint / "database.dump")], env=restored_env, capture_output=True, text=True)
            assert restore.returncode == 0, restore.stderr[-1800:]
            check(sql("SELECT string_agg(v::text,',' ORDER BY v) FROM recovery_row", restored) == "10,20",
                  "pg_restore recovers the checkpoint, including the acknowledged client mutation")
            check(sql(f"SELECT status || '|' || outcome::text FROM zebridge_private.claim_receipts({receipt_args})", restored)
                  == 'duplicate|{"committed": true}'
                  and int(sql("SELECT count(*) FROM pg_inherits WHERE inhparent = 'zebridge_private.receipts'::regclass", restored)) >= 7,
                  "restored receipts and partitions answer a replayed ID with its recorded outcome")
            restored_policy = sql(policy_sql, restored)
            check(restored_policy == source_policy and int(source_policy.split("|")[1]) > 0,
                  f"the receipt policy is restored exactly: retention and minimum accepted time ({restored_policy})")
            sql("SELECT count(*) FROM zebridge_private.maintain_receipts()", restored)  # what the replacement bridge runs at startup
            check(int(sql(policy_sql, restored).split("|")[1]) >= int(source_policy.split("|")[1]),
                  "startup maintenance on the restored database keeps or advances the minimum, never lowers it")
            recovered_conf = (checkpoint / "nats-server.conf").read_text()
            recovered_trust = recovered_conf[:recovered_conf.index('\nhost:')]
            new_url, _, _, _, _ = await start_hub(restored, 1, recovered_trust, checkpoint / "bridge.creds")
            h = await asyncio.to_thread(client, new_url, replica)
            for _ in range(40):
                await asyncio.to_thread(lambda: take(lib.zb_client_poll(h, 20)))
                if rows(h) == [[10], [20]]:
                    break
                await asyncio.sleep(.05)
            check(rows(h) == [[10], [20]], "the returning replica discards its newer lost-feed state and converges to the recovered hub")
            lib.zb_client_close(h)
            h = 0
            h = await asyncio.to_thread(client, new_url, tmp / "fresh.sqlite3")
            check(rows(h) == [[10], [20]], "a fresh device authenticates with the preserved identity and seeds from rebuilt snapshots")
        except Exception:
            for log in logs:
                if log.name.startswith("bridge"):
                    print(log.name, log.read_text(errors="replace")[-2200:])
            raise
        finally:
            if h:
                lib.zb_client_close(h)
            for connection in connections:
                await connection.close()
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=15)
            for log in logs:
                text = log.read_text(errors="replace")
                check("error(DebugAllocator)" not in text and "panic:" not in text, f"{log.name}: no allocator leaks or panics")
            for slot in slots:
                sql(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='{slot}'", "postgres")
            for db in reversed(made):
                sql(f'DROP DATABASE "{db}" WITH (FORCE)', "postgres")
            for role in (reader, writer):
                sql(f'DROP ROLE IF EXISTS "{role}"', "postgres")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

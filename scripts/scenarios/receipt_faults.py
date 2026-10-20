#!/usr/bin/env python3
"""Receipts under injected faults: a lost transaction or a failed allocation in the batch
path never lets a write run twice, or commit without its receipt.

Needs the fault-test binary, the only build with fault injection compiled in:

    zig build -Doptimize=ReleaseSafe -Dtest-faults=true -p zig-out-faults

(ZB_FAULT_BRIDGE_BIN names another one.) For each ZB_TEST_FAULT mode a private stack
(disposable database, nats-server, admission, one bridge lane) takes bursts of concurrent
writes, so the listener forms batches; the fault fires in every batch, which then replays
message by message. A BEFORE trigger counts executions in the same transaction: unlike row
counts or an AFTER trigger, it also sees a repeated upsert that changed nothing.
"""
import asyncio, datetime, json, os, pathlib, socket, subprocess, tempfile, time, uuid

import msgpack
import nats
import nkeys
import zb
from rotate_client_key import client_creds as mint_client

BRIDGE = pathlib.Path(os.environ.get("ZB_FAULT_BRIDGE_BIN", zb.ROOT / "zig-out-faults/bin/bridge"))
MODES = tuple(os.environ.get("ZB_RF_MODES", "reset_after_claims oom_in_claims oom_in_finishes").split())


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def comp(s):
    return "".join(chr(b) if b < 0x80 and (chr(b).isalnum() or chr(b) in "_:") else f"~{b:02X}" for b in s.encode())


async def main():
    psql = [zb._psql_binary(), "-X", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-Atq"]
    failed = 0
    if not BRIDGE.exists() or b"ZB_TEST_FAULT" not in BRIDGE.read_bytes():
        print(f"✗ {BRIDGE} is not a fault-test build: zig build -Doptimize=ReleaseSafe -Dtest-faults=true -p zig-out-faults")
        return 1

    def check(ok, label):
        nonlocal failed
        (zb.ok if ok else zb.bad)(label)
        failed += not ok

    for mode in MODES:
        name = "zb_rf_" + uuid.uuid4().hex[:8]
        reader, writer = name + "_r", name + "_w"
        procs = []

        def sql(statement, db=name):
            r = subprocess.run([*psql, "-d", db], input=statement, capture_output=True, text=True)
            if r.returncode:
                raise RuntimeError(r.stderr[-1500:])
            return r.stdout.strip()

        with tempfile.TemporaryDirectory(prefix="zb-rf-") as folder:
            tmp = pathlib.Path(folder)
            try:
                sql(f'CREATE DATABASE "{name}"', "postgres")
                env = dict(os.environ, DATABASE_READER_URL=f"postgres://{reader}:x@127.0.0.1:5432/{name}",
                           DATABASE_WRITER_URL=f"postgres://{writer}:x@127.0.0.1:5432/{name}", TARGET_DB=name,
                           BRIDGE_CDC_PUBLICATION="rf_pub")
                sql(subprocess.run([str(BRIDGE), "--init-sql"], env=env, capture_output=True, text=True, check=True).stdout)
                sql("CREATE TABLE fault_audit (n bigserial PRIMARY KEY, id uuid NOT NULL); "
                    "CREATE TABLE fault_row (id uuid PRIMARY KEY, v int NOT NULL, updated_at timestamptz NOT NULL DEFAULT now()); "
                    "CREATE FUNCTION fault_count() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS "
                    "$$ BEGIN INSERT INTO fault_audit(id) VALUES (NEW.id); RETURN NEW; END $$; "
                    "CREATE TRIGGER fault_count BEFORE INSERT OR UPDATE ON fault_row FOR EACH ROW EXECUTE FUNCTION fault_count(); "
                    "SELECT * FROM zebridge_enable('public.fault_row', public_reason => 'fault fixture', writable => true, "
                    "version_col => 'updated_at', allow_physical_deletes => true, publication => 'rf_pub', dry_run => false); "
                    "INSERT INTO zebridge_user_tenants VALUES ('alice', '_default');")
                ident = tmp / "id"
                subprocess.run([str(BRIDGE), "--init-nats", "operator", "--dir", str(ident), "--js-domain", "hub"], capture_output=True, check=True)
                keys = dict(l.split("=", 1) for l in (ident / "operator.store").read_text().splitlines() if "=" in l and not l.startswith("#"))
                account = nkeys.from_seed(keys["ACCOUNT_SEED"].encode()).public_key.decode()
                conf = (ident / "nats-server.conf").read_text()
                tcp = port()
                (tmp / "n.conf").write_text(conf[:conf.index("\nport:")] + f'\nhost: "127.0.0.1"\nport: {tcp}\njetstream {{ domain: hub, store_dir: "{tmp}/js" }}\n')
                (tmp / "alice.creds").write_text(mint_client(keys["SK_CLIENT_SEED"], account, "alice", "_default"))
                logs = {}

                def spawn(args, e, label):
                    logs[label] = tmp / f"{label}.log"
                    out = open(logs[label], "w")
                    procs.append(subprocess.Popen(args, env=e, stdout=out, stderr=subprocess.STDOUT))
                    return procs[-1]

                async def wait_log(label, text, timeout=60):
                    end = time.monotonic() + timeout
                    while time.monotonic() < end:
                        if text in logs[label].read_text(errors="replace"):
                            return
                        await asyncio.sleep(.1)
                    raise RuntimeError(f"{label}: no '{text}'\n" + logs[label].read_text(errors="replace")[-1500:])

                spawn(["nats-server", "-c", str(tmp / "n.conf")], dict(os.environ), "nats")
                await wait_log("nats", "Server is ready")
                url = f"nats://127.0.0.1:{tcp}"
                env = dict(env, NATS_URL=url, NATS_CREDS=str(ident / "creds/bridge.creds"), NATS_JS_DOMAIN="hub", NATS_TLS_CA="",
                           MUTATION_ADMISSION="1", GENERATIONS_ENABLED="0")
                spawn([str(BRIDGE), "--mutation-ingress"], env, "admission")
                await wait_log("admission", "mutation admission ready")
                spawn([str(BRIDGE), "--pub", "rf_pub", "--slot", name + "_slot", "--port", str(port())], dict(env, ZB_TEST_FAULT=mode), "bridge")
                await wait_log("bridge", "Ready! Pulling mutations", timeout=90)

                nc = await nats.connect(url, user_credentials=str(tmp / "alice.creds"), inbox_prefix=b"_INBOX.alice")
                js = nc.jetstream()
                verdicts, deliveries = {}, {}

                async def on_verdict(m):
                    mid = m.subject.rsplit(".", 1)[-1]
                    verdicts[mid] = json.loads(m.data).get("status")
                    deliveries[mid] = deliveries.get(mid, 0) + 1

                await nc.subscribe("mutation_ack.alice.*", cb=on_verdict)

                async def burst(n):
                    ids = []
                    async def one(i):
                        key = str(uuid.uuid4())
                        # Microseconds, as clients stamp them: a version rounded down to its
                        # second can fall below a freshly set GC watermark and be refused.
                        version = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        mid = f"zb1-t{int(time.time() * 1000)}-INSERT-c1-fault_row-k1-{comp(key)}-{comp(version)}"
                        body = msgpack.packb({"key": {"id": key}, "data": {"id": key, "v": i, "updated_at": version}, "version": version, "client_id": "c1"})
                        await js.publish("mutation.alice.fault_row.insert", body, headers={"Nats-Msg-Id": mid})
                        ids.append(mid)
                    await asyncio.gather(*(one(i) for i in range(n)))
                    end = time.monotonic() + 90
                    while time.monotonic() < end and not all(i in verdicts for i in ids):
                        await asyncio.sleep(.2)
                    return ids

                await asyncio.sleep(2)  # clear of the GC watermark the bridge sets at start
                first = await burst(200)
                second = await burst(50)  # the lane keeps operating after every recovery
                ids = first + second
                statuses = [verdicts.get(i) for i in ids]
                rows = int(sql("SELECT count(*) FROM fault_row"))
                executions = sql("SELECT count(*), count(DISTINCT id) FROM fault_audit").split("|")
                receipts = int(sql("SELECT count(*) FROM zebridge_private.receipts WHERE principal = 'alice' AND outcome->>'status' = 'accepted'"))
                fired = logs["bridge"].read_text(errors="replace").count(f"ZB_TEST_FAULT {mode} fired")
                check(fired > 0, f"{mode}: the fault was reached and fired in {fired} batch(es)")
                once = sum(1 for i in ids if deliveries.get(i) == 1)
                check(statuses.count("accepted") == len(ids) and rows == len(ids)
                      and executions == [str(len(ids)), str(len(ids))] and receipts == len(ids) and once == len(ids),
                      f"{mode}: every write ran exactly once, has its receipt and got one verdict delivery; "
                      f"the lane kept operating after each recovery (accepted {statuses.count('accepted')}/{len(ids)}, "
                      f"executions {executions[0]}, receipts {receipts}, single deliveries {once})")
                if statuses.count("accepted") != len(ids):
                    from collections import Counter
                    print("first burst:", Counter(verdicts.get(i) for i in first), "| second:", Counter(verdicts.get(i) for i in second))
                    try:
                        adm = await nats.connect(url, user_credentials=str(ident / "creds/bridge.creds"))
                        info = await adm.jetstream(domain="hub").consumer_info("MUTATIONS", "bridge_mutations_worker")
                        print("consumer:", {k: getattr(info, k) for k in ("num_pending", "num_ack_pending", "num_redelivered")})
                        await adm.close()
                    except Exception as e:
                        print("consumer info:", e)
                    noisy = [l for l in logs["bridge"].read_text(errors="replace").splitlines() if any(t in l for t in ("warn", "err", "⚠", "🔴", "fault"))]
                    print("\n".join(noisy[-30:]))
                await nc.close()
            finally:
                for p in procs:
                    p.terminate()
                for p in procs:
                    try:
                        p.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        p.kill()
                subprocess.run([*psql, "-d", "postgres"], capture_output=True, text=True,
                               input=f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{name}_slot'; "
                                     f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE); DROP ROLE IF EXISTS "{reader}"; DROP ROLE IF EXISTS "{writer}";')
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

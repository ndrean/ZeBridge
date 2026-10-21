#!/usr/bin/env python3
"""Hub fallback, stage 2: durable mutation receipts, end to end (run by hub_fallback.py).

A private stack (disposable database, nats-server, admission, a bridge with two lanes) and
writes that are retried the hard way. A BEFORE trigger counts committed executions: unlike
rows or an AFTER trigger, it also sees a repeated upsert that changed nothing.

  lost PubAck / lost verdict   retried with the same ID and bytes after JetStream's real
                               two-minute window: one execution, the recorded verdict
  ID across a restart          a libzb write queued while admission is down keeps its ID
                               when the client reopens, and commits once under it
  deferred constraint          refused at its deferred FK, recorded `rejected`, the rest
                               of its batch accepted, and a retry answered without running
  concurrent duplicates        both copies stored before the bridge looks: one execution
  direct mode                  without admission, a write with no ID is never executed

The completion guard is enabled by default. ZB_RECEIPT_GUARD=0 requires a binary built with
-Dreceipt-guard=false; installation and startup must agree without a fixture override.
"""
import asyncio, ctypes, datetime, json, os, pathlib, signal, socket, subprocess, tempfile, time, uuid

import msgpack
import nats
import nkeys
import zb
from rotate_client_key import client_creds as mint_client

BRIDGE = pathlib.Path(os.environ.get("ZB_BRIDGE_BIN", zb.ROOT / "zig-out/bin/bridge"))
LIBRARY = pathlib.Path(os.environ.get("ZB_LIBZB", zb.ROOT / "libzb/zig-out/lib" / ("libzbcore.dylib" if os.uname().sysname == "Darwin" else "libzbcore.so")))
GUARD = os.environ.get("ZB_RECEIPT_GUARD", "1") == "1"
WINDOW_S = 125  # past JetStream's default two-minute duplicate window


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def comp(s):
    return "".join(chr(b) if b < 0x80 and (chr(b).isalnum() or chr(b) in "_:") else f"~{b:02X}" for b in s.encode())


def stamp():
    return datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def write(table, key, data, client="c1", op="INSERT", version=None):
    """A write as a client sends it: its zb1 ID and its exact bytes."""
    version = version or stamp()
    mid = f"zb1-t{int(time.time() * 1000)}-{op}-{comp(client)}-{comp(table)}-k1-{comp(key)}-{comp(version)}"
    body = msgpack.packb({"key": {"id": key}, "data": {**data, "id": key, "updated_at": version}, "version": version, "client_id": client})
    return mid, body


class Stack:
    """Disposable database, nats-server, admission (optional) and one bridge."""

    def __init__(self, tmp, admission=True, lanes=2):
        self.tmp, self.admission, self.lanes = tmp, admission, lanes
        self.name = "zb_hf2_" + uuid.uuid4().hex[:8]
        self.procs, self.logs = {}, {}
        self.psql = [zb._psql_binary(), "-X", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-Atq"]

    def sql(self, statement, db=None):
        r = subprocess.run([*self.psql, "-d", db or self.name], input=statement, capture_output=True, text=True)
        if r.returncode:
            raise RuntimeError(r.stderr[-1500:])
        return r.stdout.strip()

    def spawn(self, label, args, env):
        self.logs[label] = self.tmp / f"{self.name}-{label}.log"
        self.procs[label] = subprocess.Popen(args, env=env, stdout=open(self.logs[label], "w"), stderr=subprocess.STDOUT)

    def log(self, label):
        return self.logs[label].read_text(errors="replace")

    async def wait_log(self, label, text, timeout=90):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if text in self.log(label):
                return
            await asyncio.sleep(.1)
        raise RuntimeError(f"{label}: no '{text}'\n" + self.log(label)[-1500:])

    async def start(self):
        self.sql(f'CREATE DATABASE "{self.name}"', "postgres")
        r, w = self.name + "_r", self.name + "_w"
        self.env = dict(os.environ, DATABASE_READER_URL=f"postgres://{r}:x@127.0.0.1:5432/{self.name}",
                        DATABASE_WRITER_URL=f"postgres://{w}:x@127.0.0.1:5432/{self.name}", TARGET_DB=self.name,
                        BRIDGE_CDC_PUBLICATION="hf2_pub")
        self.sql(subprocess.run([str(BRIDGE), "--init-sql"], env=self.env, capture_output=True, text=True, check=True).stdout)
        installed = self.sql("SELECT completion_guard FROM zebridge_private.receipt_policy") == "t"
        if installed != GUARD:
            raise RuntimeError("installed receipt default disagrees with ZB_RECEIPT_GUARD (enabled by default; disabling requires -Dreceipt-guard=false)")
        enable = ("SELECT * FROM zebridge_enable('public.{t}', public_reason => 'stage 2 fixture', writable => true, version_col => 'updated_at', "
                  "allow_physical_deletes => true, publication => 'hf2_pub', dry_run => false); ")
        self.sql("CREATE TABLE audit (n bigserial PRIMARY KEY, tbl text NOT NULL, id uuid NOT NULL); "
                 "CREATE FUNCTION audit_write() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS "
                 "$$ BEGIN INSERT INTO audit(tbl, id) VALUES (TG_TABLE_NAME, NEW.id); RETURN NEW; END $$; "
                 "CREATE TABLE s2_row (id uuid PRIMARY KEY, v int NOT NULL, updated_at timestamptz NOT NULL DEFAULT now()); "
                 "CREATE TABLE s2_parent (id uuid PRIMARY KEY, updated_at timestamptz NOT NULL DEFAULT now()); "
                 "CREATE TABLE s2_child (id uuid PRIMARY KEY, parent uuid NOT NULL REFERENCES s2_parent(id) DEFERRABLE INITIALLY DEFERRED, "
                 "updated_at timestamptz NOT NULL DEFAULT now()); "
                 + "".join(f"CREATE TRIGGER audit_write BEFORE INSERT OR UPDATE ON {t} FOR EACH ROW EXECUTE FUNCTION audit_write(); " for t in ("s2_row", "s2_child"))
                 + "".join(enable.format(t=t) for t in ("s2_row", "s2_parent", "s2_child"))
                 + "INSERT INTO zebridge_user_tenants VALUES ('alice', '_default'), ('bob', '_default');")
        ident = self.tmp / f"{self.name}-id"
        subprocess.run([str(BRIDGE), "--init-nats", "operator", "--dir", str(ident), "--js-domain", "hub"], capture_output=True, check=True)
        keys = dict(l.split("=", 1) for l in (ident / "operator.store").read_text().splitlines() if "=" in l and not l.startswith("#"))
        account = nkeys.from_seed(keys["ACCOUNT_SEED"].encode()).public_key.decode()
        for who in ("alice", "bob"):
            (self.tmp / f"{self.name}-{who}.creds").write_text(mint_client(keys["SK_CLIENT_SEED"], account, who, "_default"))
        conf = (ident / "nats-server.conf").read_text()
        tcp = port()
        (self.tmp / f"{self.name}.conf").write_text(conf[:conf.index("\nport:")] + f'\nhost: "127.0.0.1"\nport: {tcp}\njetstream {{ domain: hub, store_dir: "{self.tmp}/{self.name}-js" }}\n')
        self.spawn("nats", ["nats-server", "-c", str(self.tmp / f"{self.name}.conf")], dict(os.environ))
        await self.wait_log("nats", "Server is ready")
        self.url, self.bridge_creds = f"nats://127.0.0.1:{tcp}", str(ident / "creds/bridge.creds")
        self.env = dict(self.env, NATS_URL=self.url, NATS_CREDS=self.bridge_creds, NATS_JS_DOMAIN="hub", NATS_TLS_CA="",
                        MUTATION_ADMISSION="1" if self.admission else "0", ZB_INGRESS_LANES=str(self.lanes),
                        GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="1", BASE_BUF="12", ZB_FEED_RESTART="1")
        if self.admission:
            self.start_admission()
            await self.wait_log("admission", "mutation admission ready")
        self.spawn("bridge", [str(BRIDGE), "--pub", "hf2_pub", "--slot", self.name + "_slot", "--port", str(port())], self.env)
        await self.wait_log("bridge", "Ready! Pulling mutations")
        await self.wait_log("bridge", "completion guard enabled" if GUARD else "completion guard disabled")
        await asyncio.sleep(2)  # clear of the GC watermark the bridge sets at start

    def start_admission(self):
        self.spawn("admission", [str(BRIDGE), "--mutation-ingress"], self.env)

    def creds(self, who):
        return str(self.tmp / f"{self.name}-{who}.creds")

    def executions(self, table, key):
        return int(self.sql(f"SELECT count(*) FROM audit WHERE tbl = '{table}' AND id = '{key}'"))

    def stop(self):
        for p in self.procs.values():
            if p.poll() is None:
                p.send_signal(signal.SIGCONT)
                p.terminate()
        for p in self.procs.values():
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()
        subprocess.run([*self.psql, "-d", "postgres"], capture_output=True, text=True,
                       input=f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{self.name}_slot'; "
                             f'DROP DATABASE IF EXISTS "{self.name}" WITH (FORCE); DROP ROLE IF EXISTS "{self.name}_r"; DROP ROLE IF EXISTS "{self.name}_w";')


class Writer:
    """A raw NATS client that records every verdict delivery per message ID."""

    def __init__(self, stack, who="alice"):
        self.stack, self.who, self.verdicts = stack, who, {}

    async def open(self):
        self.nc = await nats.connect(self.stack.url, user_credentials=self.stack.creds(self.who), inbox_prefix=f"_INBOX.{self.who}".encode())
        self.js = self.nc.jetstream()

        async def on_verdict(m):
            self.verdicts.setdefault(m.subject.rsplit(".", 1)[-1], []).append(json.loads(m.data))
        await self.nc.subscribe(f"mutation_ack.{self.who}.*", cb=on_verdict)

    async def send(self, table, mid, body, op="insert", core=False):
        subject, headers = f"mutation.{self.who}.{table}.{op}", {"Nats-Msg-Id": mid}
        if core:
            await self.nc.publish(subject, body, headers=headers)  # no PubAck awaited: as if it were lost
            await self.nc.flush()
        else:
            await self.js.publish(subject, body, headers=headers)

    async def answered(self, mid, count=1, timeout=60):
        end = time.monotonic() + timeout
        while time.monotonic() < end and len(self.verdicts.get(mid, [])) < count:
            await asyncio.sleep(.1)
        return self.verdicts.get(mid, [])


async def main():
    passed = failed = 0

    def check(ok, label):
        nonlocal passed, failed
        (zb.ok if ok else zb.bad)(label)
        passed += bool(ok)
        failed += not ok

    lib = ctypes.CDLL(str(LIBRARY))
    lib.zb_client_connect.argtypes, lib.zb_client_connect.restype = [ctypes.c_char_p], ctypes.c_uint64
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    lib.zb_free.argtypes = [ctypes.c_void_p]
    for fn, extra in (("sync", []), ("poll", [ctypes.c_uint64]), ("flush_outbox", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                      ("mutate", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + fn)
        f.argtypes, f.restype = [ctypes.c_uint64] + extra, ctypes.c_void_p

    def take(pointer):
        try:
            return json.loads(ctypes.string_at(pointer))
        finally:
            lib.zb_free(pointer)

    with tempfile.TemporaryDirectory(prefix="zb-hf2-") as folder:
        tmp = pathlib.Path(folder)
        hub = Stack(tmp)
        try:
            await hub.start()
            alice = Writer(hub)
            await alice.open()

            # ── lost PubAck and lost verdict: first attempts ──
            kx, ky = str(uuid.uuid4()), str(uuid.uuid4())
            x, y = write("s2_row", kx, {"v": 1}), write("s2_row", ky, {"v": 2})
            await alice.send("s2_row", *x, core=True)
            await alice.send("s2_row", *y)
            first_x, first_y = (await alice.answered(x[0]))[:1], (await alice.answered(y[0]))[:1]
            sent_at = time.monotonic()
            check(first_x and first_y and first_x[0]["status"] == "accepted" and first_y[0]["status"] == "accepted",
                  "first attempts commit: a write whose PubAck was never read, and one whose verdict will be ignored")

            # ── a libzb write keeps its ID across a client restart (runs inside the window wait) ──
            db = tmp / "bob.sqlite3"
            opts = json.dumps({"natsUrl": hub.url, "credsPath": hub.creds("bob"), "principal": "bob", "jsDomain": "hub",
                               "tables": ["s2_row"], "dbPath": str(db), "heartbeatMs": 0}).encode()
            h = await asyncio.to_thread(lib.zb_client_connect, opts)
            synced = take(lib.zb_client_sync(h)) if h else {"error": "connect"}
            check(h and "error" not in synced, "a libzb client opens and syncs the table")
            hub.procs["admission"].terminate()
            hub.procs["admission"].wait(timeout=15)
            kz = str(uuid.uuid4())
            take(lib.zb_client_mutate(h, b"s2_row", b"INSERT", json.dumps({"id": kz}).encode(), b'{"v": 3}'))
            monitor = await nats.connect(hub.url, user_credentials=hub.bridge_creds)
            seen = []
            async def published(msg): seen.append(msg.headers.get('Nats-Msg-Id') if msg.headers else None)
            try:
                await monitor.subscribe('mutation.bob.s2_row.insert', cb=published)
                await monitor.flush()
                kz2 = str(uuid.uuid4())
                second = await asyncio.to_thread(lambda: take(lib.zb_client_mutate(h, b"s2_row", b"INSERT", json.dumps({"id": kz2}).encode(), b'{"v": 4}')))
                await asyncio.sleep(.15)
                check(second.get('msgId') in seen and len(seen) <= 2 and len(set(seen)) == len(seen), 'a new mutation shares the window with due backlog, without sending an ID twice in one pass')
            finally:
                await monitor.close()
            outbox = lambda handle: take(lib.zb_client_query(handle, b"SELECT msg_id FROM _zebridge_outbox ORDER BY msg_id", b"[]")).get("rows", [])
            before = outbox(h)
            lib.zb_client_close(h)
            h = await asyncio.to_thread(lib.zb_client_connect, opts)
            after = outbox(h) if h else []
            check(len(before) == 2 and before == after and all(r[0].startswith('zb1-t') for r in before),
                  "writes queued while admission is down keep their zb1 IDs when the client reopens")
            hub.start_admission()
            await hub.wait_log("admission", "mutation admission ready")
            # Exercise the explicit full-pass API. Automatic bounded passes are covered
            # separately by outbox_recovery and endpoint-failover's normal poll loop.
            for _ in range(100):
                await asyncio.to_thread(lambda: take(lib.zb_client_flush_outbox(h, 200)))
                if not outbox(h):
                    break
            receipt = hub.sql(f"SELECT outcome->>'status' FROM zebridge_private.receipts WHERE principal = 'bob' AND message_id = '{before[0][0] if before else ''}'")
            left, runs = len(outbox(h)), hub.executions("s2_row", kz) + hub.executions("s2_row", kz2)
            check(left == 0 and receipt == "accepted" and runs == 2 and hub.executions('s2_row', kz) == 1 and hub.executions('s2_row', kz2) == 1,
                  f"after restart it is sent under that ID, commits once and clears the outbox (left {left}, receipt {receipt or 'none'}, executions {runs})")
            lib.zb_client_close(h)

            # ── retries past the real window ──
            await asyncio.sleep(max(0.0, WINDOW_S - (time.monotonic() - sent_at)))
            await alice.send("s2_row", *x)
            await alice.send("s2_row", *y)
            again_x, again_y = await alice.answered(x[0], 2), await alice.answered(y[0], 2)
            same = lambda a, b: (a["status"], a["version"], a["reason"]) == (b["status"], b["version"], b["reason"])
            check(len(again_x) == 2 and same(again_x[1], first_x[0]) and hub.executions("s2_row", kx) == 1,
                  f"lost PubAck, retried after {WINDOW_S} s: one execution, the recorded verdict (version {first_x[0]['version'] if first_x else '?'})")
            check(len(again_y) == 2 and same(again_y[1], first_y[0]) and hub.executions("s2_row", ky) == 1,
                  f"lost verdict, retried after {WINDOW_S} s: one execution, the recorded verdict")

            # ── deferred constraint, refused at its own commit ──
            kp = str(uuid.uuid4())
            parent = write("s2_parent", kp, {})
            await alice.send("s2_parent", *parent)
            await alice.answered(parent[0])
            kids = [write("s2_child", str(uuid.uuid4()), {"parent": kp}) for _ in range(20)]
            kbad = str(uuid.uuid4())
            bad = write("s2_child", kbad, {"parent": str(uuid.uuid4())})
            await asyncio.gather(*(alice.send("s2_child", *k) for k in kids[:10]), alice.send("s2_child", *bad),
                                 *(alice.send("s2_child", *k) for k in kids[10:]))
            kid_verdicts = [await alice.answered(k[0]) for k in kids]
            bad_verdicts = await alice.answered(bad[0])
            refused = bad_verdicts and bad_verdicts[0]["status"] == "rejected" and bad_verdicts[0]["sqlstate"] == "23503"
            receipt = hub.sql(f"SELECT outcome->>'status' FROM zebridge_private.receipts WHERE message_id = '{bad[0]}'")
            check(refused and receipt == "rejected" and hub.executions("s2_child", kbad) == 0
                  and all(v and v[0]["status"] == "accepted" for v in kid_verdicts),
                  "a write refused at its deferred FK is recorded `rejected` (23503), never committed; its batch's other writes are accepted")

            async def set_window(seconds):
                nc = await nats.connect(hub.url, user_credentials=hub.bridge_creds)
                js = nc.jetstream(domain="hub")
                info = await js.stream_info("MUTATIONS")
                info.config.duplicate_window = seconds
                await js.update_stream(config=info.config)
                await nc.close()

            await set_window(0.5)
            await asyncio.sleep(1)
            await alice.send("s2_child", *bad)
            bad_again = await alice.answered(bad[0], 2)
            check(len(bad_again) == 2 and bad_again[1]["status"] == "rejected" and bad_again[1]["sqlstate"] == "23503"
                  and hub.executions("s2_child", kbad) == 0,
                  "its retry is answered from the rejection receipt, without running")

            # ── refusals that need no SQL stay inside their batch ──
            replays_before = hub.log("bridge").count("replayed one write at a time")
            good = [write("s2_row", str(uuid.uuid4()), {"v": n}) for n in range(20)]
            bad_keys = [str(uuid.uuid4()) for _ in range(3)]
            refused_writes = [write("s2_row", k, {"v": 0, "no_such_column": 1}) for k in bad_keys]
            mixed = good[:7] + refused_writes[:1] + good[7:14] + refused_writes[1:2] + good[14:] + refused_writes[2:]
            await asyncio.gather(*(alice.send("s2_row", *w) for w in mixed))
            good_answers = [await alice.answered(w[0]) for w in good]
            bad_answers = [await alice.answered(w[0]) for w in refused_writes]
            bad_receipts = [hub.sql(f"SELECT outcome->>'status' FROM zebridge_private.receipts WHERE message_id = '{w[0]}'") for w in refused_writes]
            replays = hub.log("bridge").count("replayed one write at a time") - replays_before
            check(all(a and a[0]["status"] == "accepted" for a in good_answers)
                  and all(a and a[0]["status"] == "rejected" for a in bad_answers) and bad_receipts == ["rejected"] * 3
                  and sum(hub.executions("s2_row", k) for k in bad_keys) == 0 and replays == 0,
                  f"writes refused before any SQL are rejected with a receipt inside their batch; the rest accepted; no batch replayed ({replays})")
            await asyncio.sleep(1)  # past the shortened window: only the receipt can answer it
            await alice.send("s2_row", *refused_writes[0])
            again = await alice.answered(refused_writes[0][0], 2)
            check(len(again) == 2 and again[1]["status"] == "rejected" and again[1]["reason"] == again[0]["reason"],
                  "a refused write's retry is answered from its receipt")

            # ── zero-affected writes before a refusal that closes the last segment ──
            # One batch (the bridge paused while it is stored): a stale UPDATE, an UPDATE of a
            # row that does not exist, then a write naming no column the table has, whose
            # re-read closes the segment and which stays refused. The first two must still
            # be classified, answered and recorded as what they are.
            old = stamp()
            await asyncio.sleep(.01)
            kr = str(uuid.uuid4())
            current = write("s2_row", kr, {"v": 1})
            await alice.send("s2_row", *current)
            await alice.answered(current[0])
            stale_w = write("s2_row", kr, {"v": 2}, op="UPDATE", version=old)
            gone_w = write("s2_row", str(uuid.uuid4()), {"v": 3}, op="UPDATE")
            bad_w = write("s2_row", str(uuid.uuid4()), {"v": 4, "no_such_column": 1})
            hub.procs["bridge"].send_signal(signal.SIGSTOP)
            await alice.send("s2_row", *stale_w, op="update")
            await alice.send("s2_row", *gone_w, op="update")
            await alice.send("s2_row", *bad_w)
            hub.procs["bridge"].send_signal(signal.SIGCONT)
            firsts = [(await alice.answered(w[0]))[:1] for w in (stale_w, gone_w, bad_w)]
            recorded = [hub.sql(f"SELECT outcome->>'status' FROM zebridge_private.receipts WHERE message_id = '{w[0]}'") for w in (stale_w, gone_w, bad_w)]
            got = [f[0]["status"] if f else None for f in firsts]
            check(got == ["stale", "row_deleted", "rejected"] and recorded == got,
                  f"a batch ending in a refusal that closes its segment still classifies its zero-affected writes ({got}, receipts {recorded})")
            await asyncio.sleep(1)  # past the shortened window: only the receipts can answer
            await alice.send("s2_row", *stale_w, op="update")
            await alice.send("s2_row", *gone_w, op="update")
            await alice.send("s2_row", *bad_w)
            replays = [(await alice.answered(w[0], 2))[1:] for w in (stale_w, gone_w, bad_w)]
            check([r[0]["status"] if r else None for r in replays] == got, "their retries replay the recorded stale / row_deleted / rejected")

            # ── a column added with no DDL event: only the batch's re-read can see it ──
            # The DDL capture trigger is off around the ALTER, so no catalog epoch bump can
            # refresh the cached layout first; the batch must close its segment and re-read.
            warm = write("s2_row", str(uuid.uuid4()), {"v": 1})
            await alice.send("s2_row", *warm)
            await alice.answered(warm[0])  # the layout is cached without `extra`
            hub.sql("ALTER EVENT TRIGGER zebridge_ddl_trigger DISABLE; ALTER TABLE s2_row ADD COLUMN extra int; "
                    "ALTER EVENT TRIGGER zebridge_ddl_trigger ENABLE;")
            rereads_before = hub.log("bridge").count("re-read from the catalog inside a batch")
            fresh = [write("s2_row", str(uuid.uuid4()), {"v": n, "extra": n}) for n in range(20)]
            hub.procs["bridge"].send_signal(signal.SIGSTOP)
            for w in fresh:
                await alice.send("s2_row", *w)
            hub.procs["bridge"].send_signal(signal.SIGCONT)
            fresh_answers = [await alice.answered(w[0]) for w in fresh]
            stored = hub.sql("SELECT count(*) FROM s2_row WHERE extra IS NOT NULL")
            rereads = hub.log("bridge").count("re-read from the catalog inside a batch") - rereads_before
            check(all(a and a[0]["status"] == "accepted" for a in fresh_answers) and stored == "20" and rereads >= 1,
                  f"writes naming a column the cached layout lacks are accepted after the batch's own re-read ({rereads}), never falsely refused")

            # ── concurrent duplicates: both copies stored before the bridge looks ──
            # Two principals interleaved, so each batch holds one claim group per principal.
            bob = Writer(hub, "bob")
            await bob.open()
            keys = [str(uuid.uuid4()) for _ in range(50)]
            dups = [((alice, bob)[n % 2], write("s2_row", k, {"v": n})) for n, k in enumerate(keys)]
            hub.procs["bridge"].send_signal(signal.SIGSTOP)
            for who, d in dups:
                await who.send("s2_row", *d)
            await asyncio.sleep(0.8)  # past the shortened window: the second copies are stored too
            for who, d in dups:
                await who.send("s2_row", *d)
            hub.procs["bridge"].send_signal(signal.SIGCONT)
            answers = [await who.answered(d[0], 2) for who, d in dups]
            runs = sum(hub.executions("s2_row", k) for k in keys)
            check(all(len(a) == 2 and a[0]["status"] == a[1]["status"] == "accepted" and a[0]["version"] == a[1]["version"] for a in answers)
                  and runs == len(keys),
                  f"{len(keys)} writes from two principals stored twice, judged by two lanes: {runs} executions, both copies answered alike")
            await bob.nc.close()
            await alice.nc.close()
        except Exception:
            for label in hub.logs:
                print(f"{label}:\n" + hub.log(label)[-1500:])
            raise
        finally:
            hub.stop()

        # ── direct mode: without admission, a write with no ID is stored but never run ──
        direct = Stack(tmp, admission=False, lanes=1)
        try:
            await direct.start()
            w = Writer(direct)
            await w.open()
            dead = []
            watch = await nats.connect(direct.url, user_credentials=direct.bridge_creds)
            async def on_dead(m):
                dead.append(m.subject)
            await watch.subscribe("mutation_error.>", cb=on_dead)
            k = str(uuid.uuid4())
            _, body = write("s2_row", k, {"v": 9})
            ack = await w.js.publish("mutation.alice.s2_row.insert", body)  # no Nats-Msg-Id at all
            await asyncio.sleep(3)
            stored_rows = direct.sql(f"SELECT count(*) FROM s2_row WHERE id = '{k}'")
            check(ack.seq > 0 and stored_rows == "0" and "without a valid message ID" in direct.log("bridge") and dead,
                  "direct mode: JetStream stores a write with no ID, the bridge never runs it and dead-letters it")
            ok = write("s2_row", str(uuid.uuid4()), {"v": 10})
            await w.send("s2_row", *ok)
            v = await w.answered(ok[0])
            check(v and v[0]["status"] == "accepted", "the lane goes on: the next write, with its ID, is accepted")
            await watch.close()
            await w.nc.close()
        except Exception:
            for label in direct.logs:
                print(f"{label}:\n" + direct.log(label)[-1500:])
            raise
        finally:
            direct.stop()
    print(f"\nstage 2: {passed} passed, {failed} failed")
    return int(failed > 0)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

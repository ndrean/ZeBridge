#!/usr/bin/env python3
"""The delta path checks the manifest it applies, not only the one it approved.

A reconnecting client may take its chain's deltas instead of the CDC backlog when the
manifest it reads qualifies (core.deltaEligible). It reads the manifest again to apply
it, and a producer update — or a fallback from a stale leaf to the hub — can publish a
newer base in between. This reproduces that window deterministically, for libzb (C ABI)
and zb-client-ts (Node): no bridge, a scratch nats-server, manifests and chain objects
written by hand, and ZB_TEST_DELTA_PAUSE stopping the client between selection and
application while the test swaps the manifest.

  1. M1 (guarded, full g1, 10 rows): the client seeds it and closes.
  2. While it is away: 200 CDC updates, and a DELETE of row 0005 among them — rows that
     went without a version move, as a TRUNCATE or a rewrite does on a guarded table.
     M2 (g1 + delta g2, which reaches the replica's watermark) is eligible.
  3. The client reopens, selects the delta path on M2 and pauses.
  4. The test publishes M3: a new base g3 without 0005, and deltas g2 + g3 that still
     reach the watermark — so the plan holds deltas only, and applying it while moving
     past the DELETE would keep 0005 for good.
  5. Expected: the client refuses M3 at application (no longer eligible: new-base),
     applies nothing, replays the CDC, and ends without 0005, every row at its final value.
"""
import asyncio, ctypes, json, os, pathlib, socket, sqlite3, subprocess, sys, tempfile, threading, time

import msgpack
import nats
from nats.js.api import KeyValueConfig, StreamConfig

import zb

LIBZB = pathlib.Path(os.environ.get("ZB_LIB", zb.ROOT / "libzb/zig-out/lib/libzbcore.dylib"))
NODE_DIR = zb.ROOT / "examples" / "04-node-consumer"
TABLE, TENANT = "dr_row", "acme"
BUCKET = zb.TOPOLOGY["generations"]["bucket_prefix"] + TENANT
KEY = f"{TENANT}.{TABLE}"
COLS = [{"name": "uid", "type": "TEXT"}, {"name": "n", "type": "INTEGER"}, {"name": "body", "type": "TEXT"},
        {"name": "updated_at", "type": "TEXT"}, {"name": "tenant_id", "type": "TEXT"}]
NAMES = [c["name"] for c in COLS]
W = lambda n: f"2026-10-22T10:00:{n:02d}.000000Z"
UPDATES, DELETED = 200, "0005"


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def row(i, n, at):
    return [f"{i:04d}", n, "x", at, TENANT]


def final_rows():
    """Each update k (1..UPDATES) sets row k % 10 to n = k; 0005 is deleted at the half."""
    last = {}
    for k in range(1, UPDATES + 1):
        last[k % 10] = k
    return {f"{i:04d}": last[i] for i in range(10) if f"{i:04d}" != DELETED}


def manifest(gen, full_gen, full_cutoff, deltas, cutoff_seq):
    return {"gen": gen, "bucket": BUCKET, "seed_epoch": 1, "deletes": "tombstoned", "version_column": "updated_at",
            "full": {"gen": full_gen, "object": f"{TABLE}-g{full_gen}-full", "cutoff": full_cutoff},
            "checkpoints": [], "deltas": deltas, "cutoff_version": deltas[-1]["cutoff"] if deltas else full_cutoff,
            "cutoff_lsn": "0/1", "cdc_stream": f"CDC_{TENANT}", "cutoff_seq": cutoff_seq,
            "shared_cdc_stream": "CDC_PUBLIC", "shared_cutoff_seq": 1}


def doc(kind, gen, cutoff, rows):
    return msgpack.packb({"columns": NAMES, "rows": rows, "version_column": "updated_at", "gen": gen, "kind": kind, "cutoff": cutoff})


async def run_case(kind: str, tmp: pathlib.Path) -> list[tuple[bool, str]]:
    checks: list[tuple[bool, str]] = []
    nport = port()
    conf = tmp / f"{kind}.conf"
    conf.write_text(f'listen: 127.0.0.1:{nport}\njetstream {{ store_dir: "{tmp}/{kind}-js" }}\n')
    server = subprocess.Popen(["nats-server", "-c", str(conf)], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    url = f"nats://127.0.0.1:{nport}"
    principal = f"dr_{kind}"
    db = tmp / f"{kind}.sqlite3"
    log = tmp / f"{kind}.log"
    try:
        for _ in range(100):  # the port first, so nats-py has nothing to log
            try:
                socket.create_connection(("127.0.0.1", nport), timeout=0.2).close()
                break
            except OSError:
                await asyncio.sleep(0.1)
        nc = await nats.connect(url)
        js = nc.jetstream()
        for bucket in (zb.TOPOLOGY["kv"]["schemas"], zb.TOPOLOGY["kv"]["tenants"], zb.TOPOLOGY["generations"]["kv"]):
            await js.create_key_value(KeyValueConfig(bucket=bucket, history=1, storage="memory", direct=True))
        await (await js.key_value(zb.TOPOLOGY["kv"]["tenants"])).put(principal, TENANT.encode())
        await (await js.key_value(zb.TOPOLOGY["kv"]["schemas"])).put(TABLE, json.dumps({
            "pk_columns": ["uid"], "tenant_column": "tenant_id", "version_column": "updated_at",
            "seed_epoch": 1, "lsn": 1, "sqlite": {"columns": COLS}}).encode())
        await js.add_stream(StreamConfig(name=f"CDC_{TENANT}", subjects=[f"cdc.{TENANT}.>"], storage="memory"))
        await js.add_stream(StreamConfig(name="CDC_PUBLIC", subjects=["cdc._default.>"], storage="memory"))
        await js.add_stream(StreamConfig(name="VERDICTS", subjects=["mutation_ack.>"], storage="memory", allow_direct=True))
        # Sequence 1 on both streams: the seed boundary, nothing this client follows.
        await js.publish(f"cdc.{TENANT}.unfollowed.insert", msgpack.packb({"operation": "INSERT", "table": "unfollowed", "data": {}, "lsn": 1}))
        await js.publish("cdc._default.unfollowed.insert", msgpack.packb({"operation": "INSERT", "table": "unfollowed", "data": {}, "lsn": 1}))
        objects = await js.create_object_store(bucket=BUCKET)
        manifests = await js.key_value(zb.TOPOLOGY["generations"]["kv"])
        await objects.put(f"{TABLE}-g1-full", doc("full", 1, W(1), [row(i, 0, W(1)) for i in range(10)]))
        await manifests.put(KEY, json.dumps(manifest(1, 1, W(1), [], 1)).encode())

        # The test's pause: the client asks after selecting; the manifest is swapped, then answered.
        paused = asyncio.Event()
        cut = {"seq": 0}  # the stream's last sequence once the backlog is written: the chains' cut

        async def on_pause(msg):
            # M3: a new base g3 without 0005, and deltas g2 + g3 that still reach the watermark.
            survivors = [row(int(u), n, W(3)) for u, n in final_rows().items()]
            await objects.put(f"{TABLE}-g3-full", doc("full", 3, W(3), survivors))
            await objects.put(f"{TABLE}-g3-delta", doc("delta", 3, W(3), []))
            await manifests.put(KEY, json.dumps(manifest(3, 3, W(3), [
                {"gen": 2, "object": f"{TABLE}-g2-delta", "prev_cutoff": W(1), "cutoff": W(2)},
                {"gen": 3, "object": f"{TABLE}-g3-delta", "prev_cutoff": W(2), "cutoff": W(3)}], cut["seq"])).encode())
            paused.set()
            await msg.respond(b"go")
        pause_subject = f"zbtest.pause.{kind}"
        await nc.subscribe(pause_subject, cb=on_pause)
        await nc.flush()

        client = Client(kind, url, principal, db, log)
        # 1. seed g1, close
        await asyncio.to_thread(client.open, None)
        ok = await asyncio.to_thread(client.wait_rows, lambda r: len(r) == 10, 30)
        checks.append((ok, f"{kind}: seeded M1 (10 rows)"))
        await asyncio.to_thread(client.close)

        # 2. while away: 200 updates, one DELETE of 0005 at the half; M2 eligible
        for k in range(1, UPDATES + 1):
            i = k % 10
            if f"{i:04d}" == DELETED and k > UPDATES // 2:
                continue
            data = dict(zip(NAMES, [f"{i:04d}", k, "y" * 2000, f"2026-10-22T10:00:01.{k:06d}Z", TENANT]))
            await js.publish(f"cdc.{TENANT}.{TABLE}.update", msgpack.packb({"table": TABLE, "operation": "UPDATE", "data": data, "lsn": 1 + k}))
            if k == UPDATES // 2:
                await js.publish(f"cdc.{TENANT}.{TABLE}.delete", msgpack.packb({"table": TABLE, "operation": "DELETE", "data": {"uid": DELETED, "tenant_id": TENANT}, "lsn": 1 + k}))
        last = cut["seq"] = (await js.stream_info(f"CDC_{TENANT}")).state.last_seq
        delta2 = [row(int(u), n, W(2)) for u, n in final_rows().items()]
        await objects.put(f"{TABLE}-g2-delta", doc("delta", 2, W(2), delta2))
        await manifests.put(KEY, json.dumps(manifest(2, 1, W(1), [
            {"gen": 2, "object": f"{TABLE}-g2-delta", "prev_cutoff": W(1), "cutoff": W(2)}], last)).encode())

        # 3-5. reopen with the pause; the swap happens in on_pause
        await asyncio.to_thread(client.open, pause_subject)
        want = final_rows()
        ok = await asyncio.to_thread(client.wait_rows, lambda r: r == want, 60)
        text = log.read_text(errors="replace")
        checks.append(("taking the chains' deltas" in text, f"{kind}: the delta path was selected on M2"))
        checks.append((paused.is_set(), f"{kind}: the manifest was replaced between selection and application"))
        checks.append(("no longer eligible (new-base" in text, f"{kind}: the replaced manifest is refused at application (new-base), nothing applied"))
        checks.append((ok, f"{kind}: the CDC replayed — {DELETED} deleted, every other row at its final value"))
        pos = await asyncio.to_thread(client.position, f"CDC_{TENANT}")
        checks.append((pos == last, f"{kind}: CDC_{TENANT} at the stream's end ({pos} of {last})"))
        await asyncio.to_thread(client.close)
        await nc.close()
    finally:
        server.terminate()
    return checks


class Client:
    """libzb in-process through the C ABI, or zb-client-ts in a Node worker; both write to `log`."""

    def __init__(self, kind, url, principal, db, log):
        self.kind, self.url, self.principal, self.db, self.log = kind, url, principal, db, log
        self.h = None
        self.proc = None
        self.stop = threading.Event()

    def open(self, pause):
        if self.kind == "ts":
            env = dict(os.environ, NATS_URL=self.url, ZB_DB=str(self.db), ZB_TABLES=TABLE, ZB_PRINCIPAL=self.principal)
            env.pop("ZB_TEST_DELTA_PAUSE", None)
            if pause:
                env["ZB_TEST_DELTA_PAUSE"] = pause
            self.proc = subprocess.Popen(["node", "--experimental-strip-types", "follow-worker.ts"], cwd=NODE_DIR, env=env,
                                         stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=self.log.open("a"), text=True)
            self.proc.stdout.readline()
            return
        lib = self.lib = ctypes.CDLL(str(LIBZB))
        lib.zb_free.argtypes = [ctypes.c_void_p]
        lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
        lib.zb_client_close.argtypes = [ctypes.c_uint64]
        for n, extra in (("sync", []), ("poll", [ctypes.c_uint64])):
            f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + extra
        if pause:
            os.environ["ZB_TEST_DELTA_PAUSE"] = pause
        else:
            os.environ.pop("ZB_TEST_DELTA_PAUSE", None)
        # libzb logs to stderr: point fd 2 at the case's log while it runs.
        self.saved = os.dup(2)
        fd = os.open(self.log, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
        os.dup2(fd, 2)
        os.close(fd)
        self.h = lib.zb_client_connect(json.dumps({"natsUrl": self.url, "dbPath": str(self.db), "tables": [TABLE],
                                                   "heartbeatMs": 0, "principal": self.principal, "clientId": self.principal}).encode())
        self.take(lib.zb_client_sync(self.h))
        self.stop.clear()
        self.poller = threading.Thread(target=self.poll_loop, daemon=True)
        self.poller.start()

    def take(self, p):
        try:
            return json.loads(ctypes.string_at(p).decode())
        finally:
            self.lib.zb_free(p)

    def poll_loop(self):
        while not self.stop.is_set():
            self.take(self.lib.zb_client_poll(self.h, 200))

    def rows(self):
        try:
            con = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True, timeout=2)
            try:
                return {u: n for u, n in con.execute(f"SELECT uid, n FROM {TABLE} WHERE tenant_id = ?", (TENANT,))}
            finally:
                con.close()
        except sqlite3.Error:
            return {}

    def wait_rows(self, pred, timeout):
        end = time.time() + timeout
        while time.time() < end:
            if pred(self.rows()):
                return True
            time.sleep(0.2)
        print(f"    {self.kind} rows: {self.rows()}")
        return False

    def position(self, stream):
        con = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True, timeout=2)
        try:
            for table in ("_zbz_stream_seq", "_zebridge_stream_seq"):
                try:
                    r = con.execute(f"SELECT last_seq FROM {table} WHERE stream = ?", (stream,)).fetchone()
                    if r:
                        return r[0]
                except sqlite3.Error:
                    continue
            return None
        finally:
            con.close()

    def close(self):
        if self.kind == "ts":
            if self.proc:
                self.proc.terminate()
                self.proc.wait(timeout=20)
            return
        self.stop.set()
        self.poller.join(timeout=5)
        self.lib.zb_client_close(self.h)
        os.dup2(self.saved, 2)
        os.close(self.saved)


async def main() -> int:
    failed = 0
    with tempfile.TemporaryDirectory(prefix="zb-delta-revalidate-") as folder:
        tmp = pathlib.Path(folder)
        for kind in os.environ.get("ZB_KINDS", "libzb ts").split():
            for ok, label in await run_case(kind, tmp):
                (zb.ok if ok else zb.bad)(label)
                failed += not ok
            if failed:
                print(f"--- {kind} log (tail)")
                print((tmp / f"{kind}.log").read_text(errors="replace")[-3000:])
    print(f"\n{'✗' if failed else '✓'} delta_revalidate: {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

#!/usr/bin/env python3
"""Durable client reconciliation of `indeterminate` writes, for libzb and zb-client-ts.

A private stack (hub_fallback_stage2's: disposable database, nats-server, admission, a
bridge) and a real client of each kind. An `indeterminate` verdict is produced the way it
happens in the field: a write whose ID is older than the receipt minimum and has no
receipt (MessageIdExpired). The admission stream's duplicate window is 0.5 s, so a resend
reaches the listener.

  never committed   queued while the listener is down, then expired: indeterminate; the
                    reseed from the hub removes the optimistic row
  pending kept      a second write, queued while admission is down, survives that reseed
                    (re-applied over the full) and commits once afterwards
  committed         committed while the client was away, its receipt pruned and its
                    stored verdict gone, then resent: indeterminate; the reseed keeps the
                    committed row
  gc gate           a PUBLISHED write below the client's GC watermark is sent, not reverted
                    locally: the bridge answers it from its receipt

Each client must report `indeterminate`, then `reconciled`, with `reconciling` back to 0
and nothing re-sent under a new ID (one execution per write, counted by a BEFORE trigger).
"""
import asyncio, ctypes, json, os, pathlib, signal, sqlite3, subprocess, tempfile, time, uuid

import nats
import zb
from hub_fallback_stage2 import BRIDGE, LIBRARY, Stack, port

KINDS = tuple(os.environ.get("ZB_RECONCILE_CLIENTS", "libzb ts").split())


class Libzb:
    """libzb through its C ABI: outcomes accumulate from each poll."""

    def __init__(self, lib, stack, who, db):
        self.lib, self.stack, self.who, self.db, self.h, self.outcomes = lib, stack, who, db, 0, []

    def take(self, pointer):
        try:
            return json.loads(ctypes.string_at(pointer))
        finally:
            self.lib.zb_free(pointer)

    async def open(self):
        opts = json.dumps({"natsUrl": self.stack.url, "credsPath": self.stack.creds(self.who), "principal": self.who, "jsDomain": "hub",
                           "tables": ["s2_row"], "dbPath": str(self.db), "heartbeatMs": 0}).encode()
        self.h = await asyncio.to_thread(self.lib.zb_client_connect, opts)
        assert self.h, "connect"
        r = await asyncio.to_thread(lambda: self.take(self.lib.zb_client_sync(self.h)))
        assert "error" not in r, r

    async def close(self):
        if self.h:
            self.lib.zb_client_close(self.h)
            self.h = 0

    async def mutate(self, key, v):
        r = await asyncio.to_thread(lambda: self.take(self.lib.zb_client_mutate(self.h, b"s2_row", b"INSERT", json.dumps({"id": key}).encode(), json.dumps({"v": v}).encode())))
        assert "error" not in r, r

    async def step(self):
        await asyncio.to_thread(lambda: self.take(self.lib.zb_client_flush_outbox(self.h, 100)))
        r = await asyncio.to_thread(lambda: self.take(self.lib.zb_client_poll(self.h, 200)))
        self.outcomes += r.get("outcomes", [])
        return r.get("reconciling", 0)

    async def outbox(self):
        return [row[0] for row in self.take(self.lib.zb_client_query(self.h, b"SELECT msg_id FROM _zebridge_outbox", b"[]")).get("rows", [])]

    async def row(self, key):
        rows = self.take(self.lib.zb_client_query(self.h, b"SELECT v FROM s2_row WHERE id = ?", json.dumps([key]).encode())).get("rows", [])
        return rows[0][0] if rows else None


class Ts:
    """zb-client-ts in Node (reconcile_client.mts), one JSON command per line."""

    def __init__(self, stack, who, db):
        self.stack, self.who, self.db, self.proc, self.outcomes = stack, who, db, None, []

    async def call(self, **cmd):
        self.proc.stdin.write((json.dumps(cmd) + "\n").encode())
        await self.proc.stdin.drain()
        r = json.loads(await asyncio.wait_for(self.proc.stdout.readline(), 120))
        if not r["ok"]:
            raise RuntimeError(r["error"])
        return r["value"]

    async def open(self):
        if self.proc is None:
            self.log = open(self.stack.tmp / f"{self.stack.name}-ts-{self.who}.log", "a")
            self.proc = await asyncio.create_subprocess_exec(
                "node", "--experimental-strip-types", "--no-warnings", str(zb.ROOT / "scripts/scenarios/reconcile_client.mts"),
                self.stack.url, self.stack.creds(self.who), str(self.db), self.who, "s2_row",
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=self.log)
        await self.call(cmd="open")

    async def close(self):
        if self.proc:
            await self.call(cmd="close")

    async def mutate(self, key, v):
        await self.call(cmd="mutate", table="s2_row", op="INSERT", key={"id": key}, values={"v": v})

    async def step(self):
        await self.call(cmd="flush")
        await asyncio.sleep(.2)
        s = await self.call(cmd="state")
        self.outcomes += s["verdicts"]
        return s["reconciling"]

    async def outbox(self):
        return (await self.call(cmd="state"))["outbox"]

    async def row(self, key):
        rows = await self.call(cmd="sql", sql=f"SELECT v FROM s2_row WHERE id = '{key}'")
        return rows[0]["v"] if rows else None

    async def quit(self):
        if self.proc:
            self.proc.stdin.close()
            await self.proc.wait()


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

    with tempfile.TemporaryDirectory(prefix="zb-reconcile-") as folder:
        tmp = pathlib.Path(folder)
        hub = Stack(tmp)
        clients = []
        try:
            await hub.start()
            admin = await nats.connect(hub.url, user_credentials=hub.bridge_creds)
            js = admin.jetstream(domain="hub")
            info = await js.stream_info("MUTATIONS")
            info.config.duplicate_window = 0.5
            await js.update_stream(config=info.config)

            async def bridge_stop():
                hub.procs["bridge"].terminate()
                hub.procs["bridge"].wait(timeout=15)

            async def bridge_start():
                hub.spawn("bridge", [str(BRIDGE), "--pub", "hf2_pub", "--slot", hub.name + "_slot", "--port", str(port())], hub.env)
                await hub.wait_log("bridge", "Ready! Pulling mutations")

            async def admission_stop():
                hub.procs["admission"].terminate()
                hub.procs["admission"].wait(timeout=15)

            async def admission_start():
                hub.start_admission()
                await hub.wait_log("admission", "mutation admission ready")

            async def expire_until_now():
                """Every ID created before now is past the receipt minimum."""
                hub.sql(f"UPDATE zebridge_private.receipt_policy SET min_accepted_ms = {int(time.time() * 1000) + 300}")
                await asyncio.sleep(1)

            def receipt_id(who, key):
                return hub.sql(f"SELECT message_id FROM zebridge_private.receipts WHERE principal = '{who}' AND message_id LIKE '%{key.replace('-', '~2D')}%'")

            async def until(c, pred, timeout=90):
                end = time.monotonic() + timeout
                while time.monotonic() < end:
                    n = await c.step()
                    if await pred(n):
                        return True
                return False

            def outcomes_for(c, key):
                return [o.get("outcome") for o in c.outcomes if (o.get("key") or {}).get("id") == key or key.replace("-", "~2D") in o.get("msgId", "")]

            for kind in KINDS:
                who = "bob" if kind == "libzb" else "alice"
                db = tmp / f"{kind}.sqlite3"
                c = Libzb(lib, hub, who, db) if kind == "libzb" else Ts(hub, who, db)
                clients.append(c)
                await c.open()
                kb, kp, ka, kg = (str(uuid.uuid4()) for _ in range(4))

                # ── never committed, with another write pending through the reseed ──
                await bridge_stop()
                await c.mutate(kb, 10)
                await expire_until_now()
                await admission_stop()
                await c.mutate(kp, 30)
                await bridge_start()
                ok = await until(c, lambda n: _done(c, kb, n))
                got = outcomes_for(c, kb)
                check(ok and got[:2] == ["indeterminate", "reconciled"],
                      f"{kind}: a write that never committed is answered indeterminate, then reconciled ({got})")
                if not (ok and got[:2] == ["indeterminate", "reconciled"]) and isinstance(c, Ts):
                    c.log.flush()
                    lines = (hub.tmp / f"{hub.name}-ts-{who}.log").read_text(errors="replace").splitlines()
                    print("\n".join(l[:300] for l in lines if any(t in l for t in ("reconcil", "rewound", "CDC", "Seed", "seed", "gap", "Gap", "indeterminate")))[-6000:])
                check(await c.row(kb) is None and hub.executions("s2_row", kb) == 0,
                      f"{kind}: the reseed from the hub removed its optimistic row; it never ran")
                pend = await c.outbox()
                check(await c.row(kp) == 30 and len(pend) == 1,
                      f"{kind}: the other pending write survived the full reseed (row re-applied, still queued: {len(pend)})")
                await admission_start()
                ok = await until(c, lambda n: _applied(c, kp))
                check(ok and hub.executions("s2_row", kp) == 1 and await c.row(kp) == 30 and not await c.outbox(),
                      f"{kind}: it then commits once and clears the outbox")

                # ── an earlier attempt committed: receipt pruned, verdict gone ──
                await bridge_stop()
                await c.mutate(ka, 20)
                await c.close()
                await bridge_start()
                for _ in range(300):
                    if hub.executions("s2_row", ka) == 1 and receipt_id(who, ka):
                        break
                    await asyncio.sleep(.1)
                mid = receipt_id(who, ka)
                hub.sql(f"DELETE FROM zebridge_private.receipts WHERE message_id = '{mid}'")
                await js.purge_stream("VERDICTS", subject=f"mutation_ack.{who}.{mid}")
                await expire_until_now()
                await c.open()
                ok = await until(c, lambda n: _done(c, ka, n))
                got = outcomes_for(c, ka)
                check(ok and mid and got[:2] == ["indeterminate", "reconciled"],
                      f"{kind}: a write that committed while its client was away, receipt pruned, is answered indeterminate, then reconciled ({got})")
                check(await c.row(ka) == 20 and hub.executions("s2_row", ka) == 1 and not await c.outbox(),
                      f"{kind}: the reseed kept the committed row; it ran once and was never re-sent under a new ID")

                # ── the GC gate: a published write is the bridge's to judge ──
                await bridge_stop()
                await c.mutate(kg, 40)
                await c.close()
                await bridge_start()
                for _ in range(300):
                    if hub.executions("s2_row", kg) == 1 and receipt_id(who, kg):
                        break
                    await asyncio.sleep(.1)
                gmid = receipt_id(who, kg)
                await js.purge_stream("VERDICTS", subject=f"mutation_ack.{who}.{gmid}")
                with sqlite3.connect(db) as lite:
                    lite.execute("CREATE TABLE IF NOT EXISTS zebridge_gc_watermark (watermark TEXT)")
                    lite.execute("DELETE FROM zebridge_gc_watermark")
                    lite.execute("INSERT INTO zebridge_gc_watermark VALUES ('2999-01-01T00:00:00.000000Z')")
                await asyncio.sleep(1)  # past the 0.5 s duplicate window
                await c.open()
                ok = await until(c, lambda n: _applied(c, kg))
                check(ok and await c.row(kg) == 40 and hub.executions("s2_row", kg) == 1,
                      f"{kind}: a published write below the client's GC watermark is sent and answered from its receipt, not reverted ({outcomes_for(c, kg)})")
                await c.close()
                with sqlite3.connect(db) as lite:
                    lite.execute("DROP TABLE zebridge_gc_watermark")
            await admin.close()
        finally:
            for c in clients:
                try:
                    if isinstance(c, Ts):
                        await c.quit()
                    else:
                        await c.close()
                except Exception:
                    pass
            hub.stop()
    print(f"{passed} passed, {failed} failed")
    return 1 if failed else 0


async def _done(c, key, reconciling):
    got = [o.get("outcome") for o in c.outcomes if _matches(o, key)]
    return "reconciled" in got and reconciling == 0


async def _applied(c, key):
    return "applied" in [o.get("outcome") for o in c.outcomes if _matches(o, key)]


def _matches(o, key):
    k = o.get("key")
    return (isinstance(k, dict) and k.get("id") == key) or key.replace("-", "~2D") in o.get("msgId", "")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

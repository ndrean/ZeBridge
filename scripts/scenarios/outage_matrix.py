#!/usr/bin/env python3
"""What an outage of one component does: the README's table, played on a private stack.

README "Scaling and availability" states the expected effect of each component being unavailable. This
takes each away from a running hub in turn — with a real libzb client following the table and writing to
it, and PostgreSQL as the only truth — and checks both halves: the documented effect while it is down,
and that everything converges when it is back.

  A  bridge down      PostgreSQL changes stop reaching the client; a write the client sends is queued but
                      not executed (mutation execution pauses). Back: the changes arrive, the queued
                      write is executed, the client's outbox drains, replica == PostgreSQL.
  B  admission down   the mutation gateway (`bridge --mutation-ingress`) is the only door into the
                      private queue: a write cannot enter (the publish gets no PubAck) and the client
                      keeps it in its outbox; CDC still flows meanwhile. Back: the outbox delivers.
  C  sweeper down     cleanup pauses (a ripe tombstone stays) and nothing else is affected. Back: the
                      backlog is reaped.
  D  hub NATS down    CDC and mutation transport stop; the client keeps reading and writing its replica
                      locally (the write waits in the outbox); the bridge survives. Back: both reconnect,
                      the changes arrive, the write lands, replica == PostgreSQL.

What this does NOT show: how long a component may stay down. Queue retention and storage limits are
finite (CDC max age and bytes, the admission queue's per-principal and stream caps, the object store);
a long outage past them is a different scenario (stream_full, cdc_wall, hub_recovery). Leaves serving
cached seeds during a hub outage: leaf_fence, hub_fallback.

Usage:  python scripts/scenarios/outage_matrix.py
Needs PostgreSQL superuser at 127.0.0.1:5432, nats-server, zig-out/bin/bridge and bridge_sweeper, libzb.
"""
import asyncio, ctypes, json, os, pathlib, subprocess, sys, tempfile, time

import nats

import zb
from recreate_recovery import Stack, T, check, note
import recreate_recovery as rr
from gc_isolation import PRINCIPAL, TENANT, Sweeper, HOUR_MS, plant, present, version_ago, Writer

POLL_MS = 300


class LibClient:
    """The libzb client in this process: host-driven, every observation a poll."""
    def __init__(self, stack, tmp):
        self.lib = lib = ctypes.CDLL(str(rr.LIBRARY))
        lib.zb_free.argtypes = [ctypes.c_void_p]
        lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
        lib.zb_client_close.argtypes = [ctypes.c_uint64]
        for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p]),
                     ("flush_outbox", [ctypes.c_uint64]), ("mutate", [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p])):
            f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a
        self.db = str(tmp / "outage-client.sqlite3")
        self.h = lib.zb_client_connect(json.dumps({
            "natsUrl": stack.url, "credsPath": stack.creds[PRINCIPAL], "principal": PRINCIPAL, "jsDomain": "hub",
            "clientId": "outage-client", "dbPath": self.db, "tables": [T], "heartbeatMs": 0}).encode())
        if not self.h: sys.exit("libzb open failed")
        r = self.take(lib.zb_client_sync(self.h))
        # The other private-stack workers ignore the first sync's answer too: PrepareFailed is reported by the
        # first sync on these stacks (it fails identically in multi_bridge.py on main) and the next polls seed.
        if r.get("error"): note(f"libzb's first sync answered {r['error']} ({r.get('detail')}); seeding goes on from the polls")

    def take(self, p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: self.lib.zb_free(p)

    def poll(self, ms=POLL_MS):
        return self.take(self.lib.zb_client_poll(self.h, ms))

    def q(self, sql, params=()):
        r = self.take(self.lib.zb_client_query(self.h, sql.encode(), json.dumps(list(params)).encode()))
        return r.get("rows") if "error" not in r else None

    def mutate(self, op, key, values=None):
        return self.take(self.lib.zb_client_mutate(self.h, T.encode(), op.encode(), json.dumps(key).encode(),
                                                  json.dumps(values).encode() if values is not None else None))

    def flush(self, ms=1500):
        return self.take(self.lib.zb_client_flush_outbox(self.h, ms))

    def live(self, prefix):
        r = self.q(f"SELECT count(*) FROM {T} WHERE note LIKE ? AND deleted_at IS NULL", [prefix + "%"])
        return r[0][0] if r else -1

    def outbox(self):
        r = self.q("SELECT count(*) FROM _zebridge_outbox")
        return r[0][0] if r else -1

    def close(self):
        self.lib.zb_client_close(self.h)


def pg_live(stack, prefix):
    return int(stack.sql(f"SELECT count(*) FROM public.{T} WHERE note LIKE '{prefix}%' AND deleted_at IS NULL"))


def pg_insert(stack, rid, note_text):
    stack.sql(f"INSERT INTO public.{T} (id, tenant_id, note) VALUES ('{rid}', '{TENANT}', '{note_text}')")


async def until(pred, timeout, client=None, step=0.3):
    """Poll the client (when given) until `pred()`, or the timeout. The last answer."""
    end = time.monotonic() + timeout
    while True:
        if client is not None:
            client.poll(int(step * 1000)); client.flush(200)
        v = pred()
        if v or time.monotonic() >= end: return v
        if client is None: await asyncio.sleep(step)


async def stays_false(pred, seconds, client):
    """The effect that must NOT happen: `pred()` is false for the whole window."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        client.poll(300)
        if pred(): return False
    return True


async def converged(stack, client, prefix, label, timeout=90):
    ok = await until(lambda: client.live(prefix) == pg_live(stack, prefix) and client.outbox() == 0, timeout, client)
    check(f"{label}: the replica equals PostgreSQL ({pg_live(stack, prefix)} live '{prefix}*' rows) and the outbox is empty", ok)
    return ok


async def run(tmp):
    stack = Stack(tmp, "outage", 120, {})
    sweepers, client = [], None
    gateway_log = tmp / "gateway.log"
    gateway = None

    def start_gateway():
        nonlocal gateway
        gateway = subprocess.Popen([str(rr.BRIDGE), "--mutation-ingress"], env=stack.env, stdout=subprocess.DEVNULL, stderr=open(gateway_log, "a"))
        end = time.monotonic() + 30
        while time.monotonic() < end and "mutation admission ready" not in gateway_log.read_text(errors="replace"):
            if gateway.poll() is not None: raise RuntimeError(gateway_log.read_text()[-1500:])
            time.sleep(0.2)

    def stop_gateway(sig=None):
        nonlocal gateway
        if gateway and gateway.poll() is None:
            gateway.kill() if sig == "kill" else gateway.terminate()
            gateway.wait(timeout=15)

    try:
        stack.start({PRINCIPAL: TENANT})
        stack.env["MUTATION_ADMISSION"] = "1"   # production shape: writes enter through the gateway's private queue
        stack.create_table({TENANT: 3}, "o")
        start_gateway()
        b1 = stack.bridge_up()
        js = await rr.Js().open(stack)
        ready = await js.tenants_ready([PRINCIPAL])   # a client that connects before its mapping is published skips every tenant table
        schemas, t0 = zb.kv_bucket("schemas"), time.monotonic()
        while await js.kv(schemas, T) is None and time.monotonic() - t0 < 60:   # nor before the table's descriptor
            await asyncio.sleep(0.5)
        await js.close()
        check("setup: the tenant mapping and the table's schema are published before the client connects", ready and time.monotonic() - t0 < 60)
        client = LibClient(stack, tmp)
        await until(lambda: len(client.q(f"SELECT id FROM {T}") or []) >= 3, 60, client)
        check("setup: admission on, the gateway and the bridge up, the client holds the 3 seeded rows", len(client.q(f"SELECT id FROM {T}") or []) >= 3)
        pg_insert(stack, "o-live", "outage-0 live")
        check("setup: a PostgreSQL change reaches the client", await until(lambda: client.live("outage-0") == 1, 30, client))
        client.mutate("INSERT", {"id": "o-mut"}, {"id": "o-mut", "tenant_id": TENANT, "note": "outage-0 mut"})
        check("setup: a client write reaches PostgreSQL through the gateway and the bridge",
              await until(lambda: pg_live(stack, "outage-0") == 2 and client.outbox() == 0, 30, client))

        # ── A. the bridge ───────────────────────────────────────────────────────────────
        print("── A. the bridge is down", flush=True)
        stack.bridge_down(b1)
        pg_insert(stack, "a-cdc", "outage-a cdc")
        client.mutate("INSERT", {"id": "a-mut"}, {"id": "a-mut", "tenant_id": TENANT, "note": "outage-a mut"})
        check("A: a PostgreSQL change does not reach the client while the bridge is down", await stays_false(lambda: client.live("outage-a cdc") > 0, 6, client))
        check("A: the client's write is queued, not executed: PostgreSQL does not have it", pg_live(stack, "outage-a mut") == 0)
        check("A: and it waits in the client's outbox for its verdict", client.outbox() >= 1)
        b2 = stack.bridge_up()
        await converged(stack, client, "outage-a", "A: bridge back")
        check("A: both the change and the queued write arrived", pg_live(stack, "outage-a") == 2 and client.live("outage-a") == 2)

        # ── B. admission ────────────────────────────────────────────────────────────────
        print("── B. mutation admission is down", flush=True)
        stop_gateway()
        writer = await Writer().open(stack)
        entered = True
        try:
            await writer.insert("b-raw", version_ago(stack, "1 minute"))
        except Exception as e:
            entered = False
            note(f"B: a direct publish is refused: {type(e).__name__}")
        await writer.close()
        check("B: a write cannot enter the queue (no PubAck from the gateway)", not entered)
        client.mutate("INSERT", {"id": "b-mut"}, {"id": "b-mut", "tenant_id": TENANT, "note": "outage-b mut"})
        pg_insert(stack, "b-cdc", "outage-b cdc")
        check("B: CDC is unaffected: a PostgreSQL change still reaches the client", await until(lambda: client.live("outage-b cdc") == 1, 30, client))
        check("B: the client keeps its write: not in PostgreSQL, still in the outbox", pg_live(stack, "outage-b mut") == 0 and client.outbox() >= 1)
        start_gateway()
        await converged(stack, client, "outage-b", "B: admission back", timeout=120)

        # ── C. the sweeper ──────────────────────────────────────────────────────────────
        print("── C. the sweeper is down", flush=True)
        sw = Sweeper(stack, "matrix1", HOUR_MS, interval_ms=500); sweepers.append(sw)
        plant(stack, "c-old1", "2 hours")
        check("C: running, the sweeper reaps a ripe tombstone", await until(lambda: not present(stack, "c-old1"), 20))
        sw.p.kill(); sw.p.wait(timeout=10)
        plant(stack, "c-old2", "2 hours")
        await asyncio.sleep(5)
        check("C: with the sweeper down the next ripe tombstone stays (cleanup pauses)", present(stack, "c-old2"))
        pg_insert(stack, "c-cdc", "outage-c cdc")
        check("C: everything else is unaffected: CDC still flows", await until(lambda: client.live("outage-c cdc") == 1, 30, client))
        sw2 = Sweeper(stack, "matrix2", HOUR_MS, interval_ms=500); sweepers.append(sw2)
        check("C: the sweeper back reaps the backlog", await until(lambda: not present(stack, "c-old2"), 30))

        # ── D. the hub NATS ─────────────────────────────────────────────────────────────
        print("── D. the hub NATS is down", flush=True)
        n = stack.procs.pop("nats"); n.terminate(); n.wait(timeout=20)
        pg_insert(stack, "d-cdc", "outage-d cdc")
        client.mutate("INSERT", {"id": "d-mut"}, {"id": "d-mut", "tenant_id": TENANT, "note": "outage-d mut"})
        client_reads = client.q(f"SELECT count(*) FROM {T}")
        check("D: the client still reads its replica locally", bool(client_reads) and client_reads[0][0] >= 3)
        check("D: the client's write waits in its outbox (PostgreSQL does not have it) and the PostgreSQL change has not reached the client",
              client.outbox() >= 1 and pg_live(stack, "outage-d mut") == 0 and client.live("outage-d cdc") == 0)
        await asyncio.sleep(6)
        check("D: the bridge survives the outage", all(p.poll() is None for k, p in stack.procs.items() if k.startswith("bridge")))
        stack.spawn("nats", ["nats-server", "-c", str(tmp / f"{stack.name}.conf")], dict(os.environ))
        stack.wait_log("nats", "Server is ready")
        await converged(stack, client, "outage-d", "D: hub NATS back", timeout=150)
        check("D: the bridge reconnected by itself, no restart", all(p.poll() is None for k, p in stack.procs.items() if k.startswith("bridge")))
    finally:
        for s in sweepers: s.stop()
        if client:
            try: client.close()
            except Exception: pass
        stop_gateway()
        stack.stop()


def main():
    with tempfile.TemporaryDirectory(prefix="zb-outage-") as d:
        try:
            asyncio.run(run(pathlib.Path(d)))
        except Exception as e:
            check(f"raised: {type(e).__name__}: {str(e)[:600]}", False)
    print()
    if rr.failed:
        zb.bad(f"{rr.failed} check(s) failed")
        return 1
    zb.ok("each component's outage has the documented effect, and the hub converges when it is back")
    return 0


if __name__ == "__main__":
    sys.exit(main())

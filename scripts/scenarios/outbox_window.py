#!/usr/bin/env python3
"""A client far from its server is not one round trip per write.

The outbox used to publish each write and wait for its PubAck before the next, and
`mutate()` sent at once: one client wrote at most one write per round trip to NATS
(26 writes/s 23 ms away, measured against the hub at 31/s). Now a window of writes is
sent, and `mutate()` does not wait for the acks.

Against the RUNNING stack, through a proxy this scenario starts in front of NATS that
delays every chunk by 11.5 ms each way (a 23 ms round trip):

  1. one libzb client calls mutate() 100 times, within the 128-write window, so every
     call publishes and the loop's time is the publication time: less than half of 100
     round trips (one per write took about one round trip each);
  2. every write is accepted, and PostgreSQL holds all 100 rows; the time to the last
     verdict is reported, not judged.

Its rows are tombstoned at the end.
"""
import asyncio, os, socket, sys, threading, time
import zb
from clients import Lib, fresh_sqlite
from drip import payload

T, N, DELAY_MS = "test_types", 100, 11.5   # N within the 128-write window: every write is published before any verdict
DB = "/tmp/zb-outbox-window.sqlite3"


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0)); return s.getsockname()[1]


def start_proxy(listen, host, port, delay):
    """Every chunk delayed by `delay` seconds in each direction, order kept."""
    async def pipe(reader, writer):
        q = asyncio.Queue()
        async def send():
            while True:
                due, data = await q.get()
                if data is None: break
                if (w := due - time.monotonic()) > 0: await asyncio.sleep(w)
                writer.write(data); await writer.drain()
            writer.close()
        sender = asyncio.create_task(send())
        try:
            while data := await reader.read(65536): q.put_nowait((time.monotonic() + delay, data))
        finally:
            q.put_nowait((0, None)); await sender

    async def handle(cr, cw):
        sr, sw = await asyncio.open_connection(host, port)
        await asyncio.gather(pipe(cr, sw), pipe(sr, cw), return_exceptions=True)

    async def serve():
        async with await asyncio.start_server(handle, "127.0.0.1", listen) as server:
            await server.serve_forever()

    threading.Thread(target=lambda: asyncio.run(serve()), daemon=True).start()


async def main():
    failed = 0

    def check(label, cond, detail=""):
        nonlocal failed
        (zb.ok if cond else zb.bad)(label + ("" if cond else f"\n      {detail}"))
        failed += 0 if cond else 1

    scheme, _, rest = zb.nats_server().partition("://")
    host, _, port = rest.rpartition(":")
    listen = free_port()
    start_proxy(listen, "127.0.0.1" if host in ("localhost", "127.0.0.1") else host, int(port), DELAY_MS / 1000)
    zb.NATS_URL = f"{scheme}://{host}:{listen}"
    fresh_sqlite(DB)
    try: os.unlink(DB + ".lock")
    except FileNotFoundError: pass

    em = Lib(DB, [T], "py-outbox-window", principal="bob")
    tag = f"ow-{int(time.time())}"
    mids = []
    t0 = time.monotonic()
    for i in range(N):
        uid, row = payload(i, em.tenant)
        row["some_text"] = f"{tag}-{i:04d}"
        mids.append(em.mutate(T, "INSERT", {"uid": uid}, row).get("msgId"))
    loop_s = time.monotonic() - t0
    budget = N * 2 * DELAY_MS / 1000 / 2
    check(f"1. {N} mutate() calls 23 ms from NATS took {loop_s:.2f} s (one round trip per write: about {N * 2 * DELAY_MS / 1000:.1f} s; budget {budget:.1f} s)",
          loop_s < budget, "mutate() waits for each write's PubAck again")

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline and sum(1 for m in mids if em.outcome(m)) < N:
        em.poll(100); em.flush(100)
    verdict_s = time.monotonic() - t0
    outcomes = [(em.outcome(m) or {}).get("outcome") for m in mids]
    in_pg = zb.psql(f"SELECT count(*) FROM {T} WHERE some_text LIKE '{tag}-%'", quiet=True).strip()
    check(f"2. every write accepted in {verdict_s:.2f} s from the first mutate() ({outcomes.count('applied')} of {N} applied, {sum(1 for o in outcomes if o is None)} without a verdict), PostgreSQL holds {in_pg}",
          outcomes.count("applied") == N and in_pg == str(N), str({o: outcomes.count(o) for o in set(outcomes)}))
    em.close()
    zb.psql(f"UPDATE {T} SET deleted_at = now() WHERE some_text LIKE '{tag}-%'", quiet=True)
    return 1 if failed else 0


zb.run(main)

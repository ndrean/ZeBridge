#!/usr/bin/env python3
"""A slow link for ONE client, without root: a TCP proxy that delays, throttles and stalls.

    scripts/scenarios/.venv/bin/python scripts/scenarios/slowproxy.py                     # :14222 → :4222, 20 Mbit/s, 30 ms each way
    scripts/scenarios/.venv/bin/python scripts/scenarios/slowproxy.py --mbit 5 --delay-ms 80 --stall-every-s 10

A follower connects to `nats://127.0.0.1:14222` instead of `:4222`; everything else (the
bridge, the services) keeps talking to NATS directly and is not slowed. Each direction
has its own pipe: every chunk read is held until `delay` after it arrived (order kept),
then written no faster than `mbit`. `--stall-every-s` freezes one direction for
`--stall-ms` at random moments, about once per that many seconds — the visible effect of a
lossy link on TCP (a retransmission timeout), which a proxy above TCP cannot drop packets
to produce.

Why not pf/dummynet (slowlink.sh): on macOS the Mac's traffic to its own LAN address
never met the dummynet rule (16,053 evaluations on lo0, 0 matches; a rule without `on lo0`
was written but its effect not confirmed — NOTES §10jh), and each attempt needs sudo. This
needs no root and shapes exactly one client's connection.
"""
import argparse, asyncio, random, time


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, delay: float, bps: float,
               stall_every: float, stall: float, label: str, stats: dict):
    q: asyncio.Queue = asyncio.Queue()

    async def pump_in():
        while True:
            data = await reader.read(64 * 1024)
            await q.put((time.monotonic() + delay, data))
            if not data:
                return

    async def pump_out():
        next_stall = time.monotonic() + random.expovariate(1 / stall_every) if stall_every > 0 else float("inf")
        while True:
            due, data = await q.get()
            if not data:
                break
            now = time.monotonic()
            if now < due:
                await asyncio.sleep(due - now)
            if time.monotonic() >= next_stall:
                stats["stalls"] += 1
                await asyncio.sleep(stall)
                next_stall = time.monotonic() + random.expovariate(1 / stall_every)
            writer.write(data)
            await writer.drain()
            stats[label] += len(data)
            if bps > 0:
                await asyncio.sleep(len(data) * 8 / bps)  # the link's pace: bytes at `mbit`
        writer.close()

    try:
        await asyncio.gather(pump_in(), pump_out())
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        writer.close()


async def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--listen", default="127.0.0.1:14222")
    ap.add_argument("--target", default="127.0.0.1:4222")
    ap.add_argument("--mbit", type=float, default=20.0, help="bandwidth each way, Mbit/s (0 = unlimited)")
    ap.add_argument("--delay-ms", type=float, default=30.0, help="one-way delay, each direction")
    ap.add_argument("--stall-every-s", type=float, default=0.0, help="about one stall per this many seconds (0 = none)")
    ap.add_argument("--stall-ms", type=float, default=300.0)
    a = ap.parse_args()
    lh, lp = a.listen.rsplit(":", 1)
    th, tp = a.target.rsplit(":", 1)
    stats = {"down": 0, "up": 0, "stalls": 0, "conns": 0}

    async def on_client(cr, cw):
        stats["conns"] += 1
        try:
            sr, sw = await asyncio.open_connection(th, int(tp))
        except OSError:
            cw.close()
            return
        d, bps = a.delay_ms / 1000, a.mbit * 1e6
        await asyncio.gather(
            pipe(cr, sw, d, bps, a.stall_every_s, a.stall_ms / 1000, "up", stats),     # client → NATS
            pipe(sr, cw, d, bps, a.stall_every_s, a.stall_ms / 1000, "down", stats),   # NATS → client
        )

    server = await asyncio.start_server(on_client, lh, int(lp))
    print(f"slow link {a.listen} → {a.target}: {a.mbit:g} Mbit/s, {a.delay_ms:g} ms each way"
          + (f", a {a.stall_ms:g} ms stall about every {a.stall_every_s:g} s" if a.stall_every_s else ""), flush=True)
    async with server:
        while True:
            await asyncio.sleep(10)
            print(f"connections {stats['conns']}, down {stats['down'] / 1e6:.1f} MB, up {stats['up'] / 1e6:.1f} MB, "
                  f"stalls {stats['stalls']}", flush=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

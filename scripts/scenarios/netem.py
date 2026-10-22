#!/usr/bin/env python3
"""A TCP relay that stands in for a long link, without root (no dnctl, no tc).

    netem.py --listen 14280 --to 127.0.0.1:14272 --delay-ms 40 --rate-mbit 50

Each direction delays every chunk by --delay-ms (one way, so the round trip is twice
that) and paces bytes to --rate-mbit, preserving order. SIGUSR1 cuts the link: every
connection is closed and new ones are refused until SIGUSR2 restores it. A NATS leaf
reconnects by itself once the link is back, which is what a lagging mirror needs.
A line per state change on stdout: `cut`, `restored`, `conn N`.
"""
import argparse, asyncio, signal, time


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", type=int, required=True)
    ap.add_argument("--to", required=True, help="host:port")
    ap.add_argument("--delay-ms", type=float, default=0)
    ap.add_argument("--rate-mbit", type=float, default=0, help="0: unlimited")
    a = ap.parse_args()
    host, port = a.to.rsplit(":", 1)
    delay = a.delay_ms / 1000
    rate = a.rate_mbit * 1e6 / 8  # bytes per second, per direction
    up = True
    conns: set = set()
    n = 0

    async def pipe(reader, writer):
        # Ordered delivery: chunks are stamped on arrival and released no earlier than
        # stamp + delay, and no faster than the rate allows.
        queue: asyncio.Queue = asyncio.Queue()

        async def read():
            try:
                while chunk := await reader.read(65536):
                    await queue.put((time.monotonic() + delay, chunk))
            finally:
                await queue.put((0, b""))

        async def write():
            free_at = 0.0
            while True:
                due, chunk = await queue.get()
                if not chunk:
                    break
                now = time.monotonic()
                if due > now:
                    await asyncio.sleep(due - now)
                if rate:
                    start = max(time.monotonic(), free_at)
                    free_at = start + len(chunk) / rate
                    if free_at > time.monotonic():
                        await asyncio.sleep(free_at - time.monotonic())
                writer.write(chunk)
                await writer.drain()

        try:
            await asyncio.gather(read(), write())
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()

    async def handle(cr, cw):
        nonlocal n
        if not up:
            cw.close()
            return
        try:
            sr, sw = await asyncio.open_connection(host, int(port))
        except OSError:
            cw.close()
            return
        n += 1
        print(f"conn {n}", flush=True)
        task = asyncio.gather(pipe(cr, sw), pipe(sr, cw))
        conns.add((task, cw, sw))
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            conns.discard((task, cw, sw))

    def cut():
        nonlocal up
        up = False
        for task, cw, sw in list(conns):
            task.cancel()
            cw.close()
            sw.close()
        print("cut", flush=True)

    def restore():
        nonlocal up
        up = True
        print("restored", flush=True)

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGUSR1, cut)
    loop.add_signal_handler(signal.SIGUSR2, restore)
    server = await asyncio.start_server(handle, "127.0.0.1", a.listen)
    print(f"listening {a.listen} → {a.to}, delay {a.delay_ms} ms one way, rate {a.rate_mbit or '∞'} Mbit/s", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

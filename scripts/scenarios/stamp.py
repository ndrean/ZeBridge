#!/usr/bin/env python3
"""The capacity stamp — saturated, fault-free, FLAT for three minutes (§10cs).

The flood bench measures a drain; this measures a STEADY STATE: a feeder keeps
the MUTATIONS backlog deep for the whole window while a sampler buckets the
applied rate every 10 s. No faults, no pacing, no clients — the one instrument
whose number belongs in a README capacity claim. (Soaks measure convergence
under abuse; floods measure drains and lie below ~4× ceiling depth — §10cs.)

A dedicated `zb_stamp` table keeps the millions out of test_types, and the
§10cq funeral teardown (catalogue row out while the bridge watches, drop prune
consumed, keys purged) takes the ~2M CDC events down with it.

Usage:  ZB_INGRESS_LANES=2 python scripts/scenarios/stamp.py [seconds]
Manual group: minutes of machine and millions of rows, not a pass/fail verdict —
though it FAILs on a sagging line (any bucket after warmup < 60% of the mean).
"""

import asyncio
import ctypes
import json
import os
import subprocess
import sys
import threading
import time
import uuid

import msgpack
import zb

sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

DURATION_S = int(sys.argv[1]) if len(sys.argv) > 1 else 180
BACKLOG_TARGET = 60_000
CHUNK = 500
LOG = os.environ.get("TMPDIR", "/tmp") + "/zb_stamp_bridge.log"


def psql(q):
    return zb.psql(q)


async def main():
    failed = 0
    who = zb.require_principal()
    lanes = os.environ.get("ZB_INGRESS_LANES", "1")

    psql("""
        CREATE TABLE IF NOT EXISTS zb_stamp (
            uid uuid PRIMARY KEY, n bigint NOT NULL,
            inserted_at timestamptz NOT NULL, updated_at timestamptz NOT NULL)
    """)
    out = psql("SELECT step || ':' || status FROM zebridge_enable('public.zb_stamp', "
               "writable => true, version_col => 'updated_at', allow_physical_deletes => true, "
               "generations => false, public_reason => 'capacity stamp', "
               f"publication => '{zb.publication()}', dry_run => false)")
    if "error" in out.lower():
        sys.exit(f"enable failed: {out}")
    psql("SELECT zebridge_grant_edge_writes('public.zb_stamp')")

    with zb.Bridge(LOG) as br:
        if not br.wait_for_log("Replication started successfully", timeout=90):
            sys.exit("bridge did not start")
        # ── the CONSUMER: the loop is only real if someone drinks from it ──
        # One libzb client applying the CDC egress for the whole window — its
        # sustained rate is the DOWNSTREAM figure (the axis Electric/PowerSync
        # quote ~5k/s on), measured simultaneously with ingress.
        lib = _env.load_lib()
        lib.zb_free.argtypes = [ctypes.c_void_p]
        lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
        lib.zb_client_close.argtypes = [ctypes.c_uint64]
        for fn, a in [("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])]:
            f = getattr(lib, "zb_client_" + fn)
            f.restype = ctypes.c_void_p
            f.argtypes = [ctypes.c_uint64] + a

        def take(ptr):
            if not ptr:
                return {}
            try:
                return json.loads(ctypes.string_at(ptr).decode())
            finally:
                lib.zb_free(ptr)

        cdb = os.environ.get("TMPDIR", "/tmp") + "/zb-stamp-consumer.sqlite3"
        for suf in ("", "-wal", "-shm"):
            try:
                os.remove(cdb + suf)
            except FileNotFoundError:
                pass
        h = lib.zb_client_open(json.dumps({
            "url": zb.nats_server(), "credsPath": zb.creds_for(who),
            "grammarPath": str(zb.ROOT / "src" / "grammar.json"), "dbPath": cdb,
            "principal": who, "clientId": "stamp-consumer",
            "tables": ["zb_stamp"]}).encode())
        if not h:
            sys.exit("consumer open failed")
        take(lib.zb_client_sync(h))
        consumer_stop = threading.Event()

        def consumer_loop():
            while not consumer_stop.is_set():
                take(lib.zb_client_poll(h, 400))

        consumer_thread = threading.Thread(target=consumer_loop, daemon=True)
        consumer_thread.start()

        def consumer_count():
            r = take(lib.zb_client_query(h, b"SELECT count(*) AS n FROM zb_stamp", b"[]"))
            rows = r.get("rows") if isinstance(r, dict) else None
            return int(rows[0][0]) if rows else 0

        nc = await zb.connect_as(who)
        js = nc.jetstream()
        subject = zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, "zb_stamp", "insert")

        published = 0

        def batch(k):
            out = []
            base = time.time()
            for i in range(k):
                uid = str(uuid.uuid4())
                v = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(base)) + f".{(published + i) % 1000000:06d}Z"
                out.append((f"st-{uid[:18]}", msgpack.packb(
                    {"key": {"uid": uid}, "version": v, "client_id": "stamp",
                     "data": {"uid": uid, "n": published + i, "inserted_at": v, "updated_at": v}})))
            return out

        def applied_count():
            return int(psql("SELECT count(*) FROM zb_stamp").strip() or 0)

        stop = time.monotonic() + DURATION_S

        async def feeder():
            nonlocal published
            while time.monotonic() < stop:
                behind = published - applied_count()
                if behind < BACKLOG_TARGET:
                    for _ in range(max(1, (BACKLOG_TARGET - behind) // (CHUNK * 4))):
                        msgs = batch(CHUNK)
                        await asyncio.gather(*(js.publish(subject, m, headers={"Nats-Msg-Id": mid})
                                               for mid, m in msgs))
                        published += CHUNK
                        if time.monotonic() >= stop:
                            break
                else:
                    await asyncio.sleep(0.2)

        async def sampler():
            buckets = []
            cbuckets = []
            prev = applied_count()
            cprev = consumer_count()
            t_prev = time.monotonic()
            while time.monotonic() < stop:
                await asyncio.sleep(10)
                n = applied_count()
                cn = consumer_count()
                t = time.monotonic()
                rate = (n - prev) / (t - t_prev)
                crate = (cn - cprev) / (t - t_prev)
                buckets.append(rate)
                cbuckets.append(crate)
                rss = subprocess.run(["ps", "-o", "rss=", "-p", str(br.proc.pid)],
                                     capture_output=True, text=True).stdout.strip()
                print(f"  {len(buckets)*10:4d}s  ingress {rate:8.0f}/s  consumer {crate:8.0f}/s "
                      f"(lag {n - cn:7d})  backlog {published - n:6d}  bridge {int(rss or 0)//1024}MB", flush=True)
                prev, cprev, t_prev = n, cn, t
            return buckets, cbuckets

        feed = asyncio.create_task(feeder())
        buckets, cbuckets = await sampler()
        await feed
        # let the tail drain, then final numbers
        end_wait = time.monotonic() + 60
        while applied_count() < published and time.monotonic() < end_wait:
            await asyncio.sleep(2)
        total = applied_count()

        steady = buckets[1:] if len(buckets) > 2 else buckets
        mean = sum(steady) / max(1, len(steady))
        lo = min(steady) if steady else 0
        csteady = cbuckets[1:] if len(cbuckets) > 2 else cbuckets
        cmean = sum(csteady) / max(1, len(csteady))
        print(f"\nSTAMP (lanes={lanes}): {total:,} mutations applied, "
              f"steady ingress mean {mean:,.0f}/s over {len(steady)*10}s, "
              f"min bucket {lo:,.0f}/s ({100*lo/max(mean,1):.0f}% of mean)")
        if lo < 0.6 * mean:
            zb.bad("the line SAGS: a bucket fell below 60% of the mean — not a flat stamp")
            failed += 1
        else:
            zb.ok("flat: every steady bucket within 60% of the mean")

        # the consumer's half: sustained downstream rate + whether it catches up
        catch_end = time.monotonic() + 90
        while consumer_count() < total and time.monotonic() < catch_end:
            await asyncio.sleep(2)
        clag = total - consumer_count()
        consumer_stop.set()
        consumer_thread.join(timeout=5)
        print(f"CONSUMER: steady mean {cmean:,.0f} rows/s applied downstream; "
              f"{'caught up whole' if clag == 0 else f'{clag:,} behind 90s after the feed stopped'}")
        if clag == 0:
            zb.ok("the consumer drank the whole river and caught up — the loop is real")
        else:
            zb.ok(f"consumer sustained {cmean:,.0f} rows/s — the honest downstream ceiling of ONE client "
                  "(ingress outran it; convergence would complete off-window)")
        lib.zb_client_close(h)
        for suf in ("", "-wal", "-shm"):
            try:
                os.remove(cdb + suf)
            except FileNotFoundError:
                pass

        await nc.close()

        # funeral while the bridge watches (§10cq/§10cs)
        psql("DELETE FROM zebridge_catalogue WHERE tbl = 'zb_stamp'")
        psql("DROP TABLE IF EXISTS zb_stamp")
        br.wait_for_log("drop prune for 'zb_stamp'", timeout=30)
    for bucket, key in (("schemas", "zb_stamp"), ("generations", "_default.zb_stamp")):
        subprocess.run(["nats", "--server", "nats://127.0.0.1:4222",
                        "--creds", str(zb.ROOT / "scripts" / "native" / "creds" / "bridge.creds"),
                        "kv", "purge", bucket, key, "-f"], capture_output=True)
    subprocess.run(["nats", "--server", "nats://127.0.0.1:4222",
                    "--creds", str(zb.ROOT / "scripts" / "native" / "creds" / "bridge.creds"),
                    "stream", "purge", "MUTATIONS", "-f"], capture_output=True)
    print("\nPASS" if not failed else f"\nFAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)

#!/usr/bin/env python3
"""Concurrent get()/put() on NATS Object Store — can an in-flight read ever tear?

Written to settle NOTES.md §1.12's "Object Store for the bulk path" open question: if a
future reseed-on-cadence "leaf" ever overwrites a snapshot object in place, what does a
client see if its get() is still mid-flight when a put() lands underneath it — the full old
body, the full new body, an error, a hang, or (the dangerous one) a mix of both?

Reading nats-py's own ObjectStore.put()/get() (nats/js/object_store.py) answers half of this
without running anything: every put() writes its chunks under a BRAND NEW nuid-keyed subject,
publishes a single ROLLUP meta message pointing at that nuid, and only THEN purges the OLD
nuid's chunk messages. get() resolves the nuid ONCE via that meta message and subscribes to
that one nuid's chunk subject for its whole read. A byte published under a different nuid can
therefore never reach a reader pinned to the old one — a torn, part-old-part-new read should
be structurally impossible, by construction of the protocol, not by luck. What the source does
NOT answer is what happens to a reader still mid-flight on the OLD nuid at the exact moment
that nuid's chunks get purged by a concurrent put() — clean error, timeout, or silent hang.
That's the part worth measuring rather than assuming, matching how this project has verified
rather than trusted NATS behaviour before (allow_direct's default, the Direct Get subject
shape, current_setting returning '' not NULL).

Two checks:

  1. Race a get() already in flight against a concurrent put() of a new revision. Confirms no
     torn (part-old, part-new) content ever reaches the reader, and characterises what DOES
     happen instead (spoiler, measured: the reader hangs forever, every time).

  2. Race a put() against a reader that instead watch()es the bucket first and only starts its
     own get() once notified — the same discipline `App.tsx` already uses against
     `$KV.snapshots` in `waitForDescriptor`, one level down on the object itself. Confirms that
     discipline is what actually avoids check 1's hang, not luck: watch() is a JetStream
     consumer on the object's META subject alone, published exactly once per put(), only after
     every chunk is durably written — a native "available" signal, not something to hand-roll.

Usage:
    scripts/scenarios/.venv/bin/python scripts/scenarios/objstore_race.py
"""

import asyncio
import random

import zb

BUCKET = "zb_race_probe"
KEY = "leaf"

# nats-py's OBJ_DEFAULT_CHUNK_SIZE is 128 KiB. 320 chunks (~40 MB) is large enough that a
# localhost read takes multiple message deliveries and real wall-clock time, giving a
# concurrent put() an actual window to land mid-transfer instead of always winning or losing
# the race by construction alone.
CHUNK_SIZE = 128 * 1024
OBJECT_SIZE = CHUNK_SIZE * 320
ROUNDS = 12
GET_TIMEOUT_S = 8
WATCH_ROUNDS = 5


def payload(tag: bytes, size: int) -> bytes:
    # A distinct 8-byte tag repeated every 4096 bytes through the WHOLE body, not just at the
    # edges — a torn read must be detectable no matter which chunk boundary it happens at.
    unit = tag * 512  # 8 * 512 = 4096
    return unit * (size // len(unit))


async def watch_then_read(store, v1, v2):
    """Check 2: watch() for the meta update, THEN get() — never touching a revision that
    might get purged out from under it mid-read. Every round must resolve cleanly; unlike
    check 1's hangs, a failure here is a regression, not documented behaviour."""
    outcomes = {"clean": 0, "hang": 0, "error": 0, "wrong_content": 0}

    for i in range(WATCH_ROUNDS):
        current = v2 if i % 2 == 0 else v1
        watcher = await store.watch(updates_only=True, meta_only=True)
        put_task = asyncio.create_task(store.put(KEY, current))

        try:
            await asyncio.wait_for(watcher.updates(), timeout=GET_TIMEOUT_S)
        except asyncio.TimeoutError:
            outcomes["hang"] += 1
            print(f"  watch round {i}: watch() itself never signalled within {GET_TIMEOUT_S}s")
            await watcher.stop()
            await put_task
            continue

        try:
            result = await asyncio.wait_for(store.get(KEY), timeout=GET_TIMEOUT_S)
        except asyncio.TimeoutError:
            outcomes["hang"] += 1
            print(f"  watch round {i}: get() after the signal HUNG within {GET_TIMEOUT_S}s")
        except Exception as e:
            outcomes["error"] += 1
            print(f"  watch round {i}: get() after the signal ERRORED — {type(e).__name__}: {e}")
        else:
            if result.data == current:
                outcomes["clean"] += 1
            else:
                outcomes["wrong_content"] += 1
                print(f"  watch round {i}: get() returned content matching neither expected version")

        await put_task
        await watcher.stop()

    return outcomes


async def main():
    nc = await zb.connect()
    js = nc.jetstream()

    try:
        await js.delete_object_store(BUCKET)
    except Exception:
        pass
    store = await js.create_object_store(BUCKET)

    v1 = payload(b"VERSION1", OBJECT_SIZE)
    v2 = payload(b"VERSION2", OBJECT_SIZE)

    outcomes = {"clean_old": 0, "clean_new": 0, "torn": 0, "get_error": 0, "get_hang": 0, "put_error": 0}

    print()
    print("check 1: get() already in flight, racing a concurrent put()")
    for i in range(ROUNDS):
        current, other = (v1, v2) if i % 2 == 0 else (v2, v1)
        await store.put(KEY, current)

        get_task = asyncio.create_task(store.get(KEY))
        # Sweep the timing window round to round — a fixed delay only ever tests one point
        # in the race; jitter across rounds is what gives a real chance of landing the put()
        # truly mid-transfer rather than always cleanly before or after it.
        await asyncio.sleep(random.uniform(0, 0.02))
        put_task = asyncio.create_task(store.put(KEY, other))

        try:
            await asyncio.wait_for(asyncio.shield(put_task), timeout=GET_TIMEOUT_S)
        except Exception as e:
            outcomes["put_error"] += 1
            print(f"  round {i:2d}: put() itself errored — {type(e).__name__}: {e}")

        try:
            result = await asyncio.wait_for(get_task, timeout=GET_TIMEOUT_S)
        except asyncio.TimeoutError:
            outcomes["get_hang"] += 1
            print(f"  round {i:2d}: HANG — get() did not complete within {GET_TIMEOUT_S}s")
            get_task.cancel()
            continue
        except Exception as e:
            outcomes["get_error"] += 1
            print(f"  round {i:2d}: get() ERROR — {type(e).__name__}: {e}")
            continue

        data = result.data
        if data == current:
            outcomes["clean_old"] += 1
        elif data == other:
            outcomes["clean_new"] += 1
        else:
            outcomes["torn"] += 1
            # Find where the content stops matching either whole version, so a mix is
            # provable rather than just asserted.
            n = min(len(data), len(current))
            split = next((j for j in range(0, n, 4096) if data[j:j + 8] not in (b"VERSION1", b"VERSION2")), None)
            print(f"  round {i:2d}: TORN — {len(data)} bytes, matches neither version cleanly (first bad tag near byte {split})")

    print()
    for k, v in outcomes.items():
        print(f"  {k:10s}: {v}")

    print()
    print("check 2: watch() for the meta signal first, THEN get()")
    watch_outcomes = await watch_then_read(store, v1, v2)
    print()
    for k, v in watch_outcomes.items():
        print(f"  {k:10s}: {v}")

    await js.delete_object_store(BUCKET)
    await nc.close()

    failed = outcomes["torn"] or watch_outcomes["hang"] or watch_outcomes["error"] or watch_outcomes["wrong_content"]
    print()
    if outcomes["torn"]:
        print("\033[31mTORN READS OBSERVED — Object Store overwrite is not read-safe as used here\033[0m")
    if watch_outcomes["hang"] or watch_outcomes["error"] or watch_outcomes["wrong_content"]:
        print("\033[31mwatch-then-read did NOT reliably avoid the hang — check 2 regressed\033[0m")
    if not failed:
        tail = " (see get_hang/get_error above — expected, not a failure)" if (outcomes["get_hang"] or outcomes["get_error"]) else ""
        print(f"\033[32mno torn reads, and watch-then-read never hung, across all rounds{tail}\033[0m")
    print()
    return 1 if failed else 0


zb.run(main)

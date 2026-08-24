#!/usr/bin/env python3
"""Object Store buckets scope per-tenant the way CDC/INIT streams do — verified, not assumed.

The delta-generations design (NOTES.md §1.13) stores generations in per-tenant
`OBJ_<TENANT>` buckets on the crosstenant lesson: a JetStream consumer's
`filter_subject` is reader-chosen and never ACL-checked, so the STREAM is the only
real boundary — and an object bucket IS a stream (`OBJ_<bucket>`, subjects
`$O.<bucket>.C.>` chunks / `$O.<bucket>.M.>` meta). The notes flagged the grant shape
as unverified. This proves it live:

  1. two buckets exist, one object each (the admin's half);
  2. alice, granted ONLY `OBJ_genacme`-named API subjects (the same per-stream grant
     shape her INIT_ACME access uses), reads her bucket's object byte-for-byte;
  3. the same read against `genglobex` — a bucket she holds no grant for — is refused
     by the broker, not by convention.

⚠️ Edits the LIVE `scripts/native/nats-server.conf` (alice's publish allow-list) and
reloads via SIGHUP; the original conf is restored and reloaded in `finally`. That is
what makes this a spike: the grants it injects are the exact lines a real
`OBJ_<TENANT>` deployment adds per principal, next to the INIT_<TENANT> ones.

Usage:  python scripts/scenarios/objgrants.py     (native stack; needs the pid file)
"""

import asyncio
import pathlib
import re
import shutil
import signal
import os
import sys

import nats

import zb

CONF = pathlib.Path("scripts/native/nats-server.conf")
PID = pathlib.Path("scripts/native/nats-server.pid")
GRANTS = '''          # STREAM.NAMES: nats-py's obj.get() resolves subject→stream through it
          # before subscribing; it leaks stream NAMES only, never data. A client that
          # binds the stream explicitly (nats.zig can) does not need it at all.
          "$JS.API.STREAM.NAMES"
          "$JS.API.STREAM.INFO.OBJ_genacme"
          "$JS.API.DIRECT.GET.OBJ_genacme.>"
          "$JS.API.STREAM.MSG.GET.OBJ_genacme"
          "$JS.API.CONSUMER.CREATE.OBJ_genacme",   "$JS.API.CONSUMER.CREATE.OBJ_genacme.>"
          "$JS.API.CONSUMER.INFO.OBJ_genacme.>"
          "$JS.API.CONSUMER.MSG.NEXT.OBJ_genacme.>"
'''


def hup():
    os.kill(int(PID.read_text().strip()), signal.SIGHUP)


async def main():
    failed = 0
    if not PID.exists():
        sys.exit("no scripts/native/nats-server.pid — this spike drives the native stack")
    conf = CONF.read_text()
    m = re.search(r'\{ user: "alice", password: "([^"]+)"', conf)
    if not m:
        sys.exit("could not find alice in the rendered conf")
    alice_pw = m.group(1)

    # Insert the OBJ grants inside ALICE's block only, anchored on her INIT_ACME line.
    a_start = conf.index('{ user: "alice"')
    a_end = conf.index('{ user: "bob"', a_start)
    anchor = '"$JS.API.CONSUMER.CREATE.INIT_ACME"'
    line_start = conf.index(anchor, a_start, a_end)
    line_end = conf.index("\n", line_start) + 1
    backup = CONF.with_suffix(".conf.objgrants-backup")
    shutil.copy(CONF, backup)

    admin = None
    alice = None
    try:
        CONF.write_text(conf[:line_end] + GRANTS + conf[line_end:])
        hup()
        await asyncio.sleep(1.5)
        zb.ok("alice granted the OBJ_genacme API subjects (conf reloaded via SIGHUP)")

        # ── admin half: two buckets, one object each ──────────────────────────
        admin = await zb.connect()
        js = admin.jetstream()
        for b in ("genacme", "genglobex"):
            try:
                await js.delete_object_store(b)
            except Exception:  # noqa: BLE001
                pass
            store = await js.create_object_store(b)
            await store.put("g1", f"generation-1 of {b}".encode())
        zb.ok("buckets genacme/genglobex created, one object each (admin)")

        # ── alice: her bucket must open and read byte-for-byte ────────────────
        alice = await nats.connect(f"nats://alice:{alice_pw}@127.0.0.1:4222")
        ajs = alice.jetstream()
        got = await asyncio.wait_for((await ajs.object_store("genacme")).get("g1"), timeout=8)
        data = got.data if hasattr(got, "data") else got
        if data == b"generation-1 of genacme":
            zb.ok("alice reads her tenant's bucket byte-for-byte through per-stream grants")
        else:
            zb.bad(f"unexpected content from genacme: {data!r}")
            failed += 1

        # ── alice: the other tenant's bucket must be refused by the broker ────
        try:
            got = await asyncio.wait_for((await ajs.object_store("genglobex")).get("g1"), timeout=6)
            data = got.data if hasattr(got, "data") else got
            zb.bad(f"alice READ another tenant's bucket ({data!r}) — per-bucket grants do NOT scope; "
                   "the OBJ design cannot rely on them")
            failed += 1
        except Exception as e:  # noqa: BLE001
            zb.ok(f"genglobex is refused by the broker ({type(e).__name__}) — the bucket is the "
                  "boundary, same as CDC_/INIT_ streams")
    finally:
        if alice:
            await alice.close()
        if admin:
            ajs2 = admin.jetstream()
            for b in ("genacme", "genglobex"):
                try:
                    await ajs2.delete_object_store(b)
                except Exception:  # noqa: BLE001
                    pass
            await admin.close()
        shutil.move(backup, CONF)
        hup()
        print("  ⓘ  conf restored and reloaded")

    return 1 if failed else 0


zb.run(main)

#!/usr/bin/env python3
"""The generation producer, live — NOTES.md §1.13 milestones 2+3: deltas and the chain.

`generations.py` proved the *reader role* can run the build recipe by hand; this
scenario proves the bridge runs it, delta chain included. A probe bridge starts with
`GENERATION_RULES=users:_default` at the minimum cadence and chain depth 3, and the
producer must:

  1. build g1 unprompted: a full (`users-g1-full`), no delta (nothing to delta
     against), a manifest whose jump-in point is that full;
  2. SKIP while nothing changes — "limited cron queries" is a promise, not a mood;
  3. ship a touched row as a DELTA: `kind: "delta"`, exactly the changed row, its
     `(prev_cutoff, cutoff]` bounds carried in object and manifest;
  4. keep the chain continuous: consecutive deltas' bounds meet exactly
     (`d[i+1].prev_cutoff == d[i].cutoff`), and the first delta after the full chains
     off the full's own cutoff;
  5. serve the client walk: a client at the full's watermark reaches the manifest
     head through deltas alone — first bound equals the full's cutoff, last equals
     the manifest's;
  6. prune past depth 3 and keep the jump-in point: rows == depth, both object kinds
     gone for pruned gens, the manifest's full still inside the window and fetchable
     (refreshed at distance depth − 1, before it can age out).

The 5s clamp margin means a write can echo into one extra build on the following
tick (a duplicate delta, absorbed by the guarded upsert — never a gap), so checks
are structural (bounds, kinds, windows), never tick-counting.

Usage:  python scripts/scenarios/genproducer.py   (⚠️ owns the only bridge)
Needs the probe-bridge env (`set -a && . ./.env.bridge && set +a`, NATS_NKEY_SEED)
plus ZB_PSQL admin access for the row touches. Cleans up its rows, objects and
pointer; the shared `generations` KV bucket and `gen-_default` store remain.
"""

import asyncio
import json
import re
import subprocess
import sys
import time

import zb

TENANT, TABLE = "_default", "users"
GEN_TOPO = zb.TOPOLOGY["generations"]          # kv bucket + object-bucket prefix
BUCKET, KEY = f"{GEN_TOPO['bucket_prefix']}{TENANT}", f"{TENANT}.{TABLE}"
KV_BUCKET = GEN_TOPO["kv"]
CADENCE = 5          # GENERATION_CADENCE_SECONDS minimum
DEPTH = 3            # GENERATION_CHAIN_DEPTH for the probe
LOG = "/tmp/zb_genproducer_bridge.log"


def gens():
    out = zb.psql(f"SELECT gen FROM public.zebridge_generations "
                  f"WHERE tenant='{TENANT}' AND tbl='{TABLE}' ORDER BY gen")
    return [int(g) for g in out.split()] if out else []


def touch():
    zb.psql(f"UPDATE public.{TABLE} SET updated_at = now() "
            f"WHERE id = (SELECT min(id) FROM public.{TABLE})")


async def poll(pred, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        await asyncio.sleep(0.5)
    return pred()


async def main():
    failed = 0
    # ⚠️ Owns the only bridge (since derivation landed): every bridge with
    # GENERATIONS_ENABLED derives the full published set, so a concurrently running
    # production bridge would produce this scenario's pair too and race its
    # controlled cadence. The probe scopes itself with GENERATION_RULES (now a
    # RESTRICTION intersected with the derived set).
    running = subprocess.run(["pgrep", "-f", "zig-out/bin/bridge"], capture_output=True, text=True)
    if running.stdout.strip():
        sys.exit("another bridge is already running — it would produce this scenario's "
                 "chain concurrently; stop it first")
    zb.psql(f"DELETE FROM public.zebridge_generations WHERE tenant='{TENANT}' AND tbl='{TABLE}'")

    nc = await zb.connect()
    js = nc.jetstream()

    async def obj_get(name):
        try:
            store = await js.object_store(BUCKET)
            return (await store.get(name)).data
        except Exception:
            return None

    async def manifest():
        kv = await js.key_value(KV_BUCKET)
        return json.loads((await kv.get(KEY)).value)

    def chain_ok(man):
        """Continuity + client walk, structurally: bounds meet, full reaches head."""
        deltas = man["deltas"]
        for a, b in zip(deltas, deltas[1:]):
            if b["prev_cutoff"] != a["cutoff"]:
                return f"gap between g{a['gen']} and g{b['gen']}"
        after = [d for d in deltas if d["gen"] > man["full"]["gen"]]
        if after:
            if after[0]["prev_cutoff"] != man["full"]["cutoff"]:
                return "first delta after the full does not chain off its cutoff"
            if after[-1]["cutoff"] != man["cutoff_version"]:
                return "chain head does not reach the manifest cutoff"
        elif man["full"]["gen"] != man["gen"]:
            return "no deltas after a full that is not the head"
        if not (man["full"]["gen"] > man["gen"] - DEPTH):
            return f"full g{man['full']['gen']} outside the kept window (head g{man['gen']})"
        return None

    try:
        with zb.Bridge(LOG,
                       GENERATION_RULES=f"{TABLE}:{TENANT}",
                       GENERATION_CADENCE_SECONDS=str(CADENCE),
                       GENERATION_CHAIN_DEPTH=str(DEPTH)) as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("producer thread never started — is GENERATION_RULES parsing broken?")
                print(bridge.text()[-2000:])
                return 1

            # ── 1. g1: full only, manifest jump-in point ───────────────────────
            if not await poll(lambda: gens() == [1], timeout=CADENCE * 4):
                zb.bad(f"no g1 within {CADENCE * 4}s — gens={gens()}")
                print(bridge.text()[-2000:])
                return 1
            nrows = int(zb.psql(f"SELECT count(*) FROM public.{TABLE}"))
            man = await manifest()
            body = await obj_get(f"{TABLE}-g1-full")
            doc = zb.decode(body) if body else None
            if man["full"] == {"gen": 1, "object": f"{TABLE}-g1-full", "cutoff": man["cutoff_version"]} \
                    and man["deltas"] == [] and doc and doc["kind"] == "full" \
                    and len(doc["rows"]) == nrows and doc["gen"] == 1 \
                    and re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", man["cutoff_lsn"]):
                zb.ok(f"g1: full only ({nrows} row(s)), manifest jump-in point set, no deltas")
            else:
                zb.bad(f"g1 wrong: man={man}, doc={str(doc)[:200]}")
                failed += 1

            # ── 2. unchanged → skip ────────────────────────────────────────────
            await asyncio.sleep(CADENCE * 2)
            if gens() == [1]:
                zb.ok(f"idle for {CADENCE * 2}s: no new generation — skip-if-unchanged holds")
            else:
                zb.bad(f"chain grew while idle: gens={gens()}")
                failed += 1

            # ── 3. a touch rides a delta ───────────────────────────────────────
            touch()
            if not await poll(lambda: len(gens()) >= 2, timeout=CADENCE * 4):
                zb.bad(f"no delta generation after touch — gens={gens()}")
                failed += 1
            else:
                man = await manifest()
                head = man["deltas"][-1] if man["deltas"] else None
                doc = zb.decode(await obj_get(head["object"])) if head else None
                if doc and doc["kind"] == "delta" and len(doc["rows"]) == 1 \
                        and doc["prev_cutoff"] == head["prev_cutoff"] \
                        and doc["cutoff"] == head["cutoff"]:
                    zb.ok(f"touch rode {head['object']}: kind=delta, exactly 1 row, "
                          f"bounds match the manifest")
                else:
                    zb.bad(f"delta wrong: head={head}, doc={str(doc)[:200]}")
                    failed += 1

            # ── 4+5. continuity and the client walk ────────────────────────────
            err = chain_ok(await manifest())
            if err is None:
                zb.ok("chain continuous: bounds meet exactly, full reaches the head via deltas")
            else:
                zb.bad(f"chain broken: {err}")
                failed += 1

            # ── 6. prune past depth, jump-in point survives ────────────────────
            while (g := gens()) and max(g) < DEPTH + 2:
                touch()
                await poll(lambda: False, timeout=CADENCE)
            await asyncio.sleep(CADENCE * 2 + 2)    # let margin echoes settle
            chain = gens()
            top = max(chain)
            man = await manifest()
            store = await js.object_store(BUCKET)
            # ignore_deletes: a pruned object leaves an ADR-20 tombstone (zero-size
            # meta) that plain list() still shows — deleted IS the state we assert.
            names = [i.name for i in await store.list(ignore_deletes=True)]
            stale = [n for n in names
                     if (m := re.match(rf"{TABLE}-g(\d+)-", n)) and int(m.group(1)) <= top - DEPTH]
            err = chain_ok(man)
            if chain == list(range(top - DEPTH + 1, top + 1)) and not stale \
                    and err is None and await obj_get(man["full"]["object"]) is not None:
                zb.ok(f"pruned to depth {DEPTH}: gens={chain}, no object ≤ g{top - DEPTH}, "
                      f"full refreshed at g{man['full']['gen']} and fetchable")
            else:
                zb.bad(f"prune wrong: gens={chain}, stale={stale}, chain={err}, man={man}")
                failed += 1

    finally:
        zb.psql(f"DELETE FROM public.zebridge_generations WHERE tenant='{TENANT}' AND tbl='{TABLE}'")
        try:
            store = await js.object_store(BUCKET)
            for info in await store.list(ignore_deletes=True):
                if info.name.startswith(f"{TABLE}-g"):
                    await store.delete(info.name)
            await (await js.key_value(KV_BUCKET)).delete(KEY)
        except Exception:
            pass
        await nc.close()

    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


zb.run(main)

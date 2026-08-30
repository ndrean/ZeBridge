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

Chain objects on the wire are msgpack under zstd (NOTES §10w/§10x): fulls a plain
frame, deltas compressed against the era's dictionary — a `<table>-g<N>-dict` object
the manifest names per delta. So every object read here is checked for the zstd
magic first and decoded through `zstd` (the CLI, or the `zstandard` module) with the
dictionary the manifest points at; a raw-msgpack or JSON object would be a regression.

Usage:  python scripts/scenarios/genproducer.py   (⚠️ owns the only bridge)
Needs the probe-bridge env (`set -a && . ./.env.bridge && set +a`, NATS_CREDS) plus
ZB_PSQL admin access for the row touches. Cleans up its rows, objects and pointer;
the shared `generations` KV bucket and `gen-<open tenant>` store remain.
"""

import asyncio
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time

import msgpack

import zb

TENANT, TABLE = zb.TOPOLOGY["open_tenant"], "users"
GEN_TOPO = zb.TOPOLOGY["generations"]          # kv bucket + object-bucket prefix
BUCKET, KEY = f"{GEN_TOPO['bucket_prefix']}{TENANT}", f"{TENANT}.{TABLE}"
KV_BUCKET = GEN_TOPO["kv"]
CADENCE = 5          # GENERATION_CADENCE_SECONDS minimum
DEPTH = 3            # GENERATION_CHAIN_DEPTH for the probe
LOG = "/tmp/zb_genproducer_bridge.log"
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
PROBE_EMAIL = "probe@zb"   # the row seeded when the fixture is empty; removed in finally


def zstd_decode(data: bytes, dictionary: bytes | None = None) -> bytes:
    """One zstd frame → bytes, through the `zstandard` module or the `zstd` CLI."""
    try:
        import zstandard  # type: ignore
        d = zstandard.ZstdCompressionDict(dictionary) if dictionary else None
        return zstandard.ZstdDecompressor(dict_data=d).decompress(data, max_output_size=1 << 30)
    except ImportError:
        pass
    if not shutil.which("zstd"):
        sys.exit("chain objects are zstd frames: install the `zstd` CLI or `pip install zstandard`")
    with tempfile.TemporaryDirectory() as tmp:
        args = ["zstd", "-d", "-c", "-q"]
        if dictionary:
            dpath = f"{tmp}/dict"
            open(dpath, "wb").write(dictionary)
            args += ["-D", dpath]
        r = subprocess.run(args, input=data, capture_output=True)
        if r.returncode != 0:
            raise ValueError(f"zstd: {r.stderr.decode(errors='replace').strip()}")
        return r.stdout


def decode_chain(body: bytes | None, dictionary: bytes | None = None):
    """A chain object → its document, insisting on the wire shape: zstd outside,
    msgpack inside. Returns None for a missing object; raises on the wrong shape."""
    if body is None:
        return None
    if not body.startswith(ZSTD_MAGIC):
        raise ValueError(f"chain object is not a zstd frame (starts {body[:4]!r})")
    return msgpack.unpackb(zstd_decode(body, dictionary), raw=False, strict_map_key=False)


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
    running = subprocess.run(["pgrep", "-f", r"zig-out/bin/bridge$"], capture_output=True, text=True)
    if running.stdout.strip():
        sys.exit("another bridge is already running — it would produce this scenario's "
                 "chain concurrently; stop it first")
    zb.psql(f"DELETE FROM public.zebridge_generations WHERE tenant='{TENANT}' AND tbl='{TABLE}'")
    # The probe needs at least one row: `touch()` updates min(id), and an EMPTY
    # fixture makes every touch a 0-row no-op — no delta ever rides, and the
    # depth loop below used to spin on that forever (measured: a 52-minute wedge
    # after the fixture baseline was reborn empty).
    # updated_at a full minute in the past — OUTSIDE the 5s clamp margin. A seed
    # at now() sits inside the delta predicate's margin window at the next tick,
    # so the idle check sees a margin-echo delta and calls skip-if-unchanged
    # broken (measured: gens=[1, 2] while idle).
    zb.psql(f"INSERT INTO public.{TABLE} (name, email, inserted_at, updated_at) "
            f"SELECT 'genproducer probe', '{PROBE_EMAIL}', now(), now() - interval '1 minute' "
            f"WHERE NOT EXISTS (SELECT 1 FROM public.{TABLE})", quiet=True)

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

    async def delta_doc(entry):
        """A manifest delta entry → its decoded document, through the dictionary the
        entry names (deltas are compressed against the era's dict; a full is not)."""
        dictionary = await obj_get(entry["dict"]) if entry.get("dict") else None
        if entry.get("dict") and dictionary is None:
            raise ValueError(f"manifest names dictionary {entry['dict']} but the store has none")
        return decode_chain(await obj_get(entry["object"]), dictionary)

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
            try:
                doc = decode_chain(body)          # a full: zstd, no dictionary
            except ValueError as e:
                zb.bad(f"g1 full is not zstd-wrapped msgpack: {e}")
                failed += 1
                doc = None
            if man["full"] == {"gen": 1, "object": f"{TABLE}-g1-full", "cutoff": man["cutoff_version"]} \
                    and man["deltas"] == [] and doc and doc["kind"] == "full" \
                    and len(doc["rows"]) == nrows and doc["gen"] == 1 \
                    and re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", man["cutoff_lsn"]):
                zb.ok(f"g1: full only ({nrows} row(s)), zstd-wrapped msgpack, manifest jump-in point set, no deltas")
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
                try:
                    doc = await delta_doc(head) if head else None
                except ValueError as e:
                    zb.bad(f"delta {head and head['object']} does not decode as dictionary-zstd msgpack: {e}")
                    failed += 1
                    doc = None
                if doc and doc["kind"] == "delta" and len(doc["rows"]) == 1 \
                        and doc["prev_cutoff"] == head["prev_cutoff"] \
                        and doc["cutoff"] == head["cutoff"]:
                    zb.ok(f"touch rode {head['object']}: kind=delta, exactly 1 row, "
                          f"bounds match the manifest"
                          + (f", decoded through {head['dict']}" if head.get("dict") else ""))
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
            # Bounded: a producer that stopped building must FAIL the scenario,
            # not wedge it — each touch gets a few cadences to ride, no more.
            attempts = 0
            while (g := gens()) and max(g) < DEPTH + 2 and attempts < (DEPTH + 2) * 4:
                touch()
                attempts += 1
                await poll(lambda: False, timeout=CADENCE)
            if (g := gens()) and max(g) < DEPTH + 2:
                zb.bad(f"producer stopped building: still gens={g} after {attempts} touches")
                failed += 1
            await asyncio.sleep(CADENCE * 2 + 2)    # let margin echoes settle
            chain = gens()
            top = max(chain)
            man = await manifest()
            store = await js.object_store(BUCKET)
            # ignore_deletes: a pruned object leaves an ADR-20 tombstone (zero-size
            # meta) that plain list() still shows — deleted IS the state we assert.
            names = [i.name for i in await store.list(ignore_deletes=True)]
            # full/delta only: a pruned full's DICTIONARY legitimately outlives it while
            # a kept delta still references it (the producer deletes it when the last
            # reference is pruned) — it is a chain member, not a stale object.
            stale = [n for n in names
                     if (m := re.match(rf"{TABLE}-g(\d+)-(full|delta)$", n)) and int(m.group(1)) <= top - DEPTH]
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
        # The row seeded above when the fixture was empty — gone, or every later run
        # (and every consumer of `users`) inherits a probe row nobody asked for.
        zb.psql(f"DELETE FROM public.{TABLE} WHERE email = '{PROBE_EMAIL}'", quiet=True)
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

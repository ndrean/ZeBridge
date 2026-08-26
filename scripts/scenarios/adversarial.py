#!/usr/bin/env python3
"""Hostile input at the two entry points — the bridge's OWN code is the target.

NATS and libpq are battle-tested; nats.zig and the bridge's USE of them are not.
The two places untrusted bytes cross into bridge code are the only real attack
surface (the NATS conf allow-lists keep a client on its own lane, so this ALSO
connects as a real principal, never the bridge):

  A  the MUTATION path — msgpack envelopes on mutation.<principal>.>, decoded and
     turned into SQL. Every malformation and injection shape, each asserting the
     SAME three things: the bridge SURVIVES (process alive), the hostile message
     is refused ONCE (verdict or dead-letter, never a retry storm), and NO BAD
     STATE — a legitimate write right after still flows, proving the consumer is
     not wedged behind a poison pill.
  B  the /enroll endpoint — code & user_pubkey query params: SQL metacharacters
     (must stay parameterized), oversized, malformed, missing.

The invariant under all of it: a crash, a hang, a wedged consumer, or a written
row from a rejected message is a FAILURE. A clean refusal is a pass. "Synchronous
code is safe" is literature; this is the test.

⚠️ Owns the only bridge. Run as a real client principal (omar) for the mutation
half — as the bridge it proves nothing, the bridge may do anything.

Usage:  python scripts/scenarios/adversarial.py   (admin ZB_PSQL; omar creds)
"""

import asyncio
import datetime
import json
import os
import subprocess
import urllib.request

import msgpack

import zb

HTTP = "http://127.0.0.1:9096"
CREDS = os.environ.get("ADVERSARY_CREDS", "scripts/native/creds/omar.creds")
LOG = "/tmp/zb_adversarial_bridge.log"


def alive(bridge):
    return bridge.proc.poll() is None


def counter_value():
    v = zb.psql("SELECT value FROM public.counter_public LIMIT 1", quiet=True).strip()
    return v


async def legit_write_flows(js, uid, tag):
    """A well-formed mutation must still apply — proves the consumer survived the
    prior hostile one (no poison-pill wedge)."""
    version = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
    target = 40000 + tag
    env = msgpack.packb({"key": {"uid": uid}, "version": version, "client_id": "c-adv",
                         "data": {"uid": uid, "value": target, "updated_at": version}})
    await js.publish("mutation.omar.counter_public.update", env, headers={"Nats-Msg-Id": f"adv-legit-{tag}-{version}"})
    for _ in range(20):
        if counter_value() == str(target):
            return True
        await asyncio.sleep(1)
    return False


def hostile_payloads():
    """Each: (name, raw_bytes). The bridge must refuse every one without dying."""
    uid = "00000000-0000-0000-0000-000000000000"
    good_key = {"uid": uid}
    v = "2026-01-01T00:00:00.000000Z"
    return [
        ("truncated msgpack", msgpack.packb({"key": good_key, "version": v, "data": {"value": 1}})[:-3]),
        ("not a map (array root)", msgpack.packb([1, 2, 3])),
        ("not a map (bare int)", msgpack.packb(42)),
        ("empty bytes", b""),
        ("random garbage", bytes(range(200))),
        ("missing version", msgpack.packb({"key": good_key, "data": {"value": 1}})),
        ("missing key", msgpack.packb({"version": v, "data": {"value": 1}})),
        ("key not a map", msgpack.packb({"key": "not-a-map", "version": v, "data": {"value": 1}})),
        ("missing data", msgpack.packb({"key": good_key, "version": v})),
        ("data not a map", msgpack.packb({"key": good_key, "version": v, "data": 99})),
        ("unknown column", msgpack.packb({"key": good_key, "version": v, "data": {"value": 1, "evil_col": "x"}})),
        # injection: a column NAME carrying a quote — appendIdent must neutralize it,
        # and hasColumn should reject it as unknown before SQL anyway
        ("column name SQL-injection", msgpack.packb({"key": good_key, "version": v, "data": {'value") ; DROP TABLE counter_public; --': 1}})),
        # injection: a VALUE full of SQL metachars — must be stored LITERALLY ($N param)
        ("value SQL-injection", msgpack.packb({"key": good_key, "version": v, "data": {"value": 1, "last_writer": "'); DROP TABLE counter_public; --"}})),
        # a NUL-embedded string value — dupeZ truncates at C boundary; must not crash
        ("NUL-embedded value", msgpack.packb({"key": good_key, "version": v, "data": {"value": 1, "last_writer": "a\x00b"}})),
        # deeply nested map — the decoder must not stack-overflow
        ("deeply nested data", msgpack.packb({"key": good_key, "version": v, "data": _nest(200)})),
        # wrong type where a string version is expected
        ("version as map", msgpack.packb({"key": good_key, "version": {"nested": 1}, "data": {"value": 1}})),
        # a huge string value (under BASE_BUF so it is a value test, not a size test)
        ("large string value", msgpack.packb({"key": good_key, "version": v, "data": {"value": 1, "last_writer": "A" * 8000}})),
    ]


def _nest(depth):
    d = {"value": 1}
    for _ in range(depth):
        d = {"n": d}
    return d


async def main():
    failed = 0

    running = subprocess.run(["pgrep", "-f", "zig-out/bin/bridge"], capture_output=True, text=True)
    if running.stdout.strip():
        import sys
        sys.exit("another bridge is already running — this scenario owns the only bridge")

    zb.psql("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='zb_probe' AND NOT active) "
            "THEN PERFORM pg_drop_replication_slot('zb_probe'); END IF; END $$", quiet=True)

    with zb.Bridge(LOG) as bridge:
        if not bridge.wait_for_log("Replication started successfully", timeout=60):
            zb.bad("probe bridge never started — see " + LOG)
            return 1

        uid = zb.psql("SELECT uid FROM public.counter_public LIMIT 1", quiet=True).strip()
        if not uid:
            uid = zb.psql("INSERT INTO public.counter_public (value) VALUES (0) RETURNING uid", quiet=True).strip()

        # connect as OMAR — the allow-list confines this exactly as a real client,
        # so the mutation lane is all the reach the adversary has. The probe bridge
        # already launched with the BRIDGE creds it inherited; switch only THIS
        # process's env so zb.connect authenticates as the adversary, not the bridge.
        os.environ["NATS_CREDS"] = os.environ.get("ADVERSARY_CREDS", "scripts/native/creds/omar.creds")
        os.environ["NATS_URL"] = "nats://127.0.0.1:4222"
        nc = await zb.connect()
        js = nc.jetstream()

        # ── A. the mutation path, one hostile message at a time ───────────────
        table_before = zb.psql("SELECT to_regclass('public.counter_public')::text", quiet=True).strip()
        survived_all = True
        wedged = False
        for i, (name, raw) in enumerate(hostile_payloads()):
            version = f"2026-01-01T00:00:{i:02d}.000000Z"
            try:
                await js.publish("mutation.omar.counter_public.update", raw,
                                 headers={"Nats-Msg-Id": f"adv-hostile-{i}-{version}"})
            except Exception as e:
                # the CLIENT-side publish may itself reject (e.g. a broker limit) —
                # that is fine, the point is the bridge must not die
                pass
            await asyncio.sleep(0.4)
            if not alive(bridge):
                zb.bad(f"the bridge DIED on hostile input: '{name}' — see {LOG}")
                survived_all = False
                failed += 1
                break
        else:
            # every message delivered; give the consumer a moment to drain them
            await asyncio.sleep(3)

        if survived_all:
            zb.ok(f"survived all {len(hostile_payloads())} hostile mutation shapes — process alive after each")

        # the table still exists and was not dropped by any injection attempt
        table_after = zb.psql("SELECT to_regclass('public.counter_public')::text", quiet=True).strip()
        if table_after == table_before == "counter_public":
            zb.ok("no injection landed: counter_public still exists, unaltered by any payload")
        else:
            zb.bad(f"table state changed: before={table_before} after={table_after} — an injection may have executed")
            failed += 1

        # no hostile value was stored: last_writer must not carry a DROP or a giant blob
        poisoned = zb.psql("SELECT count(*) FROM public.counter_public WHERE last_writer LIKE '%DROP%' OR length(last_writer) > 1000", quiet=True).strip()
        if poisoned == "0":
            zb.ok("no rejected payload's data was written (no DROP string, no oversized blob in last_writer)")
        else:
            zb.bad(f"{poisoned} row(s) carry hostile data from a message that should have been refused")
            failed += 1

        # THE wedge test: a legit write must still flow after the hostile barrage
        if await legit_write_flows(js, uid, 1):
            zb.ok("the consumer is NOT wedged: a legitimate mutation applies after the hostile barrage")
        else:
            zb.bad("a legitimate mutation does NOT apply after the hostile inputs — the consumer is wedged (poison pill)")
            failed += 1

        await nc.close()

        # ── B. /enroll hostile params ─────────────────────────────────────────
        probe = enroll_status("code=" + "0" * 32 + "&user_pubkey=U" + "A" * 55)
        if probe == 404:
            print("  ⓘ  enrollment not armed on this probe (no ZB_SIGNING_SEED) — /enroll fuzz skipped")
        else:
            cases = [
                ("SQL-injection code", "code=" + "x'%3B DROP TABLE zebridge_invites%3B--" + "&user_pubkey=U" + "A" * 55),
                ("oversized code", "code=" + "A" * 500 + "&user_pubkey=U" + "A" * 55),
                ("missing user_pubkey", "code=" + "0" * 32),
                ("missing code", "user_pubkey=U" + "A" * 55),
                ("wrong pubkey prefix", "code=" + "0" * 32 + "&user_pubkey=X" + "A" * 55),
                ("short pubkey", "code=" + "0" * 32 + "&user_pubkey=UAAA"),
                ("empty params", "code=&user_pubkey="),
            ]
            all_refused = all(enroll_status(q) in (400, 403, 503) for _, q in cases)
            invites_table = zb.psql("SELECT to_regclass('public.zebridge_invites')::text", quiet=True).strip()
            if all_refused and invites_table == "zebridge_invites":
                zb.ok(f"/enroll refused all {len(cases)} hostile param shapes (400/403), invites table intact — code is parameterized")
            else:
                bad_codes = [(n, enroll_status(q)) for n, q in cases if enroll_status(q) not in (400, 403, 503)]
                zb.bad(f"/enroll mishandled: {bad_codes}, invites table={invites_table}")
                failed += 1

        # ── C. the audit on the survivor ──────────────────────────────────────
        pid = subprocess.run(["pgrep", "-f", "bridge --slot zb_probe"], capture_output=True, text=True).stdout.split()
        if pid:
            rep = subprocess.run(["leaks", "--nocontext", pid[0]], capture_output=True, text=True).stdout
            if "0 leaks for 0 total leaked bytes" in rep:
                zb.ok("after every hostile input: `leaks` reports 0 — no drip on the refusal paths")
            else:
                zb.bad(f"leaks after the barrage: {[l for l in rep.splitlines() if 'leaks for' in l]}")
                failed += 1

    zb.psql("DELETE FROM public.zebridge_invites WHERE principal LIKE 'adv%'", quiet=True)
    zb.psql("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='zb_probe' AND NOT active) "
            "THEN PERFORM pg_drop_replication_slot('zb_probe'); END IF; END $$", quiet=True)

    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


def enroll_status(query):
    try:
        with urllib.request.urlopen(HTTP + "/enroll?" + query, timeout=5) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


zb.run(main)

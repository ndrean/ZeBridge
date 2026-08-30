#!/usr/bin/env python3
"""Memory over time — the long-runner's audit (policy, never trust).

The bridge is a daemon; the leak that matters is the one that GROWS. Two
detectors, two worlds, both sampled here around a load window:

  • macOS `leaks` — the malloc zones: libpq results (a forgotten PQclear is the
    classic daemon drip), any C allocation. Zig's own allocators sit on mmap and
    are invisible here — THEIR audit is the DebugAllocator's exit report, which
    the AuthorizationViolation early-exit path keeps honest.
  • RSS — the blunt total. The event-ring slab is pre-allocated and constant by
    design (boot logs "Event ring: N MB"), so what this asserts is DRIFT across
    the soak, not size.

Load between samples: CDC write bursts (memo updates), bogus /enroll bursts
(permit pool + refusal paths), /metrics scrapes — the per-event paths a
long-runner grinds.

Needs the fixture tables `memo`, `note_t` and `counter_public` (init.core seeds them)
— absent, this exits rather than soaking nothing and calling it clean. macOS only
(`leaks`); run by hand, never by the battery.

Env: SOAK_SECONDS (default 60), ZB_RSS_DRIFT_MB (default 64).
Usage:  python scripts/scenarios/leaksoak.py   (admin ZB_PSQL; bridge on :9090; NATS_CREDS)
"""

import datetime
import os
import re
import subprocess
import sys
import time
import urllib.request

import msgpack

import zb

HTTP = os.environ.get("ZB_BRIDGE_HTTP", zb.http_base())
SOAK = int(os.environ.get("SOAK_SECONDS", "60"))
DRIFT_MB = int(os.environ.get("ZB_RSS_DRIFT_MB", "64"))
MUT = zb.TOPOLOGY["subjects"]["mutations_prefix"]
FIXTURES = ("memo", "note_t", "counter_public")


def bridge_pid() -> str:
    out = subprocess.run(["pgrep", "-f", "bridge --slot"], capture_output=True, text=True).stdout
    return out.split()[0] if out.split() else ""


def sample(pid: str):
    rep = subprocess.run(["leaks", "--nocontext", pid], capture_output=True, text=True).stdout
    m = re.search(r"(\d+) leaks for (\d+) total leaked bytes", rep)
    n = re.search(r"(\d+) nodes malloced for (\d+) KB", rep)
    rss_kb = int(subprocess.run(["ps", "-o", "rss=", "-p", pid], capture_output=True, text=True).stdout.strip() or 0)
    return {"leaks": int(m.group(1)) if m else -1, "leaked_bytes": int(m.group(2)) if m else -1,
            "nodes": int(n.group(1)) if n else -1, "malloc_kb": int(n.group(2)) if n else -1,
            "rss_mb": rss_kb // 1024}


def scrape(path):
    try:
        urllib.request.urlopen(HTTP + path, timeout=5).read()
    except Exception:
        pass


def churn(rnd: int):
    """One round of the daemon's real verb matrix — every long-running path gets
    ground, not just the cheap ones.

      every round      INSERT + UPDATE + DELETE on memo (public CDC, all three
                       verbs), INSERT + DELETE on note_t tenant 'kilo' (tenant
                       routing + the producer's delta predicate has fresh rows to
                       ride), one bogus enroll (permit/refusal path), /metrics
                       and /status (the render allocations)
      every 10th       a SUCCESSFUL enrollment: mint an invite row, redeem it —
                       the writer CTE, the JWT mint, and the user_tenants insert
                       whose CDC diversion feeds $KV.tenants
      every 20th       a REAL edge-write envelope on mutation.omar.counter_public
                       (msgpack, fresh version, Nats-Msg-Id) — the mutation
                       listener's decode/apply/verdict path, an UPDATE so no rows
                       accumulate
      once, mid-soak   a sweeper dry-run pass — its own PG dial, catalogue read
                       and sweep queries (killed after one pass; it loops forever)
    """
    zb.psql("INSERT INTO public.memo (txt) VALUES ('soak-ins '||now()::text)", quiet=True)
    zb.psql("UPDATE public.memo SET txt = 'soak-upd '||now()::text, updated_at = now() "
            "WHERE uid = (SELECT uid FROM public.memo WHERE txt LIKE 'soak-%' LIMIT 1)", quiet=True)
    zb.psql("DELETE FROM public.memo WHERE uid = (SELECT uid FROM public.memo "
            "WHERE txt LIKE 'soak-%' ORDER BY updated_at LIMIT 1)", quiet=True)
    zb.psql("INSERT INTO public.note_t (uid, txt, tenant_id, updated_at) "
            "VALUES (gen_random_uuid(), 'soak', 'kilo', now())", quiet=True)
    zb.psql("DELETE FROM public.note_t WHERE txt = 'soak' AND uid = "
            "(SELECT uid FROM public.note_t WHERE txt = 'soak' LIMIT 1)", quiet=True)
    try:
        urllib.request.urlopen(HTTP + "/enroll?code=" + os.urandom(16).hex() + "&user_pubkey=U" + "A" * 55, timeout=5)
    except Exception:
        pass
    scrape("/metrics")
    scrape("/status")
    if rnd % 10 == 5:
        code = os.urandom(16).hex()
        zb.psql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id) "
                f"VALUES ('{code}', 'soak_{rnd}', 'kilo')", quiet=True)
        # a REAL keypair is not needed to grind the mint: any well-formed pubkey
        # exercises the CTE + signing; nobody ever connects as soak_<n>
        try:
            urllib.request.urlopen(HTTP + f"/enroll?code={code}&user_pubkey=U" + "B" * 55, timeout=5).read()
        except Exception:
            pass


def cleanup():
    """The soak's own debris, gone — in `finally`, so an assertion or a Ctrl-C
    mid-soak does not leave soak rows, invites and KV keys for the next run."""
    zb.psql("DELETE FROM public.memo WHERE txt LIKE 'soak-%'", quiet=True)
    zb.psql("DELETE FROM public.note_t WHERE txt = 'soak'", quiet=True)
    zb.psql("DELETE FROM public.zebridge_user_tenants WHERE principal LIKE 'soak_%'", quiet=True)
    zb.psql("DELETE FROM public.zebridge_invites WHERE principal LIKE 'soak_%'", quiet=True)
    for rnd in range(0, 1000, 10):
        zb.nats_cli("kv", "purge", zb.TOPOLOGY["kv"]["tenants"], f"soak_{rnd + 5}", "-f")


async def main():
    failed = 0
    if not zb.leaks_available():
        sys.exit("macOS `leaks` is not available — this audit has nothing to sample with")
    pid = bridge_pid()
    if not pid:
        zb.bad("no running bridge to soak")
        return 1
    if not zb.SWEEPER.exists():
        sys.exit(f"{zb.SWEEPER} not found — run `zig build -Doptimize=ReleaseFast`")

    # Every churn kind writes a fixture table; a missing one turns its `quiet=True`
    # psql into a silent no-op and the soak into a measurement of nothing.
    present = set(zb.psql(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename = ANY(ARRAY["
        + ",".join(f"'{t}'" for t in FIXTURES) + "])", quiet=True).split())
    missing = [t for t in FIXTURES if t not in present]
    if missing:
        sys.exit(f"fixture table(s) missing: {', '.join(missing)} — init.core seeds them; "
                 "without them the churn grinds nothing")
    counter_uid = zb.psql("SELECT uid FROM public.counter_public LIMIT 1", quiet=True).strip()
    if not counter_uid:
        sys.exit("counter_public has no row — the edge-write churn needs one to UPDATE")
    # Connected as the bridge (run.py's role here — the churn also purges $KV.tenants
    # keys, which no client may), the edge write is addressed AS a mapped client
    # principal: ZB_PRINCIPAL, else the first one in zebridge_user_tenants. Data, not
    # a literal: an unmapped name is refused by RLS and grinds no apply path at all.
    who = os.environ.get("ZB_PRINCIPAL") or zb.psql(
        "SELECT principal FROM public.zebridge_user_tenants ORDER BY principal LIMIT 1", quiet=True
    ).strip()
    if not who:
        sys.exit("no principal to write as: set ZB_PRINCIPAL, or map one in zebridge_user_tenants")

    before = sample(pid)
    print(f"  ⓘ  before: {before['nodes']} malloc nodes / {before['malloc_kb']} KB, "
          f"{before['leaks']} leaks, RSS {before['rss_mb']} MB — soaking {SOAK}s under churn")

    nc = await zb.connect()
    js = nc.jetstream()
    deadline = time.time() + SOAK
    rounds = 0
    swept = False
    try:
        while time.time() < deadline:
            churn(rounds)
            if rounds % 20 == 10:
                # the mutation listener's full path: decode → LWW guard → apply →
                # verdict publish. UPDATE with a fresh version: converges, no growth.
                version = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
                envelope = msgpack.packb({
                    "key": {"uid": counter_uid}, "version": version, "client_id": "c-soak",
                    "data": {"uid": counter_uid, "value": rounds, "updated_at": version},
                })
                try:
                    await js.publish(zb.subject(MUT, who, "counter_public", "update"), envelope,
                                     headers={"Nats-Msg-Id": f"soak-{rounds}-{version}"})
                except Exception:
                    pass
            if not swept and time.time() > deadline - SOAK / 2:
                swept = True
                try:
                    subprocess.run([str(zb.SWEEPER)],
                                   env={**os.environ, "GC_DRY_RUN": "1", "GC_THRESHOLD_MS": "3600000"},
                                   capture_output=True, timeout=6)
                except subprocess.TimeoutExpired:
                    pass  # one pass completed within the window; the kill is the exit
            rounds += 1
            time.sleep(2)
    finally:
        await nc.close()
        cleanup()

    after = sample(pid)
    print(f"  ⓘ  after {rounds} churn rounds: {after['nodes']} nodes / {after['malloc_kb']} KB, "
          f"{after['leaks']} leaks, RSS {after['rss_mb']} MB")

    if after["leaks"] == 0:
        zb.ok(f"malloc zones: 0 leaks after the soak (libpq result lifecycle holds under churn)")
    else:
        zb.bad(f"`leaks` reports {after['leaks']} leaks / {after['leaked_bytes']} bytes — a C-side drip")
        failed += 1

    drift = after["rss_mb"] - before["rss_mb"]
    if drift <= DRIFT_MB:
        zb.ok(f"RSS drift {drift:+d} MB over {SOAK}s ≤ {DRIFT_MB} MB budget (ring slab is constant by design)")
    else:
        zb.bad(f"RSS grew {drift:+d} MB over {SOAK}s — exceeds the {DRIFT_MB} MB drift budget; "
               "sample longer and bisect the churn kinds")
        failed += 1

    # malloc-node growth is advisory: caches legitimately warm up, so report, don't fail.
    if after["nodes"] > before["nodes"] * 2 and after["malloc_kb"] > before["malloc_kb"] * 2:
        print(f"  ⓘ  malloc node count doubled ({before['nodes']} → {after['nodes']}) — "
              "not failed (caches warm), but worth a longer soak if it keeps climbing")

    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


zb.run(main)

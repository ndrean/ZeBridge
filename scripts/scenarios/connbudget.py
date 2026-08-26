#!/usr/bin/env python3
"""The connection budget — policy, never trust (NOTES: 2026-08-26).

The bridge may claim at most reader+writer role-limit connections of PostgreSQL's
max_connections, BY CONSTRUCTION: `ALTER ROLE … CONNECTION LIMIT` in the init
templates, a 4-permit pool on /enroll, connect_timeout on its dial. Every one of
those is a setting someone can change — so this scenario re-derives the whole
claim from the LIVE system:

  1. both role limits EXIST (not -1) and their sum leaves the cluster real
     headroom (< half of max_connections);
  2. the writer limit BITES: opening sleeper connections as bridge_writer runs
     into "too many connections for role" at the ceiling — the rule is enforced
     by PG, not by our prose;
  3. the /enroll permit pool refuses a burst (some 503s) while still processing
     within budget (some 403s for bogus codes), and /metrics answers 200 DURING
     the burst — telemetry survives an enrollment flood;
  4. skipped gracefully when enrollment is not armed (no ZB_SIGNING_SEED bridge).

⚠️ Step 2 briefly saturates the writer role's slots (a few seconds of pg_sleep):
the live mutation listener may see one refused reconnect attempt during it — by
design, that is exactly the ceiling working.

Usage:  python scripts/scenarios/connbudget.py   (admin ZB_PSQL; bridge on :9090)
"""

import concurrent.futures
import os
import re
import subprocess
import sys
import urllib.request

import zb

HTTP = os.environ.get("ZB_BRIDGE_HTTP", "http://127.0.0.1:9090")


def http_status(path: str, timeout: float = 5) -> int:
    try:
        with urllib.request.urlopen(HTTP + path, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


def main():
    failed = 0

    # ── 1. the limits exist and leave headroom ────────────────────────────────
    rows = zb.psql("SELECT rolname||'|'||rolconnlimit FROM pg_roles "
                   "WHERE rolname IN ('bridge_reader','bridge_writer') ORDER BY 1").splitlines()
    limits = dict(r.split("|") for r in rows if "|" in r)
    max_conn = int(zb.psql("SELECT setting FROM pg_settings WHERE name='max_connections'"))
    reader, writer = int(limits.get("bridge_reader", -1)), int(limits.get("bridge_writer", -1))
    if reader > 0 and writer > 0:
        zb.ok(f"role budgets exist: reader={reader}, writer={writer} (of max_connections={max_conn})")
    else:
        zb.bad(f"a role budget is UNLIMITED: reader={reader}, writer={writer} — "
               "a bridge bug may eat the whole cluster. ALTER ROLE … CONNECTION LIMIT.")
        failed += 1
    if 0 < reader + writer < max_conn // 2:
        zb.ok(f"headroom holds: bridge's worst case {reader + writer} ≪ {max_conn} "
              f"({max_conn - reader - writer} left for everyone else)")
    else:
        zb.bad(f"budget too large: {reader}+{writer} vs max_connections={max_conn}")
        failed += 1

    # ── 1b. coherence: the writer budget must FIT its own consumers ───────────
    # mutation listener (1) + the sweeper's reserved slot (1) + the 4 enroll
    # permits. A limit lowered below that lets an enrollment burst starve edge
    # writes — the drift only this check (and the bridge's boot 🔌 warning) sees.
    if writer > 0 and writer < 2 + 4:
        zb.bad(f"writer limit {writer} < 2 + 4 enroll permits — an enroll burst can starve the mutation listener/sweeper")
        failed += 1
    elif writer > 0:
        zb.ok(f"writer budget fits its consumers: {writer} ≥ 2 + 4 enroll permits")

    # ── 2. the writer ceiling BITES ───────────────────────────────────────────
    wurl = os.environ.get("DATABASE_WRITER_URL", "")
    if writer > 0 and wurl:
        m = re.match(r"postgres://([^:]+):([^@]+)@([^:/]+):?(\d*)/(\w+)", wurl)
        if m:
            user, pw, host, port, db = m.groups()
            procs = [subprocess.Popen(
                ["psql", "-h", host, "-p", port or "5432", "-U", user, "-d", db,
                 "-Atc", "SELECT pg_sleep(4)"],
                env={**os.environ, "PGPASSWORD": pw},
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            ) for _ in range(writer + 2)]
            errs = [p.communicate()[1].decode() for p in procs]
            refused = sum("too many connections for role" in e for e in errs)
            if refused >= 1:
                zb.ok(f"the ceiling bites: {refused} of {writer + 2} sleepers refused "
                      f"with 'too many connections for role' — PG enforces the budget")
            else:
                zb.bad(f"opened {writer + 2} writer connections and NONE was refused — "
                       "the CONNECTION LIMIT is not enforcing")
                failed += 1
        else:
            print("  ⓘ  DATABASE_WRITER_URL unparseable — bite test skipped")
    else:
        print("  ⓘ  no DATABASE_WRITER_URL in env — bite test skipped")

    # ── 3. the enroll permit pool + telemetry under flood ─────────────────────
    probe = http_status("/enroll?code=" + "0" * 32 + "&user_pubkey=U" + "A" * 55)
    if probe == 404:
        print("  ⓘ  enrollment not armed on this bridge (no ZB_SIGNING_SEED) — permit test skipped")
    elif probe == 0:
        zb.bad(f"bridge HTTP unreachable at {HTTP}")
        failed += 1
    else:
        def bogus(_):
            return http_status("/enroll?code=" + os.urandom(16).hex() + "&user_pubkey=U" + "A" * 55, timeout=10)
        with concurrent.futures.ThreadPoolExecutor(max_workers=13) as ex:
            futs = [ex.submit(bogus, i) for i in range(12)]
            metrics_during = http_status("/metrics", timeout=10)
            codes = sorted(f.result() for f in futs)
        n503, n403 = codes.count(503), codes.count(403)
        if n503 >= 1 and n403 >= 1:
            zb.ok(f"permit pool holds: burst of 12 → {n403}×403 (processed within budget), "
                  f"{n503}×503 (refused AT the permit)")
        else:
            zb.bad(f"burst of 12 answered {codes} — expected a 403/503 mix; "
                   "the permit pool is gone or sized past the burst")
            failed += 1
        if metrics_during == 200:
            zb.ok("/metrics answered 200 DURING the burst — telemetry survives an enroll flood")
        else:
            zb.bad(f"/metrics returned {metrics_during} during the burst")
            failed += 1

    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


async def main_async():
    return main()


zb.run(main_async)

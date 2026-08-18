#!/usr/bin/env python3
"""Telemetry — the endpoints, their shape, and the limits that keep them safe.

Telemetry is the one part of the bridge that listens on a socket, so it is the one part an
outsider can reach. It is also the part with no authentication: every endpoint is readable
by anyone who can connect, which is why it binds loopback by default and why the checks
below care as much about what is *refused* as about what is served.

Four things, in order of what they would cost if wrong:

  1. **A silent client cannot wedge the server.** One connection that opens a socket and
     sends nothing used to make every subsequent request time out: measured, three
     consecutive `GET /metrics` returned nothing while one idle socket was held. ⚠️ A rate
     limiter would not have helped — one connection, zero requests, nothing to count. The
     fix is a task per connection plus a receive deadline.
  2. **The metrics parse**, and carry the numbers an operator actually pages on: WAL lag,
     slot activity, refusals.
  3. **Prometheus is really scraping it**, if it is running. A green `/metrics` proves the
     bridge; it does not prove the pipeline.

Usage:  python scripts/scenarios/telemetry.py [base_url]

  telemetry.py                        # http://127.0.0.1:9090
  telemetry.py http://otherhost:9090
"""

import json
import socket
import sys
import time
import urllib.error
import urllib.request

import zb

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:9090"
HOST = BASE.split("://")[1].split(":")[0]
PORT = int(BASE.rsplit(":", 1)[1])

# The bridge's own floor is 3s (Config.Http.receive_timeout_ns); allow for scheduling.
RECEIVE_TIMEOUT_S = 3.0
TIMEOUT_TOLERANCE_S = 2.0

# Metrics an operator would actually alert on. A green endpoint serving an empty document
# is not a working one.
REQUIRED_METRICS = [
    "bridge_uptime_seconds",
    "bridge_connected",
    "bridge_slot_active",
    "bridge_wal_lag_bytes",
    "bridge_wal_confirmed_lag_bytes",
    "bridge_cdc_events_published_total",
    "bridge_refused_tables",
]


def get(path: str, timeout: float = 5.0):
    """Returns (status, body). A refused connection is a status, not an exception."""
    try:
        with urllib.request.urlopen(f"{BASE}{path}", timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:  # noqa: BLE001
        return 0, str(e)


async def main():
    failed = 0

    status, _ = get("/health")
    if status == 0:
        sys.exit(f"nothing is listening on {BASE} — start the bridge first")

    # ── 1. what must be served ─────────────────────────────────────────────────
    print("endpoints")
    for path, want in (("/health", 200), ("/status", 200), ("/metrics", 200), ("/nope", 404)):
        got, body = get(path)
        if got == want:
            zb.ok(f"GET {path:<10} → {got} ({len(body)} bytes)")
        else:
            zb.bad(f"GET {path:<10} → {got}, expected {want}")
            failed += 1

    # `/shutdown`, `/streams/info` and `/streams/purge` were dev conveniences and are
    # gone. Not asserted here: a check that a removed thing stays removed only tracks
    # removed things, and the 404 above already proves unknown paths are refused.

    # ── 3. the slowloris ───────────────────────────────────────────────────────
    print("\na client that connects and sends nothing")
    hog = socket.create_connection((HOST, PORT), timeout=5)
    try:
        time.sleep(0.5)
        # The decisive assertion. Before one-task-per-connection this returned nothing.
        got, _ = get("/metrics", timeout=4)
        if got == 200:
            zb.ok("the server still answers other clients")
        else:
            zb.bad(f"one idle connection wedged the server: GET /metrics → {got}")
            failed += 1

        # And the idle one must be cut loose, or it is a leak rather than an outage.
        hog.settimeout(RECEIVE_TIMEOUT_S + TIMEOUT_TOLERANCE_S)
        t0 = time.time()
        try:
            hog.recv(64)
            waited = time.time() - t0
            if waited <= RECEIVE_TIMEOUT_S + TIMEOUT_TOLERANCE_S:
                zb.ok(f"and closes the silent connection after {waited:.1f}s")
            else:
                zb.bad(f"closed only after {waited:.1f}s")
                failed += 1
        except socket.timeout:
            zb.bad(
                f"the silent connection was still open after "
                f"{RECEIVE_TIMEOUT_S + TIMEOUT_TOLERANCE_S:.0f}s — no receive deadline"
            )
            failed += 1
    finally:
        hog.close()

    # ── 4. the metrics themselves ──────────────────────────────────────────────
    print("\nmetrics content")
    _, body = get("/metrics")
    names = {
        line.split()[0]
        for line in body.splitlines()
        if line and not line.startswith("#") and " " in line
    }
    missing = [m for m in REQUIRED_METRICS if m not in names]
    if missing:
        zb.bad(f"missing: {', '.join(missing)}")
        failed += 1
    else:
        zb.ok(f"all {len(REQUIRED_METRICS)} operator-facing metrics present ({len(names)} total)")

    # Prometheus drops the **entire scrape** on one unparseable line, so the failure looks
    # like "the dashboards went quiet" rather than like an error.
    #
    # Cannot happen today, and that is by construction rather than by luck: every metric is
    # an unsigned integer formatted with `{d}`, and the one float
    # (`bridge_cpu_seconds_total`) is assembled from two integer divisions of a nanosecond
    # counter — no float formatting, so no `nan` or `inf` to emit. Truncation *was* a real
    # risk before the HTTP rewrite, when responses over 4096 bytes were silently cut
    # mid-line; `std.http` streams the body now.
    #
    # Kept as a guard on future change: a metric that gains a float, or a label carrying a
    # table name (which would need escaping — a quote or newline in an identifier breaks
    # the exposition format the same way).
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            float(parts[1])
        except ValueError:
            zb.bad(f"unparseable metric value, Prometheus would drop the whole scrape: {line[:60]}")
            failed += 1
            break

    _, sbody = get("/status")
    try:
        st = json.loads(sbody)
        zb.ok(
            f"/status parses: connected={st.get('is_connected')} "
            f"slot_active={st.get('slot_active')} lag={st.get('wal_lag_bytes')}B"
        )
    except json.JSONDecodeError:
        zb.bad("/status is not valid JSON")
        failed += 1

    # ── 5. is anything actually scraping it? ───────────────────────────────────
    #
    # A green /metrics proves the bridge. It does not prove the pipeline — and the failure
    # that matters operationally is a dashboard that goes quiet, which lives here.
    print("\nprometheus")
    try:
        with urllib.request.urlopen("http://localhost:9091/api/v1/targets", timeout=4) as r:
            targets = json.load(r)["data"]["activeTargets"]
        zb_targets = [t for t in targets if t["labels"].get("job") == "zebridge"]
        up = [t for t in zb_targets if t["health"] == "up"]
        if up:
            zb.ok(f"prometheus is scraping the bridge ({up[0]['scrapeUrl']})")
        elif zb_targets:
            zb.bad("prometheus has a zebridge job but no healthy target")
            for t in zb_targets:
                print(f"     {t['scrapeUrl']}: {t['health']} {t.get('lastError','')[:60]}")
            failed += 1
        else:
            print("  ⓘ  prometheus is running but has no zebridge job")

        # ⚠️ Not a failure. telemetry/prometheus.yml lists both `bridge:9090` (in-compose)
        # and `host.docker.internal:9090` (bridge on the host), so exactly one is always
        # down by design. Reporting it would train an operator to ignore this check.
        down = [t for t in zb_targets if t["health"] != "up"]
        if down and up:
            print(f"  ⓘ  {len(down)} other target(s) down, as expected: the config lists both the")
            print("     in-compose and on-host addresses, and only one can be right at a time")
    except Exception:  # noqa: BLE001
        print("  ⓘ  prometheus not reachable on :9091 — skipping (not a bridge failure)")

    return 1 if failed else 0


zb.run(main)

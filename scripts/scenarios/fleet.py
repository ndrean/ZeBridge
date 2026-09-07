#!/usr/bin/env python3
"""Fleet observability and the slot inventory (NOTES §10db, §10dc), end to end.

The bridge could see its slot and its own lag; it could not see its CLIENTS, nor any
slot but its own. Now a libzb client beats into `$KV.live.<tenant>.<principal>` with
its applied sequence per CDC stream, the bridge reads the bucket on a cadence and
renders per-client lag; and the WAL monitor inventories EVERY replication slot on the
server on its own cadence.

Checks:
  1. the client's heartbeat is in the live bucket, keyed by tenant.principal, carrying
     `streams` (applied seq per stream);
  2. /metrics shows the client live for its tenant, with a lag series per stream;
  3. the slot inventory is on /metrics: this probe's own slot flagged self="true",
     active, with a retained-WAL series;
  4. lag is head − applied, in stream MESSAGES: rows written while the client does
     NOT poll raise its CDC_PUBLIC lag (one statement's rows publish as ONE batch, so
     by ≥ 1, not by the row count); polling again brings it back to 0;
  5. a closed client drops off the fleet metrics after the bucket TTL — cooperative
     liveness, a dead client just goes stale.

⚠️ Owns the only bridge (its probe runs with a 2 s fleet poll and a 10 s bucket TTL,
and deletes the `live` bucket first so the TTL is really that).

Usage:  python scripts/scenarios/fleet.py   (admin ZB_PSQL + probe-bridge env)
"""

import asyncio
import ctypes
import json
import os
import pathlib
import sys
import time
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

WHO = "omar"
TABLE = "test_types"
LIVE = zb.TOPOLOGY.get("kv", {}).get("live", "live")
LOG = str(pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_fleet_bridge.log")
POLL_S, TTL_S = 2, 10


def metrics() -> str:
    try:
        with urllib.request.urlopen(zb.http_base(probe=True) + "/metrics", timeout=5) as r:
            return r.read().decode()
    except Exception:
        return ""


def gauge(text: str, name: str, **labels) -> float | None:
    want = "".join(f'{k}="{v}"' for k, v in labels.items())
    for line in text.splitlines():
        if not line.startswith(name + "{") and not line.startswith(name + " "):
            continue
        if all(f'{k}="{v}"' in line for k, v in labels.items()):
            try:
                return float(line.rsplit(" ", 1)[1])
            except ValueError:
                return None
    return None


async def wait_for(pred, seconds=30, step=0.5):
    for _ in range(int(seconds / step)):
        if pred():
            return True
        await asyncio.sleep(step)
    return False


async def main():
    failed = 0
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")

    tenant = zb.tenant_of(WHO)
    public = zb.TOPOLOGY["cdc_streams"]["public"]
    tenant_stream = zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + tenant
    key = f"{tenant}.{WHO}"

    # A bucket left by an earlier bridge keeps ITS TTL; the probe must create it fresh.
    zb.nats_cli("kv", "del", LIVE, "-f")

    lib = _env.load_lib()
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

    def take(p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: lib.zb_free(p)

    db = f"/tmp/zb-fleet-{os.getpid()}.sqlite3"
    h = 0
    try:
        with zb.Bridge(LOG, FLEET_POLL_SECONDS=str(POLL_S), FLEET_TTL_SECONDS=str(TTL_S),
                       SLOT_INVENTORY_SECONDS="2") as bridge:
            if not bridge.wait_for_log("Replication started successfully", timeout=60):
                zb.bad("probe bridge did not start"); print(bridge.text()[-1500:]); return 1
            if not bridge.wait_for_log("fleet monitor started", timeout=10):
                zb.bad("no fleet monitor in the probe"); return 1

            h = lib.zb_client_open(json.dumps({
                "url": zb.nats_server(), "credsPath": zb.creds_for(WHO),
                "dbPath": db,
                "principal": WHO, "clientId": "py-fleet", "tables": ["users", TABLE],
                "heartbeatMs": 1000}).encode())
            if not h:
                zb.bad("libzb client could not open"); return 1
            take(lib.zb_client_sync(h))

            def beat(n=3):
                for _ in range(n):
                    take(lib.zb_client_poll(h, 300)); time.sleep(1.05)

            # ── 1. the heartbeat is in the bucket ──────────────────────────────
            beat()
            raw = zb.nats_cli("kv", "get", LIVE, key, "--raw").stdout.strip()
            try:
                hb = json.loads(raw.splitlines()[0]) if raw else {}
            except json.JSONDecodeError:
                hb = {}
            if hb.get("principal") == WHO and hb.get("tenant") == tenant and isinstance(hb.get("streams"), dict) \
                    and public in hb["streams"]:
                zb.ok(f"$KV.{LIVE}.{key} carries the client's applied seq per stream: {hb['streams']}")
            else:
                zb.bad(f"no usable heartbeat at $KV.{LIVE}.{key} (got {raw[:160]!r})"); failed += 1

            # ── 2. the fleet on /metrics ───────────────────────────────────────
            live = await wait_for(lambda: gauge(metrics(), "bridge_fleet_clients_live", tenant=tenant) == 1.0, 3 * POLL_S + 2)
            lag_series = gauge(metrics(), "bridge_fleet_client_lag_events", tenant=tenant, principal=WHO, stream=public)
            if live and lag_series is not None:
                zb.ok(f"/metrics: bridge_fleet_clients_live{{tenant=\"{tenant}\"}} 1, lag series per stream present ({public}: {lag_series:.0f})")
            else:
                zb.bad(f"fleet not on /metrics (live={live}, lag={lag_series})"); failed += 1
            m = metrics()
            age = gauge(m, "bridge_fleet_client_last_seen_seconds", tenant=tenant, principal=WHO)
            polled = gauge(m, "bridge_fleet_poll_timestamp_seconds")
            if age is not None and 0 <= age <= 30 and polled and abs(polled - time.time()) < 60:
                zb.ok(f"clocks agree: client age {age:.0f}s (wall clock on both sides), poll stamp within a minute of now")
            else:
                zb.bad(f"clock mismatch: age={age}, poll_ts={polled} vs now={time.time():.0f} — a monotonic clock where wall time was needed"); failed += 1

            # ── 3. the slot inventory ──────────────────────────────────────────
            m = metrics()
            slot = zb.BRIDGE_SLOT if hasattr(zb, "BRIDGE_SLOT") else None
            own = [l for l in m.splitlines() if l.startswith("bridge_replication_slot_active{") and 'self="true"' in l]
            retained = [l for l in m.splitlines() if l.startswith("bridge_replication_slot_retained_wal_bytes{") and 'self="true"' in l]
            total = gauge(m, "bridge_replication_slots")
            if own and own[0].endswith(" 1") and retained and total and total >= 1:
                zb.ok(f"slot inventory: {int(total)} slot(s) on the server; ours flagged self=\"true\", active, retained WAL exposed")
            else:
                zb.bad(f"slot inventory missing (self rows={own}, retained={bool(retained)}, total={total})"); failed += 1

            # ── 4. lag is head − applied ───────────────────────────────────────
            beat()  # a fresh beat with the current position, then STOP polling
            zb.psql("INSERT INTO public.users (name, email, inserted_at, updated_at) "
                    "SELECT 'fleet-'||i, 'fleet'||i||'@e.com', now(), now() FROM generate_series(1, 5) i", quiet=True)
            behind = await wait_for(lambda: (gauge(metrics(), "bridge_fleet_client_lag_events", tenant=tenant, principal=WHO, stream=public) or 0) >= 1, 2 * POLL_S + 2)
            if behind:
                zb.ok("rows written while the client slept: its CDC_PUBLIC lag rose (≥ 1 message) — head − applied, not liveness")
            else:
                zb.bad(f"lag did not rise (got {gauge(metrics(), 'bridge_fleet_client_lag_events', tenant=tenant, principal=WHO, stream=public)})"); failed += 1
            beat()  # apply + beat again
            caught_up = await wait_for(lambda: gauge(metrics(), "bridge_fleet_client_lag_events", tenant=tenant, principal=WHO, stream=public) == 0.0, 3 * POLL_S + 2)
            if caught_up:
                zb.ok("polled again: lag back to 0")
            else:
                zb.bad(f"lag did not return to 0 (got {gauge(metrics(), 'bridge_fleet_client_lag_events', tenant=tenant, principal=WHO, stream=public)})"); failed += 1

            # ── 5. a closed client goes stale and drops off ────────────────────
            lib.zb_client_close(h); h = 0
            gone = await wait_for(lambda: gauge(metrics(), "bridge_fleet_client_last_seen_seconds", tenant=tenant, principal=WHO) is None, TTL_S + 3 * POLL_S + 4)
            if gone:
                zb.ok(f"client closed: off the fleet metrics within the {TTL_S}s TTL — cooperative liveness, nothing to clean up")
            else:
                zb.bad("closed client still on /metrics after the TTL"); failed += 1
    finally:
        if h:
            lib.zb_client_close(h)
        _env.rm_sqlite(db)
        zb.psql("DELETE FROM public.users WHERE email LIKE 'fleet%@e.com'", quiet=True)

    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


zb.run(main)

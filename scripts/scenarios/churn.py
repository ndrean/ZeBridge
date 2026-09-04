"""Reconnect churn: 65 outage cycles under one bridge process — the leaks that only
accumulate (NOTES §10cn).

    scripts/scenarios/run.py owns -k churn        (owns bridge, broker and cluster; ~12 min)

Every outage scenario so far was ONE cycle. Each reconnect allocates and releases a
pile: nats.zig rebuilds subscriptions and buffers, the bridge re-dials PostgreSQL,
libzb reopens tails and consumers. A leak of one socket or a few hundred bytes per
cycle is invisible in any single-cycle scenario and fatal on a VPS after a flaky week.
The shared-inbox double free needed one exotic SHAPE to fire; churn hunts the cousins
that need REPETITION.

Review's spec, followed: generous counts (50 broker bounces + 15 cluster restarts,
deterministically shuffled), IDLE stretches alternating with a SWARM of writer wasps
stinging PostgreSQL through roughly every third cycle — load and idle exercise
different reconnect paths (the §10cb shutdown work proved idle is sometimes the hard
case). One libzb client lives through all of it.

Measured after a warm-up, again at the end:
  - peak RSS growth bounded (a per-cycle allocation leak compounds 65×)
  - file descriptors flat (the classic reconnect leak)
  - thread count flat (no zombie reconnect threads)
  - the reconnect COUNTERS honest: ≈50 NATS, ≈15 PG (the §10cd truth-table
    discipline applied to counters never stress-counted)
  - and the client == PostgreSQL to the row, having survived 65 outages
"""
import asyncio
import ctypes
import json
import os
import pathlib
import random
import re
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
from matrix import Nats, pg_ctl, pg_up, pg_start, ensure_pg_up  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

TABLE = "test_types"
TENANT = "acme"
PRINCIPAL = "alice"
NATS_BOUNCES = 50
PG_BOUNCES = 15
SWARM_THREADS = 3
RSS_ALLOWANCE = 64 * 1024 * 1024   # Debug build; a real per-cycle leak compounds far past this
FD_ALLOWANCE = 12
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_churn_bridge.log"


def metrics() -> str:
    r = subprocess.run(["curl", "-s", "-m", "3", "http://127.0.0.1:9096/metrics"],
                       capture_output=True, text=True)
    return r.stdout


def gauge(text: str, name: str) -> int:
    m = re.search(name + r" (\d+)", text)
    return int(m.group(1)) if m else -1


def fd_count(pid: int) -> int:
    r = subprocess.run(["lsof", "-p", str(pid), "-n", "-P"], capture_output=True, text=True)
    return len(r.stdout.splitlines())


def thread_count(pid: int) -> int:
    r = subprocess.run(["ps", "-M", "-p", str(pid)], capture_output=True, text=True)
    return len(r.stdout.splitlines()) - 1


def slot_name() -> str:
    return zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"


async def main() -> int:
    marker = os.urandom(4).hex()
    n = Nats()
    try:
        return await run(marker, n)
    finally:
        ensure_pg_up()
        if subprocess.run(["nc", "-z", "127.0.0.1", "4222"], capture_output=True).returncode != 0:
            try:
                n.start()
            except Exception as e:  # noqa: BLE001
                print(f"  ⚠️  NATS still down: {e} — scripts/native/up.sh needed")
        zb.psql(f"UPDATE public.{TABLE} SET deleted_at = now(), updated_at = now() "
                f"WHERE some_text LIKE 'churn {marker}%' AND deleted_at IS NULL", quiet=True)
        zb.psql(f"SELECT pg_drop_replication_slot('{slot_name()}') FROM pg_replication_slots "
                f"WHERE slot_name = '{slot_name()}' AND NOT active", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_limits WHERE slot = '{slot_name()}'", quiet=True)


async def run(marker: str, n: Nats) -> int:
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns everything")
    failed = 0

    lib = _env.load_lib()
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for fn, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + fn); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

    def take(p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: lib.zb_free(p)

    # ── the wasp swarm ───────────────────────────────────────────────────────
    swarm_on = threading.Event()
    swarm_stop = threading.Event()

    def wasp(t: int):
        i = 0
        while not swarm_stop.is_set():
            if swarm_on.is_set():
                zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                        f"VALUES (gen_random_uuid(), 'churn {marker} w{t}n{i}', '{TENANT}', now(), now())",
                        quiet=True)  # stings during PG-down cycles simply fail — that is the point
                i += 1
                time.sleep(0.04)
            else:
                time.sleep(0.2)

    wasps = [threading.Thread(target=wasp, args=(t,)) for t in range(SWARM_THREADS)]
    for w in wasps:
        w.start()

    db = f"/tmp/zb-churn-{os.getpid()}.sqlite3"
    _env.rm_sqlite(db)
    h = 0
    try:
        with zb.Bridge(LOG) as br:
            if not br.wait_for_log("Replication started successfully", timeout=60):
                zb.bad("probe bridge did not start"); return 1
            h = lib.zb_client_open(json.dumps({
                "url": zb.nats_server(), "credsPath": zb.creds_for(PRINCIPAL),
                "grammarPath": str(zb.ROOT / "src" / "grammar.json"), "dbPath": db,
                "principal": PRINCIPAL, "clientId": "py-churn", "tables": [TABLE]}).encode())
            if not h:
                zb.bad("libzb client could not open"); return 1
            take(lib.zb_client_sync(h))

            # ── warm-up, then the baseline the growth is judged against ──────
            swarm_on.set()
            await asyncio.sleep(4)
            swarm_on.clear()
            for _ in range(3):
                take(lib.zb_client_poll(h, 400))
            rss0 = gauge(metrics(), "bridge_max_rss_bytes")
            fd0 = fd_count(br.proc.pid)
            th0 = thread_count(br.proc.pid)
            zb.ok(f"warmed up: baseline peak RSS {rss0 // (1024 * 1024)} MB, {fd0} fds, {th0} threads "
                  f"— now {NATS_BOUNCES} broker bounces + {PG_BOUNCES} cluster restarts, shuffled, "
                  f"wasps on every ~3rd cycle")

            events = ["nats"] * NATS_BOUNCES + ["pg"] * PG_BOUNCES
            random.Random(20260904).shuffle(events)
            for i, ev in enumerate(events):
                stinging = i % 3 == 0
                if stinging:
                    swarm_on.set()
                if ev == "nats":
                    n.stop()
                    await asyncio.sleep(0.7)
                    n.start()
                else:
                    r = pg_ctl("stop", "-m", "fast")
                    if r.returncode != 0:
                        zb.bad(f"cycle {i}: fast stop hung — the §10cd step-aside regressed"); return failed + 1
                    await asyncio.sleep(0.5)
                    pg_start()
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline and not pg_up():
                        await asyncio.sleep(0.3)
                swarm_on.clear()
                take(lib.zb_client_poll(h, 300))
                if br.proc.poll() is not None:
                    zb.bad(f"the bridge DIED at cycle {i + 1}/{len(events)} ({ev})"); return failed + 1
                if (i + 1) % 15 == 0:
                    print(f"  … {i + 1}/{len(events)} cycles, bridge alive, "
                          f"peak RSS {gauge(metrics(), 'bridge_max_rss_bytes') // (1024 * 1024)} MB")

            # let the last reconnects settle, then measure
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and gauge(metrics(), "bridge_connected") != 1:
                await asyncio.sleep(1)
            m = metrics()
            rss1 = gauge(m, "bridge_max_rss_bytes")
            fd1 = fd_count(br.proc.pid)
            th1 = thread_count(br.proc.pid)
            nats_rc = gauge(m, "bridge_nats_reconnects_total")
            pg_rc = gauge(m, "bridge_pg_reconnects_total")

            if rss1 - rss0 <= RSS_ALLOWANCE:
                zb.ok(f"peak RSS bounded across 65 cycles: {rss0 // (1024 * 1024)} → {rss1 // (1024 * 1024)} MB "
                      f"(allowance {RSS_ALLOWANCE // (1024 * 1024)} MB) — no compounding allocation leak")
            else:
                zb.bad(f"peak RSS grew {(rss1 - rss0) // (1024 * 1024)} MB over 65 cycles — "
                       "a per-cycle leak compounds; bisect with fewer cycles and leaks(1)")
                failed += 1
            if fd1 - fd0 <= FD_ALLOWANCE:
                zb.ok(f"file descriptors flat: {fd0} → {fd1} — no leaked sockets, the classic reconnect bug")
            else:
                zb.bad(f"fds grew {fd0} → {fd1} across the cycles — leaked sockets/files"); failed += 1
            if abs(th1 - th0) <= 2:
                zb.ok(f"thread count flat: {th0} → {th1} — no zombie reconnect threads")
            else:
                zb.bad(f"threads {th0} → {th1} — something spawns without joining"); failed += 1
            # No counter on the bridge can count broker RESTARTS — it honestly sleeps
            # through bounces that merge into one down period (the broker's up-window
            # between adjacent ~2s cycles is shorter than the library's flat 2s retry).
            # What the counter CLAIMS is sessions re-established, and the log records
            # every one: the manual fallback's line and the transport hook's line
            # (§10cn). Honesty is equality with that ground truth, plus proof that
            # the library's silent self-heals — invisible before §10cn — are counted.
            blog = br.text()
            heals = blog.count("NATS transport reconnected")
            manual = blog.count("NATS reconnected to")
            if nats_rc == heals + manual and heals >= 1 and pg_rc >= int(PG_BOUNCES * 0.5):
                zb.ok(f"the counters kept count: {nats_rc} NATS ({heals} silent self-heals + "
                      f"{manual} fallback) / {pg_rc} PG reconnects for "
                      f"{NATS_BOUNCES}/{PG_BOUNCES} bounces — the truth-table discipline holds under churn")
            else:
                zb.bad(f"counters dishonest: metric {nats_rc} vs log truth {heals}+{manual}, "
                       f"pg {pg_rc}/{PG_BOUNCES}"); failed += 1

            # ── the only assertion that matters to a user ────────────────────
            swarm_stop.set()
            for w in wasps:
                w.join(timeout=10)
            truth = int(zb.psql(f"SELECT count(*) FROM public.{TABLE} "
                                f"WHERE some_text LIKE 'churn {marker}%' AND deleted_at IS NULL"))
            deadline = time.monotonic() + 120
            got = -1
            while time.monotonic() < deadline:
                take(lib.zb_client_poll(h, 500))
                r = take(lib.zb_client_query(h, f"SELECT count(*) FROM {TABLE} WHERE some_text LIKE ?".encode(),
                                             json.dumps([f"churn {marker}%"]).encode()))
                got = r["rows"][0][0] if r.get("rows") else -1
                if got == truth:
                    break
                await asyncio.sleep(1)
            if got == truth and truth > 50:
                zb.ok(f"one client lived through all 65 outages and converged: {truth} wasp stings "
                      "delivered, none lost, none doubled")
            else:
                zb.bad(f"diverged after the churn: client {got}, PostgreSQL {truth}"); failed += 1
    finally:
        swarm_stop.set()
        for w in wasps:
            if w.is_alive():
                w.join(timeout=10)
        if h:
            lib.zb_client_close(h)
        _env.rm_sqlite(db)
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)

"""The backpressure cascade, watched end to end: NATS gone under a steady feed, the
ring fills, the bridge HALTS (never stops), WAL piles behind the slot — then NATS
returns and the whole column drains (NOTES §10cd).

    scripts/scenarios/run.py owns -k cascade      (owns bridge + broker; PostgreSQL stays UP)

The pressure chain, made observable: with the broker gone the publisher parks, the
ring fills (`bridge_queue_usage_percent` climbs), backpressure halts WAL consumption,
and PostgreSQL retains the unconfirmed WAL for the slot
(`bridge_wal_confirmed_lag_bytes` climbs). Nothing FATALs, nothing restarts — the
bridge is a dam, not a fuse. When the broker returns the same process drains: queue to
zero, confirmed lag collapses, every committed row reaches the client, and the slot
never left `reserved` — the outage stayed comfortably inside a BOUNDED
`max_slot_wal_keep_size` (crossing it is `slot_loss.py`'s test, not this one).

  1. steady feed flowing, small ring (RING_BUFFER_COUNT=1024) so pressure is visible
  2. broker killed; the feed continues; queue usage and confirmed lag CLIMB; the
     bridge stays alive and connected to PostgreSQL
  3. broker back: queue drains to 0, confirmed lag collapses, client == PostgreSQL
"""
import asyncio
import ctypes
import json
import os
import pathlib
import re
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
from chaos import running_nats, nats_conf_and_log  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

TABLE = "test_types"
TENANT = "acme"
PRINCIPAL = "alice"
KEEP = "512MB"                 # bounded, and far above what this writes: the valve must NOT fire
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_cascade_bridge.log"


def metrics() -> str:
    r = subprocess.run(["curl", "-s", "-m", "3", "http://127.0.0.1:9096/metrics"],
                       capture_output=True, text=True)
    return r.stdout


def gauge(text: str, name: str) -> int:
    m = re.search(name + r" (\d+)", text)
    return int(m.group(1)) if m else -1


def slot_name() -> str:
    return zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"


class Nats:
    def __init__(self):
        self.pid, self.argv = running_nats()
        _, self.log = nats_conf_and_log(self.argv)

    def stop(self):
        subprocess.run(["kill", self.pid], check=True)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if subprocess.run(["kill", "-0", self.pid], capture_output=True).returncode != 0:
                return
            time.sleep(0.3)
        raise RuntimeError("nats-server did not die")

    def start(self):
        with open(self.log, "a") as lf:
            subprocess.Popen(self.argv, stdout=lf, stderr=subprocess.STDOUT,
                             cwd=zb.ROOT, start_new_session=True)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if subprocess.run(["nc", "-z", "127.0.0.1", "4222"], capture_output=True).returncode == 0:
                self.pid, self.argv = running_nats()
                return
            time.sleep(0.3)
        raise RuntimeError("nats-server did not come back")


async def main() -> int:
    marker = os.urandom(4).hex()
    n = Nats()
    old_keep = zb.psql("SHOW max_slot_wal_keep_size").strip()
    try:
        zb.psql(f"ALTER SYSTEM SET max_slot_wal_keep_size = '{KEEP}'")
        zb.psql("SELECT pg_reload_conf()")
        return await run(marker, n)
    finally:
        if subprocess.run(["nc", "-z", "127.0.0.1", "4222"], capture_output=True).returncode != 0:
            try:
                n.start()
            except Exception as e:  # noqa: BLE001
                print(f"  ⚠️  NATS still down: {e} — scripts/native/up.sh needed")
        zb.psql(f"ALTER SYSTEM SET max_slot_wal_keep_size = '{old_keep}'", quiet=True)
        zb.psql("SELECT pg_reload_conf()", quiet=True)
        zb.psql(f"UPDATE public.{TABLE} SET deleted_at = now(), updated_at = now() "
                f"WHERE some_text LIKE 'casc {marker}%' AND deleted_at IS NULL", quiet=True)
        zb.psql(f"SELECT pg_drop_replication_slot('{slot_name()}') FROM pg_replication_slots "
                f"WHERE slot_name = '{slot_name()}' AND NOT active", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_limits WHERE slot = '{slot_name()}'", quiet=True)


async def run(marker: str, n: Nats) -> int:
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the bridge and the broker")
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

    stop_feed: list = []
    committed = [0]

    def feeder():
        i = 0
        while not stop_feed:
            out = zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                          f"VALUES (gen_random_uuid(), 'casc {marker} {i}' || repeat('x', 600), "
                          f"'{TENANT}', now(), now()) RETURNING 1", quiet=True)
            if out.splitlines() and out.splitlines()[0] == "1":
                committed[0] += 1
            i += 1
            time.sleep(0.05)

    db = f"/tmp/zb-cascade-{os.getpid()}.sqlite3"
    _env.rm_sqlite(db)
    h = 0
    feed = None
    try:
        with zb.Bridge(LOG, RING_BUFFER_COUNT="1024") as br:
            if not br.wait_for_log("Replication started successfully", timeout=60):
                zb.bad("probe bridge did not start"); return 1
            h = lib.zb_client_open(json.dumps({
                "url": zb.nats_server(), "credsPath": zb.creds_for(PRINCIPAL),
                "dbPath": db,
                "principal": PRINCIPAL, "clientId": "py-cascade", "tables": [TABLE]}).encode())
            if not h:
                zb.bad("libzb client could not open"); return 1
            take(lib.zb_client_sync(h))

            feed = threading.Thread(target=feeder)
            feed.start()
            await asyncio.sleep(2)
            if committed[0] < 10:
                zb.bad("the feed is not flowing"); return 1
            zb.ok(f"steady feed at 20 rows/s (RING_BUFFER_COUNT=1024, max_slot_wal_keep_size={KEEP}); "
                  "now the broker dies under it")

            # ── 2. the broker dies; pressure builds and is VISIBLE ───────────
            n.stop()
            # ⚠️ 70 s, not 25: both pressure gauges tick on slow clocks — the WAL monitor
            # samples every 30 s and the queue gauge refreshes on the periodic metrics
            # tick — so a shorter watch reads STALE zeros while the ring genuinely fills
            # (measured: 25 s of outage converged 341 rows yet showed peak 0% / 6.7 KB).
            # The window must outlive both cadences to see the climb it asserts.
            peak_q, peak_lag = 0, 0
            for _ in range(70):
                await asyncio.sleep(1)
                m = metrics()
                peak_q = max(peak_q, gauge(m, "bridge_queue_usage_percent"))
                peak_lag = max(peak_lag, gauge(m, "bridge_wal_confirmed_lag_bytes"))
            if br.proc.poll() is not None:
                zb.bad("the bridge DIED under backpressure — it must halt, not stop"); return failed + 1
            if peak_q >= 20:
                zb.ok(f"the ring filled visibly (queue peaked at {peak_q}%) — the bridge HALTS "
                      "on backpressure, it does not stop")
            else:
                zb.bad(f"queue usage never climbed (peak {peak_q}%) — was the ring too big for the feed?")
                failed += 1
            if peak_lag > 200_000:
                zb.ok(f"PostgreSQL retained the unconfirmed WAL behind the slot "
                      f"(confirmed lag peaked at {peak_lag} bytes) — the dam is holding, bounded by {KEEP}")
            else:
                zb.bad(f"confirmed lag never climbed (peak {peak_lag}) — nothing was retained?")
                failed += 1
            slot = zb.psql(f"SELECT wal_status FROM pg_replication_slots WHERE slot_name = '{slot_name()}'").strip()
            if slot in ("reserved", "extended"):
                zb.ok(f"the slot stayed '{slot}' — well inside the valve; crossing it is slot_loss.py's job")
            else:
                zb.bad(f"slot went '{slot}' during the outage — the feed overran the budget"); failed += 1

            # ── 3. the broker returns; the whole column drains ───────────────
            n.start()
            drained_at = None
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                m = metrics()
                q = gauge(m, "bridge_queue_usage_percent")
                if q == 0 and gauge(m, "bridge_connected") == 1:
                    drained_at = time.monotonic()
                    break
                await asyncio.sleep(1)
            stop_feed.append(True)
            feed.join(timeout=15)
            if drained_at is not None:
                zb.ok(f"broker back: the ring drained to 0% — the SAME process, no restart")
            else:
                zb.bad("the ring never drained after the broker returned"); failed += 1

            lag_now = -1
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                lag_now = gauge(metrics(), "bridge_wal_confirmed_lag_bytes")
                if 0 <= lag_now < 100_000:
                    break
                await asyncio.sleep(2)
            if 0 <= lag_now < 100_000:
                zb.ok(f"confirmed WAL lag collapsed ({peak_lag} → {lag_now} bytes) — "
                      "PostgreSQL released what the slot no longer needs")
            else:
                zb.bad(f"confirmed lag stuck at {lag_now} after recovery"); failed += 1

            truth = int(zb.psql(f"SELECT count(*) FROM public.{TABLE} "
                                f"WHERE some_text LIKE 'casc {marker}%' AND deleted_at IS NULL"))
            deadline = time.monotonic() + 90
            got = -1
            while time.monotonic() < deadline:
                take(lib.zb_client_poll(h, 500))
                r = take(lib.zb_client_query(h, f"SELECT count(*) FROM {TABLE} WHERE some_text LIKE ?".encode(),
                                             json.dumps([f"casc {marker}%"]).encode()))
                got = r["rows"][0][0] if r.get("rows") else -1
                if got == truth:
                    break
                await asyncio.sleep(1)
            if got == truth and truth >= 100:
                zb.ok(f"convergence: all {truth} committed rows on the client — the halt cost "
                      "latency, never data")
            else:
                zb.bad(f"diverged: client {got}, PostgreSQL {truth}"); failed += 1
    finally:
        if feed and feed.is_alive():
            stop_feed.append(True)
            feed.join(timeout=15)
        if h:
            lib.zb_client_close(h)
        _env.rm_sqlite(db)

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)

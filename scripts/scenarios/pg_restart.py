"""Full PostgreSQL stop → start under a live bridge: refused connections, patient
reconnect, resume from the slot with nothing lost (NOTES §10cb).

    scripts/scenarios/run.py owns -k pg_restart      (owns the bridge; controls PG itself)

`chaos.py` kills BACKENDS — the postmaster survives and the next connect succeeds.
This stops the whole cluster: every connect is REFUSED until the postmaster returns,
which is the most common real outage (a reboot, a package upgrade, a failover) and a
different client path entirely. The contract:

  1. the bridge STAYS UP and retries patiently — PostgreSQL being down is the one
     outage with nothing to overflow (no WAL is being written anywhere), so a FATAL
     would be wrong; /metrics keeps answering with connected=0 the whole time
  2. when PostgreSQL returns, the bridge reconnects by itself, resumes from the SLOT
     (same feed, same numbering — no gap for any client), and pg_reconnects ticks up
  3. rows written before the stop and after the start all reach the client; the slot
     survives the restart because slots are durable cluster state

⚠️ Stops the shared development PostgreSQL for a few seconds. The `finally` restarts
it unconditionally — a dead PG after a failed scenario would take the whole
environment with it.
"""
import asyncio
import ctypes
import threading
import json
import os
import pathlib
import re
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

TABLE = "test_types"
TENANT = "acme"
PRINCIPAL = "alice"
PG_CTL = "/opt/homebrew/opt/postgresql@18/bin/pg_ctl"
DATADIR = str(zb.ROOT / "postgres-data")
DOWNTIME_S = 12
# ⚠️ The exact flags scripts/native/up.sh starts PostgreSQL with. wal_level=logical
# lives ONLY on the command line (postgresql.conf keeps the default), so a flagless
# `pg_ctl start` REFUSES to boot the cluster the moment any logical slot exists:
# 'logical replication slot "zb_probe" exists, but "wal_level" < "logical"'. Measured
# the hard way — the first run of this scenario left the whole environment down.
PG_OPTS = ("-p 5432 -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 "
           "-c wal_sender_timeout=300s -c logical_decoding_work_mem=256MB -c wal_buffers=64MB "
           "-c commit_delay=1000 -c commit_siblings=5")
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_pg_restart_bridge.log"


def pg_ctl(*args) -> subprocess.CompletedProcess:
    # -t 90: the shutdown checkpoint after a battery's worth of churn can outlast
    # pg_ctl's default patience — the stop then "fails" while completing anyway,
    # and everything downstream races a half-down postmaster.
    return subprocess.run([PG_CTL, "-D", DATADIR, *args, "-t", "90"],
                          capture_output=True, text=True, timeout=120)


def pg_up() -> bool:
    return subprocess.run([PG_CTL, "-D", DATADIR, "status"], capture_output=True).returncode == 0


def pg_start() -> subprocess.CompletedProcess:
    return pg_ctl("start", "-o", PG_OPTS, "-l", str(zb.ROOT / "scripts" / "native" / "postgres.log"))


def ensure_pg_up() -> None:
    """Unconditional and retrying: a dead PostgreSQL takes the whole environment
    with it, so this must survive a postmaster still mid-shutdown."""
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if pg_up():
            return
        pg_start()
        time.sleep(2)
    print("  ⚠️  ensure_pg_up: PostgreSQL still down after 120s — MANUAL START NEEDED (scripts/native/up.sh)")


def metrics() -> str:
    r = subprocess.run(["curl", "-s", "-m", "3", "http://127.0.0.1:9096/metrics"],
                       capture_output=True, text=True)
    return r.stdout


def slot_name() -> str:
    return zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"


def cleanup(marker: str) -> None:
    ensure_pg_up()
    zb.psql(f"UPDATE public.{TABLE} SET deleted_at = now(), updated_at = now() "
            f"WHERE some_text LIKE '%{marker}%' AND deleted_at IS NULL", quiet=True)
    zb.psql(f"SELECT pg_drop_replication_slot('{slot_name()}') FROM pg_replication_slots "
            f"WHERE slot_name = '{slot_name()}' AND NOT active", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_limits WHERE slot = '{slot_name()}'", quiet=True)


async def main() -> int:
    marker = os.urandom(4).hex()
    try:
        return await run(marker)
    finally:
        cleanup(marker)


async def run(marker: str) -> int:
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")
    if not pg_up():
        sys.exit(f"PostgreSQL is not running from {DATADIR} — this scenario needs to own it")
    failed = 0

    lib = _env.load_lib()
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

    def take(p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: lib.zb_free(p)

    def rows_local(h) -> int:
        take(lib.zb_client_poll(h, 400))
        r = take(lib.zb_client_query(h, f"SELECT count(*) FROM {TABLE} WHERE some_text LIKE ?".encode(),
                                     json.dumps([f"pgr {marker}%"]).encode()))
        return r["rows"][0][0] if r.get("rows") else -1

    db = f"/tmp/zb-pg-restart-{os.getpid()}.sqlite3"
    _env.rm_sqlite(db)
    h = 0
    try:
        with zb.Bridge(LOG) as br:
            if not br.wait_for_log("Replication started successfully", timeout=60):
                zb.bad("probe bridge did not start"); return 1
            h = lib.zb_client_open(json.dumps({
                "url": zb.nats_server(), "credsPath": zb.creds_for(PRINCIPAL),
                "grammarPath": str(zb.ROOT / "grammar.json"), "dbPath": db,
                "principal": PRINCIPAL, "clientId": "py-pg-restart", "tables": [TABLE]}).encode())
            if not h:
                zb.bad("libzb client could not open"); return 1
            take(lib.zb_client_sync(h))

            zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                    f"VALUES (gen_random_uuid(), 'pgr {marker} pre', '{TENANT}', now(), now())")
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and rows_local(h) < 1:
                await asyncio.sleep(0.3)
            if rows_local(h) < 1:
                zb.bad("the pipe is not flowing before the outage"); return 1
            zb.ok("client live; one row through — now PostgreSQL STOPS entirely")

            # ── 1. the cluster goes away ─────────────────────────────────────
            r = pg_ctl("stop", "-m", "fast")
            if r.returncode != 0:
                zb.bad(f"pg_ctl stop failed: {r.stderr.strip()[:120]}"); return 1
            t_stop = time.monotonic()
            saw_disconnected = False
            while time.monotonic() - t_stop < DOWNTIME_S:
                m = metrics()
                if re.search(r"bridge_connected 0", m):
                    saw_disconnected = True
                await asyncio.sleep(1)
            if br.proc.poll() is not None:
                zb.bad("the bridge DIED during a PostgreSQL outage — refused connections "
                       "must be waited out, there is nothing to overflow")
                ensure_pg_up()
                return failed + 1
            if saw_disconnected:
                zb.ok(f"through {DOWNTIME_S}s of refused connections: the bridge is alive, "
                      "retrying, and /metrics answers with connected=0 — honest telemetry")
            else:
                zb.bad("bridge_connected never showed 0 during the outage"); failed += 1

            # ── 2. PostgreSQL returns ────────────────────────────────────────
            r = pg_start()
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and not pg_up():
                await asyncio.sleep(0.5)
            if not pg_up():
                zb.bad("PostgreSQL did not come back — aborting"); return failed + 1

            deadline = time.monotonic() + 45
            reconnected = False
            while time.monotonic() < deadline:
                m = metrics()
                if re.search(r"bridge_connected 1", m) and \
                   int((re.search(r"bridge_pg_reconnects_total (\d+)", m) or [0, 0])[1]) >= 1:
                    reconnected = True
                    break
                await asyncio.sleep(1)
            if reconnected:
                zb.ok("the bridge reconnected by itself (pg_reconnects ≥ 1, connected=1) — "
                      "no restart, no operator")
            else:
                zb.bad("the bridge never reconnected after PostgreSQL returned"); failed += 1

            # ── 3. same slot, same feed, nothing lost ────────────────────────
            slot = zb.psql(f"SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{slot_name()}'").strip()
            if slot == slot_name():
                zb.ok("the slot survived the cluster restart — slots are durable state, same feed")
            else:
                zb.bad("the slot did not survive the restart"); failed += 1

            for i in range(5):
                zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                        f"VALUES (gen_random_uuid(), 'pgr {marker} post{i}', '{TENANT}', now(), now())")
            deadline = time.monotonic() + 45
            local = 0
            while time.monotonic() < deadline:
                local = rows_local(h)
                if local >= 6:
                    break
                await asyncio.sleep(0.5)
            if local == 6:
                zb.ok("convergence: the pre-outage row and all 5 post-restart rows on the "
                      "client — same numbering, no gap, no re-seed needed")
            else:
                zb.bad(f"diverged after the restart: client has {local}/6 rows"); failed += 1

            # ── 4. the same stop, UNDER FIRE ─────────────────────────────────
            # The idle stop above exercises the drained fast-ack; this one lands while
            # the pipeline is mid-flow (an INSERT every 100 ms), where `draining` gates
            # the fast-ack OFF and the walsender's shutdown wait must instead be
            # satisfied by the PubAck-driven confirms racing it. And the writes the
            # fast stop ABORTS mid-flight must stay aborted: PostgreSQL's count after
            # the restart is the only truth the client may converge to.
            fire_stop: list = []
            landed = [0]

            def writer():
                for i in range(60):
                    if fire_stop:
                        break
                    out = zb.psql(
                        f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                        f"VALUES (gen_random_uuid(), 'pgr {marker} fire{i}', '{TENANT}', now(), now()) RETURNING 1",
                        quiet=True)
                    # psql -tA still prints the command tag: a committed insert answers
                    # "1\nINSERT 0 1", so match the RETURNING tuple, not the whole blob
                    # (the first run counted "~0 committed" while PostgreSQL held 8).
                    if out.splitlines() and out.splitlines()[0] == "1":
                        landed[0] += 1
                    time.sleep(0.1)

            w = threading.Thread(target=writer)
            w.start()
            await asyncio.sleep(1.0)
            t0 = time.monotonic()
            r = pg_ctl("stop", "-m", "fast")
            stop_s = time.monotonic() - t0
            fire_stop.append(True)
            w.join(timeout=15)
            if r.returncode == 0 and stop_s < 25:
                zb.ok(f"stop UNDER FIRE completed in {stop_s:.1f}s with ~{landed[0]} committed inserts "
                      "in flight — the walsender was released by the PubAck-driven confirms, "
                      "not the (gated-off) drained fast-ack")
            else:
                zb.bad(f"stop under load: rc={r.returncode} after {stop_s:.1f}s — "
                       "the shutdown hostage is back the moment the pipeline is busy")
                failed += 1
            if br.proc.poll() is not None:
                zb.bad("the bridge died during the under-fire stop"); ensure_pg_up(); return failed + 1

            # full stop, brief hold, restart
            while pg_up():
                await asyncio.sleep(0.5)
            await asyncio.sleep(2)
            pg_start()
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and not pg_up():
                await asyncio.sleep(0.5)
            if not pg_up():
                zb.bad("PostgreSQL did not return after the under-fire stop"); return failed + 1

            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                m = metrics()
                if re.search(r"bridge_connected 1", m):
                    break
                await asyncio.sleep(1)

            # PostgreSQL is the only truth: whatever the fast stop aborted stays gone.
            truth = int(zb.psql(f"SELECT count(*) FROM public.{TABLE} "
                                f"WHERE some_text LIKE 'pgr {marker} fire%' AND deleted_at IS NULL"))
            deadline = time.monotonic() + 60
            got = -1
            while time.monotonic() < deadline:
                take(lib.zb_client_poll(h, 400))
                r3 = take(lib.zb_client_query(
                    h, f"SELECT count(*) FROM {TABLE} WHERE some_text LIKE ?".encode(),
                    json.dumps([f"pgr {marker} fire%"]).encode()))
                got = r3["rows"][0][0] if r3.get("rows") else -1
                if got == truth:
                    break
                await asyncio.sleep(0.5)
            if got == truth and truth >= 3:
                zb.ok(f"under-fire convergence: PostgreSQL committed {truth} of the loaded inserts "
                      f"and the client holds exactly {got} — every survivor delivered, every "
                      "aborted write stayed aborted")
            else:
                zb.bad(f"under-fire divergence: PostgreSQL {truth}, client {got} "
                       f"(landed-by-writer ~{landed[0]})")
                failed += 1
    finally:
        if h:
            lib.zb_client_close(h)
        _env.rm_sqlite(db)

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)

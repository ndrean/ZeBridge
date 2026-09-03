"""A CDC stream that REFUSES writes: the retry budget burns, the bridge stops itself,
and a restart after the repair loses nothing (NOTES §10bu).

    scripts/scenarios/run.py owns -k stream_full     (owns the bridge; edits CDC_<tenant>)

`nats_outage.py` proved the other arm: a broker that is GONE parks the publisher in
reconnect and the retry budget never burns. The budget is for a publish that FAILS on a
CONNECTED broker — and the one realistic way to get that is storage: a stream at its
byte cap with `discard: new` answers every publish with an error PubAck. That is the
path that ends in the documented deliberate stop (batch_publisher.zig):

    ❌ NATS publish failed (attempt 1/6) … ⏳ backoff …
    🔴 FATAL: Exhausted retries for batch publish - stopping bridge to prevent WAL overflow

Stopping is the DESIGN: the LSN is acked only on PubAck, so continuing would grow an
unbounded gap between WAL read and WAL delivered; a stopped bridge instead leaves the
slot holding everything undelivered.

  1. the tenant stream is capped just above its current size with `discard: new`
     (the bridge does not repair configs at boot — create-when-missing only — so the
     drift persists until this scenario restores it)
  2. writes fill the gap; the next batch is refused → 6 attempts → FATAL → exit
  3. the slot survives, holding the undelivered WAL
  4. config restored, bridge restarted: everything replays, a client converges, no loss
"""
import asyncio
import ctypes
import json
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

TABLE = "test_types"
TENANT = "acme"
PRINCIPAL = "alice"
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_stream_full_bridge.log"
SAVED = {}


def stream() -> str:
    return zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + TENANT


def stream_json() -> dict:
    return json.loads(zb.nats_cli("stream", "info", stream(), "-j").stdout)


def slot_name() -> str:
    return zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"


def restore_stream() -> None:
    if SAVED:
        zb.nats_cli("stream", "edit", stream(),
                    "--max-bytes", str(SAVED["max_bytes"]), "--discard", SAVED["discard"], "-f")


def cleanup(marker: str) -> None:
    restore_stream()
    # Soft delete — a physical DELETE on a tombstoned table never reaches a client (§10bm).
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


def write_row(marker: str, i: int, size: int = 400) -> None:
    zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
            f"VALUES (gen_random_uuid(), 'full {marker} {i} ' || repeat('x', {size}), "
            f"'{TENANT}', now(), now())")


async def run(marker: str) -> int:
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")
    failed = 0

    info = stream_json()
    SAVED["max_bytes"] = info["config"]["max_bytes"]
    SAVED["discard"] = info["config"]["discard"]

    lib = _env.load_lib()
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

    def take(p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: lib.zb_free(p)

    db = f"/tmp/zb-stream-full-{os.getpid()}.sqlite3"
    _env.rm_sqlite(db)
    h = 0
    try:
        with zb.Bridge(LOG) as br:
            if not br.wait_for_log("Replication started successfully", timeout=40):
                zb.bad("probe bridge did not start"); return 1
            h = lib.zb_client_open(json.dumps({
                "url": zb.nats_server(), "credsPath": zb.creds_for(PRINCIPAL),
                "grammarPath": str(zb.ROOT / "grammar.json"), "dbPath": db,
                "principal": PRINCIPAL, "clientId": "py-stream-full", "tables": [TABLE]}).encode())
            if not h:
                zb.bad("libzb client could not open"); return 1
            take(lib.zb_client_sync(h)); take(lib.zb_client_poll(h, 500))

            def rows_local() -> int:
                take(lib.zb_client_poll(h, 400))
                r = take(lib.zb_client_query(
                    h, f"SELECT count(*) FROM {TABLE} WHERE some_text LIKE ?".encode(),
                    json.dumps([f"full {marker} %"]).encode()))
                return r["rows"][0][0] if r.get("rows") else -1

            # one row through, to prove the pipe before constricting it
            write_row(marker, 0)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and rows_local() < 1:
                await asyncio.sleep(0.3)
            if rows_local() < 1:
                zb.bad("the pipe is not flowing before the test even starts"); return 1
            zb.ok(f"client {PRINCIPAL}/{TENANT} live on {stream()}; one row through")

            # ── 1. cap the stream just above its current size, refuse new ────────
            cur = stream_json()["state"]["bytes"]
            cap = cur + 2048
            r = zb.nats_cli("stream", "edit", stream(), "--max-bytes", str(cap), "--discard", "new", "-f")
            if r.returncode != 0:
                zb.bad(f"could not edit the stream: {r.stderr.strip()[:120]}"); return 1
            zb.ok(f"{stream()} capped at {cap} bytes with discard:new "
                  f"({cur} used — ~2KB of headroom, then every publish is refused)")

            # ── 2. write until the budget burns and the bridge stops itself ──────
            for i in range(1, 40):
                write_row(marker, i)
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline and br.proc.poll() is None:
                await asyncio.sleep(1)
            log_text = LOG.read_text(errors="replace")
            if br.proc.poll() is None:
                zb.bad("the bridge is still running 90s after the stream went full — "
                       "the FATAL retry-budget path never fired")
                br.proc.kill()
                return failed + 1
            attempts = log_text.count("NATS publish failed (attempt")
            if "Exhausted retries" in log_text and attempts >= 6:
                zb.ok(f"the bridge STOPPED ITSELF: {attempts} refused attempts, then "
                      "'FATAL: Exhausted retries … stopping bridge to prevent WAL overflow'")
            else:
                zb.bad(f"the bridge exited but not through the retry budget "
                       f"(attempts={attempts}, exhausted={'Exhausted retries' in log_text})")
                failed += 1

        # ── 3. the slot holds the undelivered WAL ────────────────────────────────
        slot = zb.psql(f"SELECT wal_status || ' ' || active FROM pg_replication_slots "
                       f"WHERE slot_name = '{slot_name()}'").strip()
        if slot.startswith(("reserved", "extended")) and slot.endswith(("f", "false")):
            zb.ok(f"the slot survives, inactive and retaining ({slot}) — nothing acked past the refusal")
        else:
            zb.bad(f"unexpected slot state after the stop: '{slot}'")
            failed += 1

        # ── 4. repair, restart, converge ─────────────────────────────────────────
        restore_stream()
        with zb.Bridge(LOG.with_suffix(".2.log")) as br:
            if not br.wait_for_log("Replication started successfully", timeout=40):
                zb.bad("bridge did not come back after the repair"); return failed + 1
            expected = int(zb.psql(f"SELECT count(*) FROM public.{TABLE} "
                                   f"WHERE some_text LIKE 'full {marker} %' AND deleted_at IS NULL"))
            deadline = time.monotonic() + 90
            local = 0
            while time.monotonic() < deadline:
                local = rows_local()
                if local >= expected:
                    break
                await asyncio.sleep(0.5)
            if local == expected:
                zb.ok(f"NO LOSS: after the repair every row replayed from the slot — "
                      f"{local}/{expected} on the client, including the batch that burned the budget")
            else:
                zb.bad(f"rows lost across the stop: client {local}, PostgreSQL {expected}")
                failed += 1
    finally:
        if h:
            lib.zb_client_close(h)
        _env.rm_sqlite(db)

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)

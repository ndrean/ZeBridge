"""A CDC stream deleted wholesale under a live client (NOTES §10ca).

    scripts/scenarios/run.py owns -k stream_wipe      (owns the bridge)

The one path to the gap rule's THIRD shape (`stored > last_seq`: the feed restarted
under you) that does not go through a slot loss — and it chains three behaviours this
suite already proved separately:

  1. mid-run, the deleted stream refuses every publish → the retry budget burns → the
     bridge STOPS ITSELF and actually exits (§10bu), the slot keeping every unacked row
  2. on restart, boot reconciliation is create-when-missing → the stream comes back,
     EMPTY, numbering restarted at 1 — and the slot replays the whole gap into it
  3. the returning client's position is beyond the new stream's tail: third gap shape,
     position reset, re-seed from the chain, tail from the fresh start — convergence
     with NOTHING lost, because the LSN was never acked past the deletion

Unlike slot_loss.py there is no data-loss window at all: the slot survived, so this is
the benign feed restart. What it must NOT be is silent divergence.
"""
import asyncio
import ctypes
import json
import os
import pathlib
import re
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

TABLE = "test_types"
TENANT = "acme"
PRINCIPAL = "alice"
ROWS_WHILE_GONE = 12
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_stream_wipe_bridge.log"


def stream() -> str:
    return zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + TENANT


def slot_name() -> str:
    return zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"


def cleanup(marker: str) -> None:
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

    def q1(h, sql, params=()):
        r = take(lib.zb_client_query(h, sql.encode(), json.dumps(list(params)).encode()))
        return r["rows"][0][0] if r.get("rows") else None

    db = f"/tmp/zb-stream-wipe-{os.getpid()}.sqlite3"
    _env.rm_sqlite(db)
    h = 0
    try:
        with zb.Bridge(LOG) as br:
            if not br.wait_for_log("Replication started successfully", timeout=60):
                zb.bad("probe bridge did not start"); return 1
            h = lib.zb_client_open(json.dumps({
                "url": zb.nats_server(), "credsPath": zb.creds_for(PRINCIPAL),
                "grammarPath": str(zb.ROOT / "grammar.json"), "dbPath": db,
                "principal": PRINCIPAL, "clientId": "py-stream-wipe", "tables": [TABLE]}).encode())
            if not h:
                zb.bad("libzb client could not open"); return 1
            take(lib.zb_client_sync(h))

            # one row through, so the client's stored position is meaningfully > 0
            zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                    f"VALUES (gen_random_uuid(), 'wipe {marker} pre', '{TENANT}', now(), now())")
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                take(lib.zb_client_poll(h, 400))
                if q1(h, "SELECT count(*) FROM test_types WHERE some_text = ?", [f"wipe {marker} pre"]) == 1:
                    break
                await asyncio.sleep(0.3)
            pos = int(q1(h, "SELECT last_seq FROM _zbz_stream_seq WHERE stream = ?", [stream()]) or 0)
            if pos == 0:
                zb.bad("the client never established a stream position"); return 1
            zb.ok(f"client {PRINCIPAL}/{TENANT} live, position {pos} on {stream()}; then the stream is DELETED wholesale")

            # ── 1. the stream vanishes; the bridge must stop itself ──────────
            r = zb.nats_cli("stream", "rm", stream(), "-f")
            if r.returncode != 0:
                zb.bad(f"could not delete the stream: {r.stderr.strip()[:100]}"); return 1
            for i in range(ROWS_WHILE_GONE):
                zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                        f"VALUES (gen_random_uuid(), 'wipe {marker} {i}', '{TENANT}', now(), now())")
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline and br.proc.poll() is None:
                await asyncio.sleep(1)
            log_text = LOG.read_text(errors="replace")
            if br.proc.poll() is not None and "Exhausted retries" in log_text:
                zb.ok("the bridge burned its retry budget on the missing stream and EXITED "
                      "(the §10bu stop, on a second refusal shape)")
            else:
                if br.proc.poll() is None:
                    br.proc.kill()
                    zb.bad("the bridge kept running against a deleted stream — publishes go "
                           "nowhere and the WAL gap grows unacked")
                else:
                    zb.bad("the bridge exited but not through the retry budget")
                return failed + 1

        slot = zb.psql(f"SELECT wal_status FROM pg_replication_slots WHERE slot_name = '{slot_name()}'").strip()
        if slot in ("reserved", "extended"):
            zb.ok(f"the slot survives ({slot}) — every row written into the outage is retained, unacked")
        else:
            zb.bad(f"unexpected slot state '{slot}'"); failed += 1

        # ── 2. restart: reconciliation recreates, the slot replays ──────────
        with zb.Bridge(LOG.with_suffix(".2.log")) as br:
            if not br.wait_for_log("Replication started successfully", timeout=90):
                zb.bad("bridge did not come back"); return failed + 1
            text2 = br.text()
            if re.search(r"created stream " + re.escape(stream()), text2):
                zb.ok(f"boot reconciliation recreated {stream()} (create-when-missing) — fresh numbering")
            else:
                zb.bad(f"the recreated stream is not in the boot log — who made it?"); failed += 1

            # ── 3. the client returns: third gap shape, reset, converge ─────
            take(lib.zb_client_sync(h)); take(lib.zb_client_poll(h, 1500))
            expected = int(zb.psql(f"SELECT count(*) FROM public.{TABLE} "
                                   f"WHERE some_text LIKE 'wipe {marker}%' AND deleted_at IS NULL"))
            deadline = time.monotonic() + 60
            local = 0
            while time.monotonic() < deadline:
                take(lib.zb_client_poll(h, 500))
                local = q1(h, "SELECT count(*) FROM test_types WHERE some_text LIKE ?", [f"wipe {marker}%"]) or 0
                if local >= expected:
                    break
                await asyncio.sleep(0.5)
            newpos = int(q1(h, "SELECT last_seq FROM _zbz_stream_seq WHERE stream = ?", [stream()]) or -1)
            if local == expected == ROWS_WHILE_GONE + 1:
                zb.ok(f"convergence: all {expected} rows present ({ROWS_WHILE_GONE} written while the "
                      f"stream did not exist) — position reset {pos} → {newpos} on the fresh numbering, "
                      "re-seeded and tailing; nothing lost, nothing phantom")
            else:
                zb.bad(f"diverged: replica {local}, PostgreSQL {expected} — the third gap shape "
                       f"did not recover the wipe (position {pos} → {newpos})")
                failed += 1
    finally:
        if h:
            lib.zb_client_close(h)
        _env.rm_sqlite(db)

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)

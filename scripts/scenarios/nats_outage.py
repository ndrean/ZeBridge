"""NATS down for minutes: the bridge WAITS — it ACKs nothing, the slot retains WAL,
and when the broker returns everything lands without a bridge restart.

    scripts/scenarios/run.py owns -k nats_outage        (stops the broker; owns the bridge)

Two different budgets, and the first run of this scenario confused them (2026-08-29):
`Retry.publish_max_retries` is the budget for a publish that FAILS on a connected
broker — exhausting it is FATAL, the bridge exits rather than ACK an LSN it never
delivered. A broker that is simply GONE is the publisher's reconnect loop instead
(exponential backoff to `max_backoff_ms`), which never gives up: the bridge stays on the
WAL stream, confirms no LSN, and resumes when the broker is back. Measured: 3 minutes
down, 12 rows committed meanwhile, all 12 in CDC_PUBLIC after the restart, 0 duplicates,
no bridge exit. That is the contract asserted here. JetStream dedups by msg id inside its
120 s window — beyond it a republished batch CAN duplicate, so the count is convergence
(every uid present), duplicates reported, never failed.

  1. probe bridge up, baseline write → CDC_PUBLIC
  2. kill nats-server; commit N rows while it is down
  3. the bridge stays up and the slot's confirmed_flush_lsn does NOT advance (nothing ACKed)
  4. restart nats-server (same conf) — the SAME bridge process reconnects
  5. every uid written during the outage is in CDC_PUBLIC; duplicates counted, not failed
"""
import asyncio
import os
import pathlib
import subprocess
import sys
import time
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
from chaos import running_nats, nats_conf_and_log  # noqa: E402

TABLE = "memo"
N = 12
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_nats_outage_bridge.log"
OUTAGE_S = 45


def write(uids: list[str]) -> None:
    values = ", ".join(f"(gen_random_uuid(), 'outage {u}', now())" for u in uids)
    # memo(uid uuid, txt text, updated_at) — the fixture is public and read-only
    zb.psql(f"INSERT INTO public.{TABLE} (uid, txt, updated_at) VALUES {values}")


async def main() -> int:
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge and kills the broker")
    failed = 0
    pid, argv = running_nats()
    conf, natslog = nats_conf_and_log(argv)

    with zb.Bridge(LOG) as bridge:
        if not bridge.wait_for_log("Replication started successfully", timeout=40):
            zb.bad("bridge did not start"); return 1
        nc = await zb.connect()
        js = nc.jetstream()
        public = zb.TOPOLOGY["cdc_streams"]["public"]
        before = (await js.stream_info(public)).state.last_seq

        base = str(uuid.uuid4())[:8]
        write([f"{base}-baseline"])
        for _ in range(30):
            if (await js.stream_info(public)).state.last_seq > before: break
            await asyncio.sleep(0.5)
        else:
            zb.bad("baseline write never reached CDC_PUBLIC"); return 1
        zb.ok("baseline: a write reaches CDC_PUBLIC")
        await nc.close()

        # ── 2. broker down, writes keep committing ────────────────────────────
        subprocess.run(["kill", pid], check=True)
        for _ in range(50):
            if subprocess.run(["kill", "-0", pid], capture_output=True).returncode != 0: break
            time.sleep(0.2)
        outage = [f"{base}-{i:02d}" for i in range(N)]
        t0 = time.monotonic()
        write(outage)
        zb.ok(f"broker killed; {N} rows committed while it is down")

        # ── 3. the bridge waits: alive, and ACKing nothing ────────────────────
        slot = zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1]
        flushed_before = zb.psql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{slot}'").strip()
        time.sleep(OUTAGE_S)
        flushed_after = zb.psql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{slot}'").strip()
        alive = bridge.proc.poll() is None
        if alive and flushed_after == flushed_before:
            zb.ok(f"{OUTAGE_S}s without a broker: bridge still up, confirmed_flush_lsn held at {flushed_before} — nothing ACKed, WAL retained")
        else:
            zb.bad(f"during the outage: alive={alive}, confirmed_flush_lsn {flushed_before} → {flushed_after} (an ACK without a publish would be data loss)")
            failed += 1

        # ── 4. broker back — the SAME bridge reconnects ───────────────────────
        with open(natslog, "ab") as lf:
            subprocess.Popen(argv, stdout=lf, stderr=subprocess.STDOUT, cwd=zb.ROOT, start_new_session=True)
        for _ in range(50):
            if subprocess.run(["nats", "--server", zb.nats_server(), "--creds", zb.creds_for("bridge"), "server", "check", "connection"],
                              capture_output=True).returncode == 0: break
            time.sleep(0.2)
        nc = await zb.connect()
        js = nc.jetstream()
        subject = zb.subject(zb.TOPOLOGY["subjects"]["cdc_prefix"], TABLE, ">")
        seen: dict[str, int] = {}
        sub = await js.subscribe(subject, stream=public, ordered_consumer=True)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and len(seen) < N:
            try:
                m = await sub.next_msg(timeout=2)
            except Exception:
                continue
            doc = zb.decode(m.data)
            for ev in (doc if isinstance(doc, list) else [doc]):
                body = str((ev.get("data") or {}).get("txt", ""))
                for u in outage:
                    if u in body:
                        seen[u] = seen.get(u, 0) + 1
        await nc.close()
        missing = [u for u in outage if u not in seen]
        dups = sum(v - 1 for v in seen.values() if v > 1)
        if not missing:
            zb.ok(f"every row committed during the outage is in CDC_PUBLIC ({N}/{N}); duplicates: {dups} (dedup window 120 s — counted, not failed)")
        else:
            zb.bad(f"{len(missing)} outage row(s) never reached CDC_PUBLIC: {missing[:3]}…")
            failed += 1

    zb.psql(f"DELETE FROM public.{TABLE} WHERE txt LIKE 'outage {base}%'", quiet=True)
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)

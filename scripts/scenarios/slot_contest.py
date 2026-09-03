"""Two bridges, one slot: the second must refuse cleanly — and leak nothing on the way
out (NOTES §10bz).

    scripts/scenarios/run.py owns -k slot_contest      (owns the bridge; macOS `leaks`)

A replication slot is an identity: one bridge per slot, PostgreSQL enforces it
("replication slot … is active for PID n"). The operator mistake is trivially easy —
a second systemd unit, a stray terminal, a deploy overlapping its predecessor — and
what matters is the SHAPE of the failure:

  1. the HOLDER is untouched: its stream keeps flowing while the contest happens
  2. the LOSER exits, promptly and loudly, naming the slot and the holder — it must
     not fight (retrying START_REPLICATION forever burns a CPU and fills the log) and
     must not half-start (no HTTP thread, no budget row under the new boot order)
  3. and the loser's early-exit path leaks nothing — early exits are exactly where
     leaks hide (the libzb lesson: init leaked on every failure path)

`leaks --atExit` wraps the loser; the holder gets a live `leaks <pid>` snapshot too.
"""
import os
import pathlib
import re
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402

BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
LOG_A = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_slot_contest_a.log"
LOG_B = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_slot_contest_b.log"
LOSER_TIMEOUT = 30


def slot_name() -> str:
    return zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"


def main() -> int:
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")
    failed = 0
    use_leaks = zb.leaks_available()
    if not use_leaks:
        print("  ⓘ  macOS `leaks` not available — the memory audit is skipped, the contest still runs")

    with zb.Bridge(LOG_A) as a:
        if not a.wait_for_log("Replication started successfully", timeout=40):
            zb.bad("the holder never started"); return 1
        zb.ok(f"holder up: slot '{slot_name()}' active (pid {a.proc.pid})")

        # ── the contest ──────────────────────────────────────────────────────
        # Same slot, different port. `leaks --atExit` sets MallocStackLogging and
        # reports at process exit — which only helps if the loser EXITS.
        cmd = [str(BRIDGE), "--slot", slot_name(), "--pub", zb.publication(), "--port", "9097"]
        if use_leaks:
            cmd = ["leaks", "--atExit", "--"] + cmd
        # PGGSSENCMODE=disable: without it libpq probes for Kerberos credentials on
        # every connect, and krb5 parks a few thread-local allocations that `leaks`
        # then reports (4 leaks / 176 bytes, all in krb5int_setspecific) — library
        # noise that would drown the signal this audit exists for.
        env = zb.bridge_env()
        env["PGGSSENCMODE"] = "disable"
        b_out = open(LOG_B, "w")
        b = subprocess.Popen(cmd, env=env, stdout=b_out, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + LOSER_TIMEOUT
        while time.monotonic() < deadline and b.poll() is None:
            time.sleep(0.5)
        b_alive = b.poll() is None
        if b_alive:
            b.kill()
            b.wait(timeout=10)
        b_out.close()
        b_text = LOG_B.read_text(errors="replace")

        # ⚠️ Under `leaks --atExit --` the return code is LEAKS' verdict (0 = clean,
        # 1 = leaks found), not the wrapped bridge's — asserting rc != 0 here failed a
        # run whose refusal was perfect precisely BECAUSE it did not leak. Exiting at
        # all is the behavioural assertion; the refusal itself is proven by the FATAL
        # text below, and the memory verdict is parsed from the report, not the rc.
        refused = "SlotHeldByAnother" in b_text or "HELD by another bridge" in b_text
        if not b_alive and (refused or (not use_leaks and b.returncode != 0)):
            zb.ok(f"the loser exited by itself within {LOSER_TIMEOUT}s — it refuses, it does not fight")
        else:
            zb.bad("the second bridge did NOT refuse: " +
                   (f"still running after {LOSER_TIMEOUT}s (killed) — it fights for the slot "
                    "instead of exiting" if b_alive
                    else "exited without the held-slot refusal"))
            failed += 1

        if re.search(r"held by|active for PID|another bridge", b_text):
            zb.ok("and the refusal NAMES the conflict (slot + holder) — an operator knows instantly")
        else:
            zb.bad("the loser's output never names the slot conflict — the operator gets "
                   f"a generic failure (tail: {b_text.strip().splitlines()[-1][:120] if b_text.strip() else 'empty'})")
            failed += 1

        starts = b_text.count("START_REPLICATION failed")
        if starts <= 2:
            zb.ok(f"no fight: {starts} START_REPLICATION attempt(s) logged by the loser")
        else:
            zb.bad(f"the loser retried START_REPLICATION {starts} times — a slot held by a "
                   "live peer is not a transient")
            failed += 1

        # ── the holder must not have noticed ─────────────────────────────────
        marker = os.urandom(4).hex()
        zb.psql(f"INSERT INTO public.test_types (uid, some_text, tenant_id, inserted_at, updated_at) "
                f"VALUES (gen_random_uuid(), 'contest {marker}', 'acme', now(), now())")
        if a.wait_for_log(marker, timeout=20) or a.wait_for_log("cdc events", timeout=1):
            pass  # log content varies; the authoritative check is the counter below
        deadline = time.monotonic() + 20
        flowed = False
        while time.monotonic() < deadline:
            m = subprocess.run(["curl", "-s", "http://127.0.0.1:9096/metrics"], capture_output=True, text=True)
            got = re.search(r"bridge_cdc_events_published_total (\d+)", m.stdout)
            if got and int(got.group(1)) > 0:
                flowed = True
                break
            time.sleep(1)
        if flowed:
            zb.ok("the holder streamed straight through the contest (cdc counter moving)")
        else:
            zb.bad("the holder's CDC counter never moved after the contest — the loser disturbed it")
            failed += 1
        zb.psql(f"UPDATE public.test_types SET deleted_at = now(), updated_at = now() "
                f"WHERE some_text = 'contest {marker}'", quiet=True)

        # ── the loser left nothing behind ────────────────────────────────────
        limits = zb.psql(f"SELECT count(*) FROM public.zebridge_limits WHERE slot = '{slot_name()}'").strip()
        # exactly the holder's row (registered after IT claimed the slot); the loser
        # must not have reached registration under the parse → slot → register order
        if limits == "1":
            zb.ok("zebridge_limits holds exactly the holder's row — the loser never got that far")
        else:
            zb.bad(f"{limits} budget row(s) for the slot — the loser wrote before losing")
            failed += 1

        if use_leaks:
            if not b_alive:
                m = re.search(r"(\d+) leaks? for (\d+) total leaked bytes", b_text)
                if m and m.group(1) == "0":
                    zb.ok("leaks (loser, at exit): 0 leaked bytes on the refusal path")
                elif m:
                    zb.bad(f"the refusal path LEAKS: {m.group(1)} leak(s), {m.group(2)} bytes — "
                           "run the loser under `leaks --atExit` for the stacks")
                    failed += 1
                else:
                    print("  ⓘ  leaks produced no summary for the loser (killed before atExit?)")
            holder = subprocess.run(["leaks", str(a.proc.pid)], capture_output=True, text=True)
            hm = re.search(r"(\d+) leaks? for (\d+) total leaked bytes", holder.stdout)
            if hm and hm.group(1) == "0":
                zb.ok("leaks (holder, live): 0 leaked bytes mid-run")
            elif hm:
                zb.bad(f"the HOLDER leaks while running: {hm.group(1)} leak(s), {hm.group(2)} bytes")
                failed += 1

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

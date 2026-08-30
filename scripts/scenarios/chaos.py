#!/usr/bin/env python3
"""Chaos — exhaustion and jitter, and how clean the bridge REMAINS.

faults.py owns the NATS listener faults, credentials.py the PG credential
shapes; THIS owns the operational chaos an ops week actually brings, each phase
asserting two things: the documented impact shape, and the cleanliness AFTER —
the bridge either recovers by itself or exits by contract, never limps.

  0  wrong NATS creds at boot → clean refusal, non-zero exit, and ZERO
     DebugAllocator reports (the catalogue-lists leak found on exactly this
     path stays fixed — failure exits must keep the exit audit trustworthy);
  1  baseline: a psql write reaches CDC_PUBLIC (seq grows);
  2  NATS jitter: the broker is KILLED and restarted under the bridge —
     nats_reconnect_count must move, the process must survive, and the next
     write must flow;
  3  PG loss: pg_terminate_backend on every bridge_reader/bridge_writer
     backend (walsender included) — pg reconnect machinery re-attaches, the
     slot resumes, and both a CDC write and a mutation round-trip land after;
  4  HTTP exhaustion (the probe's port, zb.http_base(probe=True)): hold
     max_connections idle sockets — scrapes fail while
     held, then the receive watchdog reaps them and /metrics answers again
     WITHOUT any help from this side (recovery is the server's, not ours);
  5  the audit: macOS `leaks` reports 0 on the still-running probe.

⚠️ Owns the only bridge AND restarts nats-server (browser tabs and any other
consumer WILL disconnect during phase 2). Destructive to comfort, not to data:
JetStream state is file-backed and the PG slot retains WAL through every phase.

Env: ZB_HTTP_RECV_GRACE seconds to wait for the watchdog (default 8).
Usage:  python scripts/scenarios/chaos.py   (admin ZB_PSQL; NATS_CREDS=bridge.creds)
"""

import datetime
import json
import os
import pathlib
import socket
import subprocess
import sys
import time
import urllib.request

import msgpack

import zb

HTTP = zb.http_base(probe=True)   # the probe's --port, from zb.BRIDGE_ARGS
NATS_PID_FILE = zb.ROOT / "scripts" / "native" / "nats-server.pid"
GRACE = int(os.environ.get("ZB_HTTP_RECV_GRACE", "8"))
LOG = str(pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_chaos_bridge.log")
MUT = zb.TOPOLOGY["subjects"]["mutations_prefix"]
PRINCIPAL = os.environ.get("ZB_MUTATION_PRINCIPAL", "omar")   # a client principal mapped to counter_public's tenant


def running_nats() -> tuple[str, list[str]]:
    """(pid, argv) of the live nats-server. The conf it was started with is read
    from ITS argv (`ps -o args=`), never hardcoded: up.sh historically started
    nats-server.conf and the JWT stack runs nats-server-jwt.conf — restarting with
    the wrong one would turn phase 2 into an auth test."""
    pid = ""
    if NATS_PID_FILE.exists():
        pid = NATS_PID_FILE.read_text().strip()
    if not pid or subprocess.run(["kill", "-0", pid], capture_output=True).returncode != 0:
        pid = subprocess.run(["pgrep", "-x", "nats-server"], capture_output=True, text=True).stdout.split()[:1]
        pid = pid[0] if pid else ""
    if not pid:
        sys.exit("no running nats-server found (pid file stale, pgrep empty) — phase 2 restarts it and cannot")
    argv = subprocess.run(["ps", "-o", "args=", "-p", pid], capture_output=True, text=True).stdout.split()
    if not argv:
        sys.exit(f"cannot read nats-server {pid}'s argv")
    return pid, argv


def nats_conf_and_log(argv: list[str]) -> tuple[pathlib.Path, pathlib.Path]:
    """The conf from `-c <path>` in the running argv (relative to zb.ROOT, which is
    where up.sh runs it), and the log named after it (`<stem>.log` beside it)."""
    conf = None
    for i, a in enumerate(argv):
        if a in ("-c", "--config") and i + 1 < len(argv):
            conf = pathlib.Path(argv[i + 1])
    if conf is None:
        sys.exit(f"running nats-server has no -c <conf> in its argv: {' '.join(argv)}")
    if not conf.is_absolute():
        conf = zb.ROOT / conf
    return conf, conf.with_suffix(".log")


def status(field):
    try:
        with urllib.request.urlopen(HTTP + "/status", timeout=5) as r:
            return json.load(r).get(field)
    except Exception:
        return None


def http_code(path, timeout=4):
    try:
        with urllib.request.urlopen(HTTP + path, timeout=timeout) as r:
            return r.status
    except Exception:
        return 0


def cdc_seq():
    r = zb.nats_cli("stream", "info", "CDC_PUBLIC", "--json")
    return json.loads(r.stdout)["state"]["last_seq"] if r.returncode == 0 else -1


def write_flows(tag, timeout=20):
    """A psql INSERT must surface in CDC_PUBLIC. Every failure mode is NAMED:
    a failed seq read, a failed INSERT, and a write that truly never flowed are
    three different bugs and must not share one message (learned the hard way —
    the first run's 'never reached' hid which one it was)."""
    before = cdc_seq()
    if before < 0:
        print(f"  ⓘ  {tag}: cannot read CDC_PUBLIC seq (nats_cli failing) — NOT a flow verdict")
        return False
    row = zb.psql(f"INSERT INTO public.memo (txt) VALUES ('chaos {tag}') RETURNING uid").strip()
    if not row:
        print(f"  ⓘ  {tag}: the INSERT itself failed (psql) — NOT a flow verdict")
        return False
    deadline = time.time() + timeout
    while time.time() < deadline:
        after = cdc_seq()
        if after > before:
            return True
        time.sleep(1)
    print(f"  ⓘ  {tag}: INSERT {row[:8]} committed but CDC_PUBLIC stayed at seq {before} for {timeout}s")
    return False


async def main():
    failed = 0

    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge (and restarts NATS)")

    # Phase 3's mutation round-trip overwrites counter_public.value; restore it at exit.
    value_before = zb.psql("SELECT value FROM public.counter_public LIMIT 1", quiet=True).strip()

    # ── 0. wrong NATS creds: clean refusal, audit-silent ──────────────────────
    # NATS_CREDS=/dev/null is an EMPTY creds file: no JWT, no seed. Under the JWT
    # stack that is a credential the server cannot accept, which is the boot path
    # being tested — a clean refusal, not a hang and not an allocator report.
    bad = subprocess.run(
        [str(zb.BRIDGE), *zb.BRIDGE_ARGS],
        env={**zb.bridge_env(), "NATS_CREDS": "/dev/null"},
        capture_output=True, text=True, timeout=60,
    )
    out = bad.stderr + bad.stdout
    if bad.returncode != 0 and "DebugAllocator" not in out:
        zb.ok(f"bad creds: refused (exit {bad.returncode}) with a SILENT allocator audit — the early-exit leak stays dead")
    else:
        zb.bad(f"bad creds boot: exit {bad.returncode}, DebugAllocator in output: {'DebugAllocator' in out}")
        failed += 1

    # Phase 0's boot reaches replication setup before NATS refuses it, leaving a
    # zb_probe slot at a position phase 1 would silently inherit. Fresh slot, no
    # inherited variable.
    zb.psql("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='zb_probe' AND NOT active) "
            "THEN PERFORM pg_drop_replication_slot('zb_probe'); END IF; END $$", quiet=True)

    try:
      with zb.Bridge(LOG) as bridge:
          if not bridge.wait_for_log("Replication started successfully", timeout=60):
              zb.bad("probe bridge never started — see " + LOG)
              return 1

          # ── 1. baseline ───────────────────────────────────────────────────────
          if write_flows("baseline"):
              zb.ok("baseline: a psql write reaches CDC_PUBLIC")
          else:
              zb.bad("baseline write never reached CDC_PUBLIC")
              return failed + 1

          # ── 2. NATS jitter ────────────────────────────────────────────────────
          before_rc = status("nats_reconnect_count") or 0
          nats_pid, nats_argv = running_nats()
          conf, nats_log = nats_conf_and_log(nats_argv)
          subprocess.run(["kill", nats_pid])
          # wait for the port to actually free, bounded, instead of a fixed sleep
          deadline = time.time() + 15
          while time.time() < deadline and subprocess.run(["kill", "-0", nats_pid], capture_output=True).returncode == 0:
              time.sleep(0.2)
          with open(nats_log, "a") as lg:
              # the SAME argv the server was running with (conf included), from zb.ROOT as up.sh does
              p = subprocess.Popen(nats_argv, cwd=zb.ROOT, stdout=lg, stderr=lg)
          NATS_PID_FILE.write_text(str(p.pid))
          deadline = time.time() + 40
          while time.time() < deadline and (status("nats_reconnect_count") or 0) <= before_rc:
              time.sleep(1)
          rc_after = status("nats_reconnect_count") or 0
          alive = bridge.proc.poll() is None
          flowed = alive and write_flows("post-jitter", timeout=30)
          if alive and flowed:
              zb.ok(f"NATS jitter: broker killed+restarted, nats_reconnects {before_rc}→{rc_after}, "
                    f"process survived, writes flow again")
          elif not alive:
              zb.bad(f"the bridge DIED during broker jitter (exit {bridge.proc.returncode}) — "
                     "a restartable outage must be survivable; see " + LOG)
              failed += 1
          else:
              zb.bad(f"bridge survived but writes do not flow after jitter (reconnects {before_rc}→{rc_after})")
              failed += 1

          # ── 3. PG loss ────────────────────────────────────────────────────────
          before_pg = status("pg_reconnect_count") or 0
          zb.psql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                  "WHERE usename IN ('bridge_reader','bridge_writer')", quiet=True)
          deadline = time.time() + 40
          while time.time() < deadline and (status("pg_reconnect_count") or 0) <= before_pg:
              time.sleep(1)
          pg_after = status("pg_reconnect_count") or 0
          flowed = write_flows("post-pg-kill", timeout=30)
          # a mutation round-trip proves the WRITER reconnected too
          uid = zb.psql("SELECT uid FROM public.counter_public LIMIT 1", quiet=True).strip()
          mut_ok = False
          if uid:
              version = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
              env_bytes = msgpack.packb({"key": {"uid": uid}, "version": version, "client_id": "c-chaos",
                                         "data": {"uid": uid, "value": 777, "updated_at": version}})
              # as a CLIENT principal: the mutation lane is confined per principal by JWT
              nc = await zb.connect_as(PRINCIPAL)
              await nc.jetstream().publish(zb.subject(MUT, PRINCIPAL, "counter_public", "update"), env_bytes,
                                           headers={"Nats-Msg-Id": "chaos-" + version})
              await nc.close()
              deadline = time.time() + 20
              while time.time() < deadline and not mut_ok:
                  mut_ok = zb.psql("SELECT value FROM public.counter_public", quiet=True).strip() == "777"
                  time.sleep(1)
          if flowed and mut_ok:
              zb.ok(f"PG loss: every backend terminated, pg_reconnects {before_pg}→{pg_after}, "
                    "CDC write AND mutation round-trip land after — reader and writer both re-attached")
          else:
              zb.bad(f"PG loss not absorbed: reconnects {before_pg}→{pg_after}, cdc={flowed}, mutation={mut_ok}")
              failed += 1

          # ── 4. HTTP exhaustion on the probe's port, recovery unassisted ───────
          socks = []
          try:
              for _ in range(16):
                  s = socket.create_connection(("127.0.0.1", zb.bridge_port()), timeout=3)
                  socks.append(s)
              during = http_code("/metrics", timeout=3)
              time.sleep(GRACE)  # the receive watchdog's window, from the OTHER side
              after = http_code("/metrics", timeout=5)
              if during == 0 and after == 200:
                  zb.ok(f"exhaustion: 16 idle sockets starve a scrape (as designed), and the watchdog "
                        f"reaps them — /metrics answers 200 within {GRACE}s with the sockets STILL held")
              elif after == 200:
                  zb.ok(f"/metrics survived even during the hold ({during}) and after (200) — cap not reached, still clean")
              else:
                  zb.bad(f"exhaustion not recovered: during={during}, after={after}")
                  failed += 1
          finally:
              for s in socks:
                  try: s.close()
                  except Exception: pass

          # ── 5. the audit on the survivor ──────────────────────────────────────
          if not zb.leaks_available():
              print("  ⓘ  `leaks` not available (macOS only) — the memory audit is skipped, not passed")
          elif bridge.proc.poll() is None:
              rep = subprocess.run(["leaks", "--nocontext", str(bridge.proc.pid)], capture_output=True, text=True).stdout
              if "0 leaks for 0 total leaked bytes" in rep:
                  zb.ok("after all of it: `leaks` reports 0 on the still-running bridge")
              else:
                  tail = [l for l in rep.splitlines() if "leaks for" in l]
                  zb.bad(f"leaks after chaos: {tail}")
                  failed += 1
    finally:
      zb.psql("DELETE FROM public.memo WHERE txt LIKE 'chaos %'", quiet=True)
      if value_before:
          zb.psql(f"UPDATE public.counter_public SET value = {int(value_before)}, updated_at = now()", quiet=True)
      zb.psql("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='zb_probe' AND NOT active) "
            "THEN PERFORM pg_drop_replication_slot('zb_probe'); END IF; END $$", quiet=True)

    print("PASS" if failed == 0 else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":  # importable: nats_outage.py borrows running_nats()
    zb.run(main)

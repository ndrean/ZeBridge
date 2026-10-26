"""PostgreSQL restarts under a RUNNING sweeper: the reconnected session must re-arm
everything, or tombstones pile up silently (NOTES §10cc).

    scripts/scenarios/run.py owns -k sweeper_restart      (stops PostgreSQL briefly)

The sweeper's whole state is per-session: the UTC pin, `zb.principal`, and — since the
PQprepare refactor — its prepared statements, which DIE with the connection. A sweeper
that survives a PostgreSQL restart but fails to re-run its session setup would either
reap as nobody (RLS: 0 rows), reap on the wrong clock, or error every pass with
"prepared statement gc_sweep_0 does not exist" — all three silent in the way §7.5
warns about: the table grows and the watermark quietly stops being true.

  1. a scoped sweeper (SWEEP_ONLY_TABLES = its own fixture) reaps a first batch of
     ripe tombstones — the baseline proves the pipeline
  2. PostgreSQL stops mid-daemon; the sweeper must NOT crash — it warns and retries
  3. PostgreSQL returns; fresh ripe tombstones are seeded
  4. the NEXT pass reaps them — which is only possible if the reconnect re-ran the
     session setup (statements re-prepared, principal re-set, UTC re-pinned) — and
     the output carries no "prepared statement" error and no FATAL
  5. (one sweeper per database, NOTES "One floor, one sweeper") the sweeper's lock
     died with its session and was taken again by the reconnected one: exactly one
     holder before the outage and after it, the floor row survived and never went
     down; and a sweeper killed outright (SIGKILL) frees the lock with its session,
     so a replacement takes it and reaps
"""
import os
import pathlib
import re
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402

FIX = "zb_sweeper_restart_probe"
PG_CTL = zb.PG_CTL
DATADIR = zb.PG_DATA
PG_OPTS = zb.pg_opts()
SWEEPER = zb.ROOT / "zig-out" / "bin" / "bridge_sweeper"
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_sweeper_restart.log"
LOG2 = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_sweeper_restart_replacement.log"


def pg_ctl(*args):
    return subprocess.run([PG_CTL, "-D", DATADIR, *args, "-t", "90"],
                          capture_output=True, text=True, timeout=120)


def pg_up() -> bool:
    return subprocess.run([PG_CTL, "-D", DATADIR, "status"], capture_output=True).returncode == 0


def ensure_pg_up() -> None:
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if pg_up():
            return
        pg_ctl("start", "-o", PG_OPTS, "-l", zb.PG_LOG)
        time.sleep(2)
    print("  ⚠️  PostgreSQL still down after 120s — scripts/native/up.sh needed")


def seed(n: int, tag: str) -> None:
    ripe = zb.ago_ms(zb.gc_retention_ms() + 2 * 3600 * 1000)   # past the stack's own retention
    zb.psql(f"INSERT INTO public.{FIX} (uid, some_text, inserted_at, updated_at, deleted_at) "
            f"SELECT gen_random_uuid(), '{tag}', now(), {ripe}, {ripe} FROM generate_series(1, {n})")


def holders() -> int:
    """Sessions holding the sweeper lock on this database."""
    out = zb.psql("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND classid = 23106 AND objid = 1 "
                  "AND objsubid = 2 AND granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
                  quiet=True)
    return int(out) if out else -1


def floor_epoch() -> float:
    out = zb.psql("SELECT extract(epoch FROM watermark) FROM public.zebridge_gc_watermark WHERE id = 1", quiet=True)
    return float(out) if out else float("nan")


def live_tombstones() -> int:
    out = zb.psql(f"SELECT count(*) FROM public.{FIX} WHERE deleted_at IS NOT NULL")
    return int(out) if out else -1


def main() -> int:
    failed = 0
    proc = None
    logf = None
    try:
        ensure_pg_up()
        zb.psql(f"DROP TABLE IF EXISTS public.{FIX} CASCADE", quiet=True)
        zb.psql(f"CREATE TABLE public.{FIX} (uid uuid PRIMARY KEY, some_text text, "
                "inserted_at timestamptz NOT NULL DEFAULT now(), "
                "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)")
        out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable("
                      f"'public.{FIX}', writable => true, version_col => 'updated_at', "
                      f"tombstone_col => 'deleted_at', "
                      f"public_reason => 'sweeper_restart scenario fixture', "
                      f"publication => '{zb.publication()}', dry_run => false)")
        if "ERROR" in out:
            zb.bad(f"could not enable the fixture: {out[:140]}"); return 1

        zb.require_sweeper_free()
        floor_before = None
        seed(4, "pre-restart")
        # The stack's own retention, never a shorter one (a pass moves the GC floor for good); the
        # tombstones are two hours past it — ripe.
        env = zb.sweeper_env(SWEEP_ONLY_TABLES=FIX, GC_THRESHOLD_MS=zb.gc_retention_ms(),
                             GC_INTERVAL_MS=1500)   # a daemon, sweeping continuously
        logf = open(LOG, "w")
        proc = subprocess.Popen([str(SWEEPER)], env=env, stdout=logf, stderr=subprocess.STDOUT)

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and live_tombstones() != 0:
            time.sleep(1)
        if live_tombstones() == 0:
            zb.ok("baseline: the scoped sweeper reaped all 4 ripe tombstones")
            floor_before = floor_epoch()
            if holders() == 1:
                zb.ok("baseline: exactly one session holds the sweeper lock (the daemon's)")
            else:
                zb.bad(f"baseline: {holders()} sessions hold the sweeper lock, expected the daemon's alone"); failed += 1
        else:
            zb.bad(f"baseline sweep never completed ({live_tombstones()} tombstones left)")
            return failed + 1

        # ── 2. PostgreSQL stops under the running daemon ─────────────────────
        r = pg_ctl("stop", "-m", "fast")
        if r.returncode != 0:
            zb.bad(f"pg stop failed: {r.stderr.strip()[:100]}"); return failed + 1
        time.sleep(8)
        if proc.poll() is not None:
            zb.bad("the sweeper DIED during the PostgreSQL outage — it must warn and retry")
            failed += 1
        else:
            zb.ok("through 8s of refused connections: the sweeper is alive and retrying")

        # ── 3. PostgreSQL returns; fresh ripe tombstones ─────────────────────
        pg_ctl("start", "-o", PG_OPTS, "-l", zb.PG_LOG)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not pg_up():
            time.sleep(0.5)
        seed(4, "post-restart")

        # ── 4. the reconnected session must reap them ────────────────────────
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline and live_tombstones() != 0:
            time.sleep(1)
        logf.flush()
        text = LOG.read_text(errors="replace")
        if live_tombstones() == 0:
            zb.ok("after the restart the daemon reaped the fresh batch — the reconnect "
                  "re-ran the session setup (statements re-prepared, principal re-set, UTC re-pinned)")
        else:
            zb.bad(f"the reconnected sweeper never reaped ({live_tombstones()} left) — "
                   "the exact failure the setup_connection refactor exists to prevent")
            failed += 1
        if "prepared statement" in text and "does not exist" in text:
            zb.bad("the log carries 'prepared statement … does not exist' — reconnect "
                   "reused a dead session's statements")
            failed += 1
        elif re.search(r"Reconnected and initialized", text):
            zb.ok("the log shows the full re-initialization on reconnect, and no "
                  "prepared-statement errors")
        else:
            print("  ⓘ  no explicit 'Reconnected and initialized' line — reap-after-restart "
                  "already proves the session was rebuilt")
        wm = zb.psql("SELECT reaped FROM public.zebridge_gc_watermark WHERE id = 1").strip()
        if wm and int(wm) >= 4:
            zb.ok(f"the watermark row carries the last pass (reaped={wm}) — telemetry survived too")

        # ── 5. the lock and the floor across the outage ──────────────────────
        if re.search(r"holding the database's sweeper lock again", text):
            zb.ok("the reconnected session took the sweeper lock again (the old one died with its connection)")
        else:
            zb.bad("the log never says the sweeper took its lock again after the reconnect — "
                   "it would have idled for good, or reaped without owning the database's GC")
            failed += 1
        if holders() == 1:
            zb.ok("exactly one session holds the sweeper lock after the restart")
        else:
            zb.bad(f"{holders()} sessions hold the sweeper lock after the restart, expected one"); failed += 1
        floor_after = floor_epoch()
        if floor_before is not None and floor_after >= floor_before:
            zb.ok(f"the floor survived the cluster restart and did not go down ({floor_before:.0f} → {floor_after:.0f})")
        else:
            zb.bad(f"the floor went from {floor_before} to {floor_after} across the restart"); failed += 1

        # A sweeper killed outright frees the lock with its session; a replacement takes it and reaps.
        proc.kill(); proc.wait(timeout=10)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 15 and holders() != 0:
            time.sleep(0.5)
        if holders() == 0:
            zb.ok("a SIGKILLed sweeper freed the lock with its session")
        else:
            zb.bad(f"{holders()} session(s) still hold the lock {time.monotonic() - t0:.0f} s after the sweeper was killed"); failed += 1
        seed(4, "after-kill")
        logf2 = open(LOG2, "w")
        proc = subprocess.Popen([str(SWEEPER)], env=env, stdout=logf2, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline and live_tombstones() != 0:
            time.sleep(1)
        logf2.flush()
        if live_tombstones() == 0 and "holding the database's sweeper lock" in LOG2.read_text(errors="replace"):
            zb.ok("the replacement sweeper took the lock and reaped the fresh batch")
        else:
            zb.bad(f"the replacement sweeper did not take over ({live_tombstones()} tombstones left)"); failed += 1
        logf2.close()
    finally:
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        if logf:
            logf.close()
        ensure_pg_up()
        zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{FIX}'", quiet=True)
        zb.forget_table(FIX)
        zb.psql(f"ALTER PUBLICATION {zb.publication()} DROP TABLE public.{FIX}", quiet=True)
        zb.psql(f"DROP TABLE IF EXISTS public.{FIX} CASCADE", quiet=True)

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

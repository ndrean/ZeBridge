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
PG_CTL = "/opt/homebrew/opt/postgresql@18/bin/pg_ctl"
DATADIR = str(zb.ROOT / "postgres-data")
PG_OPTS = ("-p 5432 -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 "
           "-c wal_sender_timeout=300s -c logical_decoding_work_mem=256MB -c wal_buffers=64MB "
           "-c commit_delay=1000 -c commit_siblings=5")
SWEEPER = zb.ROOT / "zig-out" / "bin" / "bridge_sweeper"
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_sweeper_restart.log"


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
        pg_ctl("start", "-o", PG_OPTS, "-l", str(zb.ROOT / "scripts" / "native" / "postgres.log"))
        time.sleep(2)
    print("  ⚠️  PostgreSQL still down after 120s — scripts/native/up.sh needed")


def seed(n: int, tag: str) -> None:
    zb.psql(f"INSERT INTO public.{FIX} (uid, some_text, inserted_at, updated_at, deleted_at) "
            f"SELECT gen_random_uuid(), '{tag}', now(), now() - interval '2 hours', "
            f"now() - interval '2 hours' FROM generate_series(1, {n})")


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

        seed(4, "pre-restart")
        env = dict(os.environ)
        env["SWEEP_ONLY_TABLES"] = FIX
        env["GC_THRESHOLD_MS"] = "3600000"   # 1 h; the tombstones are 2 h old — ripe
        env["GC_INTERVAL_MS"] = "1500"       # a daemon, sweeping continuously
        logf = open(LOG, "w")
        proc = subprocess.Popen([str(SWEEPER)], env=env, stdout=logf, stderr=subprocess.STDOUT)

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and live_tombstones() != 0:
            time.sleep(1)
        if live_tombstones() == 0:
            zb.ok("baseline: the scoped sweeper reaped all 4 ripe tombstones")
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
        pg_ctl("start", "-o", PG_OPTS, "-l", str(zb.ROOT / "scripts" / "native" / "postgres.log"))
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
        zb.psql(f"ALTER PUBLICATION {zb.publication()} DROP TABLE public.{FIX}", quiet=True)
        zb.psql(f"DROP TABLE IF EXISTS public.{FIX} CASCADE", quiet=True)

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

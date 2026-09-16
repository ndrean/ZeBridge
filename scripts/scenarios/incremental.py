#!/usr/bin/env python3
"""The returning client on an incremental chain (§10gs–§10gu, docs/plans incremental fulls).

Four clients go away for different lengths of time and come back. Each is compared with
PostgreSQL row by row — and, because "the rows matched" does not say HOW they matched, each
one's PLAN is read from the core itself: libzb's `chainPlan` over the live manifest and that
client's own stored watermark. A client that quietly reloaded the table would pass a row
comparison and fail here, which is the whole point.

  1. away a moment          → deltas alone; no base, no checkpoint
  2. away across checkpoints → checkpoints and deltas; still no base
  3. away past the chain     → the base
  4. away past the gc watermark, with a row deleted AND REAPED meanwhile
                             → the base, and the ghost row is gone from the replica

Case 4 is the one the watermark rule exists for: a tombstone the sweeper reaped is in no
checkpoint and no delta, so a client that took the incremental path would keep a row
PostgreSQL no longer has, for ever.

⚠️ Short clocks: a 5 s cadence, 15 s checkpoints, a 5 s GC threshold (the sweeper's own
`GC_ALLOW_SHORT_THRESHOLD`). The production defaults are 600 s / 1800 s / 1 h; what this
tests is the RULES, and they do not know the difference.

Usage:  python scripts/scenarios/incremental.py [--keep]
Needs `.env.admin` and `.env.bridge` sourced, a ReleaseFast bridge, bridge_sweeper and
libzb (`zig build -Doptimize=ReleaseFast` in both roots). It builds its own scratch
database and nats-server, like the firehose harness it borrows them from.
"""

import argparse
import ctypes
import json
import os
import pathlib
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import burst_tls as bt  # the isolation helpers: scratch database, nats-server, URLs

ROOT = pathlib.Path(__file__).resolve().parents[2]
LIBZB = ROOT / "libzb" / "zig-out" / "lib" / ("libzbcore.dylib" if sys.platform == "darwin" else "libzbcore.so")
SWEEPER = ROOT / "zig-out" / "bin" / "bridge_sweeper"
TABLE = "inc_rows"
CADENCE = 5
CHECKPOINT_S = 15
GC_MS = 5000

checks: list = []
STARTED = time.time()


def since() -> str:
    """Seconds into the run — a checkpoint cadence is a clock, so the timeline is evidence."""
    return f"[{time.time() - STARTED:5.0f}s]"


def check(ok: bool, name: str, detail: str = "") -> None:
    checks.append(ok)
    print(f"{'✅' if ok else '❌'} {since()} {len(checks)}. {name}{(' — ' + detail) if detail else ''}", flush=True)


def psql(sql: str, **kw):
    return bt.psql(sql, **kw)


def rows_in_pg() -> dict:
    out = psql(f"SELECT uid::text, n::text FROM public.{TABLE} WHERE deleted_at IS NULL ORDER BY uid").stdout
    return {line.split("|")[0]: line.split("|")[1] for line in out.splitlines() if line}


class Client:
    """libzb through its C ABI: the Flutter/iOS path, and the one with a watermark on disk."""

    def __init__(self, lib, url: str, db: pathlib.Path, name: str):
        self.lib, self.db, self.name = lib, db, name
        self.h = lib.zb_client_open(json.dumps({
            "url": url, "dbPath": str(db), "tables": [TABLE], "heartbeatMs": 0,
            "clientId": name, "principal": "incremental", "seedStreaming": True,
        }).encode())
        if not self.h:
            sys.exit(f"{name}: libzb open failed")

    def take(self, p):
        try:
            return json.loads(ctypes.string_at(p).decode())
        finally:
            self.lib.zb_free(p)

    def sync(self):
        return self.take(self.lib.zb_client_sync(self.h))

    def poll(self, ms=1000):
        return self.take(self.lib.zb_client_poll(self.h, ms))

    def close(self):
        self.lib.zb_client_close(self.h)

    def watermark(self) -> str | None:
        """What the replica itself stored — the value the plan is decided on."""
        con = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True, timeout=5)
        try:
            row = con.execute("SELECT watermark FROM _zbz_generations WHERE tbl = ?", (TABLE,)).fetchone()
            return row[0] if row else None
        finally:
            con.close()

    def rows(self) -> dict:
        con = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True, timeout=5)
        try:
            return {str(u): str(n) for u, n in con.execute(f"SELECT uid, n FROM {TABLE}")}
        finally:
            con.close()


def plan_for(lib, manifest: dict, watermark: str | None) -> list:
    """The core's own answer: the steps this watermark would take against this manifest."""
    p = lib.zb_call(b"chainPlan", json.dumps({"manifest": manifest, "watermark": watermark}).encode())
    try:
        return [s["kind"] for s in json.loads(ctypes.string_at(p).decode())]
    finally:
        lib.zb_free(p)


def checkpoint_state(cli: list, watermark: str | None) -> str:
    """Where a missing checkpoint went: the producer never cut one (the bookkeeping is
    empty), or it cut one the manifest does not name (a bug worth the noise)."""
    man = [(ck["gen"], ck["cutoff"]) for ck in manifest(cli).get("checkpoints", [])]
    book = psql(f"SELECT gen || ' ' || cutoff_version::text FROM public.zebridge_generations "
                f"WHERE tbl = '{TABLE}' AND has_checkpoint ORDER BY gen").stdout.split("\n")
    return f"watermark {watermark}; manifest {man}; bookkeeping {[b for b in book if b.strip()]}"


def manifest(cli: list) -> dict:
    raw = subprocess.run(cli + ["kv", "get", "generations", f"_default.{TABLE}", "--raw"],
                         capture_output=True, text=True).stdout
    return json.loads(raw) if raw.strip().startswith("{") else {}


def wait_for(fn, what: str, timeout: int = 90, diagnose=None, each=None):
    """`each` runs on every turn of the loop — a table that is not written to is SKIPPED by
    the producer's tick ("unchanged since gN"), so it cuts no delta and no checkpoint. A
    test that waits for one while writing nothing waits for ever, which is what the first
    version of this scenario did for three minutes."""
    print(f"   ⏱  {since()} waiting up to {timeout}s for {what}", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return True
        if each:
            each()
        time.sleep(1)
    print(f"   ⏳ {since()} gave up waiting for {what}" + (f" — {diagnose()}" if diagnose else ""), flush=True)
    return False


def insert(n: int, base: int):
    psql(f"INSERT INTO public.{TABLE} (n, updated_at) SELECT i, now() FROM generate_series({base}, {base + n - 1}) AS i")


def catch_up(c: Client, want: dict, budget: int = 120) -> dict:
    """Poll until the replica equals PostgreSQL, or the budget runs out."""
    deadline = time.time() + budget
    got = c.rows()
    while time.time() < deadline and got != want:
        c.poll(1000)
        got = c.rows()
    return got


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="leave the scratch database and nats-server behind")
    a = ap.parse_args()
    for path, what in ((LIBZB, "libzb"), (bt.BRIDGE, "the bridge"), (SWEEPER, "bridge_sweeper")):
        if not path.exists():
            sys.exit(f"{what} missing at {path} — build it with zig build -Doptimize=ReleaseFast")

    bt.keep_awake()
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb_incremental_"))
    bridge = nats_proc = None
    try:
        bt.setup_database()
        psql(f"""CREATE TABLE public.{TABLE} (
            uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), n integer NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)""")
        r = psql(f"SELECT step, status, detail FROM zebridge_enable('public.{TABLE}'::regclass, public_reason => 'incremental scenario', "
                 f"tombstone_col => 'deleted_at', publication => '{bt.PUB}', dry_run => false) WHERE status = 'ERROR'")
        if r.stdout.strip():
            sys.exit(f"enable refused: {r.stdout}")
        # The delete guard: a DELETE writes deleted_at, which is what makes a delete an
        # ordinary versioned row a checkpoint can carry (§10gt).
        psql(f"CREATE TRIGGER zebridge_soft_delete_t BEFORE DELETE ON public.{TABLE} "
             f"FOR EACH ROW EXECUTE FUNCTION public.zebridge_soft_delete('deleted_at', 'updated_at')")
        # ⚠️ The SWEEPER needs DELETE on the table, and only an edge-writable table grants
        # it: without this it refused every pass with "permission denied for table
        # inc_rows" while reporting a clean run, and case 4 tested nothing. A table with a
        # tombstone column is normally writable anyway — that is what tombstones are for.
        psql(f"SELECT zebridge_grant_edge_writes('public.{TABLE}')")
        psql(f"DELETE FROM public.zebridge_generations")
        psql(f"SELECT pg_drop_replication_slot('{bt.SLOT}') FROM pg_replication_slots WHERE slot_name = '{bt.SLOT}'", db=None, stop=False)
        # ⚠️ Above `Generations.min_checkpoint_rows` (100,000): below it a pair is left to
        # its full on purpose (§10gu), and the checkpoint cases would test nothing. The
        # first run of this scenario used 5,000 rows and no checkpoint was ever cut.
        insert(150_000, 1)
        # ⚠️ The producer's "is this table big enough to checkpoint" test reads
        # `n_live_tup`, which is ZERO until the statistics collector has seen the table:
        # without this the first minutes of a table's life cut no checkpoint at all, and
        # this scenario waited 180 s for one that was never coming.
        psql(f"ANALYZE public.{TABLE}")

        nats_proc, url, cli = bt.start_nats(tmp, tls=False)
        env = {k: v for k, v in os.environ.items() if k not in ("NATS_BRIDGE_NKEY_SEED", "NATS_CREDS", "ZB_SIGNING_SEED", "NATS_TLS_CA")}
        env.update({
            "DATABASE_READER_URL": bt.db_url(os.environ["DATABASE_READER_URL"], bt.DB),
            "DATABASE_WRITER_URL": bt.db_url(os.environ["DATABASE_WRITER_URL"], bt.DB),
            "NATS_URL": url, "LOG_LEVEL": "debug", "GENERATIONS_ENABLED": "true",
            "GENERATION_CADENCE_SECONDS": str(CADENCE),
            "GENERATION_CHECKPOINT_SECONDS": str(CHECKPOINT_S),
            # ⚠️ 1%: the base here is ~3 MB and a checkpoint ~25 KB, so the production
            # default (100%) would need a hundred of them — half an hour — before the base
            # is rebuilt. Case 3 needs that rebuild, and it is the §10gw rule itself that
            # must produce it, not a TRUNCATE standing in for one.
            "GENERATION_BASE_REBUILD_PERCENT": "1",
            # ⚠️ Deep enough that the depth rotation does NOT rebuild a full every few
            # cuts: a checkpoint is due `CHECKPOINT_S` after the last checkpoint OR FULL,
            # so a chain that keeps making fulls never makes a checkpoint. At depth 3 and a
            # 5 s cadence the first run of this scenario cut none at all.
            "GENERATION_CHAIN_DEPTH": "12",
            "BASE_BUF": "11",
        })
        log = tmp / "bridge.log"
        bridge = subprocess.Popen([str(bt.BRIDGE), "--pub", bt.PUB, "--slot", bt.SLOT, "--port", str(bt.HTTP_PORT)],
                                  env=env, stdout=log.open("w"), stderr=subprocess.STDOUT)
        if not wait_for(lambda: "Replication started" in log.read_text(errors="replace"), "the bridge"):
            sys.exit(f"the bridge did not start:\n{log.read_text(errors='replace')[-1200:]}")
        wait_for(lambda: bool(manifest(cli).get("full")), "the first full")

        lib = ctypes.CDLL(str(LIBZB))
        lib.zb_free.argtypes = [ctypes.c_void_p]
        lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
        lib.zb_client_close.argtypes = [ctypes.c_uint64]
        lib.zb_call.restype, lib.zb_call.argtypes = ctypes.c_void_p, [ctypes.c_char_p, ctypes.c_char_p]
        for n, extra in (("sync", []), ("poll", [ctypes.c_uint64])):
            f = getattr(lib, "zb_client_" + n)
            f.restype, f.argtypes = ctypes.c_void_p, [ctypes.c_uint64] + extra

        def away(name: str) -> tuple:
            """A client that seeds, then leaves. Returns (client, its watermark)."""
            c = Client(lib, url, tmp / f"{name}.sqlite3", name)
            err = c.sync().get("error")
            if err:
                sys.exit(f"{name}: sync failed: {err}")
            wm = c.watermark()
            c.close()
            return c, wm

        # ── 1. away a moment: deltas alone ────────────────────────────────────
        c1, wm1 = away("brief")
        insert(500, 10_001)
        wait_for(lambda: any(d["cutoff"] > wm1 for d in manifest(cli).get("deltas", [])), "a delta after the client left")
        steps = plan_for(lib, manifest(cli), wm1)
        check(steps and "full" not in steps and "checkpoint" not in steps,
              "away a moment: deltas alone", f"plan {steps}")
        c1 = Client(lib, url, tmp / "brief.sqlite3", "brief")
        c1.sync()
        want = rows_in_pg()
        got = catch_up(c1, want)
        check(got == want, "away a moment: the replica equals PostgreSQL", f"{len(got)} rows")
        c1.close()

        # ── 2. away across checkpoints: checkpoints and deltas, no base ───────
        c2, wm2 = away("checkpoints")
        trickle = [20_001]

        def write_a_little():
            trickle[0] += 50
            insert(50, trickle[0])

        wait_for(lambda: len([ck for ck in manifest(cli).get("checkpoints", []) if ck["cutoff"] > wm2]) >= 1,
                 "a checkpoint after the client left", timeout=180,
                 diagnose=lambda: checkpoint_state(cli, wm2), each=write_a_little)
        insert(500, 30_001)
        man2 = manifest(cli)
        covering = [ck for ck in man2.get("checkpoints", []) if ck["cutoff"] > wm2]
        check(bool(covering), "away across checkpoints: the chain HAS one covering the absence",
              f"checkpoints {[(ck['gen'], ck['lower']) for ck in covering]}")
        steps = plan_for(lib, man2, wm2)
        # ⚠️ Today the plan is deltas alone, and that is RIGHT: retention still counts
        # generations, so the deltas themselves reach back past this watermark and the
        # planner prefers them (§10gs rule 2 before rule 3). The promise this case makes
        # either way is the one a client feels — NO base, no reload. When retention is by
        # checkpoints (plan step 3) the deltas stop reaching and the same client will take
        # `['checkpoint', …, 'delta']` here; the assertion below holds for both.
        check("full" not in steps, "away across checkpoints: no base — the client does not reload",
              f"plan {steps}")
        c2 = Client(lib, url, tmp / "checkpoints.sqlite3", "checkpoints")
        c2.sync()
        want = rows_in_pg()
        got = catch_up(c2, want)
        check(got == want, "away across checkpoints: the replica equals PostgreSQL", f"{len(got)} rows")
        c2.close()

        # ── 3. away until the base is REBUILT: the base ───────────────────────
        # With retention by levels a client away a long time is carried by checkpoints —
        # §10gw's whole point, and what this case asserted before it. The base comes back
        # only when the base itself is replaced: the checkpoints since it outweigh it, the
        # lane rebuilds it, and the checkpoints below it retire with everything else older.
        c3, wm3 = away("stale")
        # The chain rebuilds its full and prunes past this watermark (depth 3 here).
        for i in range(14):  # past the depth, so the chain's full is rebuilt beyond this client
            insert(300, 40_001 + i * 1000)
            time.sleep(CADENCE)
        wait_for(lambda: (manifest(cli).get("full") or {}).get("cutoff", "") > wm3,
                 "the base to be rebuilt past the client", timeout=180,
                 diagnose=lambda: checkpoint_state(cli, wm3), each=write_a_little)
        steps = plan_for(lib, manifest(cli), wm3)
        check(steps[:1] == ["full"], "away past the rebuilt base: the base", f"plan {steps}")
        c3 = Client(lib, url, tmp / "stale.sqlite3", "stale")
        c3.sync()
        want = rows_in_pg()
        got = catch_up(c3, want)
        check(got == want, "away past the rebuilt base: the replica equals PostgreSQL", f"{len(got)} rows")
        c3.close()

        # ── 4. away past the gc watermark, with a reaped delete ───────────────
        c4, wm4 = away("reaped")
        victim = psql(f"SELECT uid::text FROM public.{TABLE} WHERE deleted_at IS NULL ORDER BY uid LIMIT 1").stdout.strip()
        check(victim in c4.rows(), "the doomed row is in the replica before it leaves", victim[:8])
        psql(f"DELETE FROM public.{TABLE} WHERE uid = '{victim}'")  # the guard makes it a tombstone
        sweep_env = dict(env, GC_THRESHOLD_MS=str(GC_MS), GC_ALLOW_SHORT_THRESHOLD="1")
        # The tombstone must be older than the threshold when the pass runs, so the first
        # sweep can legitimately find nothing: wait it out and ask again, a few times.
        reaped, sweep = False, None
        for _ in range(4):
            time.sleep(GC_MS / 1000 + 1)
            sweep = subprocess.run([str(SWEEPER), "--once"], env=sweep_env, capture_output=True, text=True, timeout=120)
            reaped = psql(f"SELECT count(*) FROM public.{TABLE} WHERE uid = '{victim}'").stdout.strip() == "0"
            if reaped:
                break
        tail = ((sweep.stdout or "") + (sweep.stderr or "")).strip().splitlines() if sweep else []
        check(reaped, "the sweeper reaped the tombstone", " / ".join(t.strip()[:70] for t in tail[-2:]))
        wait_for(lambda: (manifest(cli).get("gc_watermark") or "") > (wm4 or ""), "a gc watermark past the client", timeout=90)
        man = manifest(cli)
        steps = plan_for(lib, man, wm4)
        check(steps[:1] == ["full"], "away past the gc watermark: the base, whatever the checkpoints say",
              f"plan {steps}, gc {man.get('gc_watermark')} > watermark {wm4}")
        c4 = Client(lib, url, tmp / "reaped.sqlite3", "reaped")
        c4.sync()
        want = rows_in_pg()
        got = catch_up(c4, want)
        check(got == want, "away past the gc watermark: the replica equals PostgreSQL", f"{len(got)} rows")
        check(victim not in got, "the reaped row is gone from the replica — no ghost", victim[:8])
        c4.close()

        passed = sum(1 for c in checks if c)
        print(f"\n{passed}/{len(checks)} checks passed", flush=True)
        return 0 if passed == len(checks) else 1
    finally:
        for p in (bridge, nats_proc):
            if p:
                p.terminate()
                try:
                    p.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    p.kill()
        if a.keep:
            print(f"kept: {tmp}")
        else:
            shutil.rmtree(tmp, ignore_errors=True)
            psql(f"DROP DATABASE IF EXISTS {bt.DB}", db=None, stop=False)


if __name__ == "__main__":
    sys.exit(main())

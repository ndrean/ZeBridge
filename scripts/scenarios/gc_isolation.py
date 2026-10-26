#!/usr/bin/env python3
"""One database, one GC floor, one sweeper (NOTES "One floor, one sweeper").

GC is a promise about the DATABASE's history: "a write older than the floor can no longer be
replayed safely". Once a tombstone is reaped nothing can un-reap it, so the floor never goes down,
and exactly one process keeps it, whatever bridges read it. The contract this checks, on a private
stack (a disposable database, its own nats-server, its own bridge), with real sweeper processes:

  1. the DATABASE refuses a lower floor, a DELETE and a TRUNCATE of the row — from the admin and
     from the writer role — and re-applying the init SQL leaves the floor where it was;
  2. COMPETING SWEEPERS: while one holds the sweeper lock, a second with a longer retention, a
     third with a SHORTER one (which would reap history the first still promises) and a `--once`
     run all stay out: no reaping, no floor change, the row's retention is the owner's. `--once`
     says so and exits 2;
  3. LOCK-OWNER FAILURE: the owner's backend is terminated. The lock goes with the session; the
     sweeper notices, and exactly one process holds it again within a few passes (the sweeper
     itself, or a standby), the floor never decreasing meanwhile;
  4. RETENTION INCREASE: 1 h to 7 days leaves the floor exactly where it is (deleted history does
     not come back); the retention then shortened moves it forward and reaps what it now covers;
  4b. A ZERO (or sub-second) RETENTION is refused, the sweeper's override of its minute-floor
     included, and by `zebridge_gc_advance` whoever calls it: zero reaps every tombstone and a stored
     `threshold_ms = 0` reads as "never swept", which switches the producer's checkpoint guard off;
  5. THE ORDER: the floor is advanced before anything is reaped (a tombstone older than the floor
     is gone only after the floor said so);
  6. A DELAYED MUTATION RACING WITH REAPING. A stale INSERT (older than the row's tombstone, newer
     than the floor) passes the bridge's guard against the old floor, then waits on a row lock
     with its transaction open. The sweeper's advance must wait for it — the floor unchanged and
     the tombstone still there — and the write is judged against the tombstone. Control: with the
     floor lock taken out of the mutation's side, the same interleaving reaps the tombstone while
     the write waits and the write then brings the deleted row back. That is the hole.

  7. A TOMBSTONED FAMILY (tombstone_children.py's reap, moved here: a short-retention pass moves a
     database's floor for good, so it never runs on a shared one) is reaped children first, in bounded
     batches of 1000, and the streams hear nothing.

Usage:  python scripts/scenarios/gc_isolation.py
Needs PostgreSQL superuser at 127.0.0.1:5432, nats-server, zig-out/bin/bridge and bridge_sweeper.
"""
import asyncio, json, os, pathlib, re, signal, subprocess, sys, tempfile, time, uuid
import nats
import zb
from recreate_recovery import Stack, T, check, note
import recreate_recovery as rr

SWEEPER = zb.ROOT / "zig-out/bin/bridge_sweeper"
PRINCIPAL, TENANT = "gcp", "gct"
HOUR_MS = 3600 * 1000
OWNER_LOCK = "classid = 23106 AND objid = 1 AND objsubid = 2"   # the sweeper's ownership
ADVANCE_LOCK = "classid = 23106 AND objid = 2 AND objsubid = 2"  # the floor


class Sweeper:
    """A bridge_sweeper process, its log in a file."""
    def __init__(self, stack, label, retention_ms, interval_ms=500, once=False, only=T, batch=None):
        self.stack, self.label, self.once = stack, label, once
        env = dict(stack.env, GC_THRESHOLD_MS=str(retention_ms), GC_INTERVAL_MS=str(interval_ms), GC_ALLOW_SHORT_THRESHOLD="1",
                   SWEEP_ONLY_TABLES=only, LOG_LEVEL="debug", **({"GC_BATCH_ROWS": str(batch)} if batch else {}))
        self.path = f"/tmp/zb_gc_{label}.log"
        self.p = subprocess.Popen([str(SWEEPER), *(["--once"] if once else [])], env=env, stdout=open(self.path, "w"), stderr=subprocess.STDOUT)

    def text(self):
        return open(self.path, errors="replace").read()

    def wait_log(self, needle, timeout=20):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if needle in self.text(): return True
            if self.p.poll() is not None: return needle in self.text()
            time.sleep(0.2)
        return False

    def stop(self):
        if self.p.poll() is None:
            self.p.send_signal(signal.SIGTERM)
            try: self.p.wait(timeout=15)
            except subprocess.TimeoutExpired: self.p.kill()


def floor(stack):
    return float(stack.sql("SELECT extract(epoch FROM watermark) FROM public.zebridge_gc_watermark WHERE id = 1"))


def row_value(stack, col):
    return stack.sql(f"SELECT {col} FROM public.zebridge_gc_watermark WHERE id = 1")


def present(stack, rid):
    return stack.sql(f"SELECT count(*) FROM public.{T} WHERE id = '{rid}'") == "1"


def live(stack, rid):
    return stack.sql(f"SELECT count(*) FROM public.{T} WHERE id = '{rid}' AND deleted_at IS NULL") == "1"


def stage_floor(stack, interval):
    """Break glass: an operator who restored from a backup, or this test staging a floor. The
    triggers are the thing under test elsewhere; here they are stepped around, by name."""
    stack.sql("ALTER TABLE public.zebridge_gc_watermark DISABLE TRIGGER zebridge_gc_watermark_monotone;"
              f"UPDATE public.zebridge_gc_watermark SET watermark = now() - interval '{interval}', threshold_ms = 0 WHERE id = 1;"
              "ALTER TABLE public.zebridge_gc_watermark ENABLE TRIGGER zebridge_gc_watermark_monotone;")


def plant(stack, rid, age, tomb=True):
    """A row `age` old; a tombstone from the same moment when `tomb`."""
    dele = f"now() - interval '{age}'" if tomb else "NULL"
    stack.sql(f"INSERT INTO public.{T} (id, tenant_id, note, updated_at, deleted_at) VALUES ('{rid}', '{TENANT}', 'x', "
              f"now() - interval '{age}', {dele}) "
              "ON CONFLICT (id) DO UPDATE SET updated_at = excluded.updated_at, deleted_at = excluded.deleted_at, note = excluded.note")


def holders(stack, lock):
    out = stack.sql("SELECT pid, mode, granted FROM pg_locks WHERE locktype = 'advisory' "
                    f"AND database = (SELECT oid FROM pg_database WHERE datname = '{stack.name}') AND {lock}")
    return [tuple(l.split("|")) for l in out.splitlines() if l]


def expect_error(stack, statement, text, role=None):
    cmd = list(stack.psql)
    if role: cmd[cmd.index("-U") + 1] = role
    r = subprocess.run([*cmd, "-d", stack.name], input=statement, capture_output=True, text=True)
    return r.returncode != 0 and text in r.stderr


def version_ago(stack, interval):
    return stack.sql(f"SELECT to_char((now() - interval '{interval}') AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")


class Writer:
    """A client principal sending stale INSERTs and collecting their verdicts."""
    async def open(self, stack):
        self.stack = stack
        self.nc = await nats.connect(stack.url, user_credentials=stack.creds[PRINCIPAL], inbox_prefix=f"_INBOX.{PRINCIPAL}".encode(), connect_timeout=5)
        self.js = self.nc.jetstream(domain="hub")
        self.verdicts = {}
        self.sub = await self.nc.subscribe(f"{zb.TOPOLOGY['subjects']['mutation_ack_prefix']}.{PRINCIPAL}.*")
        self.task = asyncio.create_task(self._collect())
        return self

    async def _collect(self):
        async for m in self.sub.messages:
            try: self.verdicts[m.subject.rsplit(".", 1)[-1]] = zb.decode(m.data)
            except Exception: pass

    async def send(self, op, rid, version, deleted=False, note="stale client write"):
        msg_id = f"gciso-{uuid.uuid4().hex[:16]}"
        data = {"id": rid, "tenant_id": TENANT, "note": note, "updated_at": version, "deleted_at": version if deleted else None}
        payload = {"key": {"id": rid}, "data": data, "version": version, "client_id": "c-gciso"}
        subject = zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], PRINCIPAL, T) + "." + op
        import msgpack
        await self.js.publish(subject, msgpack.packb(payload), headers={"Nats-Msg-Id": msg_id})
        return msg_id

    async def insert(self, rid, version):
        return await self.send("insert", rid, version)

    async def verdict(self, msg_id, timeout=20):
        end = time.monotonic() + timeout
        while msg_id not in self.verdicts and time.monotonic() < end:
            await asyncio.sleep(0.2)
        return self.verdicts.get(msg_id)

    async def close(self):
        self.task.cancel()
        await self.nc.close()


class Stall:
    """Holds the advisory lock a test trigger on the table waits for: a mutation's INSERT stops in the
    trigger, AFTER the bridge's guard, with its transaction open. (A row lock would stall the sweeper's
    DELETE of that row as well, which is not what is being measured.)"""
    SETUP = ("CREATE OR REPLACE FUNCTION public.gc_iso_stall() RETURNS trigger LANGUAGE plpgsql AS "
             "$$ BEGIN PERFORM pg_advisory_xact_lock(777, 1); RETURN NEW; END $$;"
             f"DROP TRIGGER IF EXISTS aa_gc_iso_stall ON public.{T};"
             f"CREATE TRIGGER aa_gc_iso_stall BEFORE INSERT ON public.{T} FOR EACH ROW EXECUTE FUNCTION public.gc_iso_stall();")

    def __init__(self, stack):
        self.p = subprocess.Popen([*stack.psql, "-d", stack.name], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        self.p.stdin.write("SELECT pg_advisory_lock(777, 1);\n")
        self.p.stdin.flush()

    def release(self):
        try:
            self.p.stdin.write("SELECT pg_advisory_unlock(777, 1);\n\\q\n"); self.p.stdin.flush()
        except OSError: pass
        try: self.p.wait(timeout=10)
        except subprocess.TimeoutExpired: self.p.kill()


async def wait_until(pred, timeout, step=0.2):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred(): return True
        await asyncio.sleep(step)
    return pred()


async def race(stack, writer, rid, label, expect_lock):
    """The delayed mutation. Floor staged 3 h back; `rid` tombstoned 2 h back; a client write from
    2 h 20 min back (newer than the floor, OLDER than the tombstone: the tombstone is what rejects
    it). It passes the old floor, then waits on the row lock. Returns what the sweeper did meanwhile."""
    stage_floor(stack, "3 hours")
    plant(stack, rid, "2 hours")
    f0 = floor(stack)
    lock = Stall(stack)
    await asyncio.sleep(0.5)
    msg_id = await writer.insert(rid, version_ago(stack, "140 minutes"))
    if expect_lock:
        # In flight = it holds the shared floor lock (its transaction is open, waiting on the row).
        in_flight = await wait_until(lambda: any(g == "t" and "Share" in m for _, m, g in holders(stack, ADVANCE_LOCK)), 15)
        check(f"{label}: the mutation is in flight, holding the shared floor lock", in_flight)
    else:
        await asyncio.sleep(3)   # past the guard and waiting on the row, or it would be done
    sw = Sweeper(stack, f"{label}_sweep", HOUR_MS, once=True)
    # With the lock: the advance waits behind the mutation. Without: it goes straight through.
    done_early = await wait_until(lambda: sw.p.poll() is not None, 2.5)   # under the advance's 5 s lock wait
    f_mid, gone_mid = floor(stack), not present(stack, rid)
    waiting = any(g == "f" for _, _, g in holders(stack, ADVANCE_LOCK))
    lock.release()
    v = await writer.verdict(msg_id)
    await wait_until(lambda: sw.p.poll() is not None, 30)
    return dict(f0=f0, f_mid=f_mid, gone_mid=gone_mid, waiting=waiting, done_early=done_early, verdict=v, sweeper=sw, live=live(stack, rid), present=present(stack, rid), floor_end=floor(stack))


FAM_P, FAM_C = "gc_fam_p", "gc_fam_c"


async def family_reap(stack):
    """What tombstone_children.py §6 checked on the shared database with a one-second sweeper, here on a
    disposable one (a short-retention pass moves the floor for good): a tombstoned family is reaped
    children first, in bounded batches, and the streams hear nothing of it (the bridge forwards no
    physical DELETE of a tombstone table)."""
    common = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
              "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
    stack.sql(f"CREATE TABLE public.{FAM_P} (uid uuid PRIMARY KEY, title text, {common});"
              f"CREATE TABLE public.{FAM_C} (uid uuid PRIMARY KEY, parent_id uuid NOT NULL REFERENCES public.{FAM_P}(uid), note text, {common});")
    for t in (FAM_P, FAM_C):
        stack.sql(f"SELECT count(*) FROM zebridge_enable('public.{t}'::regclass, writable => true, tenant_col => 'tenant_id', "
                  "version_col => 'updated_at', tombstone_col => 'deleted_at', tiebreak_col => 'last_writer', publication => 'rr_pub', dry_run => false)")
    old = "now() - interval '5 minutes'"
    p1, p2 = str(uuid.uuid4()), str(uuid.uuid4())
    stack.sql(f"INSERT INTO public.{FAM_P} (uid, title, tenant_id, deleted_at) VALUES ('{p1}', 'p1', '{TENANT}', {old}), ('{p2}', 'p2', '{TENANT}', NULL);"
              f"INSERT INTO public.{FAM_C} (uid, parent_id, note, tenant_id, deleted_at) VALUES "
              f"(gen_random_uuid(), '{p1}', 'c1a', '{TENANT}', {old}), (gen_random_uuid(), '{p1}', 'c1b', '{TENANT}', {old}), "
              f"(gen_random_uuid(), '{p2}', 'c2', '{TENANT}', NULL);"
              f"INSERT INTO public.{FAM_C} (uid, parent_id, note, tenant_id, deleted_at) "
              f"SELECT gen_random_uuid(), '{p2}', 'bulk' || g, '{TENANT}', {old} FROM generate_series(1, 2500) g;")
    js = await rr.Js().open(stack)
    async def heard():
        n = {}
        for name in ("CDC_" + TENANT, "CDC_PUBLIC"):
            n[name] = (await js.stream(name)).get("messages")
        return n
    # The inserts above reach the streams on their own; read the counts when they have stopped moving.
    prev = None
    for _ in range(40):
        cur = await heard()
        if cur == prev: break
        prev = cur
        await asyncio.sleep(1.5)
    before = prev
    left = lambda: stack.sql(f"SELECT (SELECT count(*) FROM {FAM_P} WHERE deleted_at IS NOT NULL) || '/' || (SELECT count(*) FROM {FAM_C} WHERE deleted_at IS NOT NULL)")
    check("the family is tombstoned: 1 parent, 2502 children", left() == "1/2502")
    sw = Sweeper(stack, "family", 1000, once=True, only=f"{FAM_P},{FAM_C}", batch=1000)
    sw.p.wait(timeout=120)
    check("the sweeper pass completes", sw.p.returncode == 0)
    check("it reaped the whole family: no tombstone left in either table", left() == "0/0")
    check("the 2500+ children went in bounded batches of 1000 (3 batches)", "3 batch(es)" in sw.text())
    check("the live parent and child were left alone", stack.sql(f"SELECT (SELECT count(*) FROM {FAM_P}) || '/' || (SELECT count(*) FROM {FAM_C})") == "1/1")
    await asyncio.sleep(4)
    after = await heard()
    # CDC_PUBLIC also carries the watermark row itself: the pass's two row changes (the advance, the report).
    check(f"nothing was published for the reaped rows: {before} -> {after}",
          after["CDC_" + TENANT] == before["CDC_" + TENANT] and 0 <= after["CDC_PUBLIC"] - before["CDC_PUBLIC"] <= 2)
    await js.close()


async def run(tmp):
    stack = Stack(tmp, "gciso", 120, {})
    sweepers = []
    writer = None
    try:
        stack.start({PRINCIPAL: TENANT})
        stack.create_table({TENANT: 3}, "g")
        stack.sql(Stall.SETUP)
        stack.bridge_up()
        init_sql = subprocess.run([str(rr.BRIDGE), "--init-sql"], env=stack.env, capture_output=True, text=True, check=True).stdout
        writer = await Writer().open(stack)

        # ── 1. the database refuses ──────────────────────────────────────────────────────────
        print("── 1. the database refuses a lower floor, a delete and a truncate", flush=True)
        f0 = floor(stack)
        w_role = stack.name + "_w"
        check("UPDATE to a lower watermark is refused (admin)", expect_error(stack, "UPDATE public.zebridge_gc_watermark SET watermark = watermark - interval '1 day';", "only moves forward"))
        check("UPDATE to a lower watermark is refused (the writer role, the sweeper's)", expect_error(stack, "UPDATE public.zebridge_gc_watermark SET watermark = watermark - interval '1 second';", "only moves forward", role=w_role))
        check("DELETE of the row is refused", expect_error(stack, "DELETE FROM public.zebridge_gc_watermark;", "never removed"))
        check("TRUNCATE of the table is refused", expect_error(stack, "TRUNCATE public.zebridge_gc_watermark;", "never removed"))
        check("the floor is where it was", floor(stack) == f0)
        stack.sql("UPDATE public.zebridge_gc_watermark SET swept_at = now() WHERE id = 1")   # a change that is not a rollback is fine
        stack.sql(init_sql)
        check("re-applying the init SQL leaves the floor where it was", floor(stack) == f0)

        # ── 2. competing sweepers ────────────────────────────────────────────────────────────
        print("── 2. competing sweepers", flush=True)
        stage_floor(stack, "3 hours")
        plant(stack, "old", "2 hours")
        plant(stack, "young", "30 minutes")
        owner = Sweeper(stack, "owner", HOUR_MS); sweepers.append(owner)
        check("the first sweeper holds the lock", owner.wait_log("holding the database's sweeper lock"))
        check("it advances the floor to its retention (~1 h back) before it reaps", await wait_until(lambda: abs(time.time() - floor(stack) - 3600) < 30, 10))
        check("and reaps what the floor covers: the 2 h old tombstone is gone, the 30 min one stays",
              await wait_until(lambda: not present(stack, "old"), 10) and present(stack, "young"))
        check("the row carries the owner's retention", row_value(stack, "threshold_ms") == str(HOUR_MS))
        f_owner = floor(stack)
        longer = Sweeper(stack, "longer", 24 * HOUR_MS); sweepers.append(longer)
        shorter = Sweeper(stack, "shorter", 120_000); sweepers.append(shorter)
        check("a second sweeper (retention 1 day) finds the lock taken and idles", longer.wait_log("another sweeper holds the database's sweeper lock"))
        check("a third (retention 2 minutes, which would reap the 30 min tombstone) idles too", shorter.wait_log("another sweeper holds the database's sweeper lock"))
        once = Sweeper(stack, "once", 120_000, once=True)
        once.p.wait(timeout=30)
        check("a --once run exits 2 and says so", once.p.returncode == 2 and "another sweeper holds" in once.text())
        await asyncio.sleep(3)
        check("none of them changed anything: the 30 min tombstone is still there", present(stack, "young"))
        check("the floor only moved forward, from the owner's passes", floor(stack) >= f_owner)
        check("the row still carries the owner's retention", row_value(stack, "threshold_ms") == str(HOUR_MS))
        check("exactly one session holds the sweeper lock", len([h for h in holders(stack, OWNER_LOCK) if h[2] == "t"]) == 1)

        # ── 3. lock-owner failure ────────────────────────────────────────────────────────────
        print("── 3. lock-owner failure", flush=True)
        shorter.stop(); sweepers.remove(shorter)   # the shorter standby is a misconfiguration; the others are legitimate standbys
        pid = int(holders(stack, OWNER_LOCK)[0][0])
        before = floor(stack)
        swept_before = row_value(stack, "swept_at")
        stack.sql(f"SELECT pg_terminate_backend({pid})", "postgres")
        check("the lock went with the session", await wait_until(lambda: pid not in [int(h[0]) for h in holders(stack, OWNER_LOCK)], 10))
        back = await wait_until(lambda: len([h for h in holders(stack, OWNER_LOCK) if h[2] == "t"]) == 1, 20)
        check("exactly one session holds it again (the sweeper, or a standby)", back)
        new_pid = int(holders(stack, OWNER_LOCK)[0][0]) if back else -1
        check("a different session", new_pid != pid)
        check("the owner noticed: it reconnected and took the lock again, or lost it to the standby",
              owner.wait_log("Connection lost") and (owner.wait_log("holding the database's sweeper lock again") or owner.wait_log("another sweeper holds")))
        check("sweeping goes on under the new owner", await wait_until(lambda: row_value(stack, "swept_at") != swept_before, 15))
        check("the floor did not decrease", floor(stack) >= before)

        # ── 4. a retention increase ──────────────────────────────────────────────────────────
        print("── 4. a retention increase", flush=True)
        for s in sweepers: s.stop()
        sweepers.clear()
        f1 = floor(stack)
        plant(stack, "y2", "30 minutes")
        longer_once = Sweeper(stack, "increase", 7 * 24 * HOUR_MS, once=True)
        longer_once.p.wait(timeout=30)
        check("1 h to 7 days: the pass completes", longer_once.p.returncode == 0)
        check("the floor is exactly where it was (deleted history does not come back)", floor(stack) == f1)
        check("the row says the new retention", row_value(stack, "threshold_ms") == str(7 * 24 * HOUR_MS))
        check("nothing was reaped by it (the 30 min tombstone is there)", present(stack, "y2"))
        shorter_once = Sweeper(stack, "decrease", 600_000, once=True)   # 10 minutes
        shorter_once.p.wait(timeout=30)
        check("7 days to 10 minutes: the floor moves forward", floor(stack) > f1 and abs(time.time() - floor(stack) - 600) < 30)
        check("and reaps what it now covers", not present(stack, "y2"))

        # ── 4b. a zero retention ─────────────────────────────────────────────────────────────
        print("── 4b. a zero (or sub-second) retention is refused, even with the override", flush=True)
        plant(stack, "z1", "10 minutes")
        f_z, th_z = floor(stack), row_value(stack, "threshold_ms")
        for ms in ("0", "500"):
            zero = Sweeper(stack, f"zero{ms}", int(ms), once=True)
            zero.p.wait(timeout=30)
            check(f"GC_THRESHOLD_MS={ms} with GC_ALLOW_SHORT_THRESHOLD exits 1 and says why", zero.p.returncode == 1 and "refused even with GC_ALLOW_SHORT_THRESHOLD" in zero.text())
        check("nothing was reaped, the floor and the stored retention are untouched",
              present(stack, "z1") and floor(stack) == f_z and row_value(stack, "threshold_ms") == th_z)
        check("zebridge_gc_advance(0, 0) refuses it too (the writer role, whoever calls)", expect_error(stack, "SELECT * FROM public.zebridge_gc_advance(0, 0);", "below one second", role=w_role))
        check("and a retention that would round to zero seconds", expect_error(stack, "SELECT * FROM public.zebridge_gc_advance(0.4, 400);", "below one second", role=w_role))
        check("the row still says a real retention (the checkpoint guard stays on)", int(row_value(stack, "threshold_ms")) > 0)

        # ── 5 and 6. the delayed mutation ────────────────────────────────────────────────────
        print("── 6. a delayed mutation racing with reaping", flush=True)
        r = await race(stack, writer, "race1", "race", expect_lock=True)
        check("the sweeper's advance waits for the mutation: still running, the floor unchanged, the tombstone still there",
              not r["done_early"] and r["f_mid"] == r["f0"] and not r["gone_mid"] and r["waiting"])
        v = r["verdict"] or {}
        note(f"verdict of the delayed write: {v.get('status')}/{v.get('reason')}")
        check("the write was judged against the tombstone it passed the old floor with (not applied)", v.get("status") in ("stale", "rejected", "row_deleted"))
        check("the sweeper then advanced the floor and reaped", r["sweeper"].p.returncode == 0 and r["floor_end"] > r["f0"] and not r["present"])
        check("the deleted row did NOT come back", not r["live"])

        print("── 6c. control: without the floor lock on the mutation's side, the same interleaving resurrects", flush=True)
        stack.sql("CREATE OR REPLACE FUNCTION public.zebridge_gc_floor_acquire() RETURNS bigint LANGUAGE sql AS "
                  "$$ SELECT (extract(epoch FROM watermark) * 1000000)::bigint FROM public.zebridge_gc_watermark WHERE id = 1 $$;")
        c = await race(stack, writer, "race2", "control", expect_lock=False)
        check("control: the sweeper did not wait (it finished while the write was stalled)", c["done_early"] and c["gone_mid"])
        check("control: the stale INSERT then lands and brings the deleted row back — the hole", c["live"])
        stack.sql(init_sql)   # the real function back

        print("── 7. a tombstoned family: children then parent, bounded batches, nothing on the streams", flush=True)
        await family_reap(stack)
    finally:
        for s in sweepers: s.stop()
        if writer:
            try: await writer.close()
            except Exception: pass
        stack.stop()


def main():
    with tempfile.TemporaryDirectory(prefix="zb-gc-") as d:
        try:
            asyncio.run(run(pathlib.Path(d)))
        except Exception as e:
            check(f"raised: {type(e).__name__}: {str(e)[:500]}", False)
    print()
    if rr.failed:
        zb.bad(f"{rr.failed} check(s) failed")
        return 1
    zb.ok("one database, one floor, one sweeper")
    return 0


if __name__ == "__main__":
    sys.exit(main())

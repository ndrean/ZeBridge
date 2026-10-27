#!/usr/bin/env python3
"""The bridge's fixed limits: what each does at its edge, and that none of them stops the bridge.

Found 2026-10-26 (CLAUDE.md, "Troubleshooting: when a fixed limit is the cause"): an error that
escaped `main` left a bridge that was alive, silent and dead (the `errdefer` that sets
`should_stop` ran after the join it should have released; the HTTP listener was closed, which on
Linux does not wake a blocked `accept`), and three limits reached it. Reviewed 2026-10-27: a row
the bridge drops is missing from every replica, so no SUPPORTED identifier may reach the drop, the
database refuses new unsupported tenants, `--diagnose` finds the old ones, and a drop is counted.

Limits are in BYTES and are read from `bridge --limits` (the tenant limit follows the client
library: 128 with the fork's `zebridge 42` (the pinned one), 46 without it). Each case is its own stack:

  A. a 62-byte table boots, publishes its rows and builds a chain
  B. a tenant of the longest supported length, with the longest supported table, publishes and
     builds a chain: no accepted combination loses CDC
  C. a tenant over the limit: the tenant guard refuses it in the DATABASE (counted in bytes, not
     characters); with the guard off (a legacy row), the bridge drops it, counts it by table and
     reason (/metrics, /dropped), marks the table degraded, lives on, and another tenant still
     flows; fixing the value publishes the UPDATE but does not replay the INSERT, and
     zebridge_reseed is what puts the row on the replicas
  D. a 90-byte tenant with a 40-byte table (the combination that overflowed the old 128-byte
     buffer) publishes and builds a chain
  E. an error that escapes the replication loop ends the process (exit 1, `error:` in the log)
     instead of leaving it alive: needs the fault build (ZB_TEST_LOOP_ERROR)
  F. SIGTERM ends an idle bridge without any HTTP request arriving
  G. zebridge_enable refuses a 63-byte table (its derived index name is the table's own)
     with an instruction, instead of '"<table>" is not an index'
  I. `bridge --diagnose` finds the tenant values already in a table that cannot be subject tokens
     and says what to do; once they are fixed it says the identifiers are fine

Usage:  python scripts/scenarios/walls.py [--only A,C]
Needs PostgreSQL superuser at 127.0.0.1:5432, nats-server, zig-out/bin/bridge, and for E the
fault build:  zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults
"""
import asyncio, json, os, pathlib, re, signal, subprocess, sys, tempfile, time, urllib.request

import zb
from recreate_recovery import Stack, check, note, T
import recreate_recovery as rr


def table_sql(name):
    return (f"CREATE TABLE public.{name} (id text PRIMARY KEY, tenant_id text NOT NULL, note text, "
            f"updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz);"
            f"SELECT count(*) FROM zebridge_enable('public.{name}'::regclass, writable => true, tenant_col => 'tenant_id', "
            f"version_col => 'updated_at', tombstone_col => 'deleted_at', publication => 'rr_pub', dry_run => false);")


def insert(stack, name, tenant, rid):
    stack.sql(f"INSERT INTO public.{name} (id, tenant_id, note) VALUES ('{rid}', '{tenant}', 'probe')")


def bridge_limits():
    out = subprocess.run([str(zb.ROOT / "zig-out" / "bin" / "bridge"), "--limits"], capture_output=True, text=True, check=True).stdout
    return json.loads(out)


LIMITS = bridge_limits()
TMAX = LIMITS["tenant_bytes"]


def http(stack, label, path):
    p = stack.procs[label]
    port = p.args[p.args.index("--port") + 1]
    return urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5).read().decode()


def dropped(stack, label):
    return json.loads(http(stack, label, "/dropped"))["dropped"]


def log_text(stack, label):
    return open(stack.log(label), errors="replace").read()


async def until(fn, timeout, step=0.5):
    """The first truthy value of `fn()` (sync or async) within `timeout` seconds, else the last one."""
    end, v = time.monotonic() + timeout, None
    while time.monotonic() < end:
        v = fn()
        if asyncio.iscoroutine(v): v = await v
        if v: return v
        await asyncio.sleep(step)
    return v


async def messages(js, stream):
    return (await js.stream(stream)).get("messages", 0)


async def count_at_least(js, stream, n):
    return await messages(js, stream) >= n


def alive(stack, label):
    return stack.procs[label].poll() is None


async def case_a(tmp):
    name, tenant = "t" * 62, "acme"
    stack = Stack(tmp, "wallsa", 120, {})
    try:
        stack.start({"wp": tenant})
        stack.sql(table_sql(name))
        label = stack.bridge_up()
        insert(stack, name, tenant, "a-1")
        js = await rr.Js().open(stack)
        n = await until(lambda: messages(js, "CDC_" + tenant), 30)
        check("A. a 62-character table publishes its row on CDC_acme", n >= 1)
        man = await until(lambda: js.kv(js.gen, f"{tenant}.{name}"), 90)
        check("A. ... and builds a generation chain", man is not None)
        check("A. ... and the bridge is alive", alive(stack, label))
        await js.close()
    finally:
        stack.stop()


async def case_b(tmp):
    tenant, name = "t" * TMAX, "b" * LIMITS["tenant_scoped_table_bytes"]
    stack = Stack(tmp, "wallsb", 120, {})
    try:
        stack.start({"wp": tenant})
        stack.sql(table_sql(name))
        label = stack.bridge_up()
        insert(stack, name, tenant, "b-live")
        js = await rr.Js().open(stack)
        n = await until(lambda: messages(js, "CDC_" + tenant), 30)
        check(f"B. a {TMAX}-byte tenant with a {len(name)}-byte table (the longest supported pair) publishes its row", n >= 1)
        man = await until(lambda: js.kv(js.gen, f"{tenant}.{name}"), 90)
        check("B. ... and gets a generation chain", man is not None)
        check("B. ... nothing was dropped and the bridge is alive", alive(stack, label) and dropped(stack, label) == [])
        await js.close()
    finally:
        stack.stop()


async def case_c(tmp):
    over = "t" * (TMAX + 1)
    stack = Stack(tmp, "wallsc", 120, {})
    try:
        stack.start({"wp": "acme"})
        stack.sql(table_sql(T))
        # 1. the DATABASE refuses a new unsupported tenant, in bytes
        try:
            insert(stack, T, over, "c-over"); why = ""
        except RuntimeError as e:
            why = str(e)
        check(f"C. the tenant guard refuses {TMAX + 1} bytes in the database and says so in bytes", f"is {TMAX + 1} bytes" in why and f"at most {TMAX} bytes" in why)
        wide = "é" * (TMAX // 2 + 1)    # fewer characters than the limit, more bytes
        try:
            insert(stack, T, wide, "c-wide"); why = ""
        except RuntimeError as e:
            why = str(e)
        check(f"C. {len(wide)} two-byte characters ({len(wide.encode())} bytes) are refused: the limit is bytes, not characters", f"is {len(wide.encode())} bytes" in why)
        try:
            insert(stack, T, "a.b", "c-dot"); why = ""
        except RuntimeError as e:
            why = str(e)
        check("C. a dot is still refused", "metacharacter" in why)
        # 2. a legacy row that got past it (guard off): the bridge drops it, counts it, lives on
        stack.sql(f"ALTER TABLE public.{T} DISABLE TRIGGER zebridge_guard_tenant_t")
        label = stack.bridge_up()
        js = await rr.Js().open(stack)
        insert(stack, T, "acme", "c-ok-0")
        first = await until(lambda: messages(js, "CDC_acme"), 30)
        check("C. control: an ordinary tenant's row is published", first >= 1)
        insert(stack, T, over, "c-bad")
        said = await until(lambda: re.search(r"Dropping insert on '%s' \(#1 since boot\): its tenant value \(%d bytes" % (T, TMAX + 1), log_text(stack, label)), 30)
        check("C. the bridge drops the legacy row with a line that names the bytes and the remedy", bool(said))
        check("C. ... and does not blame the replica identity", "replica identity" not in "".join(l for l in log_text(stack, label).splitlines() if f"{TMAX + 1} bytes" in l))
        d = await until(lambda: dropped(stack, label), 10)
        check("C. /dropped counts it by table and reason, with the fix",
              len(d) == 1 and d[0]["table"] == T and d[0]["reason"] == "tenant_unusable" and d[0]["dropped"] == 1 and "zebridge_reseed" in d[0]["fix"])
        metrics = http(stack, label, "/metrics")
        check("C. /metrics has the counter and marks the table degraded",
              f'bridge_dropped_events_total{{table="{T}",reason="tenant_unusable"}} 1' in metrics and f'bridge_table_degraded{{table="{T}"}} 1' in metrics)
        status = json.loads(http(stack, label, "/status"))
        check("C. /status carries the count, the degraded tables and the limits",
              status["dropped_events"] == 1 and status["degraded_tables"] == 1 and status["limits"]["tenant_bytes"] == TMAX)
        insert(stack, T, "acme", "c-ok-1")
        n = await until(lambda: count_at_least(js, "CDC_acme", 2), 30)
        check("C. the bridge lives on and the other tenant's next row is published", bool(n) and alive(stack, label))
        # 3. recovery: the fix flows as an UPDATE; the dropped INSERT is not replayed; reseed re-publishes
        before = await messages(js, "CDC_acme")
        stack.sql(f"UPDATE public.{T} SET tenant_id = 'acme' WHERE id = 'c-bad'")
        n = await until(lambda: count_at_least(js, "CDC_acme", before + 1), 30)
        check("C. recovery: correcting the value publishes the UPDATE (the dropped INSERT is not replayed)", bool(n))
        epoch0 = int(stack.sql(f"SELECT seed_epoch FROM public.zebridge_catalogue WHERE tbl = '{T}'"))
        stack.sql(f"SELECT zebridge_reseed('{T}')")
        epoch1 = int(stack.sql(f"SELECT seed_epoch FROM public.zebridge_catalogue WHERE tbl = '{T}'"))
        check(f"C. ... and zebridge_reseed bumps the seed epoch ({epoch0} -> {epoch1}) so every replica re-seeds from a fresh full", epoch1 == epoch0 + 1)
        await js.close()
    finally:
        stack.stop()


async def case_d(tmp):
    # The review's example: 90 + 40 bytes. Under the library's old 255-byte subject check the tenant
    # limit is lower (limits.zig), so the case runs at the longest tenant the build supports.
    t_len = min(90, TMAX)
    if t_len < 90: note(f"D at {t_len} bytes: this build's tenant limit is {TMAX}; 90 needs a nats.zig with the fork's zebridge 42 (NATS_ZIG_NOTES 43)")
    tenant, name = "x" * t_len, "n" * 40
    stack = Stack(tmp, "wallsd", 120, {})
    try:
        stack.start({"wp": tenant})
        stack.sql(table_sql(name))
        label = stack.bridge_up()
        insert(stack, name, tenant, "d-1")
        js = await rr.Js().open(stack)
        n = await until(lambda: messages(js, "CDC_" + tenant), 30)
        check(f"D. a {t_len}-byte tenant with a 40-byte table ({3 + 1 + t_len + 1 + 40 + 1 + 6} bytes of subject; at 90 the old 128-byte buffer dropped this) publishes", n >= 1)
        man = await until(lambda: js.kv(js.gen, f"{tenant}.{name}"), 90)
        check("D. ... builds a chain, drops nothing, and the bridge is alive", man is not None and dropped(stack, label) == [] and alive(stack, label))
        await js.close()
    finally:
        stack.stop()


async def case_e(tmp):
    if not rr.FAULT_BRIDGE.exists():
        zb.bad(f"E. {rr.FAULT_BRIDGE} is missing: {rr.FAULT_BRIDGE_BUILD}")
        rr.failed += 1
        return
    stack = Stack(tmp, "wallse", 120, {})
    try:
        stack.start({"wp": "acme"})
        stack.create_table({"acme": 1}, "e")
        stack.bridge_bin = rr.FAULT_BRIDGE
        stack.env = dict(stack.env, ZB_TEST_LOOP_ERROR=T)
        label = stack.bridge_up()
        p = stack.procs[label]
        insert(stack, T, "acme", "e-1")
        end = time.monotonic() + 40
        while p.poll() is None and time.monotonic() < end:
            await asyncio.sleep(0.5)
        text = log_text(stack, label)
        check("E. the fault fired in the replication loop", "ZB_TEST_LOOP_ERROR fired" in text)
        check("E. an error escaping the loop ends the process (it used to stay alive, dead)", p.poll() is not None)
        check("E. ... with a non-zero exit code", p.poll() not in (None, 0))
        check("E. ... and the error is in the log", "error: TestLoopError" in text)
    finally:
        stack.stop()


async def case_f(tmp):
    stack = Stack(tmp, "wallsf", 120, {})
    try:
        stack.start({"wp": "acme"})
        stack.create_table({"acme": 1}, "f")
        label = stack.bridge_up()
        await asyncio.sleep(3)
        p = stack.procs[label]
        t0 = time.monotonic()
        p.send_signal(signal.SIGTERM)
        end = t0 + 25
        while p.poll() is None and time.monotonic() < end:
            await asyncio.sleep(0.25)
        took = time.monotonic() - t0
        note(f"SIGTERM to exit: {took:.1f}s, code {p.poll()}")
        check("F. SIGTERM ends an idle bridge with no HTTP request (it used to wait for the next one)", p.poll() == 0)
    finally:
        stack.stop()


async def case_g(tmp):
    stack = Stack(tmp, "wallsg", 120, {})
    try:
        stack.start({"wp": "acme"})
        try:
            stack.sql(table_sql("t" * 63))
            said = ""
        except RuntimeError as e:
            said = str(e)
        check("G. zebridge_enable refuses a 63-character table and says what to do "
              "(it used to fail with '\"<table>\" is not an index')", "63 bytes" in said and "Rename the table" in said)
    finally:
        stack.stop()


async def case_i(tmp):
    over = "t" * (TMAX + 1)
    stack = Stack(tmp, "wallsi", 120, {})
    try:
        stack.start({"wp": "acme"})
        stack.sql(table_sql(T))
        stack.sql(f"ALTER TABLE public.{T} DISABLE TRIGGER zebridge_guard_tenant_t")
        insert(stack, T, "acme", "i-ok")
        insert(stack, T, over, "i-bad-1")
        insert(stack, T, "a b", "i-bad-2")
        stack.sql(f"INSERT INTO public.{T} (id, tenant_id) VALUES ('i-bad-3', '')")
        argv = [str(stack.bridge_bin), "--diagnose", "--pub", "rr_pub", "--slot", stack.name + "_slot", "--port", str(rr.port())]
        def diagnose():
            r = subprocess.run(argv, env=stack.env, capture_output=True, text=True, errors="replace", timeout=120)
            return r.returncode, r.stdout + r.stderr
        rc, out = diagnose()
        check("I. --diagnose finds the 3 rows whose tenant cannot be a subject token and exits 1",
              rc == 1 and f"table '{T}': 3 row(s) hold a tenant value" in out)
        check("I. ... names the limit in bytes, a sample, and the recovery (re-seed)",
              f"longer than {TMAX} bytes" in out and "e.g. " in out and f"zebridge_reseed('{T}')" in out)
        stack.sql(f"UPDATE public.{T} SET tenant_id = 'acme' WHERE id LIKE 'i-bad-%'")
        rc, out = diagnose()
        check("I. once the values are fixed it says the identifiers are fine", "identifiers: tenant values of 1 tenant-scoped table(s)" in out and "hold a tenant value" not in out)
    finally:
        stack.stop()


CASES = {"A": case_a, "B": case_b, "C": case_c, "D": case_d, "E": case_e, "F": case_f, "G": case_g, "I": case_i}


def main():
    only = None
    if "--only" in sys.argv:
        only = sys.argv[sys.argv.index("--only") + 1].upper().split(",")
    for key, fn in CASES.items():
        if only and key not in only: continue
        print(f"\n── {key}: {fn.__doc__ or fn.__name__}", flush=True)
        with tempfile.TemporaryDirectory(prefix="zb-walls-") as d:
            try:
                asyncio.run(fn(pathlib.Path(d)))
            except Exception as e:
                check(f"{key} raised: {type(e).__name__}: {str(e)[:500]}", False)
    print()
    if rr.failed:
        zb.bad(f"{rr.failed} check(s) failed")
        return 1
    zb.ok("no fixed limit stops the bridge, and each says what it refused")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""The bridge's fixed limits: what each does at its edge, and that none of them stops the bridge.

Found 2026-10-26 (CLAUDE.md, "Troubleshooting: when a fixed limit is the cause"): an error that
escaped `main` left a bridge that was alive, silent and dead (the `errdefer` that sets
`should_stop` ran after the join it should have released; the HTTP listener was closed, which on
Linux does not wake a blocked `accept`), and three limits reached it:

  1. a table name of 40+ characters   → `MsgIdTooLong` at boot (the slot's id buffer was 64 bytes)
  2. a tenant id of 93+ characters    → no generation was ever built (`InvalidSubject`)
  3. len(tenant) + len(table) > 115   → the CDC subject overflowed its buffer: `NoSpaceLeft`
                                        escaped `main` and killed the bridge without a line

What must hold now (each case is its own private stack):

  A. a 62-character table boots, publishes its rows and builds a chain
  B. a 92-character tenant (the ceiling) publishes and builds a chain
  C. a 93-character tenant is refused with a line that says why, the bridge lives on, and
     another tenant's rows still flow
  D. a subject too long for its buffer drops that row with a line that says why, the bridge
     lives on, and another tenant's rows still flow
  E. an error that escapes the replication loop ends the process (exit 1, `error:` in the log)
     instead of leaving it alive: needs the fault build (ZB_TEST_LOOP_ERROR)
  F. SIGTERM ends an idle bridge without any HTTP request arriving
  G. zebridge_enable refuses a 63-character table (its derived index name is the table's own)
     with an instruction, instead of '"<table>" is not an index'

Usage:  python scripts/scenarios/walls.py [--only A,C]
Needs PostgreSQL superuser at 127.0.0.1:5432, nats-server, zig-out/bin/bridge, and for E the
fault build:  zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults
"""
import asyncio, pathlib, re, signal, sys, tempfile, time

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
    tenant = "t" * 92
    stack = Stack(tmp, "wallsb", 120, {})
    try:
        stack.start({"wp": tenant})
        stack.create_table({tenant: 1}, "b")
        label = stack.bridge_up()
        insert(stack, T, tenant, "b-live")
        js = await rr.Js().open(stack)
        n = await until(lambda: messages(js, "CDC_" + tenant), 30)
        check("B. a 92-character tenant publishes its rows", n >= 1)
        man = await until(lambda: js.manifest(tenant), 90)
        check("B. ... and gets a generation chain (93 did not, with InvalidSubject)", man is not None)
        check("B. ... and the bridge is alive", alive(stack, label))
        await js.close()
    finally:
        stack.stop()


async def two_tenants(stack, big, name, tag, preexisting=False):
    """`big` is the tenant that must be refused, `acme` the one that must keep flowing."""
    stack.start({"wp": big, "wq": "acme"})
    stack.sql(table_sql(name))
    if preexisting: insert(stack, name, big, f"{tag}-pre")   # in the table before the bridge starts: its first build sees it
    label = stack.bridge_up()
    insert(stack, name, "acme", f"{tag}-0")
    js = await rr.Js().open(stack)
    first = await until(lambda: messages(js, "CDC_acme"), 30)
    return label, js, first


async def case_c(tmp):
    big = "t" * 93
    stack = Stack(tmp, "wallsc", 120, {})
    try:
        label, js, first = await two_tenants(stack, big, T, "c", preexisting=True)
        check("C. control: the ordinary tenant's row is published", first >= 1)
        built = await until(lambda: "no generation is built" in log_text(stack, label), 60)
        check("C. ... and the producer says why it builds no chain for it (it used to fail every cadence with a bare InvalidSubject)", bool(built))
        insert(stack, T, big, "c-big")
        said = await until(lambda: re.search(r"Dropping insert on '%s': its tenant value \(93 characters" % T, log_text(stack, label)), 30)
        check("C. a 93-character tenant is dropped with a line that names the length", bool(said))
        check("C. ... and does not blame the replica identity",
              "replica identity" not in "".join(l for l in log_text(stack, label).splitlines() if "93 characters" in l))
        insert(stack, T, "acme", "c-1")
        n = await until(lambda: count_at_least(js, "CDC_acme", 2), 30)
        check("C. ... the bridge lives on and the other tenant's next row is published", bool(n) and alive(stack, label))
        await js.close()
    finally:
        stack.stop()


async def case_d(tmp):
    big, name = "x" * 90, "n" * 40
    stack = Stack(tmp, "wallsd", 120, {})
    try:
        label, js, first = await two_tenants(stack, big, name, "d")
        check("D. control: the ordinary tenant's row is published", first >= 1)
        insert(stack, name, big, "d-big")
        said = await until(lambda: "does not fit the 128-byte subject buffer" in log_text(stack, label), 30)
        check("D. a subject that does not fit is dropped with a line that says so", bool(said))
        insert(stack, name, "acme", "d-1")
        n = await until(lambda: count_at_least(js, "CDC_acme", 2), 30)
        check("D. ... the bridge lives on and the other tenant's next row is published", bool(n) and alive(stack, label))
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
              "(it used to fail with '\"<table>\" is not an index')", "63 characters" in said and "Rename the table" in said)
    finally:
        stack.stop()


CASES = {"A": case_a, "B": case_b, "C": case_c, "D": case_d, "E": case_e, "F": case_f, "G": case_g}


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

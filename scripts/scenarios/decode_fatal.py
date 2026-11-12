"""A pgoutput message the bridge cannot decode stops it, and the slot keeps the change.

A decode failure used to be logged and skipped: the next commit, or a drained keepalive,
confirmed the slot past it, and every replica stayed healthy-looking without the row. Now
the bridge exits instead. On a private stack (own database, slot, nats-server, bridge) with
a libzb replica:

  1. one transaction writes two identifiable rows; the fault build fails to decode the
     second (ZB_TEST_DECODE_FAIL, a marker in its payload); a third row commits behind it;
  2. the bridge exits non-zero, its log names the message, the slot and the last
     acknowledged LSN; neither the transaction's first row nor the later one reaches the
     CDC stream or the replica;
  3. the stopped slot, peeked, still holds all three changes (confirmed_flush_lsn before them);
  4. the same slot, the fault disabled: all three arrive and the replica equals PostgreSQL.

Usage:  python scripts/scenarios/decode_fatal.py
Needs PostgreSQL superuser at 127.0.0.1:5432, nats-server, libzb, zig-out/bin/bridge and the
fault build:  zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults
"""
import asyncio, pathlib, sys, tempfile, time
import zb
import recreate_recovery as rr
from recreate_recovery import Stack, Client, check, T

MARKER = "zbdecodefail"
IDS = ("df-first", "df-" + MARKER, "df-after")


def log_text(stack, label):
    return open(stack.log(label), errors="replace").read()


async def until(fn, timeout, step=0.5):
    end, v = time.monotonic() + timeout, None
    while time.monotonic() < end:
        v = fn()
        if asyncio.iscoroutine(v): v = await v
        if v: return v
        await asyncio.sleep(step)
    return v


async def run(tmp):
    check("the ordinary build carries no decode-fault hook", b"ZB_TEST_DECODE_FAIL" not in rr.BRIDGE.read_bytes())
    if not rr.FAULT_BRIDGE.exists():
        check(f"{rr.FAULT_BRIDGE} exists ({rr.FAULT_BRIDGE_BUILD})", False)
        return
    stack = Stack(tmp, "decf", 120, {})
    try:
        stack.start({"wp": "acme"})
        stack.create_table({"acme": 1}, "d")
        stack.bridge_bin = rr.FAULT_BRIDGE
        env = stack.env
        stack.env = dict(env, ZB_TEST_DECODE_FAIL=MARKER)
        label = stack.bridge_up()
        client = Client(stack, "decf", "wp", "acme")
        client.start()
        stack.sql(f"INSERT INTO public.{T} (id, tenant_id, note) VALUES ('df-before', 'acme', 'before')")
        ok = await until(lambda: client.rows() == stack.truth("acme"), 120)
        check(f"the replica follows before the fault ({len(stack.truth('acme'))} rows)", bool(ok))
        js = await rr.Js().open(stack)
        before = (await js.stream("CDC_acme")).get("messages", 0)

        stack.sql(f"BEGIN; INSERT INTO public.{T} (id, tenant_id, note) VALUES ('{IDS[0]}', 'acme', 'one'); "
                  f"INSERT INTO public.{T} (id, tenant_id, note) VALUES ('{IDS[1]}', 'acme', 'two'); COMMIT;")
        stack.sql(f"INSERT INTO public.{T} (id, tenant_id, note) VALUES ('{IDS[2]}', 'acme', 'three')")
        p = stack.procs[label]
        await until(lambda: p.poll() is not None, 40)
        text = log_text(stack, label)
        check("the fault fired on the transaction's second row", "ZB_TEST_DECODE_FAIL fired" in text)
        check(f"the bridge exited, non-zero ({p.poll()})", p.poll() not in (None, 0))
        line = next((l for l in text.splitlines() if "pgoutput decode failed" in l), "")
        check(f"the log names the message, the slot and the last acknowledged LSN: {line[:220]}",
              "'I' message" in line and stack.name + "_slot" in line and "last acknowledged LSN" in line and "error: PgoutputDecodeFailed" in text)

        await asyncio.sleep(3)
        after = (await js.stream("CDC_acme")).get("messages", 0)
        held = set(client.rows() or {})
        check(f"nothing of the transaction or behind it was published: CDC_acme {before} → {after} messages", after == before)
        check(f"the replica holds none of the three rows: {sorted(held & set(IDS))}", not (held & set(IDS)))

        confirmed = stack.sql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{stack.name}_slot'")
        peek = stack.sql(f"SELECT lsn, encode(data, 'hex') FROM pg_logical_slot_peek_binary_changes('{stack.name}_slot', NULL, NULL, "
                         f"'proto_version', '1', 'publication_names', 'rr_pub')")
        kept = [i for i in IDS if i.encode().hex() in peek]
        first = next((l.split("|")[0] for l in peek.splitlines() if IDS[0].encode().hex() in l), "?")
        check(f"the stopped slot still holds all three changes (confirmed_flush_lsn {confirmed}, first row at {first}): {kept}", kept == list(IDS))

        stack.env = env  # the fault disabled; the same slot resumes
        label = stack.bridge_up()
        truth = stack.truth("acme")
        ok = await until(lambda: client.rows() == truth, 90)
        check(f"after a restart without the fault, all three arrive and the replica equals PostgreSQL ({len(truth)} rows)",
              bool(ok) and set(IDS) <= set(truth))
        check("... and the restarted bridge is alive", stack.procs[label].poll() is None)
        await js.close()
    finally:
        stack.stop()


def main():
    with tempfile.TemporaryDirectory(prefix="zb-decode-fatal-") as d:
        try:
            asyncio.run(run(pathlib.Path(d)))
        except Exception as e:
            check(f"raised: {type(e).__name__}: {str(e)[:500]}", False)
    if rr.failed:
        zb.bad(f"{rr.failed} check(s) failed")
        return 1
    zb.ok("a change the bridge cannot decode stops it; the slot keeps the change and a fixed bridge replays it")
    return 0


if __name__ == "__main__":
    sys.exit(main())

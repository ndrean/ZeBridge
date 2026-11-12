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
  3. the stopped slot, peeked, still holds both transactions, with confirmed_flush_lsn before the first commit;
  4. the fault recurs at the same position; the ordinary bridge resumes the same slot: all three arrive and the replica equals PostgreSQL.

Usage:  python scripts/scenarios/decode_fatal.py
Needs PostgreSQL superuser at 127.0.0.1:5432, nats-server, libzb, zig-out/bin/bridge and the
fault build:  zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults
"""
import asyncio, pathlib, re, sys, tempfile, time
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
    js = None
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
        before = (await js.js.stream_info("CDC_acme")).state.last_seq

        stack.sql(f"BEGIN; INSERT INTO public.{T} (id, tenant_id, note) VALUES ('{IDS[0]}', 'acme', 'one'); "
                  f"INSERT INTO public.{T} (id, tenant_id, note) VALUES ('{IDS[1]}', 'acme', 'two'); COMMIT;")
        stack.sql(f"INSERT INTO public.{T} (id, tenant_id, note) VALUES ('{IDS[2]}', 'acme', 'three')")
        p = stack.procs[label]
        await until(lambda: p.poll() is not None, 40)
        text = log_text(stack, label)
        check("the fault fired on the transaction's second row", "ZB_TEST_DECODE_FAIL fired" in text)
        check(f"the bridge exited with status 1 ({p.poll()})", p.poll() == 1)
        line = next((l for l in text.splitlines() if "pgoutput decode failed" in l), "")
        check(f"the log names the message, the slot and the last acknowledged LSN: {line[:220]}",
              "UnexpectedEndOfData" in line and "'I' message" in line and stack.name + "_slot" in line and MARKER not in line and "last acknowledged LSN" in line and "error: PgoutputDecodeFailed" in text)

        await asyncio.sleep(3)
        after = (await js.js.stream_info("CDC_acme")).state.last_seq
        held = client.rows()
        check(f"nothing of the transaction or behind it was published: CDC_acme sequence {before} → {after}", after == before)
        check("the replica still holds the control row and none of the three new rows",
              held is not None and "df-before" in held and set(held).isdisjoint(IDS))

        slot = stack.name + "_slot"
        records = stack.sql(f"SELECT lsn, encode(data, 'hex') FROM pg_logical_slot_peek_binary_changes('{slot}', NULL, NULL, "
                            "'proto_version', '1', 'publication_names', 'rr_pub')")
        transactions, inserts = [], []
        for record in records.splitlines():
            lsn, data = record.split("|")
            payload = bytes.fromhex(data)
            if payload.startswith(b"I"):
                inserts.extend(key for key in IDS if key.encode() in payload)
            elif payload.startswith(b"C"):
                if inserts:
                    transactions.append((lsn, inserts))
                inserts = []
        check("the stopped slot retains both transactions with all three inserts in order",
              [keys for _, keys in transactions] == [list(IDS[:2]), [IDS[2]]])
        if transactions:
            commit = transactions[0][0]
            behind = stack.sql(f"SELECT confirmed_flush_lsn < '{commit}'::pg_lsn "
                               f"FROM pg_replication_slots WHERE slot_name = '{slot}'")
            check(f"the slot has not confirmed the affected commit at {commit}", behind == "t")

        # The decoder can fail on every restart. A second failure must leave the slot usable.
        retry = stack.bridge_up()
        await until(lambda: stack.procs[retry].poll() is not None, 40)
        position = re.search(r"at WAL ([0-9A-F]+/[0-9A-F]+);", line)
        retry_text = log_text(stack, retry)
        check("the same fault stops the restarted bridge at the same WAL position",
              stack.procs[retry].poll() == 1 and position is not None
              and any("pgoutput decode failed (UnexpectedEndOfData)" in l and position.group() in l
                      for l in retry_text.splitlines()))

        stack.env = env
        stack.bridge_bin = rr.BRIDGE
        label = stack.bridge_up()
        truth = stack.truth("acme")
        ok = await until(lambda: client.rows() == truth, 90)
        check(f"after a restart without the fault, all three arrive and the replica equals PostgreSQL ({len(truth)} rows)",
              bool(ok) and set(IDS) <= set(truth))
        last = (await js.js.stream_info("CDC_acme")).state.last_seq
        replayed = []
        for seq in range(before + 1, last + 1):
            message = await js.js.get_msg("CDC_acme", seq=seq)
            body = zb.decode(message.data)
            for event in body if isinstance(body, list) else [body]:
                replayed.append((event["table"], event["operation"].upper(), event["data"]["id"]))
        check(f"the ordinary bridge replayed the three CDC inserts in order: {replayed}",
              replayed == [(T, "INSERT", key) for key in IDS])
        check("... and the restarted bridge is alive", stack.procs[label].poll() is None)
    finally:
        try:
            if js is not None:
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

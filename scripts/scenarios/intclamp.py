#!/usr/bin/env python3
"""An integer version column cannot be frozen by one write (§10fj).

  set -a && . ./.env.bridge && set +a
  ZB_PRINCIPAL=bob scripts/scenarios/.venv/bin/python scripts/scenarios/intclamp.py

Needs a running bridge. A table whose version column is a bigint counter is created
and enabled here and dropped at the end. Writes go straight over NATS as a client
would send them, verdicts are read on the principal's ack lane.

Last-write-wins on a counter has the same hole the timestamp clamp closes for
clocks: a client that writes 2^62 wins once and freezes the row for everyone, since
nothing is ever greater. The bridge caps what it STORES at one step — a fresh row at
most 1, an update or a delete at most stored + 1 — and tells the client in the
verdict (`version_clamped`, with the value stored). The write still wins when it is
newer: the comparison is with the raw value, the bound is on what is recorded.

Checked: a normal insert at 1 is stored as sent; a write at five billion wins and is
stored as 2, verdict `version_clamped` naming 2; the next write at 3 lands — the row
is not frozen; a write at 2 is `stale`; a fresh row at five billion is stored as 1;
an UPDATE (not an upsert) at 99 is stored as 4; a DELETE at 1000 tombstones the row
at 5 with `deleted_at` stamped now(); a later stale write on the tombstoned row is
`row_deleted` (the classify probe casts by the column's type).
"""
import asyncio, sys, uuid
import msgpack
import zb

T = "intv_t"
FAILED = []
VERDICT_WAIT_S = 10.0


def check(msg, cond):
    (zb.ok if cond else zb.bad)(msg)
    if not cond: FAILED.append(msg)


def stored(uid):
    r = zb.psql(f"SELECT ver, note, deleted_at IS NOT NULL FROM public.{T} WHERE uid = '{uid}'", quiet=True).strip()
    return r.split("|") if r else None


async def main():
    who = zb.require_principal()
    tenant = zb.psql(f"SELECT tenant_id FROM zebridge_user_tenants WHERE principal = '{who}' LIMIT 1").strip()
    zb.psql(f"DROP TABLE IF EXISTS public.{T}", quiet=True)
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), note text, ver bigint NOT NULL DEFAULT 1, tenant_id text NOT NULL, "
            f"inserted_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)", quiet=True)
    en = zb.psql(f"SELECT step || ': ' || detail FROM public.zebridge_enable('public.{T}'::regclass, tenant_col => 'tenant_id', writable => true, version_col => 'ver', "
                 f"tombstone_col => 'deleted_at', tiebreak_col => NULL, publication => 'my_pub', dry_run => false)", quiet=True)
    enabled = "publication:" in en and ("ADD TABLE" in en or "already" in en)
    check(f"{T} enabled with a bigint version column", enabled)
    if not enabled: print(en)
    await asyncio.sleep(2)  # the bridge reloads the catalogue live

    nc = await zb.connect_as(who)
    js = nc.jetstream()
    verdicts = {}
    ack_prefix = zb.TOPOLOGY["subjects"]["mutation_ack_prefix"]
    sub = await zb.subscribe(nc, f"{ack_prefix}.{who}.*")

    async def collect():
        async for m in sub.messages:
            try: verdicts[m.subject.rsplit(".", 1)[-1]] = zb.decode(m.data)
            except Exception: pass
    task = asyncio.create_task(collect())
    await asyncio.sleep(0.5)

    async def write(op, uid, ver, note=None):
        data = {"uid": uid, "ver": ver, "tenant_id": tenant}
        if note is not None: data["note"] = note
        if op == "delete": data = {}
        msg_id = f"intclamp-{uuid.uuid4().hex[:12]}"
        await js.publish(zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, T, op),
                         msgpack.packb({"key": {"uid": uid}, "data": data, "version": str(ver), "client_id": "c-intclamp"}),
                         headers={"Nats-Msg-Id": msg_id})
        deadline = asyncio.get_event_loop().time() + VERDICT_WAIT_S
        while msg_id not in verdicts and asyncio.get_event_loop().time() < deadline: await asyncio.sleep(0.2)
        return verdicts.get(msg_id) or {}

    a = str(uuid.uuid4())
    v = await write("insert", a, 1, "one")
    check(f"a normal insert at 1 is stored as sent (verdict {v.get('status')}, reason {v.get('reason', '')!r})", v.get("status") == "accepted" and not v.get("reason") and stored(a) == ["1", "one", "f"])

    v = await write("insert", a, 5_000_000_000, "far ahead")
    s = stored(a)
    check(f"a write at five billion wins and is stored as 2: stored {s}, verdict {v.get('status')} {v.get('reason')} version {v.get('version')!r}",
          v.get("status") == "accepted" and v.get("reason") == "version_clamped" and v.get("version") == "2" and s == ["2", "far ahead", "f"])

    v = await write("insert", a, 3, "three")
    check(f"the next write at 3 lands — the row is not frozen: {stored(a)}", v.get("status") == "accepted" and stored(a) == ["3", "three", "f"])

    v = await write("insert", a, 2, "two again")
    check(f"a write at 2 is stale: verdict {v.get('status')}", v.get("status") == "stale" and stored(a) == ["3", "three", "f"])

    b = str(uuid.uuid4())
    v = await write("insert", b, 5_000_000_000, "fresh, far ahead")
    check(f"a fresh row at five billion is stored as 1: {stored(b)}, verdict {v.get('reason')} {v.get('version')!r}",
          v.get("status") == "accepted" and v.get("reason") == "version_clamped" and v.get("version") == "1" and stored(b) == ["1", "fresh, far ahead", "f"])

    v = await write("update", a, 99, "ninety-nine")
    check(f"an UPDATE at 99 is stored as 4: {stored(a)}, verdict {v.get('reason')} {v.get('version')!r}",
          v.get("status") == "accepted" and v.get("reason") == "version_clamped" and v.get("version") == "4" and stored(a) == ["4", "ninety-nine", "f"])

    v = await write("delete", a, 1000)
    check(f"a DELETE at 1000 tombstones the row at 5, deleted_at now(): {stored(a)}, verdict {v.get('status')} {v.get('reason')}",
          v.get("status") == "accepted" and stored(a) == ["5", "ninety-nine", "t"])

    v = await write("insert", a, 4, "late")
    check(f"a later stale write on the tombstoned row is row_deleted: verdict {v.get('status')}", v.get("status") == "row_deleted")

    task.cancel()
    await nc.close()
    zb.psql(f"DROP TABLE public.{T}", quiet=True)
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(zb.run(main))

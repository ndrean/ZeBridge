#!/usr/bin/env python3
"""A tombstone the sweeper reaped must not let an old write bring its row back (§10km).

The dangerous case for a sync engine with soft deletes:

  1. a client goes offline holding a write for a row;
  2. the row is deleted (a tombstone);
  3. the sweeper reaps the tombstone: PostgreSQL forgets the row was ever deleted;
  4. the client reconnects and sends its old write.

Without a guard, step 4 lands: an UPDATE matches no row, but an INSERT creates the row
again, and every replica gets it back. The clients refuse to send a write stamped before
the GC watermark (PROTOCOL MUST 6), but a client back from a long sleep can hold an old
watermark. So the bridge refuses it too, against the current one.

The four steps, with versions placed around PostgreSQL's clock:

  T_seed = now - 3h    the row is written
  T_del  = now - 2h    the row is soft-deleted, then reaped (a physical DELETE, as the
                       sweeper does)
  W      = now - 1h    the GC watermark, past the deletion
  T_old  = now - 90m   the offline client's write: after the delete, before the watermark

What must hold:

  A. a stale UPDATE at T_old  → rejected / PredatesGcWatermark; the row stays gone
  B. a stale INSERT at T_old  → rejected / PredatesGcWatermark; the row stays gone
  C. a fresh INSERT           → accepted: the guard blocks nothing else
  D. control: with the watermark moved back behind the deletion, the same stale INSERT
     lands. That is the resurrection the guard exists to stop.

The watermark row is restored at the end, whatever happens.

Usage:  NATS_CREDS=scripts/native/creds/omar.creds python scripts/scenarios/gc_resurrect.py [table]
"""

import asyncio
import sys
import uuid

import msgpack
import zb

TABLE = sys.argv[1] if len(sys.argv) > 1 else "test_types"
VERDICT_WAIT_S = 10.0


async def main():
    failed = 0
    who = zb.require_principal()
    r = zb.require_rules(TABLE, "version", "tombstone")
    version_col, tomb_col = r["version"], r["tombstone"]

    columns = zb.psql(
        f"SELECT attname FROM pg_attribute WHERE attrelid='public.{TABLE}'::regclass "
        "AND attnum>0 AND NOT attisdropped ORDER BY attnum"
    ).splitlines()
    required = [
        c for c in zb.psql(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{TABLE}' AND is_nullable='NO' AND column_default IS NULL"
        ).splitlines() if c
    ]
    tenant = zb.tenant_of(who)

    def at(minutes_ago: int) -> str:
        return zb.recent_version(minutes_ago)

    t_seed, t_del, t_old = at(180), at(120), at(90)

    def row(uid: str, text: str, version: str, deleted: bool = False) -> dict:
        d = {
            "uid": uid, "age": 1, "temperature": 1.5, "price": "1.00000000",
            "is_true": True, "some_text": text, "tags": "{}", "matrix": "{}",
            "metadata": "{}", "inserted_at": t_seed, version_col: version,
            tomb_col: version if deleted else None,
        }
        for c in required:
            if c == "tenant_id" and tenant:
                d[c] = tenant
            elif c not in d or d[c] is None:
                d.setdefault(c, version)
        return {c: d.get(c) for c in columns}

    nc = await zb.connect_as(who)
    js = nc.jetstream()
    base = zb.subject(zb.TOPOLOGY["subjects"]["mutations_prefix"], who, TABLE)
    verdicts: dict[str, dict] = {}
    sub = await zb.subscribe(nc, f"{zb.TOPOLOGY['subjects']['mutation_ack_prefix']}.{who}.*")

    async def collect():
        async for m in sub.messages:
            try:
                verdicts[m.subject.rsplit(".", 1)[-1]] = zb.decode(m.data)
            except Exception:  # noqa: BLE001
                pass

    collector = asyncio.create_task(collect())
    await asyncio.sleep(0.5)

    async def send(op: str, uid: str, text: str, version: str, deleted: bool = False) -> dict:
        msg_id = f"gcres-{uuid.uuid4().hex[:16]}"
        payload = {"key": {"uid": uid}, "data": row(uid, text, version, deleted),
                   "version": version, "client_id": "c-gcres"}
        await js.publish(f"{base}.{op}", msgpack.packb(payload), headers={"Nats-Msg-Id": msg_id})
        deadline = asyncio.get_event_loop().time() + VERDICT_WAIT_S
        while msg_id not in verdicts and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.2)
        return verdicts.get(msg_id) or {}

    def reap(*uids: str):
        """A physical DELETE as the sweeper makes it. Any other session's DELETE on a
        tombstone table is turned into a soft delete by `zebridge_soft_delete`; the
        sweeper gets through by naming itself, as it does in bridge_sweeper.zig."""
        quoted = ", ".join(f"'{u}'" for u in uids)
        zb.psql(f"SELECT set_config('zb.principal', 'zb_sweeper', false); "
                f"DELETE FROM public.{TABLE} WHERE uid IN ({quoted})", quiet=True)

    def exists(uid: str) -> bool:
        return zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE uid='{uid}'").strip() == "1"

    def set_watermark(sql_value: str):
        zb.psql(f"UPDATE public.zebridge_gc_watermark SET watermark = {sql_value} WHERE id = 1")

    def check(label: str, cond: bool, detail: str):
        nonlocal failed
        (zb.ok if cond else zb.bad)(f"{label}: {detail}")
        failed += 0 if cond else 1

    original = zb.psql("SELECT watermark FROM public.zebridge_gc_watermark WHERE id = 1").strip()
    if not original:
        sys.exit("no zebridge_gc_watermark row: run the sweeper once (bridge_sweeper --once)")
    uid, fresh = str(uuid.uuid4()), str(uuid.uuid4())
    try:
        # ── steps 1–3: write, soft-delete, reap ──────────────────────────────────
        set_watermark("'1970-01-01Z'")  # behind everything, so the setup writes land
        v = await send("insert", uid, "seed", t_seed)
        check("setup: seed", v.get("status") == "accepted", f"insert at now-3h → {v}")
        v = await send("update", uid, "deleted", t_del, deleted=True)
        check("setup: soft delete", v.get("status") == "accepted", f"tombstone at now-2h → {v}")
        reap(uid)
        set_watermark("now() - interval '1 hour'")
        check("setup: reaped", not exists(uid), "the row is physically gone, its tombstone with it")

        # ── A, B: the offline client's old writes ───────────────────────────────
        v = await send("update", uid, "OFFLINE update", t_old)
        check("A. stale UPDATE", v.get("status") == "rejected" and v.get("reason") == "PredatesGcWatermark",
              f"verdict {v.get('status')}/{v.get('reason')}")
        check("A. row", not exists(uid), "stays gone")
        v = await send("insert", uid, "OFFLINE insert", t_old)
        check("B. stale INSERT", v.get("status") == "rejected" and v.get("reason") == "PredatesGcWatermark",
              f"verdict {v.get('status')}/{v.get('reason')}")
        check("B. row", not exists(uid), "stays gone: no resurrection")

        # ── C: a fresh write is untouched ───────────────────────────────────────
        v = await send("insert", fresh, "fresh", at(1))
        check("C. fresh INSERT", v.get("status") == "accepted" and exists(fresh),
              f"verdict {v.get('status')}, row {'present' if exists(fresh) else 'absent'}")

        # ── D: control — without the guard, the same write resurrects the row ─────
        set_watermark("'1970-01-01Z'")
        v = await send("insert", uid, "OFFLINE insert", t_old)
        check("D. control", v.get("status") == "accepted" and exists(uid),
              "with the watermark behind the deletion the stale INSERT lands and the deleted "
              "row is back: what the guard prevents")
    finally:
        set_watermark(f"'{original}'")
        reap(uid, fresh)
        collector.cancel()
        await nc.close()

    print()
    if failed:
        zb.bad(f"{failed} check(s) failed")
        sys.exit(1)
    zb.ok("a write from before the GC watermark cannot resurrect a reaped row")


if __name__ == "__main__":
    zb.run(main)

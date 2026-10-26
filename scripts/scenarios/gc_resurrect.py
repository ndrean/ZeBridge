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
  W      = now - 1h    the GC floor, past the deletion
  T_old  = now - 90m   the offline client's write: after the delete, before the floor

What must hold:

  A. a stale UPDATE at T_old  → rejected / PredatesGcWatermark; the row stays gone
  B. a stale INSERT at T_old  → rejected / PredatesGcWatermark; the row stays gone
  C. a fresh INSERT           → accepted: the guard blocks nothing else
  D. control: with the floor staged behind the deletion, the same stale INSERT lands. That is
     the resurrection the guard exists to stop.

⚠️ It runs on a disposable stack (its own database, nats-server and bridge), not on a shared
database: D needs a floor BEHIND a reaped deletion, and the floor of a shared database never goes
down (a trigger refuses it; NOTES "One floor, one sweeper"). Staging one is the operator's
break-glass on a database nobody keeps, never a scenario's teardown on a database somebody does.

Usage:  python scripts/scenarios/gc_resurrect.py
Needs PostgreSQL superuser at 127.0.0.1:5432, nats-server and zig-out/bin/bridge.
"""
import asyncio, pathlib, sys, tempfile

import zb
from recreate_recovery import Stack, check
import recreate_recovery as rr
from gc_isolation import PRINCIPAL, TENANT, Writer, stage_floor, present, version_ago
from recreate_recovery import T


def reap(stack, rid):
    """A physical DELETE as the sweeper makes it. Any other session's DELETE on a tombstone table
    is turned into a soft delete by `zebridge_soft_delete`; the sweeper gets through by naming itself."""
    stack.sql(f"SELECT set_config('zb.principal', 'zb_sweeper', false); DELETE FROM public.{T} WHERE id = '{rid}';")


def verdict_is(v, status, reason=None):
    return (v or {}).get("status") == status and (reason is None or (v or {}).get("reason") == reason)


async def run(tmp):
    stack = Stack(tmp, "gcres", 120, {})
    writer = None
    try:
        stack.start({PRINCIPAL: TENANT})
        stack.create_table({TENANT: 1}, "g")
        stack.bridge_up()
        writer = await Writer().open(stack)
        uid, fresh = "res-old", "res-fresh"
        t_seed, t_del, t_old, t_now = (version_ago(stack, x) for x in ("180 minutes", "120 minutes", "90 minutes", "1 minute"))

        # ── steps 1–3: write, soft-delete, reap ──────────────────────────────────
        stage_floor(stack, "10 years")   # behind everything, so the setup writes land
        v = await writer.verdict(await writer.send("insert", uid, t_seed, note="seed"))
        check(f"setup: seed — insert at now-3h → {v}", verdict_is(v, "accepted"))
        v = await writer.verdict(await writer.send("update", uid, t_del, deleted=True, note="deleted"))
        check(f"setup: soft delete — tombstone at now-2h → {v}", verdict_is(v, "accepted"))
        reap(stack, uid)
        stage_floor(stack, "1 hour")
        check("setup: reaped — the row is physically gone, its tombstone with it", not present(stack, uid))

        # ── A, B: the offline client's old writes ───────────────────────────────
        v = await writer.verdict(await writer.send("update", uid, t_old, note="OFFLINE update"))
        check(f"A. stale UPDATE → {v and (v.get('status'), v.get('reason'))}", verdict_is(v, "rejected", "PredatesGcWatermark"))
        check("A. the row stays gone", not present(stack, uid))
        v = await writer.verdict(await writer.send("insert", uid, t_old, note="OFFLINE insert"))
        check(f"B. stale INSERT → {v and (v.get('status'), v.get('reason'))}", verdict_is(v, "rejected", "PredatesGcWatermark"))
        check("B. the row stays gone: no resurrection", not present(stack, uid))

        # ── C: a fresh write is untouched ───────────────────────────────────────
        v = await writer.verdict(await writer.send("insert", fresh, t_now, note="fresh"))
        check(f"C. fresh INSERT → {v and v.get('status')}", verdict_is(v, "accepted") and present(stack, fresh))

        # ── D: control — without the guard's floor, the same write resurrects the row ─────
        stage_floor(stack, "10 years")
        v = await writer.verdict(await writer.send("insert", uid, t_old, note="OFFLINE insert"))
        check("D. control: with the floor behind the deletion the stale INSERT lands and the deleted row is back "
              "(what the guard prevents)", verdict_is(v, "accepted") and present(stack, uid))
    finally:
        if writer:
            try: await writer.close()
            except Exception: pass
        stack.stop()


def main():
    with tempfile.TemporaryDirectory(prefix="zb-gcres-") as d:
        try:
            asyncio.run(run(pathlib.Path(d)))
        except Exception as e:
            check(f"raised: {type(e).__name__}: {str(e)[:500]}", False)
    print()
    if rr.failed:
        zb.bad(f"{rr.failed} check(s) failed")
        return 1
    zb.ok("a write from before the GC floor cannot resurrect a reaped row")
    return 0


if __name__ == "__main__":
    sys.exit(main())

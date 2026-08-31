"""A `row_too_large` suspension lifts LIVE, once the table can be replicated again
(NOTES §10bp).

    scripts/scenarios/run.py owns -k suspension_lift      (owns the bridge)

Before this, such a suspension was held until the next bridge boot or DDL event — even
after the cause was gone. `legacybait.py` left `test_types` frozen with its 20 KB bait
already deleted, and only a restart brought it back; every client sat on a suspension
descriptor meanwhile. The refusal is DATA-dependent now (`Reason.probesEveryEvent`):
past a cooldown each event for such a table is retried, and the first one that fits
lifts the suspension and republishes the descriptor.

**Getting an oversized row into CDC takes care.** PostgreSQL's width guard is baked at
every bridge boot to `MIN(zebridge_limits.max_row_bytes)`, so a row that passes the
guard cannot overflow the bridge that registered that budget — by design. Two dead ends
found on the way (2026-08-31): disabling the guard to plant a wide row is `ALTER TABLE`,
i.e. DDL, and the DDL path treats DDL as "the migration that fixed it" and lifts the
suspension moments after it is raised; and a probe running at a smaller BASE_BUF bakes
the guard down to its own buffer, so even a 1500-byte row is refused at the source.

What is left is the hazard preflight already warns about — *lowering* the buffer below
what is already stored ("do NOT lower BASE_BUF below what is already stored"). The row
is written while the guard is the live bridge's 4 KB, and the probe then boots at 2 KB
and replays it.

  1. a stored row the probe's buffer cannot carry suspends the table, live
  2. INSIDE the 30 s cooldown nothing flows and nothing lifts — the hysteresis that stops
     a table whose rows straddle the buffer from flapping (two KV writes per event)
  3. after it, the next ordinary write lifts the suspension WITHOUT a restart, and the
     descriptor is republished
  4. and CDC flows again
"""
import asyncio
import json
import os
import pathlib
import sys
import time
import urllib.request
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402

TABLE = "test_types"
PROBE_BASE_BUF = "11"       # 2048-byte event buffer for the probe bridge only
BAIT_BYTES = 3000           # fits the live bridge's 4 KB, not the probe's 2 KB
COOLDOWN_S = 30             # refused_tables.zig `lift_cooldown_ms`
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_suspension_lift_bridge.log"
STATE: dict = {"marker": ""}


def slot_name() -> str:
    return zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"


def refused_count() -> int:
    """`bridge_refused_tables` from the probe's own /metrics — the registry as the bridge
    sees it, with no publish latency between the decision and the observation."""
    try:
        with urllib.request.urlopen(zb.http_base(probe=True) + "/metrics", timeout=3) as r:
            for line in r.read().decode().splitlines():
                if line.startswith("bridge_refused_tables"):
                    return int(float(line.split()[-1]))
    except Exception:  # noqa: BLE001
        return -1
    return -1


def wait_for(pred, seconds: float, step: float = 0.5) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(step)
    return False


def schema_state() -> str:
    """'suspended:<reason>', 'live', or 'absent' — what a client reads from KV. The
    descriptor is {"table":…,"suspended":true,"reason":"row_too_large",…}."""
    raw = zb.kv_get("schemas", TABLE)
    if not raw:
        return "absent"
    try:
        d = json.loads(raw)
    except json.JSONDecodeError:
        return "absent"
    if d.get("suspended"):
        return "suspended:" + str(d.get("reason") or "?")
    return "live" if d.get("pg", {}).get("columns") else "absent"


def small_write(text: str) -> None:
    zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
            f"VALUES (gen_random_uuid(), '{text}', 'acme', now(), now())")


def rebake_guard() -> None:
    """Re-bake PostgreSQL's width guard from the budgets that remain.

    ⚠️ Deleting the probe's `zebridge_limits` row is not enough: the budget is a LITERAL
    inside the trigger function, written at each bridge boot by `zebridge_register_limits`
    from `MIN(max_row_bytes)`. A probe that booted at BASE_BUF=11 therefore leaves every
    guard baked at 2048 — refusing rows the LIVE bridge could carry — until something
    re-bakes it. Called in setup (so the bait can be stored) and in cleanup (so the stack
    is left as it was found)."""
    zb.psql("SELECT public.zebridge_rebudget_width_guard(c.oid::regclass, "
            "(SELECT MIN(max_row_bytes)::int FROM public.zebridge_limits)) "
            "FROM pg_class c JOIN pg_trigger t ON t.tgrelid = c.oid "
            "AND t.tgname = 'zebridge_width_guard'", quiet=True)


def cleanup() -> None:
    """⚠️ In a `finally`: every check can return early, and an aborted run used to leave
    the bait stored, the probe's budget registered (which bakes every guard down to it)
    and the slot retaining WAL — the next run then read the previous one's evidence."""
    # ⚠️ NARROW, then tombstone — not `DELETE`. With the write guards installed a DELETE
    # is rewritten into a soft delete, so the wide row stays STORED (just flagged) and the
    # next run counts it as a leftover bait; and the tombstoning UPDATE would itself carry
    # the oversized row into CDC and re-suspend the table on the way out. Emptying the
    # column in the same statement makes the event small (measured 2026-08-31).
    zb.psql(f"UPDATE public.{TABLE} SET some_text = '', deleted_at = now(), updated_at = now() "
            f"WHERE length(some_text) >= {BAIT_BYTES}", quiet=True)
    if STATE["marker"]:
        zb.psql(f"DELETE FROM public.{TABLE} WHERE some_text LIKE '%{STATE['marker']}'", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_limits WHERE slot = '{slot_name()}'", quiet=True)
    zb.psql(f"SELECT pg_drop_replication_slot('{slot_name()}') FROM pg_replication_slots "
            f"WHERE slot_name = '{slot_name()}' AND NOT active", quiet=True)
    rebake_guard()


async def main() -> int:
    try:
        return await run()
    finally:
        cleanup()


async def run() -> int:
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge "
                 "(it suspends test_types on purpose; a live bridge would carry that to real clients)")
    failed = 0
    marker = str(uuid.uuid4())[:8]
    STATE["marker"] = marker

    # A clean slate: no leftover bait, no leftover budget, and a slot positioned HERE so
    # the bait below is the first thing the probe replays.
    cleanup()   # includes rebake_guard(), so the bait below can be stored
    zb.psql(f"SELECT pg_create_logical_replication_slot('{slot_name()}', 'pgoutput')", quiet=True)

    # Written while the guard is still the live bridge's 4 KB — this is the row the
    # probe's 2 KB buffer will not be able to carry.
    zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
            f"VALUES (gen_random_uuid(), repeat('x', {BAIT_BYTES}), 'acme', now(), now())")
    # LIVE rows only: a tombstoned bait from an earlier scenario is still stored.
    stored = zb.psql(f"SELECT count(*) FROM public.{TABLE} "
                     f"WHERE length(some_text) >= {BAIT_BYTES} AND deleted_at IS NULL").strip()
    if stored != "1":
        zb.bad(f"could not store the bait ({stored!r}) — is a stale probe budget still in zebridge_limits?")
        return 1

    with zb.Bridge(LOG, BASE_BUF=PROBE_BASE_BUF) as bridge:
        if not bridge.wait_for_log("Replication started successfully", timeout=40):
            zb.bad("probe bridge did not start")
            return 1

        # ── 1. the stored row does not fit: suspended, live ──────────────────
        suspended = bridge.wait_for_log("SUSPENDING 'test_types'", timeout=40)
        in_registry = wait_for(lambda: refused_count() >= 1, 15)
        kv_says = wait_for(lambda: schema_state() == "suspended:row_too_large", 20)
        if suspended and in_registry and kv_says:
            zb.ok(f"a stored {BAIT_BYTES}-byte row replayed into a {PROBE_BASE_BUF}-bit buffer suspended "
                  f"{TABLE} live: clients hold a row_too_large descriptor")
        else:
            zb.bad(f"no live suspension (log={suspended}, registry={refused_count()}, KV={schema_state()})")
            return 1
        t_suspend = time.monotonic()

        # ── 2. inside the cooldown: nothing flows, nothing lifts ─────────────
        small_write(f"cooldown {marker}")
        await asyncio.sleep(3)
        if refused_count() >= 1 and schema_state() == "suspended:row_too_large":
            zb.ok("a small row inside the cooldown did NOT lift it — the anti-flap hysteresis holds")
        else:
            zb.bad(f"lifted inside the cooldown (registry={refused_count()}, KV={schema_state()}): a table "
                   "straddling the buffer would flap, two KV writes per event")
            failed += 1

        # ── 3. past it, the next ordinary write lifts the suspension ─────────
        wait_s = COOLDOWN_S - (time.monotonic() - t_suspend) + 1
        if wait_s > 0:
            await asyncio.sleep(wait_s)
        small_write(f"after {marker}")
        lifted = bridge.wait_for_log("a row fits again", timeout=30)
        cleared = wait_for(lambda: refused_count() == 0, 15)
        republished = wait_for(lambda: schema_state() == "live", 20)
        if lifted and cleared and republished:
            zb.ok("the next fitting write lifted the suspension WITHOUT a restart, and the descriptor "
                  "was republished")
        else:
            zb.bad(f"still suspended after the table could be carried again (log={lifted}, "
                   f"registry={refused_count()}, KV={schema_state()}) — this is the defect the change fixes")
            failed += 1

        # ── 4. and CDC flows again ───────────────────────────────────────────
        nc = await zb.connect()
        js = nc.jetstream()
        stream = zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + "acme"
        before = (await js.stream_info(stream)).state.last_seq
        small_write(f"flowing {marker}")
        seq = before
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and seq <= before:
            await asyncio.sleep(0.5)
            seq = (await js.stream_info(stream)).state.last_seq
        await nc.close()
        if seq > before:
            zb.ok(f"CDC flows again on {stream} ({before} → {seq})")
        else:
            zb.bad(f"{stream} did not advance after the lift ({before}) — routable but silent")
            failed += 1

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)

#!/usr/bin/env python3
"""The sweeper's reaps must not reach clients — and real deletes must.

A table with a tombstone column never deletes from the edge: a client's delete becomes an
UPDATE that sets the tombstone, and that update is the event clients act on. The only
physical DELETEs such a table ever produces are the sweeper reaping rows whose tombstones
have aged out — rows every client removed when the soft delete arrived.

Forwarding those costs one message per reaped row per client, for a row they know is gone.
A sweep of a million tombstones is a million such messages.

⚠️ **PostgreSQL cannot express this, and that is measured, not assumed** (all three routes
tried before the bridge-side drop was written):

  * `publish` is a **publication-level** option — it cannot be set per table;
  * publications **union**, so a second one with `publish='insert,update'` still emits the
    delete when both are named on the slot (`I` alone via the new one, `ID` via both);
  * a row filter (`WHERE deleted_at IS NULL`) makes the table **unwritable** — the filter
    column is not in the replica identity, so PostgreSQL refuses every UPDATE and DELETE,
    including the application's own.

So the bridge drops them, conditionally. The condition is what this scenario guards.

⚠️ **The suppression is safe because a redundant delete is a no-op**, verified on both
engines: PostgreSQL answers `DELETE 0` and SQLite reports no error when the row is already
gone. So the cost of dropping a reap is nothing, while the cost of dropping a *real* delete
is a row stranded in every replica with no later event able to remove it — which is why the
condition is "does this table have a **configured** tombstone", not "is there a column
called deleted_at".

Usage:  python scripts/scenarios/reaps.py [tombstoned_table] [plain_table]

  reaps.py                       # test_types (soft-deletes) vs users (physical)

Needs the bridge running with the table's SYNC_RULES entry, and a live NATS.
"""

import asyncio
import os
import sys
import uuid

import zb

SOFT = sys.argv[1] if len(sys.argv) > 1 else "test_types"
HARD = sys.argv[2] if len(sys.argv) > 2 else "users"


def rules_for(table: str) -> list[str]:
    entry = next(
        (r for r in os.environ.get("SYNC_RULES", "").split(";") if r.strip().startswith(f"{table}:")),
        None,
    )
    return [c.strip() for c in entry.split(":", 1)[1].split(",")] if entry else []


async def main():
    failed = 0
    cols = rules_for(SOFT)
    if len(cols) < 2:
        sys.exit(
            f"'{SOFT}' has no tombstone column in SYNC_RULES, so its deletes are physical\n"
            f"  and there is nothing to suppress.\n"
            f"    SYNC_RULES={SOFT}:updated_at,deleted_at"
        )
    version_col, tombstone_col = cols[0], cols[1]
    print(f"{SOFT}: version={version_col} tombstone={tombstone_col}")
    print(f"{HARD}: no tombstone — its deletes are real\n")

    # Required columns, from the catalog. Hardcoding this list is how two other scenarios
    # silently stopped inserting when the table grew a NOT NULL tenant_id.
    required = [
        c for c in zb.psql(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{SOFT}' AND is_nullable='NO' AND column_default IS NULL "
            "AND column_name <> 'uid'"
        ).splitlines() if c
    ]
    tenant = zb.psql(
        "SELECT tenant_id FROM zebridge_user_tenants WHERE principal='zb_sweeper' LIMIT 1"
    ).strip()

    nc = await zb.connect()
    seen: list[str] = []
    sub = await nc.subscribe("cdc.>")

    async def collect():
        async for m in sub.messages:
            seen.append(m.subject)

    task = asyncio.create_task(collect())
    await asyncio.sleep(0.5)

    # ── the soft-deleting table ────────────────────────────────────────────────
    uid = str(uuid.uuid4())
    extra_cols = "".join(f", {c}" for c in required)
    extra_vals = "".join(
        f", '{tenant}'" if c == "tenant_id" else ", now()" for c in required
    )
    zb.psql(
        f"INSERT INTO public.{SOFT} (uid, some_text{extra_cols}) "
        f"VALUES ('{uid}', 'reap scenario'{extra_vals})"
    )
    await asyncio.sleep(2.5)

    # Aged past any sane threshold, so one sweep takes it.
    zb.psql(
        f"UPDATE public.{SOFT} SET {tombstone_col} = now() - interval '2 hours', "
        f"{version_col} = now() WHERE uid = '{uid}'"
    )
    await asyncio.sleep(3)

    before = list(seen)
    if any(s.endswith(f"{SOFT}.update") for s in before):
        zb.ok(f"the soft delete reached clients as `cdc.{SOFT}.update`")
    else:
        zb.bad(f"no update for the soft delete — clients never learned the row was deleted")
        failed += 1

    # ── the reap ───────────────────────────────────────────────────────────────
    #
    # Run against the same threshold the bridge is configured with; the sweeper is a
    # separate binary, so this is the real path rather than a simulated DELETE.
    sweeper = zb.ROOT / "zig-out" / "bin" / "bridge_sweeper"
    if not sweeper.exists():
        sys.exit(f"{sweeper} not built — run `zig build`")

    import subprocess

    env = dict(os.environ)
    env["GC_THRESHOLD_MS"] = "3600000"
    env["GC_INTERVAL_MS"] = "999999999"
    try:
        subprocess.run([str(sweeper)], env=env, capture_output=True, text=True, timeout=12)
    except subprocess.TimeoutExpired:
        pass  # one pass then it sleeps; the work is done
    await asyncio.sleep(4)

    gone = zb.psql(f"SELECT count(*) FROM public.{SOFT} WHERE uid = '{uid}'").strip()
    if gone != "0":
        zb.bad(f"the sweeper did not reap the row — check its principal and SYNC_RULES")
        failed += 1

    reap_events = [s for s in seen[len(before):] if s.endswith(f"{SOFT}.delete")]
    if reap_events:
        zb.bad(
            f"the reap reached clients as {reap_events[0]} — one message per reaped row, "
            "per client, for a row they already removed"
        )
        failed += 1
    else:
        zb.ok(f"the reap was suppressed: no `cdc.{SOFT}.delete` on the wire")

    # ── and a table WITHOUT a tombstone must still emit deletes ────────────────
    #
    # The other half, and the one that matters more: suppressing a real delete strands the
    # row in every replica forever, because no later event can remove it.
    hard_id = 2_000_000_000 + int(uuid.uuid4().int % 1_000_000)
    zb.psql(
        f"INSERT INTO public.{HARD} (id, name, inserted_at, updated_at) "
        f"VALUES ({hard_id}, 'reap scenario', now(), now())"
    )
    await asyncio.sleep(2.5)
    mark = len(seen)
    zb.psql(f"DELETE FROM public.{HARD} WHERE id = {hard_id}")
    await asyncio.sleep(3)

    if any(s.endswith(f"{HARD}.delete") for s in seen[mark:]):
        zb.ok(f"a real delete on '{HARD}' still reaches clients as `cdc.{HARD}.delete`")
    else:
        zb.bad(
            f"no delete event for '{HARD}': suppression is too broad, and rows deleted "
            "there will stay in every replica forever"
        )
        failed += 1

    task.cancel()
    await nc.close()
    return 1 if failed else 0


zb.run(main)

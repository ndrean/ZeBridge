#!/usr/bin/env python3
"""The version column is only as true as the writer who set it — unless a trigger insists.

Last-write-wins compares `stored.version < incoming.version`, and the bridge stamps the
version on every write **it** applies. The bridge is not the only writer. A cron job, a
`psql` session, another service, or an ORM path that forgets can run

    UPDATE users SET name = 'x' WHERE id = 1;      -- updated_at untouched

and the row changes while the version does not. The stored version then no longer describes
the row, so a client's **older** edit beats a **newer** server write — silently, and
correctly by the rule. PROTOCOL.md §7.3 has warned about this in prose for a while.

The same hole exists on deletes. §7.5 makes an edge delete a *soft* delete so an offline
client's queued edit is overruled instead of resurrecting the row. A direct `DELETE` is
physical — no tombstone — so that guard is simply absent for that row.

⚠️ **The delete guard has one hard constraint**: the sweeper must still be able to reap, or
tombstones accumulate forever and the whole soft-delete design deadlocks. It identifies
itself with `zb.principal`, the same setting the RLS policies read, so §4 and §5 below are
the two halves that must both hold — the sweeper gets through, everyone else does not.

⚠️ This installs guards on a live table and **removes them again**, because leaving them on
changes the rest of the suite: cleanup `DELETE`s in other scenarios would become tombstones
and rows would accumulate.

Usage:  python scripts/scenarios/guards.py [table]
"""

import sys
import uuid

import zb

TABLE = sys.argv[1] if len(sys.argv) > 1 else "test_types"


async def main():
    failed = 0
    # From zebridge_catalogue (SYNC_RULES is only an override), exactly as the bridge
    # resolves them — reading the env alone exits on every catalogue-configured stack.
    cols = zb.require_rules(TABLE, "version", "tombstone")
    version_col, tombstone_col = cols["version"], cols["tombstone"]
    print(f"version={version_col}  tombstone={tombstone_col}\n")

    required = [
        c for c in zb.psql(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{TABLE}' AND is_nullable='NO' AND column_default IS NULL"
        ).splitlines() if c
    ]
    tenant = zb.psql(
        "SELECT tenant_id FROM zebridge_user_tenants LIMIT 1"
    ).strip()

    def insert(uid: str, text: str, aged: bool = False):
        stamp = "now() - interval '1 hour'" if aged else "now()"
        extra = "".join(f", {c}" for c in required if c not in ("inserted_at", version_col, "tenant_id"))
        vals = "".join(f", {stamp}" for c in required if c not in ("inserted_at", version_col, "tenant_id"))
        zb.psql(
            f"INSERT INTO public.{TABLE} (uid, some_text, inserted_at, {version_col}, tenant_id{extra}) "
            f"VALUES ('{uid}', '{text}', {stamp}, {stamp}, '{tenant}'{vals})",
            quiet=True,
        )

    def as_sweeper(sql: str):
        """Run something with the sweeper's identity, which the delete guard honours."""
        zb.psql(f"SELECT set_config('zb.principal','zb_sweeper',false); {sql}", quiet=True)

    zb.psql(
        f"SELECT public.zebridge_install_write_guards('public.{TABLE}'::regclass, "
        f"'{version_col}', '{tombstone_col}')",
        quiet=True,
    )

    try:
        audit = zb.psql(
            f"SELECT version_guard||','||delete_guard FROM public.zebridge_audit_write_guards() "
            f"WHERE tbl='{TABLE}'"
        ).strip()
        if audit == "true,true":
            zb.ok("both guards installed, and the audit view reports them")
        else:
            zb.bad(f"the audit view says {audit!r} after installing both guards")
            failed += 1

        # ── 1. a writer that forgets ───────────────────────────────────────────
        uid = str(uuid.uuid4())
        insert(uid, "guards: seeded", aged=True)
        zb.psql(f"UPDATE public.{TABLE} SET some_text='changed by a cron job' WHERE uid='{uid}'", quiet=True)
        fresh = zb.psql(
            f"SELECT ({version_col} > now() - interval '10 seconds') FROM public.{TABLE} WHERE uid='{uid}'"
        ).strip()
        if fresh == "t":
            zb.ok(f"a writer that ignored '{version_col}' had it stamped anyway")
        else:
            zb.bad(
                f"'{version_col}' was left stale by a direct UPDATE — a client's older edit "
                "would now beat this newer server write, silently"
            )
            failed += 1

        # ── 2. a writer that sets it must be left alone ────────────────────────
        #
        # ⚠️ The other half, and the one that keeps the protocol intact. If the trigger
        # overwrote unconditionally, the version would become server-assigned and the value
        # the client is told to send back (§7.2) would be silently discarded — including
        # the bridge's own clamped value.
        zb.psql(
            f"UPDATE public.{TABLE} SET some_text='explicit', {version_col}='2030-01-01 00:00:00+00' "
            f"WHERE uid='{uid}'",
            quiet=True,
        )
        kept = zb.psql(
            f"SELECT ({version_col} = '2030-01-01 00:00:00+00') FROM public.{TABLE} WHERE uid='{uid}'"
        ).strip()
        if kept == "t":
            zb.ok("a writer that set the version explicitly kept its value")
        else:
            zb.bad("the trigger overwrote an explicitly-set version — the client's value is discarded")
            failed += 1
        as_sweeper(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'")

        # ── 3. a physical DELETE becomes a tombstone ───────────────────────────
        uid = str(uuid.uuid4())
        insert(uid, "guards: delete me")
        zb.psql(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'", quiet=True)
        # ⚠️ `::text` on a boolean gives 'true'/'false', while psql prints a bare boolean
        # as 't'/'f'. Comparing against the wrong one made this report a passing behaviour
        # as a failure.
        state = zb.psql(
            f"SELECT count(*)||','||coalesce(bool_or({tombstone_col} IS NOT NULL)::text,'-') "
            f"FROM public.{TABLE} WHERE uid='{uid}'"
        ).strip()
        if state == "1,true":
            zb.ok("a direct DELETE wrote a tombstone instead of removing the row")
        else:
            zb.bad(
                f"a direct DELETE removed the row (state {state!r}) — an offline client's "
                "queued edit can now resurrect it, with no tombstone to overrule"
            )
            failed += 1

        # ── 4. the sweeper must still get through ──────────────────────────────
        #
        # ⚠️ If this fails the design deadlocks: nothing can ever physically remove a
        # tombstoned row, and the table grows without bound.
        as_sweeper(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'")
        left = zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE uid='{uid}'").strip()
        if left == "0":
            zb.ok("the sweeper reaped it physically, as it must")
        else:
            zb.bad(
                "the sweeper could NOT delete the row — tombstones would accumulate forever "
                "and the soft-delete design deadlocks"
            )
            failed += 1

        # ── 5. and nobody else can ─────────────────────────────────────────────
        uid = str(uuid.uuid4())
        insert(uid, "guards: not the sweeper")
        zb.psql(
            f"SELECT set_config('zb.principal','alice',false); "
            f"DELETE FROM public.{TABLE} WHERE uid='{uid}'",
            quiet=True,
        )
        still = zb.psql(f"SELECT count(*) FROM public.{TABLE} WHERE uid='{uid}'").strip()
        if still == "1":
            zb.ok("a named principal that is not the sweeper still gets a tombstone")
        else:
            zb.bad("a non-sweeper principal physically deleted the row — the bypass is too wide")
            failed += 1
        as_sweeper(f"DELETE FROM public.{TABLE} WHERE uid='{uid}'")

    finally:
        # ⚠️ Not optional. Left installed, every other scenario's cleanup DELETE becomes a
        # tombstone and rows accumulate across the suite.
        zb.psql(f"SELECT public.zebridge_remove_write_guards('public.{TABLE}'::regclass)", quiet=True)
        as_sweeper(f"DELETE FROM public.{TABLE} WHERE some_text LIKE 'guards:%' OR some_text='changed by a cron job' OR some_text='explicit'")

    return 1 if failed else 0


zb.run(main)

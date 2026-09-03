#!/usr/bin/env python3
"""The tenant write model, exercised — the behavioural counterpart to `check.py`.

`check.py` asserts the machinery is *installed* (guard trigger present, RLS on, tenant in
the replica identity). This asserts it *behaves*: it drives writes as different principals
and checks where each row lands and which are refused. Together they cover "wired" and
"works"; neither alone would have caught the fail-open bug this pins.

⚠️ The bug it exists to keep fixed: a tenanted client that omits `tenant_id` must NOT have
its row silently published to the open tenant. It must be stamped with the writer's OWN
tenant, derived from `zb.principal`. Coalescing to a constant open value (an earlier
version) meant one forgotten field turned a private row public with no error.

Everything runs on a throwaway table this scenario creates and drops, with throwaway
principals in `zebridge_user_tenants`, so it touches no fixture. ⚠️ Not quite nothing
left behind: the throwaway tenants `tw_acme`/`tw_globex` are new to a running bridge,
which onboards them live — `CDC_tw_acme`/`CDC_tw_globex` streams and
`$KV.tenants.tw_*` keys appear. The keys are deleted in `finally` when `NATS_CREDS` is
set; the streams are left (empty, and dyntenant.py's territory to prove removable).

⚠️ Runs its writes as `bridge_writer` with `zb.principal` set **inside the transaction** —
the one thing that makes RLS and the guard work, and the thing autocommit silently breaks
(the bridge's pipeline gets this right; a naive client would not).

Usage:  python scripts/scenarios/tenant_writes.py

Needs the write profile (guards + RLS functions) and the OPEN_TENANT carve-out in
`zebridge_scope_writes_by_tenant`. Read-only or single-tenant deployments will skip.
"""

import os
import subprocess
import sys
import uuid

import zb

T = "zb_tenant_probe"                 # a table this scenario owns
A, B, P = "tw_alice", "tw_bob", "tw_pub"
TA, TB = "tw_acme", "tw_globex"
OPEN = "_default"

failed = 0


def sql(text: str) -> str:
    return zb.psql(text)


def attempt(principal: str, body: str):
    """Run `body` as bridge_writer with zb.principal set, in ONE transaction.

    Returns (ok, output). `body` may be several statements; a RAISE or RLS refusal makes
    the whole transaction roll back and `ok` False.
    """
    stmt = (f"BEGIN; SET LOCAL ROLE bridge_writer; "
            f"SELECT set_config('zb.principal', '{principal}', true); {body} COMMIT;")
    res = subprocess.run(zb.PSQL.split() + ["-v", "ON_ERROR_STOP=1", "-tA", "-c", stmt],
                         capture_output=True, text=True)
    return res.returncode == 0, (res.stdout + res.stderr).strip()


def check(label: str, cond: bool, detail: str):
    global failed
    if cond:
        zb.ok(f"{label}: {detail}")
    else:
        zb.bad(f"{label}: {detail}")
        failed += 1


def tenant_of(note: str) -> str:
    return sql(f"SELECT tenant_id FROM public.{T} WHERE note = '{note}'").strip()


def main():
    # ── preconditions ──────────────────────────────────────────────────────────
    if not sql("SELECT 1 FROM pg_proc WHERE proname='zebridge_guard_tenant'").strip():
        print("zebridge_guard_tenant() not installed — write profile required. Skipping.")
        return 0
    carve = sql("SELECT (prosrc LIKE '%_default%')::text FROM pg_proc "
                "WHERE proname='zebridge_scope_writes_by_tenant'").strip()

    try:
        return checks(carve)
    finally:
        # ── cleanup, whatever happened above ───────────────────────────────────
        sql(f"DROP TABLE IF EXISTS public.{T} CASCADE")
        # Their principals AND the sweeper rows the autogrant trigger copied from them
        # (§10bv): cleaning by principal alone leaves zb_sweeper→tw_* behind, and boot
        # then creates CDC_tw_* streams for tenants that no longer exist.
        sql(f"DELETE FROM public.zebridge_user_tenants WHERE principal IN ('{A}','{B}','{P}') "
            f"OR tenant_id IN ('{TA}','{TB}')")
        if os.environ.get("NATS_CREDS"):
            bucket = zb.TOPOLOGY["kv"]["tenants"]
            for who in (A, B, P):
                zb.nats_cli("kv", "del", bucket, who, "-f")


def checks(carve: str) -> int:
    # ── setup: a throwaway table + throwaway principals ─────────────────────────
    sql(f"DROP TABLE IF EXISTS public.{T} CASCADE")
    sql(f"CREATE TABLE public.{T} (uid uuid, tenant_id text NOT NULL, note text, "
        f"updated_at timestamptz NOT NULL, deleted_at timestamptz, PRIMARY KEY (tenant_id, uid))")
    # Their principals AND the sweeper rows the autogrant trigger copied from them
    # (§10bv): cleaning by principal alone leaves zb_sweeper→tw_* behind, and boot then
    # creates CDC_tw_* streams for tenants that no longer exist.
    sql(f"DELETE FROM public.zebridge_user_tenants WHERE principal IN ('{A}','{B}','{P}') "
        f"OR tenant_id IN ('{TA}','{TB}')")
    sql(f"INSERT INTO public.zebridge_user_tenants VALUES "
        f"('{A}','{TA}'),('{B}','{TB}'),('{P}','{OPEN}')")
    sql(f"SELECT public.zebridge_grant_edge_writes('public.{T}')")
    sql(f"SELECT public.zebridge_scope_writes_by_tenant('public.{T}','tenant_id')")
    sql(f"SELECT public.zebridge_install_write_guards('public.{T}','updated_at','deleted_at','tenant_id')")

    def row(note, tenant=None):
        u = uuid.uuid4()
        cols = "uid, note, updated_at" + (", tenant_id" if tenant is not None else "")
        vals = f"'{u}','{note}',now()" + (f",'{tenant}'" if tenant is not None else "")
        return u, f"INSERT INTO public.{T} ({cols}) VALUES ({vals});"

    print()
    # ── 1. omission is stamped from IDENTITY, not a constant ───────────────────
    _, ins = row("a-omit")
    ok, _ = attempt(A, ins)
    check("alice omits tenant", ok and tenant_of("a-omit") == TA,
          f"stamped '{tenant_of('a-omit')}' (her own), not the open tenant")

    # ── 2. explicit own tenant passes ──────────────────────────────────────────
    _, ins = row("a-own", TA)
    ok, _ = attempt(A, ins)
    check("alice writes her own tenant", ok and tenant_of("a-own") == TA, "accepted")

    # ── 3. forging another tenant is refused (RLS WITH CHECK) ──────────────────
    _, ins = row("a-forge", TB)
    ok, out = attempt(A, ins)
    check("alice forges another tenant", (not ok) and "row-level security" in out,
          "refused by RLS" if not ok else "WRONGLY ACCEPTED — cross-tenant injection")

    # ── 4. empty string is absence, stamped from identity ──────────────────────
    _, ins = row("a-empty", "")
    ok, _ = attempt(A, ins)
    check("alice sends empty tenant", ok and tenant_of("a-empty") == TA,
          f"treated as absence, stamped '{tenant_of('a-empty')}'")

    # ── 5. a malformed real value is REFUSED, not coalesced ────────────────────
    _, ins = row("a-bad", "ev.il")
    ok, out = attempt(A, ins)
    check("alice sends malformed tenant", (not ok) and "metacharacter" in out,
          "refused (would route to a subject no consumer receives)"
          if not ok else "WRONGLY ACCEPTED — bad subject token")

    # ── 6. an unmapped principal omitting is fail-CLOSED ───────────────────────
    _, ins = row("orphan")
    ok, out = attempt("nobody", ins)
    check("unmapped principal omits", (not ok),
          "refused — no tenant to derive, and a tenant-routed row cannot be left unrouted"
          if not ok else "WRONGLY ACCEPTED — an unroutable row was written")

    # ── 7. the open tenant is a mapping, not a coalesce target ─────────────────
    _, ins = row("p-omit")
    ok, _ = attempt(P, ins)
    check("open-tenant member omits", ok and tenant_of("p-omit") == OPEN,
          f"stamped '{tenant_of('p-omit')}' — because {P} is MAPPED to it, not by default")

    # ── 8 + 9. open bar: a restricted user may write AND update open rows ───────
    if carve in ("t", "true"):
        uid, ins = row("a-open", OPEN)
        ok1, _ = attempt(A, ins)
        ok2, _ = attempt(A,
            f"UPDATE public.{T} SET note='a-open-v2', updated_at=now() "
            f"WHERE tenant_id='{OPEN}' AND uid='{uid}';")
        got = sql(f"SELECT note FROM public.{T} WHERE tenant_id='{OPEN}' AND uid='{uid}'").strip()
        check("restricted user writes open tenant", ok1 and ok2 and got == "a-open-v2",
              "alice (tenant-only) wrote and then UPDATED an open row")
    else:
        print(f"  – open-bar carve-out not present in zebridge_scope_writes_by_tenant — "
              f"skipping (writing {OPEN} would be refused)")

    # ── 10. DELETE integrity: alice cannot reach bob's row ─────────────────────
    _, ins = row("b-row", TB)
    attempt(B, ins)                                  # bob writes his own
    ok, _ = attempt(A, f"DELETE FROM public.{T} WHERE note='b-row';")
    still_there = sql(f"SELECT count(*) FROM public.{T} WHERE note='b-row'").strip()
    check("alice deletes another tenant's row", still_there == "1",
          "row survived — RLS USING hid it from her" if still_there == "1"
          else "row was DELETED — cross-tenant delete")

    # ── 11. a soft delete of her OWN row tombstones it ─────────────────────────
    ok, _ = attempt(A, f"DELETE FROM public.{T} WHERE note='a-own';")
    dead = sql(f"SELECT (deleted_at IS NOT NULL)::text FROM public.{T} WHERE note='a-own'").strip()
    check("alice soft-deletes her own row", dead in ("t", "true"),
          "tombstoned (DELETE became an UPDATE)" if dead in ("t", "true") else "not tombstoned")

    print()
    return 1 if failed else 0


sys.exit(main())

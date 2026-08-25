#!/usr/bin/env python3
"""Drift — every place two copies of one fact can disagree, compared.

Activation is spread over four places and two times (NOTES.md §4.5): SQL at bootstrap, SQL
per table, bridge environment, NATS conf. Nothing compares them. Every hard-to-find bug in
this project has been the same shape — two copies of one fact, disagreeing quietly, with
both sides looking healthy:

  • `nats-server.conf` rendered with `${NATS_BOB_PASSWORD}` unsubstituted. The generator
    printed `✓ Generated NATS config with credentials` and exited 0.
  • `users` published with no `TENANT_RULES` entry, so it emitted `cdc.users.insert` — a
    subject no tenant-scoped consumer filter can match. Counters incremented; nothing arrived.
  • the catalogue still calling `test_types` public after it gained `tenant_id`.
  • `BRIDGE_CDC_TABLES` in an env file, read by no code, free to disagree with the publication.

This changes nothing. It reads what is *declared* (conf, env, grammar.json) and what is
*actual* (PostgreSQL, NATS) and reports where they differ.

⚠️ Exits non-zero on drift, but **nothing should `depends_on` it**. The exit code is how the
verdict travels; `depends_on` is what decides whether anyone waits. Keeping those separate
means individual checks can be promoted to gating by editing compose, not this file.

⚠️ The declaration is often *legitimately* ahead of reality — you add a table, then migrate,
then set TENANT_RULES, then reload NATS. Findings during that window are correct and not
bugs. That is why this starts advisory.

Usage:
    NATS_URL=nats://127.0.0.1:4222 NATS_NKEY_SEED=SU... \
        python scripts/scenarios/check.py

    ZB_NATS_CONF=/config/nats-server.conf python scripts/scenarios/check.py   # in compose
"""

import json
import os
import re
import subprocess
import sys

import zb

# The rendered conf — the *live* one, not the template. They are different artifacts and
# their disagreement is itself a finding (the bob password bug lived exactly there).
NATS_CONF = os.environ.get("ZB_NATS_CONF", "")
NATS_VOLUME = os.environ.get("ZB_NATS_VOLUME", "zebridge_nats-config")

# Principals that are infrastructure, not clients. N-1 does not apply to them: the sweeper
# is mapped to every tenant it may reap, and that N-ness *is* how its reach stays auditable
# (NOTES.md §1.12, bridge_sweeper.zig).
INFRA = set(filter(None, os.environ.get("ZB_INFRA_PRINCIPALS", "zb_sweeper").split(",")))

findings = []


def bad(msg, detail=""):
    findings.append(msg)
    zb.bad(msg)
    if detail:
        for line in detail.splitlines():
            print(f"      {line}")


def read_conf() -> str:
    if NATS_CONF and os.path.exists(NATS_CONF):
        return open(NATS_CONF).read()
    # Host case: the conf lives in a docker volume, not on this filesystem.
    r = subprocess.run(
        ["docker", "run", "--rm", "-v", f"{NATS_VOLUME}:/config", "alpine",
         "cat", "/config/nats-server.conf"],
        capture_output=True, text=True,
    )
    return r.stdout if r.returncode == 0 else ""


def parse_grants(conf: str) -> dict:
    """principal -> subscribe subjects.

    ⚠️ Heuristic, and says so. The conf hoists each principal's read list into a
    `<NAME>_READ = [...]` variable because NATS will not splice a variable into a list, so
    the grants are not syntactically inside the user block. Matching the convention is
    good enough to compare against the database and honest about being a convention.
    """
    lists = {
        m.group(1).lower(): re.findall(r'"([^"]+)"', m.group(2))
        for m in re.finditer(r"^(\w+)_READ\s*=\s*\[(.*?)^\]", conf, re.S | re.M)
    }
    users = re.findall(r'\{\s*user:\s*"([^"]+)"', conf)
    return {u: lists.get(u.lower(), []) for u in users}


def main():
    conf = read_conf()
    if not conf:
        print("⚠️  could not read the rendered NATS conf — NATS checks skipped")
        print(f"    set ZB_NATS_CONF, or ensure the volume '{NATS_VOLUME}' exists\n")

    # ── 1. the rendered conf actually rendered ────────────────────────────────
    if conf:
        left = re.findall(r"\$\{(\w+)\}", conf)
        if left:
            bad(f"{len(left)} unsubstituted variable(s) in the LIVE conf: {sorted(set(left))}",
                "NATS accepts these as literal values, so the account exists with a password\n"
                "nobody can type and every login fails with `Authorization Violation`.")
        else:
            zb.ok("rendered NATS conf has no unsubstituted variables")

    grants = parse_grants(conf) if conf else {}
    mapping = {}
    for line in zb.psql("SELECT principal || '|' || tenant_id FROM zebridge_user_tenants").splitlines():
        if "|" in line:
            p, t = line.split("|", 1)
            mapping.setdefault(p, set()).add(t)

    # ── 2. N-1 for client principals ──────────────────────────────────────────
    multi = {p: t for p, t in mapping.items() if len(t) > 1 and p not in INFRA}
    if multi:
        bad(f"client principal(s) hold more than one tenant: "
            + ", ".join(f"{p}={sorted(t)}" for p, t in multi.items()),
            "N-1 is the rule for clients (NOTES.md §1.12). Broaden access by moving the\n"
            "principal to a wider tenant, not by accumulating tenants against its name.")
    else:
        zb.ok(f"every client principal holds at most one tenant (infrastructure exempt: {sorted(INFRA)})")

    # ── 3. CDC read grants are tenant-scoped ──────────────────────────────────
    if grants:
        wide = [p for p, subs in grants.items()
                if any(s in ("cdc.>", "cdc.*.>") for s in subs) and p in mapping]
        if wide:
            bad(f"principal(s) granted the WHOLE change feed: {sorted(wide)}",
                "Tenant isolation on reads is then enforced only by the client's own\n"
                "filter_subject, which the client chooses. Verified: alice (acme) received\n"
                "cdc.globex.test_types.insert using her own credentials.\n"
                "Expected instead: cdc.<her tenant>.>")
        else:
            zb.ok("no principal is granted the whole change feed")

        # ── 4. snapshot reach == subscribe reach ──────────────────────────────
        for p, subs in grants.items():
            if p not in mapping:
                continue
            cdc = {s for s in subs if s.startswith("cdc.")}
            snap = {s for s in subs if s.startswith("init.")}
            cdc_t = {s.split(".")[1] for s in cdc if s.count(".") >= 2 and s.split(".")[1] != ">"}
            snap_t = {s.split(".")[2] for s in snap if s.startswith("init.snap.") and s.count(".") >= 3}
            if "init.>" in snap and cdc_t:
                bad(f"'{p}' has tenant-scoped CDC {sorted(cdc_t)} but wholesale 'init.>'",
                    "A client must not be able to dump what it cannot subscribe to.")
            elif snap_t and cdc_t and snap_t != cdc_t:
                bad(f"'{p}': snapshot reach {sorted(snap_t)} != subscribe reach {sorted(cdc_t)}")

    # ── 5. the catalogue's public set vs CDC_PUBLIC's bound subjects ──────────
    #
    # The bridge reconciles CDC_PUBLIC's subject filter from `zebridge_catalogue`
    # (tenant_col IS NULL) at BOOT, so drift here has exactly one meaning: the
    # catalogue changed after the bridge booted, and a restart is due.
    publics = {r for r in zb.psql(
        "SELECT tbl FROM zebridge_catalogue WHERE tenant_col IS NULL").splitlines() if r}
    cdc_prefix = zb.TOPOLOGY["subjects"]["cdc_prefix"]
    open_t = zb.TOPOLOGY.get("open_tenant", "_default")
    pinfo = zb.nats_cli("stream", "info", "CDC_PUBLIC", "--json")
    if pinfo.returncode != 0:
        print("  ⓘ  could not read CDC_PUBLIC — public-subject check skipped")
    else:
        bound = set(json.loads(pinfo.stdout).get("config", {}).get("subjects", []))
        unbound = {t for t in publics if f"{cdc_prefix}.{t}.>" not in bound}
        if unbound:
            bad(f"catalogue-public but not bound in CDC_PUBLIC: {sorted(unbound)}",
                "Declared after the bridge booted. Restart the bridge — boot reconciles\n"
                "CDC_PUBLIC's subjects from the catalogue.")
        else:
            zb.ok(f"CDC_PUBLIC binds every catalogue-public table ({len(publics)})")
        stale = sorted(x.split(".")[1] for x in bound
                       if x.count(".") >= 2 and x.split(".")[1] not in publics
                       and x.split(".")[1] != open_t)
        if stale:
            print(f"  ⓘ  bound but no longer catalogue-public (stale until the next boot): {stale}")

    # The contradiction that matters: a catalogue row calling a table public while
    # the table HAS a tenant column. The CHECK constraint guarantees a reason was
    # recorded; nothing recomputes the decision when a migration adds the column.
    for tbl in sorted(publics):
        col = zb.psql(
            "SELECT a.attname FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
            f"WHERE c.relname='{tbl}' AND a.attname='tenant_id' AND a.attnum>0", quiet=True
        ).strip()
        if col:
            reason = zb.psql(
                f"SELECT public_reason FROM zebridge_catalogue WHERE tbl='{tbl}'").strip()
            bad(f"'{tbl}' is catalogue-public but HAS a tenant column ({col})",
                f"recorded reason: {reason!r}\n"
                "Every consumer may read it while its rows belong to individual tenants.\n"
                "Either delete the catalogue row or drop the tenant column.")

    # ── 6. tenant columns (catalogue + env overrides) vs the schema ───────────
    tenant_rules = {}
    for entry in os.environ.get("TENANT_RULES", "").split(";"):
        if ":" in entry:
            t, c = entry.split(":", 1)
            tenant_rules[t.strip()] = c.strip()
    for r in zb.psql(
            "SELECT tbl||':'||tenant_col FROM zebridge_catalogue "
            "WHERE tenant_col IS NOT NULL").splitlines():
        if ":" in r:
            t, c = r.split(":", 1)
            tenant_rules.setdefault(t.strip(), c.strip())

    for tbl, col in tenant_rules.items():
        info = zb.psql(
            "SELECT attnotnull::text FROM pg_attribute "
            f"WHERE attrelid='public.{tbl}'::regclass AND attname='{col}' AND attnum>0"
        ).strip()
        if not info:
            bad(f"tenant rule names {tbl}.{col}, which does not exist")
        # ⚠️ `attnotnull::text` renders as 'true'/'false', not psql's usual 't'/'f'. Comparing
        # against 't' reported every NOT NULL column as nullable — a checker that cries wolf
        # is worse than no checker, so accept both spellings.
        elif info not in ("t", "true"):
            bad(f"tenant column {tbl}.{col} is NULLABLE",
                "A NULL tenant belongs to nobody and cannot carry a replica identity.")
        else:
            zb.ok(f"tenant rule {tbl}:{col} exists and is NOT NULL")

    published = [r for r in zb.psql(
        "SELECT tablename FROM pg_publication_tables").splitlines() if r]
    for tbl in published:
        if tbl in tenant_rules or tbl.startswith("zebridge_"):
            continue
        has_tenant = zb.psql(
            "SELECT attname FROM pg_attribute "
            f"WHERE attrelid='public.{tbl}'::regclass AND attname='tenant_id' AND attnum>0"
        ).strip()
        if has_tenant:
            bad(f"'{tbl}' is published and has tenant_id, but no tenant rule anywhere",
                f"It publishes to cdc.{tbl}.<op> with no tenant token, which no tenant-scoped\n"
                f"consumer filter can match. Declare it: zebridge_enable(..., tenant_col =>\n"
                f"'tenant_id', dry_run => false), then restart the bridge.")

    # ── 7. SYNC_RULES columns exist ───────────────────────────────────────────
    for entry in os.environ.get("SYNC_RULES", "").split(";"):
        if ":" not in entry:
            continue
        tbl, cols = entry.split(":", 1)
        tbl = tbl.strip()
        for col in [c.strip() for c in cols.split(",") if c.strip()]:
            if not zb.psql("SELECT attname FROM pg_attribute "
                           f"WHERE attrelid='public.{tbl}'::regclass AND attname='{col}' AND attnum>0").strip():
                bad(f"SYNC_RULES names {tbl}.{col}, which does not exist",
                    "Every mutation to this table fails permanently and dead-letters on first\n"
                    "delivery — the client sees a rejection it cannot act on.")
        else:
            zb.ok(f"SYNC_RULES {tbl}: every named column exists")

    # catalogue LWW columns: enable() validated them at write time, but a later
    # DROP COLUMN invalidates the row silently — same failure shape as SYNC_RULES.
    for r in zb.psql(
            "SELECT tbl||'|'||version_col||'|'||COALESCE(tombstone_col::text,'')||'|'||"
            "COALESCE(tiebreak_col::text,'') FROM zebridge_catalogue").splitlines():
        parts = r.split("|")
        if len(parts) != 4:
            continue
        tbl = parts[0]
        exists = zb.psql(
            f"SELECT 1 FROM pg_class WHERE relname='{tbl}' AND relkind='r'", quiet=True).strip()
        if not exists:
            print(f"  ⓘ  catalogue row for '{tbl}' but no such table (dropped? delete the row)")
            continue
        for col in [c for c in parts[1:] if c]:
            if not zb.psql(
                    "SELECT a.attname FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
                    f"WHERE c.relname='{tbl}' AND a.attname='{col}' AND a.attnum>0", quiet=True).strip():
                bad(f"catalogue names {tbl}.{col}, which does not exist",
                    "Every mutation to this table fails permanently and dead-letters on first\n"
                    "delivery — fix the catalogue row or restore the column.")

    # ── 8. replication slots: orphans retain WAL for everyone ─────────────────
    #
    # A permanent slot outlives the connection, the process and the reboot — that is the
    # point, it holds the LSN so a restart can resume. The cost is that a slot nobody
    # consumes still pins WAL, invisibly: `bridge_slot_active` and `bridge_wal_lag_bytes`
    # describe the bridge's OWN slot, so an abandoned one is reported by nothing.
    #
    # Left alone it grows to `max_slot_wal_keep_size` and is then INVALIDATED, at which
    # point it cannot resume at all and whatever it fed needs a full resnapshot. So the
    # window between "orphaned" and "unrecoverable" is exactly that setting.
    declared_slot = os.environ.get("BRIDGE_CDC_SLOT", "").strip()
    rows = [r for r in zb.psql(
        "SELECT slot_name || '|' || active::text || '|' || coalesce(wal_status,'?') || '|' || "
        "coalesce(invalidation_reason,'') || '|' || "
        "pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) || '|' || "
        "coalesce(date_trunc('second', now() - inactive_since)::text,'') "
        "FROM pg_replication_slots"
    ).splitlines() if r]

    if not declared_slot:
        print("  \u24d8  BRIDGE_CDC_SLOT unset — cannot tell a bridge's slot from an orphan")
    for row in rows:
        name, active, wal_status, invalid, retained, idle = (row.split("|") + [""] * 6)[:6]
        # ⚠️ Second time this file has been bitten by it: `bool::text` in a `||` expression
        # renders 'true'/'false', not psql's bare 't'/'f'. Comparing to "t" reported a
        # healthy, attached bridge as INACTIVE — a checker's worst failure, since the fix
        # you would reach for is restarting something that was never broken.
        live = active in ("t", "true")
        if invalid:
            bad(f"slot '{name}' is INVALIDATED ({invalid})",
                "It can no longer resume. Whatever it fed needs a full resnapshot; the slot "
                "itself is now only holding a name.")
        elif not live and declared_slot and name != declared_slot:
            bad(f"orphaned slot '{name}': inactive, retaining {retained}"
                + (f", idle for {idle}" if idle else ""),
                f"Not the declared slot ('{declared_slot}') and nothing is consuming it, but it "
                f"pins WAL for the whole cluster until max_slot_wal_keep_size invalidates it.\n"
                f"  SELECT pg_drop_replication_slot('{name}');")
        elif not live:
            bad(f"declared slot '{name}' is INACTIVE, retaining {retained}"
                + (f", idle for {idle}" if idle else ""),
                "The bridge is not attached. WAL accumulates until it reconnects.")
        elif wal_status != "reserved":
            bad(f"slot '{name}' wal_status={wal_status} (retaining {retained})",
                "Past 'reserved' the retained WAL is no longer safely within limits; "
                "'lost' means it is already unrecoverable.")
        else:
            zb.ok(f"slot '{name}': active, wal_status=reserved, retaining {retained}")

    # ── 9. the open tenant: one env, three places ────────────────────────────
    #
    # `OPEN_TENANT` names the shared/open tenant. It is NOT what the guard writes any more —
    # the guard derives a missing tenant from the WRITER'S identity, so the open tenant is
    # simply what a principal *mapped to it* carries (e.g. `(pub, _default)`). But the name
    # still has to agree in two rendered places plus the mapping: the CDC_PUBLIC stream's
    # subjects (nats-init) and every principal's read grant (nats-config-gen), each rendered
    # from whatever .env.admin said at ITS run. Change the value, re-run one container, and
    # rows carrying the old open tenant land on a subject no stream captures — no PubAck,
    # the bridge FATALs. This checks the two renders agree with the env.
    open_tenant = os.environ.get("OPEN_TENANT", "").strip()
    if not open_tenant:
        print("  ⓘ  OPEN_TENANT unset — comparing against '_default', the .env.admin default")
        open_tenant = "_default"
    tenants = set(zb.tenants())
    open_subject = f"{zb.TOPOLOGY['subjects']['cdc_prefix']}.{open_tenant}.>"

    if re.search(r"[.*> ]", open_tenant):
        bad(f"OPEN_TENANT={open_tenant!r} is not a legal NATS subject token",
            "It becomes `cdc.<OPEN_TENANT>.<table>.<op>`; a dot splits it into two tokens.")
    if open_tenant in tenants:
        bad(f"OPEN_TENANT={open_tenant!r} is also a live tenant (zebridge_user_tenants)",
            "The bridge's boot reconciliation would bind the same subject into two streams,\n"
            "and NATS refuses overlapping streams — whichever comes second does not exist.")

    if tenants:
        # what NATS actually stores: the public stream's bound subjects
        public_stream = (zb.TOPOLOGY.get("cdc_streams") or {}).get("public", "CDC_PUBLIC")
        info = zb.nats_cli("stream", "info", public_stream, "--json")
        if info.returncode != 0:
            print(f"  ⓘ  could not read stream {public_stream} ({info.stderr.strip()[:60]}) — stream check skipped")
        else:
            bound = json.loads(info.stdout).get("config", {}).get("subjects", [])
            if open_subject not in bound:
                bad(f"stream {public_stream} does not bind {open_subject} (bound: {bound})",
                    "Rows that opted into no tenant publish there and NO stream captures them.\n"
                    "Restart the bridge; boot reconciles CDC_PUBLIC from the catalogue + OPEN_TENANT.")
            else:
                zb.ok(f"stream {public_stream} binds {open_subject}")

        # what the principals may READ: the rendered conf
        if grants:
            lacking = sorted(
                p for p, subs in grants.items()
                if p in mapping and open_subject not in subs
                and not any(s in ("cdc.>", "cdc.*.>") for s in subs))
            if lacking:
                bad(f"principal(s) not granted the open tenant {open_subject}: {lacking}",
                    "They hold CDC_PUBLIC, so a JetStream consumer still delivers those rows —\n"
                    "but a core subscription is refused, and the conf no longer says what it\n"
                    "means. Add the line to each <NAME>_READ list and reload NATS.")
            else:
                zb.ok(f"every mapped principal is granted {open_subject}")

    # ── 10. tenant-CAPABLE tables are fully wired, not just configured ────────
    #
    # ⚠️ NOT "sensitive". There is no table-level sensitivity: a `tenant_id` column makes a
    # table tenant-*capable*, and such a table legitimately holds BOTH real-tenant (sensitive)
    # and _default (open) rows — sensitivity is a per-row property, not a per-table one. So
    # this does not assert "these rows are private"; it asserts that every table which CAN
    # route by tenant has the machinery to do it correctly. A tenant-capable table with a
    # broken chain silently exposes or fails to route whatever tenant data it does hold.
    #
    # The signal is the column, independent of TENANT_RULES (which is what we are checking
    # got set). ⚠️ Heuristic: a column named tenant_id for an unrelated reason is swept in;
    # in this schema that does not happen.
    sensitive = [r for r in zb.psql(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='public' AND c.relkind='r' AND NOT c.relname LIKE 'zebridge_%' "
        "AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid "
        "  AND a.attname='tenant_id' AND a.attnum>0 AND NOT a.attisdropped)"
    ).splitlines() if r]

    for tbl in sensitive:
        problems = []
        # (a) routed by the bridge — else events publish bare, invisible to tenant consumers
        if tbl not in tenant_rules:
            problems.append(f"no TENANT_RULES entry (set TENANT_RULES={tbl}:tenant_id)")
        # (b) the guard trigger — else omission/malformed is not corrected at the source
        if zb.psql("SELECT count(*) FROM pg_trigger WHERE tgname='zebridge_guard_tenant_t' "
                   f"AND tgrelid='public.{tbl}'::regclass").strip() == "0":
            problems.append("no tenant guard (run zebridge_install_write_guards with tenant_col, "
                            "or zebridge_enable — a forgotten or malformed tenant is not caught)")
        # (c) RLS on + the write policy — else a client can forge rows into another tenant
        if zb.psql(f"SELECT relrowsecurity::text FROM pg_class WHERE oid='public.{tbl}'::regclass").strip() not in ("t","true"):
            problems.append("row-level security is OFF (a writer can forge another tenant's rows)")
        elif zb.psql("SELECT count(*) FROM pg_policy p JOIN pg_class c ON c.oid=p.polrelid "
                     f"WHERE c.relname='{tbl}' AND p.polname='zb_tenant_write'").strip() == "0":
            problems.append("no zb_tenant_write policy (run zebridge_scope_writes_by_tenant)")
        # (d) tenant in the replica identity — else a DELETE arrives with no tenant to route by
        in_ri = zb.psql(
            "SELECT count(*) FROM pg_index i JOIN pg_attribute a "
            "  ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
            f"WHERE i.indrelid='public.{tbl}'::regclass AND a.attname='tenant_id' "
            "  AND ((SELECT relreplident FROM pg_class WHERE oid=i.indrelid)='d' AND i.indisprimary "
            "    OR (SELECT relreplident FROM pg_class WHERE oid=i.indrelid)='i' AND i.indisreplident)"
        ).strip()
        full_ri = zb.psql(f"SELECT (relreplident='f')::text FROM pg_class WHERE oid='public.{tbl}'::regclass").strip() in ("t","true")
        if in_ri == "0" and not full_ri:
            problems.append("tenant_id is not in the replica identity (a DELETE arrives without "
                            "it and cannot be routed — REPLICA IDENTITY USING INDEX (tenant_id, pk))")

        if problems:
            bad(f"tenant-capable table '{tbl}' (has tenant_id) is not fully wired",
                "\n".join(f"- {x}" for x in problems))
        else:
            zb.ok(f"tenant-capable table '{tbl}': routed, guarded, RLS-scoped, tenant in replica identity")

    print()
    if findings:
        print(f"\033[31m{len(findings)} disagreement(s)\033[0m — declared and actual differ\n")
        return 1
    print("\033[32mno drift\033[0m\n")
    return 0


sys.exit(main())

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
  • `zebridge_public_tables` still calling `test_types` public after it gained `tenant_id`.
  • `BRIDGE_CDC_TABLES` in an env file, read by no code, free to disagree with the publication.

This changes nothing. It reads what is *declared* (conf, env, topology.json) and what is
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

    # ── 5. topology.json public_tables vs the database ────────────────────────
    declared = set(zb.TOPOLOGY.get("public_tables") or [])
    actual = {r.split(".")[-1] for r in zb.psql(
        "SELECT tbl::text FROM zebridge_public_tables").splitlines() if r}
    if declared - actual:
        bad(f"topology.json declares public but the DB does not: {sorted(declared - actual)}")
    if actual - declared:
        # Bridge-owned tables are public by construction, so this alone is information.
        extra = sorted(t for t in (actual - declared) if not t.startswith("zebridge_"))
        infra = sorted(t for t in (actual - declared) if t.startswith("zebridge_"))
        if infra:
            print(f"  \u24d8  bridge-owned and public by construction: {infra}")
        if extra:
            print(f"  \u24d8  public in the DB but not in topology.json: {extra}")

    # ⚠️ The contradiction that matters: a table declared readable by EVERY consumer while
    # being tenant-scoped. `zebridge_public_tables` records a decision and its reason, so a
    # stale row is a stale *decision* — it says the table has no tenant column when it has
    # one, and nothing recomputes that when a migration adds the column.
    for tbl in sorted(actual):
        col = zb.psql(
            "SELECT attname FROM pg_attribute "
            f"WHERE attrelid='public.{tbl}'::regclass AND attname='tenant_id' AND attnum>0"
        ).strip()
        if col:
            reason = zb.psql(
                f"SELECT reason FROM zebridge_public_tables WHERE tbl='public.{tbl}'::regclass"
            ).strip()
            bad(f"'{tbl}' is declared public but HAS a tenant column ({col})",
                f"recorded reason: {reason!r}\n"
                "Every consumer may read it while its rows belong to individual tenants.\n"
                "Either drop the zebridge_public_tables row or drop the tenant column.")
    if not (declared - actual):
        zb.ok(f"topology.json public_tables agrees with zebridge_public_tables ({sorted(declared)})")

    # ── 6. TENANT_RULES vs the schema, and published tables that need one ─────
    tenant_rules = {}
    for entry in os.environ.get("TENANT_RULES", "").split(";"):
        if ":" in entry:
            t, c = entry.split(":", 1)
            tenant_rules[t.strip()] = c.strip()

    for tbl, col in tenant_rules.items():
        info = zb.psql(
            "SELECT attnotnull::text FROM pg_attribute "
            f"WHERE attrelid='public.{tbl}'::regclass AND attname='{col}' AND attnum>0"
        ).strip()
        if not info:
            bad(f"TENANT_RULES names {tbl}.{col}, which does not exist")
        # ⚠️ `attnotnull::text` renders as 'true'/'false', not psql's usual 't'/'f'. Comparing
        # against 't' reported every NOT NULL column as nullable — a checker that cries wolf
        # is worse than no checker, so accept both spellings.
        elif info not in ("t", "true"):
            bad(f"TENANT_RULES column {tbl}.{col} is NULLABLE",
                "A NULL tenant belongs to nobody and cannot carry a replica identity.")
        else:
            zb.ok(f"TENANT_RULES {tbl}:{col} exists and is NOT NULL")

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
            bad(f"'{tbl}' is published and has tenant_id, but no TENANT_RULES entry",
                f"It publishes to cdc.{tbl}.<op> with no tenant token, which no tenant-scoped\n"
                f"consumer filter can match. Set TENANT_RULES={tbl}:tenant_id and restart.")

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

    print()
    if findings:
        print(f"\033[31m{len(findings)} disagreement(s)\033[0m — declared and actual differ\n")
        return 1
    print("\033[32mno drift\033[0m\n")
    return 0


sys.exit(main())

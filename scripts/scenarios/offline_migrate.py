#!/usr/bin/env python3
"""A client offline across a schema change, with a write queued, comes back right.

  1. Both clients (libzb, zb-client-ts in Node) sync a writable table of three rows.
  2. Both go offline: libzb is closed on its database file, zb-client-ts hangs up. While
     offline, zb-client-ts queues an UPDATE on a column that is about to be dropped.
  3. PostgreSQL migrates meanwhile: ADD COLUMN extra DEFAULT 'filled', DROP COLUMN note,
     and an ordinary update of another row.
  4. Both come back (libzb reopens the same file, zb-client-ts reconnects).

What must hold: each replica has `extra`, filled on the old rows, and no `note`; the
server's update is there; the queued write is settled (it named a dropped column, so it
is refused, and the optimistic copy is reverted), nothing is left in the outbox; and both
replicas equal PostgreSQL.

Owns the only bridge (a probe bridge on the dev NATS): stop the dev bridge first.
Usage:  NATS_CREDS=scripts/native/creds/bridge.creds ZB_PRINCIPAL=omar python scripts/scenarios/offline_migrate.py
"""
import os
import sys
import time
import uuid

import zb
from clients import Lib, Node, both, fresh_sqlite

LOG = "/tmp/zb_offline_migrate_bridge.log"
T = "om_t"
PUB = zb.publication()
PY_DB, NODE_DB = "/tmp/zb-offline-migrate-py.sqlite3", "/tmp/zb-offline-migrate-node.sqlite3"
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")


def teardown():
    zb.forget_table(T)
    for sql in (f"DROP TABLE IF EXISTS public.{T}",
                f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'",
                f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'"):
        zb.psql(sql, quiet=True)


def pg_rows():
    out = zb.psql(f"SELECT id || '|' || coalesce(title,'') || '|' || coalesce(extra,'') FROM public.{T} "
                  "WHERE deleted_at IS NULL ORDER BY id")
    return out.splitlines()


def replica_rows(c):
    return [f"{r[0]}|{r[1] or ''}|{r[2] or ''}" for r in
            c.q(f"SELECT id, title, extra FROM {T} WHERE deleted_at IS NULL ORDER BY id")]


def main():
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")
    failed = 0

    def check(label, cond, detail=""):
        nonlocal failed
        (zb.ok if cond else zb.bad)(f"{label}{': ' + detail if detail else ''}")
        failed += 0 if cond else 1

    who = zb.require_principal()
    tenant = zb.tenant_of(who)
    os.environ["ZB_TABLES"] = T  # the Node worker follows this table only, not the whole tenant
    teardown()
    zb.psql(f"CREATE TABLE public.{T} (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), title text, note text, "
            "tenant_id varchar(255) NOT NULL, last_writer varchar(255), updated_at timestamptz NOT NULL DEFAULT now(), "
            "deleted_at timestamptz)")
    ids = sorted(str(uuid.uuid4()) for _ in range(3))
    for i, rid in enumerate(ids):
        zb.psql(f"INSERT INTO public.{T} (id, title, note, tenant_id) VALUES ('{rid}', 'row {i}', 'n{i}', '{tenant}')")
    fresh_sqlite(PY_DB); fresh_sqlite(NODE_DB)
    py = nd = None
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5") as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); return 1
            out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE})")
            if "ERROR" in out:
                zb.bad(f"zebridge_enable: {out}"); return 1
            zb.wait_for_schema(T)

            # ── 1. both clients sync ──────────────────────────────────────────────
            py = Lib(PY_DB, [T], principal=who)
            nd = Node(NODE_DB, principal=who)
            synced = both(py, nd, lambda c: len(c.q(f"SELECT id FROM {T}")) == 3, budget=90)
            check("1. synced", synced is not None, "both replicas hold the three rows")

            # ── 2. both go offline; zb-client-ts queues a write on `note` ────────
            py.close(); py = None
            nd.disconnect()
            nd.mutate(T, "UPDATE", {"id": ids[0]}, {"note": "written offline"})
            queued = nd.q("SELECT count(*) FROM _zebridge_outbox")[0][0]
            check("2. queued", queued >= 1, f"zb-client-ts holds {queued} write(s) in its outbox")

            # ── 3. PostgreSQL migrates while they are away ────────────────────────
            zb.psql(f"ALTER TABLE public.{T} ADD COLUMN extra text DEFAULT 'filled'")
            zb.psql(f"ALTER TABLE public.{T} DROP COLUMN note")
            zb.psql(f"UPDATE public.{T} SET title = 'moved while away' WHERE id = '{ids[1]}'")
            time.sleep(2)

            # ── 4. both come back ─────────────────────────────────────────────────
            nd.connect()
            py = Lib(PY_DB, [T], principal=who)
            want = pg_rows()
            same = both(py, nd, lambda c: replica_rows(c) == want, budget=90)
            check("4. converged", same is not None, f"both replicas equal PostgreSQL ({len(want)} rows)")
            for name, c in (("libzb", py), ("zb-client-ts", nd)):
                cols = c.cols(T)
                check(f"4. {name} columns", "extra" in cols and "note" not in cols,
                      "has the added column, not the dropped one")
            check("4. defaults", all(r.endswith("|filled") for r in want),
                  "the old rows carry the added column's default")
            check("4. server update", any("moved while away" in r for r in replica_rows(nd)),
                  "the update made while they were away is there")
            left = nd.q("SELECT count(*) FROM _zebridge_outbox")[0][0]
            check("4. outbox", left == 0, f"the queued write is settled ({left} left)")
    finally:
        if py: py.close()
        if nd: nd.close()
        teardown()
        fresh_sqlite(PY_DB); fresh_sqlite(NODE_DB)

    print()
    if failed:
        zb.bad(f"{failed} check(s) failed")
        return 1
    zb.ok("a client offline across ADD / DROP COLUMN, with a write queued, comes back equal to PostgreSQL")
    return 0


if __name__ == "__main__":
    sys.exit(main())

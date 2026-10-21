#!/usr/bin/env python3
"""Mutation receipts in a disposable local PostgreSQL database (hub fallback, stage 2a).

Exercises the zebridge_private receipt storage through the writer's functions (receipts-locks
prototype: lock_receipts, lookup_receipts, record_receipts, close_guard): month derivation
from the ID, claimed/duplicate/conflict/expired/future/invalid verdicts, one receipt per
write, a concurrent duplicate waiting on the lock, the legacy sentinel, the monotonic
minimum, privileges, DDL silence, pruning and pg_dump.

ZB_RECEIPT_GUARD=1 runs variant B: the database's completion guard is switched on, every
finish closes it, and a lookup committed without its receipt must be refused.
"""
import datetime
import os
import pathlib
import subprocess
import time
import uuid

import zb

BRIDGE = pathlib.Path(os.environ.get("ZB_BRIDGE_BIN", zb.ROOT / "zig-out/bin/bridge"))
GUARD = os.environ.get("ZB_RECEIPT_GUARD") == "1"


def ms(dt):
    return int(dt.timestamp() * 1000)


def main():
    name = "zb_receipts_" + uuid.uuid4().hex[:10]
    reader, writer = name + "_reader", name + "_writer"
    base = [zb._psql_binary(), "-X", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-Atq"]
    failures = 0

    def sql(statement, database=name, check=True):
        result = subprocess.run([*base, "-d", database], input=statement, text=True, capture_output=True)
        if check and result.returncode:
            raise RuntimeError(result.stderr[-2500:])
        return result

    def check(ok, label):
        nonlocal failures
        (zb.ok if ok else zb.bad)(label)
        failures += not ok

    def claim(ids, payloads=None, principal="alice", as_writer=True, extra=""):
        """lock_receipts then lookup_receipts for `ids` (same route, payload per id or 'p'), then `extra` (a finish)."""
        payloads = payloads or ["p"] * len(ids)
        arr = lambda xs: "ARRAY[" + ",".join(f"convert_to('{x}','UTF8')" for x in xs) + "]::bytea[]"
        ids_sql = "ARRAY[" + ",".join(f"'{i}'" for i in ids) + "]::text[]"
        role = f"SET ROLE {writer};" if as_writer else ""
        return sql(f"{role} BEGIN; SELECT zebridge_private.lock_receipts(ARRAY[" + ",".join([f"'{principal}'"] * len(ids)) + f"]::text[], {ids_sql}); "
                   f"SELECT ord, status, coalesce(outcome::text, '') FROM zebridge_private.lookup_receipts("
                   f"'{principal}', {ids_sql}, {arr(['mutation.' + principal + '.t.insert'] * len(ids))}, {arr(payloads)}); {extra} COMMIT;",
                   check=False)

    def statuses(result):
        return [line.split("|")[1] for line in result.stdout.splitlines() if line.count("|") == 2]

    def finish(ids, principal="alice", payloads=None, outcome='{"ok":1}'):
        """record_receipts for `ids` (their fingerprints over the route and payload, 'p' unless given), then close_guard in B."""
        payloads = payloads or ["p"] * len(ids)
        route = f"convert_to('mutation.{principal}.t.insert','UTF8')"
        fps = ",".join(f"sha256(int4send(length({route})) || {route} || convert_to('{x}','UTF8'))" for x in payloads)
        return (f"SELECT zebridge_private.record_receipts('{principal}', ARRAY[" + ",".join(f"'{i}'" for i in ids) + f"]::text[], ARRAY[{fps}]::bytea[], ARRAY["
                + ",".join(f"'{outcome}'::jsonb" for _ in ids) + "]);" + (" SELECT zebridge_private.close_guard();" if GUARD else ""))

    now = datetime.datetime.now(datetime.timezone.utc)
    this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_ms_prev_month = ms(this_month) - 1
    first_ms_this_month = ms(this_month)

    sql(f'CREATE DATABASE "{name}"', "postgres")
    try:
        env = dict(os.environ, DATABASE_READER_URL=f"postgres://{reader}:fixture@127.0.0.1:5432/{name}",
                   DATABASE_WRITER_URL=f"postgres://{writer}:fixture@127.0.0.1:5432/{name}", TARGET_DB=name)
        env.pop("BRIDGE_CDC_PUBLICATION", None)
        rendered = subprocess.run([str(BRIDGE), "--init-sql"], env=env, capture_output=True, text=True, check=True).stdout
        first = sql(rendered)
        sql(rendered)  # idempotent re-apply
        if GUARD:
            sql("UPDATE zebridge_private.receipt_policy SET completion_guard = true")
            sql(rendered)  # a re-apply keeps the guard on
            check(sql("SELECT completion_guard FROM zebridge_private.receipt_policy").stdout.strip() == "t",
                  "variant B: the completion guard is on and survives a re-apply of --init-sql")
        check(first.stdout.strip() == "", "installing receipts prints nothing into --init-sql's output")
        count_parts = "SELECT count(*) FROM pg_inherits WHERE inhparent = 'zebridge_private.receipts'::regclass"
        check(sql(count_parts).stdout.strip() == "0", "installation creates no partition (pg_restore --clean must not meet one)")
        sql("SELECT zebridge_private.maintain_receipts()")  # what the bridge runs at startup

        parts = sql("SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                    "WHERE i.inhparent = 'zebridge_private.receipts'::regclass ORDER BY 1").stdout.split()
        want = {"receipts_legacy", "receipts_" + this_month.strftime("%Y_%m"),
                "receipts_" + (this_month + datetime.timedelta(days=32)).strftime("%Y_%m"),
                "receipts_" + (this_month - datetime.timedelta(days=1)).strftime("%Y_%m")}
        check(want <= set(parts) and 7 <= len(parts) <= 9,
              f"partitions cover the legacy month and the 180-day range through next month ({len(parts)})")
        check(sql("SELECT count(*) FROM pg_partitioned_table p JOIN pg_class c ON c.oid = p.partdefid "
                  "WHERE p.partrelid = 'zebridge_private.receipts'::regclass").stdout.strip() == "0", "there is no default partition")

        month = lambda i: sql(f"SELECT coalesce(zebridge_private.receipt_month('{i}')::text, 'NULL')").stdout.strip()
        check(month(f"zb1-t{last_ms_prev_month}-x") == (this_month - datetime.timedelta(days=1)).strftime("%Y-%m-01")
              and month(f"zb1-t{first_ms_this_month}-x") == this_month.strftime("%Y-%m-01"),
              "the month boundary is exact to the millisecond (UTC)")
        check(month("c-1a2b-users-7-v") == "1900-01-01", "an ID without the zb1- prefix is legacy (sentinel month)")
        check(month("zb1-tnope-x") == "NULL" and month("zb1-t123") == "NULL", "a malformed zb1- ID has no month")
        refused = sql("INSERT INTO zebridge_private.receipts VALUES ('alice', 'c-x', '2026-01-01', sha256('x'), '{}')", check=False)
        check(refused.returncode != 0, "the stored month must be the one derived from the ID")

        new_id = f"zb1-t{ms(now)}-c1-t-1"
        r = claim([new_id], extra=finish([new_id]))
        check(r.returncode == 0 and statuses(r) == ["claimed"], "a new ID is claimed, finished and committed")
        r = claim([new_id])
        check(statuses(r) == ["duplicate"] and '{"ok": 1}' in r.stdout, "a retry returns the recorded outcome, not 'claimed'")
        r = claim([new_id], payloads=["other"])
        check(statuses(r) == ["conflict"], "the same ID with different bytes is a conflict")

        a, b = f"zb1-t{ms(now)}-c1-t-2", f"zb1-t{ms(now)}-c1-t-3"
        r = claim([a, a, b, b], payloads=["p", "p", "p", "q"], extra=finish([a, b]))
        check(statuses(r) == ["claimed", "repeat", "claimed", "conflict"], "repeats inside one batch: same bytes repeat, other bytes conflict")
        r = claim([f"zb1-t{ms(now - datetime.timedelta(days=400))}-c1-t-4", f"zb1-t{ms(now + datetime.timedelta(minutes=10))}-c1-t-5",
                   "has.dot", "zb1-tbad-x", "x" * 513])
        check(statuses(r) == ["expired", "future", "invalid", "invalid", "invalid"], "expired, future and invalid IDs are refused before any claim")
        # One byte policy with admission and both builders: Unicode spaces are ordinary UTF-8,
        # ASCII space/control bytes are not, and the limit counts bytes, not characters.
        uni = [f"zb1-t{ms(now)}-c1-t-a b", f"zb1-t{ms(now)}-c1-t-a\u0085b"]
        r = claim(uni, extra=finish(uni))
        check(statuses(r) == ["claimed", "claimed"], "IDs with U+2003 or U+0085, which admission accepts, are claimed, not invalid")
        r = claim([f"zb1-t{ms(now)}-c1-t-a\tb", " " * 171])
        check(statuses(r) == ["invalid", "invalid"], "an ASCII tab is invalid; so is a 513-byte ID of 171 characters")

        unfinished = f"zb1-t{ms(now)}-c1-t-6"
        r = claim([unfinished])
        if GUARD:
            check(r.returncode != 0 and "would commit without their receipts" in r.stderr,
                  "variant B: a lookup that found the ID undecided cannot commit without its receipt")
        else:
            check(r.returncode == 0 and statuses(r) == ["claimed"],
                  "variant A: a lookup inserts nothing, so committing without a receipt is not refused (A's limitation)")
        r = claim([unfinished], extra=finish([unfinished]))
        check(statuses(r) == ["claimed"], "nothing was left behind: the ID is still undecided, then recorded")
        r = sql(f"SET ROLE {writer}; BEGIN; {finish([new_id])} COMMIT;", check=False)
        check(r.returncode != 0, "a decided receipt cannot be recorded again")

        boundary = f"zb1-t{last_ms_prev_month}-c1-t-7"
        claim([boundary], extra=finish([boundary]))
        r = claim([boundary])
        where = sql(f"SELECT tableoid::regclass FROM zebridge_private.receipts WHERE message_id = '{boundary}'").stdout.strip()
        check(statuses(r) == ["duplicate"] and where.endswith((this_month - datetime.timedelta(days=1)).strftime("%Y_%m")),
              "an ID from the last millisecond of last month stays in last month's partition when retried")
        legacy = "c-1a2b-users-8-v"
        claim([legacy], extra=finish([legacy]))
        r = claim([legacy])
        where = sql(f"SELECT tableoid::regclass FROM zebridge_private.receipts WHERE message_id = '{legacy}'").stdout.strip()
        check(statuses(r) == ["duplicate"] and where.endswith("receipts_legacy"), "a legacy ID is deduplicated in the sentinel partition")
        r = claim([new_id], principal="bob", extra=finish([new_id], principal="bob"))
        check(statuses(r) == ["claimed"], "receipts are per principal: another principal's identical ID is its own")

        # Concurrent duplicate: A claims and holds; B's claim waits on A's index entry,
        # then sees A's committed outcome. B must not execute.
        racing = f"zb1-t{ms(now)}-c1-t-9"
        ids, route, payload = f"ARRAY['{racing}']::text[]", "ARRAY[convert_to('r','UTF8')]::bytea[]", "ARRAY[convert_to('p','UTF8')]::bytea[]"
        a_proc = subprocess.Popen([*base, "-d", name], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        lock = f"SELECT zebridge_private.lock_receipts(ARRAY['alice']::text[], {ids});"
        fps = "ARRAY[sha256(int4send(1) || convert_to('r','UTF8') || convert_to('p','UTF8'))]::bytea[]"
        close = " SELECT zebridge_private.close_guard();" if GUARD else ""
        a_proc.stdin.write(f"SET ROLE {writer}; BEGIN; {lock} SELECT status FROM zebridge_private.lookup_receipts('alice', {ids}, {route}, {payload}); "
                           f"SELECT pg_sleep(2); SELECT zebridge_private.record_receipts('alice', {ids}, {fps}, ARRAY['{{\"by\":\"A\"}}'::jsonb]);{close} COMMIT;")
        a_proc.stdin.close()
        time.sleep(.7)
        started = time.monotonic()
        b_out = sql(f"SET ROLE {writer}; BEGIN; {lock} SELECT status || '|' || coalesce(outcome::text, '') FROM zebridge_private.lookup_receipts("
                    f"'alice', {ids}, {route}, {payload}); COMMIT;", check=False)
        waited = time.monotonic() - started
        a_out, _ = a_proc.communicate(timeout=30)
        check(a_out.split()[-1] == "claimed" and b_out.stdout.strip() == 'duplicate|{"by": "A"}' and waited > .8,
              f"a concurrent duplicate waits on the winner's lock and returns its outcome ({waited:.1f} s)")

        floor = lambda: int(sql("SELECT min_accepted_ms FROM zebridge_private.receipt_policy").stdout)
        before = floor()
        check(sql(f"UPDATE zebridge_private.receipt_policy SET min_accepted_ms = {before - 1}", check=False).returncode != 0
              and sql("DELETE FROM zebridge_private.receipt_policy", check=False).returncode != 0,
              "the minimum accepted time never falls, and the policy row cannot be deleted")
        sql("UPDATE zebridge_private.receipt_policy SET retention_days = 30; SELECT zebridge_private.maintain_receipts();")
        raised = floor()
        sql("UPDATE zebridge_private.receipt_policy SET retention_days = 180; SELECT zebridge_private.maintain_receipts();")
        check(raised > before + 100 * 86400000 and floor() == raised,
              "a shorter retention raises the minimum; restoring 180 days does not lower it again")

        events = lambda: sql("SELECT count(*) FROM public.zebridge_ddl_events").stdout.strip()
        n = events()
        nxt = "receipts_" + (this_month + datetime.timedelta(days=32)).strftime("%Y_%m")
        sql(f"DROP TABLE zebridge_private.{nxt}")
        created = sql("SELECT * FROM zebridge_private.maintain_receipts()").stdout.split()
        check(created == [nxt] and events() == n, "creating or dropping a private partition announces nothing")

        for statement, label in (
            (f"SET ROLE {reader}; SELECT count(*) FROM zebridge_private.receipts", "the reader cannot read receipts"),
            (f"SET ROLE {reader}; SELECT zebridge_private.maintain_receipts()", "the reader cannot run receipt functions"),
            (f"SET ROLE {writer}; SELECT count(*) FROM zebridge_private.receipts", "the writer cannot read receipts directly"),
            (f"SET ROLE {writer}; SELECT count(*) FROM zebridge_private.{nxt}", "the writer cannot address a partition directly"),
            (f"SET ROLE {writer}; INSERT INTO zebridge_private.receipts VALUES ('alice', 'c-y', '1900-01-01', sha256('y'), '{{}}')",
             "the writer cannot insert a receipt except through record_receipts"),
            (f"SET ROLE {writer}; SELECT zebridge_private.prune_receipts()", "the writer cannot prune"),
            ("SELECT * FROM zebridge_grant_edge_writes('zebridge_private.receipts'::regclass)", "receipts cannot be opened to edge writes"),
            (f"SELECT * FROM zebridge_grant_edge_writes('zebridge_private.{nxt}'::regclass)", "nor can one of their partitions"),
        ):
            check(sql(statement, check=False).returncode != 0, label)

        pg_bin = pathlib.Path(zb._psql_binary()).parent
        dump = subprocess.run([str(pg_bin / "pg_dump"), "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-Fc", name], capture_output=True, check=True).stdout
        listing = subprocess.run([str(pg_bin / "pg_restore"), "-l"], input=dump, capture_output=True, check=True).stdout.decode()
        check("TABLE DATA zebridge_private receipts_legacy" in listing and "FUNCTION zebridge_private lookup_receipts" in listing
              and "TABLE DATA zebridge_private receipt_policy" in listing, "a full pg_dump carries the private schema, its partitions and policy")

        sql(f"UPDATE zebridge_private.receipt_policy SET min_accepted_ms = {ms(this_month)}")
        pruned = sql("SELECT * FROM zebridge_private.prune_receipts()").stdout.split()
        left = sql("SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                   "WHERE i.inhparent = 'zebridge_private.receipts'::regclass ORDER BY 1").stdout.split()
        check(pruned and all(p < "receipts_" + this_month.strftime("%Y_%m") for p in pruned)
              and "receipts_legacy" in left and "receipts_" + this_month.strftime("%Y_%m") in left,
              f"pruning drops only whole months before the minimum, never legacy ({len(pruned)} dropped)")

        # The receipt is looked up before the time checks (last: the minimum only rises).
        kept, late = f"zb1-t{ms(now)}-c1-t-k", f"zb1-t{ms(now)}-c1-t-late"
        claim([kept], extra=finish([kept]))
        sql(f"UPDATE zebridge_private.receipt_policy SET min_accepted_ms = {ms(now) + 1000}")
        r = claim([kept, late])
        check(statuses(r) == ["duplicate", "expired"] and '{"ok": 1}' in r.stdout,
              "past the minimum, an ID with a receipt still gets its recorded outcome; one without is expired")
        ahead = f"zb1-t{ms(now + datetime.timedelta(minutes=10))}-c1-t-ahead"
        sql(f"INSERT INTO zebridge_private.receipts VALUES ('alice', '{ahead}', zebridge_private.receipt_month('{ahead}'), "
            "sha256(int4send(length(convert_to('mutation.alice.t.insert','UTF8'))) || convert_to('mutation.alice.t.insert','UTF8') "
            "|| convert_to('p','UTF8')), '{\"ok\": 2}')")
        r = claim([ahead, f"zb1-t{ms(now + datetime.timedelta(minutes=10))}-c1-t-ahead2"])
        check(statuses(r) == ["duplicate", "future"],
              "a future-dated ID with a receipt (a clock stepped back) gets its outcome; one without is future")
    finally:
        sql(f'DROP DATABASE "{name}" WITH (FORCE)', "postgres")
        for role in (reader, writer):
            sql(f'DROP ROLE IF EXISTS "{role}"', "postgres")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

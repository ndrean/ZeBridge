#!/usr/bin/env python3
"""Variant B's completion guard, driven in SQL as the writer role (receipts-locks prototype).

A disposable database gets `bridge --init-sql` and the guard switched on. Each case is one
transaction through psql, run as the writer: the guard must hold the exact set of undecided
(principal, ID, fingerprint) entries from the lookup, refuse a COMMIT that skipped the
receipts or close_guard, refuse a recorded set that differs in identity (not only in count),
exclude duplicates, survive ROLLBACK TO SAVEPOINT, and stay out of the writer's hands.
"""
import os, pathlib, subprocess, time, uuid

import zb

BRIDGE = pathlib.Path(os.environ.get("ZB_BRIDGE_BIN", zb.ROOT / "zig-out/bin/bridge"))


def main():
    psql = [zb._psql_binary(), "-X", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-Atq"]
    name = "zb_rg_" + uuid.uuid4().hex[:8]
    reader, writer = name + "_r", name + "_w"
    failed = 0

    def run(statement, db=name, role=None):
        if role:
            statement = f'SET ROLE "{role}";\n' + statement
        return subprocess.run([*psql, "-d", db], input=statement, capture_output=True, text=True)

    def check(ok, label):
        nonlocal failed
        (zb.ok if ok else zb.bad)(label)
        failed += not ok

    def ident(n):
        return f"zb1-t{int(time.time() * 1000)}-INSERT-c1-t-k1-{n}-v{uuid.uuid4().hex[:8]}"

    def lookup(ids, body=b"x"):
        arr = ",".join(f"'{i}'" for i in ids)
        return (f"SELECT string_agg(status || '=' || encode(fingerprint, 'hex'), ' ' ORDER BY ord) FROM zebridge_private.lookup_receipts("
                f"'alice', ARRAY[{arr}], ARRAY[{','.join(['convert_to(%s,%s)' % (repr('r'), repr('UTF8'))] * len(ids))}]::bytea[], "
                f"ARRAY[{','.join(['convert_to(%s,%s)' % (repr(body.decode()), repr('UTF8'))] * len(ids))}]::bytea[]);\n")

    def lock(ids):
        arr = ",".join(f"'{i}'" for i in ids)
        return f"SELECT zebridge_private.lock_receipts(ARRAY[{','.join(["'alice'"] * len(ids))}], ARRAY[{arr}]);\n"

    def record(ids, fp_sql):
        arr = ",".join(f"'{i}'" for i in ids)
        return (f"SELECT zebridge_private.record_receipts('alice', ARRAY[{arr}], ARRAY[{','.join([fp_sql] * len(ids))}]::bytea[], "
                f"ARRAY[{','.join(["'{\"status\":\"accepted\"}'"] * len(ids))}]::jsonb[]);\n")

    fp = "sha256(int4send(1) || convert_to('r','UTF8') || convert_to('x','UTF8'))"
    wrong_fp = "sha256(convert_to('other','UTF8'))"
    close = "SELECT zebridge_private.close_guard();\n"

    def tx(body):
        return run("BEGIN;\n" + body + "COMMIT;\n", role=writer)

    try:
        r = run(f'CREATE DATABASE "{name}"', "postgres")
        assert r.returncode == 0, r.stderr
        env = dict(os.environ, DATABASE_READER_URL=f"postgres://{reader}:x@127.0.0.1:5432/{name}",
                   DATABASE_WRITER_URL=f"postgres://{writer}:x@127.0.0.1:5432/{name}", TARGET_DB=name, BRIDGE_CDC_PUBLICATION="rg_pub")
        r = run(subprocess.run([str(BRIDGE), "--init-sql"], env=env, capture_output=True, text=True, check=True).stdout)
        assert r.returncode == 0, r.stderr
        r = run("UPDATE zebridge_private.receipt_policy SET completion_guard = true; SELECT string_agg(p, ',') FROM zebridge_private.maintain_receipts() p;")
        assert r.returncode == 0, r.stderr

        x, y, z = ident("X"), ident("Y"), ident("Z")
        r = tx(lock([x, y]) + lookup([x, y]) + record([x, y], fp) + close)
        check(r.returncode == 0, "the exact undecided set recorded and closed: COMMIT passes")

        r = tx(lock([x]) + lookup([x]))
        check(r.returncode == 0, "a duplicate is excluded: its lookup opens no guard and COMMIT passes without close_guard")

        a, b = ident("A"), ident("B")
        r = tx(lock([a, b]) + lookup([a, b]) + record([a, z], fp) + close)
        check(r.returncode != 0 and "not the undecided writes" in r.stderr,
              "same count, different identity (A,Z for A,B): close_guard refuses")
        r = tx(lock([a]) + lookup([a]) + record([a], wrong_fp) + close)
        check(r.returncode != 0 and "not the undecided writes" in r.stderr, "same ID, different fingerprint: close_guard refuses")

        r = tx(lock([a]) + lookup([a]))
        check(r.returncode != 0 and "would commit without their receipts" in r.stderr, "COMMIT with no receipt: the deferred guard check refuses it")
        r = tx(lock([a]) + lookup([a]) + record([a], fp))
        check(r.returncode != 0 and "would commit without their receipts" in r.stderr, "receipts recorded but close_guard skipped: COMMIT refused")
        r = tx(lock([a]) + lookup([a]) + "SET CONSTRAINTS ALL IMMEDIATE;\n")
        check(r.returncode != 0 and "would commit without their receipts" in r.stderr, "SET CONSTRAINTS ALL IMMEDIATE already refuses an open guard")
        r = run(f"SELECT count(*) FROM zebridge_private.receipts WHERE message_id = '{a}'")
        check(r.stdout.strip() == "0", "none of the refused transactions left a receipt for A")

        r = tx(lock([a]) + lookup([a]) + "SAVEPOINT zb_write;\nROLLBACK TO SAVEPOINT zb_write;\n" + record([a], fp) + close)
        check(r.returncode == 0, "the guard, taken before the savepoint, survives ROLLBACK TO SAVEPOINT (rejection recovery)")

        r = tx(lock([b]) + lookup([b]) + f"DELETE FROM zebridge_private.receipt_guards;\n")
        check(r.returncode != 0 and "permission denied" in r.stderr, "the writer cannot delete its guard row directly")
        r = run("UPDATE zebridge_private.receipt_policy SET completion_guard = false;", role=writer)
        check(r.returncode != 0 and "permission denied" in r.stderr, "the writer cannot switch the guard off")
        r = run("UPDATE zebridge_private.receipt_policy SET completion_guard = false;")
        check(r.returncode != 0 and "never switched off" in r.stderr, "the owner cannot switch it off either (one-way)")

        r = run("DO $$ BEGIN PERFORM zebridge_private.close_guard(); EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'state=%', SQLSTATE; END $$;", role=writer)
        check("state=ZBG01" in r.stderr, "guard errors carry SQLSTATE ZBG01, never class 23")
        r = run("SELECT count(*) FROM zebridge_private.receipt_guards")
        check(r.stdout.strip() == "0", "no guard row outlives its transaction")
    finally:
        run(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE); DROP ROLE IF EXISTS "{reader}"; DROP ROLE IF EXISTS "{writer}";', "postgres")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

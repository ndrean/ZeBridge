#!/usr/bin/env python3
"""Completion-guard defaults, policy-preserving upgrades and startup compatibility.

Uses only stage 2's disposable PostgreSQL/NATS stack. ZB_BRIDGE_BIN must be a normal
build. ZB_UNGUARDED_BRIDGE_BIN optionally names an explicit -Dreceipt-guard=false
build, so the same scenario also verifies unguarded defaults, warning and refusal against a guarded database.
Neither init SQL nor environment overrides may silently switch an existing policy.
"""
import asyncio, os, pathlib, subprocess, tempfile

import zb
from hub_fallback_stage2 import BRIDGE, GUARD, Stack, port


async def main():
    assert GUARD, "receipt_boot exercises the default completion guard; unset ZB_RECEIPT_GUARD"
    unguarded = os.environ.get("ZB_UNGUARDED_BRIDGE_BIN")
    passed = failed = 0

    def check(ok, label):
        nonlocal passed, failed
        (zb.ok if ok else zb.bad)(label)
        passed += bool(ok)
        failed += not ok

    with tempfile.TemporaryDirectory(prefix="zb-receipt-boot-") as folder:
        hub = Stack(pathlib.Path(folder), lanes=1)
        try:
            await hub.start()

            def stop_bridge():
                p = hub.procs["bridge"]
                if p.poll() is None:
                    p.terminate()
                    p.wait(timeout=15)

            def render(binary, override):
                # --init-sql must follow the compiled build, even if the external
                # envsubst helper's variable has the opposite value.
                return subprocess.run([str(binary), "--init-sql"], env=dict(hub.env, RECEIPT_COMPLETION_GUARD=override),
                                      capture_output=True, text=True, check=True).stdout

            install = render(BRIDGE, "false")

            async def boot(binary=BRIDGE, error=None, warning=None):
                stop_bridge()
                hub.spawn("bridge", [str(binary), "--pub", "hf2_pub", "--slot", hub.name + "_slot", "--port", str(port())], hub.env)
                p = hub.procs["bridge"]
                if error:
                    await asyncio.wait_for(asyncio.to_thread(p.wait), timeout=45)
                    log = hub.log("bridge")
                    check(p.returncode == 1 and error in log and "Ready! Pulling mutations" not in log,
                          f"startup refuses before lanes: {error}")
                else:
                    await hub.wait_log("bridge", "Ready! Pulling mutations")
                    check(p.poll() is None and (not warning or warning in hub.log("bridge")),
                          "startup reaches the mutation lane" + (f" with {warning}" if warning else ""))
                stop_bridge()

            check(hub.sql("SELECT completion_guard FROM zebridge_private.receipt_policy") == "t",
                  "normal build and fresh installation enable the guard without overrides")
            check(int(hub.sql("SELECT count(*) FROM pg_inherits WHERE inhparent = 'zebridge_private.receipts'::regclass")) > 1,
                  "normal startup created monthly partitions and the legacy partition")
            stop_bridge()
            hub.sql("UPDATE zebridge_private.receipt_policy SET retention_days = 200; "
                    "INSERT INTO zebridge_private.receipts(principal, message_id, id_month, fingerprint, outcome) "
                    "VALUES ('alice', 'legacy-finalize-b', '1900-01-01', sha256(int4send(1) || convert_to('rx','UTF8')), "
                    "'{\"status\":\"accepted\",\"version\":\"saved\"}');")
            policy = hub.sql("SELECT row_to_json(p) FROM zebridge_private.receipt_policy p")
            receipt = hub.sql("SELECT row_to_json(r) FROM zebridge_private.receipts r")
            hub.sql("BEGIN;\n" + install + "\nCOMMIT;")
            check(hub.sql("SELECT row_to_json(p) FROM zebridge_private.receipt_policy p") == policy
                  and hub.sql("SELECT row_to_json(r) FROM zebridge_private.receipts r") == receipt,
                  "reapplying SQL preserves the exact policy, retry minimum and receipt")
            check(hub.sql("BEGIN; SELECT zebridge_private.lock_receipts(ARRAY['alice'], ARRAY['legacy-finalize-b']); "
                          "SELECT status || ':' || (outcome->>'version') FROM zebridge_private.lookup_receipts("
                          "ARRAY['alice'], ARRAY['legacy-finalize-b'], ARRAY[convert_to('r','UTF8')], ARRAY[convert_to('x','UTF8')]); COMMIT;")
                  .endswith("duplicate:saved"), "the preserved receipt still answers a retry")
            await boot()

            writer = hub.name + "_w"
            for privilege in (f'REVOKE USAGE ON SCHEMA zebridge_private FROM "{writer}"',
                              f'REVOKE EXECUTE ON FUNCTION zebridge_private.record_receipts(text[],text[],bytea[],jsonb[]) FROM "{writer}"'):
                hub.sql(privilege)
                await boot(error="writer role may not use the mutation receipts")
                hub.sql(install)

            if unguarded:
                await boot(unguarded, error="requires false")
                hub.sql(render(unguarded, "true"))
                check(hub.sql("SELECT row_to_json(p) FROM zebridge_private.receipt_policy p") == policy,
                      "unguarded init SQL cannot downgrade or reset an enabled policy")

            # Model the actual pre-guard policy shape, not an UPDATE that the
            # one-way trigger correctly refuses. Only our disposable schema is reset.
            hub.sql("DROP SCHEMA zebridge_private CASCADE")
            await boot(error="mutation receipts are not installed")
            hub.sql("CREATE SCHEMA zebridge_private; CREATE TABLE zebridge_private.receipt_policy ("
                    "singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), retention_days integer NOT NULL DEFAULT 180, "
                    "min_accepted_ms bigint NOT NULL DEFAULT 0); "
                    "INSERT INTO zebridge_private.receipt_policy VALUES (true, 200, 123456789);")
            hub.sql(install)
            check(hub.sql("SELECT retention_days, min_accepted_ms, completion_guard FROM zebridge_private.receipt_policy") == "200|123456789|f",
                  "upgrading a pre-guard policy preserves its values and leaves the guard disabled")
            await boot(error="requires true")
            check("After draining and stopping old workers" in hub.log("bridge"),
                  "a disabled-policy refusal gives the explicit migration instruction")
            hub.sql("UPDATE zebridge_private.receipt_policy SET completion_guard = true WHERE singleton")
            await boot()
            check(int(hub.sql("SELECT min_accepted_ms FROM zebridge_private.receipt_policy")) >= 123456789,
                  "startup maintenance never lowers the restored retry minimum")
            r = subprocess.run([*hub.psql, "-d", hub.name], capture_output=True, text=True,
                               input="UPDATE zebridge_private.receipt_policy SET completion_guard = false;")
            check(r.returncode != 0 and "never switched off" in r.stderr, "administrator enablement of the guard is one-way")

            if unguarded:
                hub.sql("DROP SCHEMA zebridge_private CASCADE")
                hub.sql(render(unguarded, "true"))
                check(hub.sql("SELECT completion_guard FROM zebridge_private.receipt_policy") == "f",
                      "an unguarded build renders matching fresh SQL")
                await boot(unguarded, warning="Receipt completion guard disabled")
                hub.sql(install)
                check(hub.sql("SELECT completion_guard FROM zebridge_private.receipt_policy") == "f",
                      "guard-enabled init SQL preserves an existing disabled policy")
                await boot(error="requires true")
                hub.sql("UPDATE zebridge_private.receipt_policy SET completion_guard = true WHERE singleton")
                await boot()
            else:
                print("  ◇ unguarded-build compatibility checks skipped: set ZB_UNGUARDED_BRIDGE_BIN")
        finally:
            hub.stop()
    print(f"\nreceipt_boot: {passed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

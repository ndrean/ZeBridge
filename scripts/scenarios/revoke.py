"""Revocation, the KV half: deleting a principal's mapping purges its `$KV.tenants`
key LIVE (NOTES §10ce).

    scripts/scenarios/run.py live -k revoke

`DELETE FROM zebridge_user_tenants` rides the WAL like any row, and the bridge routes
it to `purgeTenantKey` — the principal's `$KV.tenants.<principal>` key is deleted, so
the next `resolveTenant()` reads "no mapping" and the client falls to the open tenant.
That path had NEVER run in a test (a stale comment still called it a deferred gap).

Scope, stated honestly: this pins the KV lifecycle (mapping → key appears; revocation →
key purged) and nothing more. Write-refusal for an unmapped principal is the guard
scenarios' territory, and READ revocation is the JWT TTL's — a revoked-but-hostile
client holding old creds still connects and reads its old tenant's CDC until expiry,
because NATS authorizes from the signed JWT alone, never from this bucket.
"""
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402

PRINCIPAL = "revoke_probe"
TENANT = "kilo"                # an existing tenant: the autogrant row it triggers is a no-op


def kv_key() -> str:
    r = zb.nats_cli("kv", "get", zb.TOPOLOGY["kv"]["tenants"], PRINCIPAL, "--raw")
    return r.stdout.strip() if r.returncode == 0 else ""


def main() -> int:
    failed = 0
    try:
        zb.psql(f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{PRINCIPAL}'", quiet=True)
        zb.psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) "
                f"VALUES ('{PRINCIPAL}', '{TENANT}')")
        deadline = time.time() + 20
        while time.time() < deadline and kv_key() != TENANT:
            time.sleep(0.5)
        if kv_key() == TENANT:
            zb.ok(f"mapping projected live: $KV.tenants.{PRINCIPAL} → '{TENANT}'")
        else:
            zb.bad(f"the mapping never reached KV (got {kv_key()!r})"); return 1

        zb.psql(f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{PRINCIPAL}'")
        deadline = time.time() + 20
        while time.time() < deadline and kv_key() != "":
            time.sleep(0.5)
        if kv_key() == "":
            zb.ok("revocation purged the key LIVE — the next resolveTenant() reads \"no "
                  "mapping\", the same state a principal that never existed has")
        else:
            zb.bad(f"the key SURVIVED the revocation (still {kv_key()!r}) — a revoked "
                   "principal keeps resolving its old tenant on every reconnect")
            failed += 1
    finally:
        zb.psql(f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{PRINCIPAL}'", quiet=True)
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

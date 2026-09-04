"""Revocation, driven by the operator command: `bridge --revoke` closes the mapping,
voids the way back in, and the live bridge purges `$KV.tenants` (NOTES §10ce, §10ck).

    scripts/scenarios/run.py live -k revoke

`ADMIN_DATABASE_URL=… bridge --revoke <principal>` is the RDS-friendly revocation
(review: nobody should hunt for the right psql at the worst moment). It deletes the
mapping AND the principal's UNUSED invites — the ticket raw-psql revocations forget —
and narrates the three clocks: writes die NOW, resolution at the next connect (the KV
purge riding this delete's WAL), reads at JWT expiry. Scope stated honestly, as
before: the KV lifecycle and the invite void are pinned here; write-refusal is the
guard scenarios' territory, read expiry is jwt_expiry.py's.
"""
import os
import pathlib
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402

BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
PRINCIPAL = "revoke_probe"
TENANT = "kilo"                # an existing tenant: the autogrant row it triggers is a no-op
ADMIN_URL = "postgres://postgres@127.0.0.1:5432/postgres"


def kv_key() -> str:
    r = zb.nats_cli("kv", "get", zb.TOPOLOGY["kv"]["tenants"], PRINCIPAL, "--raw")
    return r.stdout.strip() if r.returncode == 0 else ""


def revoke(env_url: str | None):
    env = dict(os.environ)
    env.pop("ADMIN_DATABASE_URL", None)
    if env_url:
        env["ADMIN_DATABASE_URL"] = env_url
    return subprocess.run([str(BRIDGE), "--revoke", PRINCIPAL], env=env,
                          capture_output=True, text=True, timeout=30)


def main() -> int:
    failed = 0
    try:
        zb.psql(f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{PRINCIPAL}'", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_invites WHERE principal = '{PRINCIPAL}'", quiet=True)
        zb.psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) "
                f"VALUES ('{PRINCIPAL}', '{TENANT}')")
        zb.psql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id, role, expires_at) "
                f"VALUES ('{os.urandom(16).hex()}', '{PRINCIPAL}', '{TENANT}', 'client', now() + interval '1 hour')")
        deadline = time.time() + 20
        while time.time() < deadline and kv_key() != TENANT:
            time.sleep(0.5)
        if kv_key() == TENANT:
            zb.ok(f"mapping projected live: $KV.tenants.{PRINCIPAL} → '{TENANT}' (and one unused invite waits)")
        else:
            zb.bad(f"the mapping never reached KV (got {kv_key()!r})"); return 1

        # ── the capability must be non-ambient ───────────────────────────────
        r = revoke(None)
        if r.returncode != 0 and "ADMIN_DATABASE_URL" in r.stdout + r.stderr:
            zb.ok("without ADMIN_DATABASE_URL the command refuses and says why — the "
                  "capability lives in the invocation, never in the bridge's env")
        else:
            zb.bad(f"revoke without the admin URL did not refuse (rc={r.returncode})"); failed += 1

        # ── the real revocation ──────────────────────────────────────────────
        r = revoke(ADMIN_URL)
        outp = r.stdout + r.stderr
        if r.returncode == 0 and "1 mapping(s)" in outp and "1 unused invite(s)" in outp:
            zb.ok("bridge --revoke: 1 mapping removed AND 1 unused invite voided — the "
                  "ticket back in is torn up with the mapping, one command")
        else:
            zb.bad(f"revoke failed or missed something (rc={r.returncode}): {outp[:160]}"); failed += 1
        if "refused as of NOW" in outp and "JWT EXPIRES" in outp:
            zb.ok("and the three clocks are narrated where the operator is looking")
        else:
            zb.bad("the three-clock narration is missing from the output"); failed += 1

        invites = zb.psql(f"SELECT count(*) FROM public.zebridge_invites WHERE principal = '{PRINCIPAL}'").strip()
        deadline = time.time() + 20
        while time.time() + 0 < deadline and kv_key() != "":
            time.sleep(0.5)
        if kv_key() == "" and invites == "0":
            zb.ok("downstream: the live bridge purged the KV key, and no invite survives — "
                  "the next resolveTenant() reads \"no mapping\"")
        else:
            zb.bad(f"residue after revoke (kv={kv_key()!r}, invites={invites})"); failed += 1

        # ── revoking a ghost says so ─────────────────────────────────────────
        r = revoke(ADMIN_URL)
        if r.returncode == 1 and "nothing to revoke" in r.stdout + r.stderr:
            zb.ok("revoking again exits 1 with 'nothing to revoke' — scripts can tell")
        else:
            zb.bad(f"double-revoke was not distinguishable (rc={r.returncode})"); failed += 1
    finally:
        zb.psql(f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{PRINCIPAL}'", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_invites WHERE principal = '{PRINCIPAL}'", quiet=True)
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

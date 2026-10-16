#!/usr/bin/env python3
"""`bridge --invite` and the zebridge_inviter role: invites without the admin.

A login granted zebridge_inviter (an app's backend) may create invites through
public.zebridge_invite() and do nothing else:

  A  `bridge --invite` prints the code alone on stdout; the row has the principal, the
     tenant, the asked lifetime, and is unused;
  B  without --expires, the invite lasts one day;
  C  refused, with nothing on stdout: a lifetime over 30 days or not positive, a principal
     or tenant the tables refuse, a revoked principal;
  D  a login without the grant is refused, and told which GRANT it lacks;
  E  the inviter cannot read, change or delete invites, nor touch the roster;
  F  without INVITER_DATABASE_URL the command says how to pass it.

Needs the dev database with the init SQL applied (zebridge_invite) and the bridge built;
the bridge itself may be running or not.

Usage:  scripts/scenarios/.venv/bin/python scripts/scenarios/invite_cli.py
"""
import os, re, secrets, subprocess, sys

import zb

BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
HOST = os.environ.get("ZB_PG_HOST", "127.0.0.1")
PORT = os.environ.get("ZB_PG_PORT", "5432")
WHO = "zz-inv"            # every principal here starts with it: the cleanup's key
INVITER = "zz_inviter_scn"  # a login granted zebridge_inviter
STRANGER = "zz_noinvite_scn"  # a login without it


def url(role: str, password: str) -> str:
    return f"postgres://{role}:{password}@{HOST}:{PORT}/postgres"


def invite(env_url: str | None, *args: str) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k != "INVITER_DATABASE_URL"}
    if env_url:
        env["INVITER_DATABASE_URL"] = env_url
    return subprocess.run([str(BRIDGE), "--invite", *args], capture_output=True, text=True, timeout=30, env=env)


def as_role(env_url: str, sql: str) -> subprocess.CompletedProcess:
    return subprocess.run([zb._psql_binary(), env_url, "-XAtc", sql], capture_output=True, text=True, timeout=30)


def cleanup():
    zb.psql(f"DELETE FROM public.zebridge_invites WHERE principal LIKE '{WHO}%'", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_principal_keys WHERE principal LIKE '{WHO}%'", quiet=True)
    for role in (INVITER, STRANGER):
        zb.psql(f"DROP ROLE IF EXISTS {role}", quiet=True)


def main() -> int:
    failed = 0

    def check(good, label):
        nonlocal failed
        (zb.ok if good else zb.bad)(label)
        failed += 0 if good else 1

    if zb.psql("SELECT count(*) FROM pg_proc WHERE proname = 'zebridge_invite'", quiet=True).strip() != "1":
        zb.bad("zebridge_invite() is missing: apply the init SQL first")
        return 1
    cleanup()
    pw_inviter, pw_stranger = secrets.token_hex(16), secrets.token_hex(16)
    try:
        zb.psql(f"CREATE ROLE {INVITER} LOGIN PASSWORD '{pw_inviter}'; GRANT zebridge_inviter TO {INVITER}", quiet=True)
        zb.psql(f"CREATE ROLE {STRANGER} LOGIN PASSWORD '{pw_stranger}'", quiet=True)
        good_url, stranger_url = url(INVITER, pw_inviter), url(STRANGER, pw_stranger)

        # A — one invite, one hour
        r = invite(good_url, "--principal", f"{WHO}-alice", "--tenant", "acme", "--expires", "1 hour")
        code = r.stdout.strip()
        row = zb.psql(f"SELECT principal, tenant_id, round(extract(epoch FROM expires_at - created_at)), used_at IS NULL "
                      f"FROM public.zebridge_invites WHERE code = '{code}'", quiet=True).strip() if re.fullmatch(r"[0-9a-f]{32}", code) else ""
        check(r.returncode == 0 and r.stdout.count("\n") == 1 and row == f"{WHO}-alice|acme|3600|t",
              f"A the code alone on stdout; the row: principal, tenant, 1 hour, unused ({row or r.stderr.strip()[:120]})")

        # B — the default lifetime
        r = invite(good_url, "--principal", f"{WHO}-bob", "--tenant", "acme")
        life = zb.psql(f"SELECT round(extract(epoch FROM expires_at - created_at)) FROM public.zebridge_invites "
                       f"WHERE code = '{r.stdout.strip()}'", quiet=True).strip()
        check(r.returncode == 0 and life == "86400", f"B no --expires: one day ({life})")

        # C — what the function and the tables refuse
        zb.psql(f"INSERT INTO public.zebridge_principal_keys (user_pubkey, principal, tenant_id, revoked_at) "
                f"VALUES ('UZZINVITESCENARIO{secrets.token_hex(8).upper()}', '{WHO}-gone', 'acme', now())", quiet=True)
        refusals = {
            "31 days": (["--principal", f"{WHO}-carol", "--tenant", "acme", "--expires", "31 days"], "at most 30 days"),
            "0 seconds": (["--principal", f"{WHO}-carol", "--tenant", "acme", "--expires", "0 seconds"], "more than 0"),
            "principal a.b": (["--principal", f"{WHO}.carol", "--tenant", "acme"], "letters, digits"),
            "tenant with a space": (["--principal", f"{WHO}-carol", "--tenant", "ac me"], "no '.', '*', '>' or space"),
            "revoked principal": (["--principal", f"{WHO}-gone", "--tenant", "acme"], "revoked"),
        }
        for label, (args, words) in refusals.items():
            r = invite(good_url, *args)
            check(r.returncode == 1 and r.stdout == "" and words in r.stderr,
                  f"C refused: {label} ({r.stderr.strip().splitlines()[0][:100] if r.stderr.strip() else 'no message'})")
        made = zb.psql(f"SELECT count(*) FROM public.zebridge_invites WHERE principal LIKE '{WHO}%'", quiet=True).strip()
        check(made == "2", f"C nothing refused was written ({made} invite(s), 2 expected)")

        # D — a login without the grant
        r = invite(stranger_url, "--principal", f"{WHO}-dan", "--tenant", "acme")
        check(r.returncode == 1 and "GRANT zebridge_inviter" in r.stderr, f"D a login without the grant: told what it lacks")

        # E — nothing else
        for sql in ("SELECT count(*) FROM public.zebridge_invites",
                    f"UPDATE public.zebridge_invites SET used_at = NULL WHERE principal = '{WHO}-alice'",
                    f"DELETE FROM public.zebridge_invites WHERE principal = '{WHO}-alice'",
                    f"INSERT INTO public.zebridge_invites (principal, tenant_id) VALUES ('{WHO}-eve', 'acme')",
                    f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) VALUES ('{WHO}-eve', 'acme')"):
            r = as_role(good_url, sql)
            check(r.returncode != 0 and "permission denied" in r.stderr, f"E refused to the inviter: {sql.split(' WHERE')[0][:60]}")

        # F — no URL
        r = invite(None, "--principal", f"{WHO}-fay", "--tenant", "acme")
        check(r.returncode == 1 and "INVITER_DATABASE_URL is not set" in r.stderr, "F without INVITER_DATABASE_URL: how to pass it")
    finally:
        cleanup()
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())

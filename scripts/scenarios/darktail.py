#!/usr/bin/env python3
"""§10fq: one unreadable stream must not stall a client's other streams.
    scripts/scenarios/run.py live -k darktail     (live: the running bridge, its /enroll)

A throwaway principal in two tenants: `acme` and a throwaway tenant whose CDC stream
this scenario creates itself (the bridge untouched, as in dyntenant.py). The client
follows both; then the throwaway stream is DELETED under the live tail — what an
operator retiring a tenant does. Before §10fq the failed re-open aborted every poll.
Checks:
  1. both tails are open and polls succeed;
  2. after the deletion, a row written to acme still arrives, no poll errors, and no
     poll takes much longer than its wait;
  3. the poll report names the throwaway tenant as `unreadable`;
  4. the stream recreated, the tenant becomes readable again on its own (backoff).
No row is ever written to the throwaway tenant: a row published after its stream is
gone would stop the dev bridge (NOTES §10fn, the dyntenant hazard).
"""
import base64, json, os, pathlib, re, subprocess, sys, time, urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb
from clients import Lib

TABLE = "pois"
TA, TD = "acme", "dtail"
PRINCIPAL = "dt_" + os.urandom(3).hex()
TMP = pathlib.Path(os.environ.get("TMPDIR", "/tmp"))
DB = str(TMP / f"zb_darktail_{PRINCIPAL}.sqlite3")
STREAM = zb.TOPOLOGY["cdc_streams"]["tenant_prefix"] + TD
PREFIX = zb.TOPOLOGY["subjects"]["cdc_prefix"]


def sql(text):
    return zb.psql(text, quiet=True).strip()


def add_stream():
    r = zb.nats_cli("stream", "add", STREAM, f"--subjects={PREFIX}.{TD}.>", "--storage=file",
                    "--retention=limits", "--max-age=1h", "--replicas=1", "--defaults")
    if r.returncode != 0:
        sys.exit(f"could not create {STREAM}: {r.stderr.strip()}")


def enrol(tenant):
    code = os.urandom(16).hex()
    sql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id, expires_at) "
        f"VALUES ('{code}', '{PRINCIPAL}', '{tenant}', now() + interval '5 minutes')")
    gen = subprocess.run([str(zb.BRIDGE), "--gen-nkey"], capture_output=True, text=True)
    pub = re.search(r"NATS_BRIDGE_NKEY_PUB=(U[A-Z0-9]+)", gen.stdout).group(1)
    seed = re.search(r"NATS_BRIDGE_NKEY_SEED=(SU[A-Z0-9]+)", gen.stdout).group(1)
    with urllib.request.urlopen(zb.http_base() + f"/enroll?code={code}&user_pubkey={pub}", timeout=10) as r:
        jwt = json.loads(r.read().decode())["jwt"]
    path = TMP / f"zb_{PRINCIPAL}.creds"
    path.write_text("-----BEGIN NATS USER JWT-----\n" + jwt + "\n------END NATS USER JWT------\n\n"
                    "-----BEGIN USER NKEY SEED-----\n" + seed + "\n------END USER NKEY SEED------\n")
    return path


def cleanup(client):
    if client is not None:
        try: client.close()
        except Exception: pass
    sql(f"UPDATE public.{TABLE} SET deleted_at = now() WHERE note LIKE 'dt-%' AND deleted_at IS NULL")
    sql(f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{PRINCIPAL}' OR (principal = 'zb_sweeper' AND tenant_id = '{TD}')")
    sql(f"DELETE FROM public.zebridge_principal_keys WHERE principal = '{PRINCIPAL}'")
    sql(f"DELETE FROM public.zebridge_invites WHERE principal = '{PRINCIPAL}'")
    zb.nats_cli("kv", "purge", zb.kv_bucket("tenants"), PRINCIPAL, "-f")
    zb.nats_cli("stream", "rm", STREAM, "-f")
    for p in (DB, DB + "-wal", DB + "-shm", str(TMP / f"zb_{PRINCIPAL}.creds")):
        pathlib.Path(p).unlink(missing_ok=True)


def timed_poll(c, wait_ms=300):
    t0 = time.monotonic()
    r = c.poll(wait_ms)
    return r, time.monotonic() - t0


def main():
    failed = 0
    client = None
    if zb.nats_cli("stream", "info", STREAM).returncode == 0:
        sys.exit(f"{STREAM} exists — a previous run was interrupted; remove it first")
    try:
        add_stream()
        sql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) VALUES ('{PRINCIPAL}', '{TA}'), ('{PRINCIPAL}', '{TD}')")
        creds = enrol(TA)
        client = c = Lib(DB, [TABLE], client_id="py-darktail", principal=PRINCIPAL, creds=creds)
        errs = [r for r in (c.poll(300) for _ in range(6)) if "error" in r]
        streams = {x[0] for x in c.q("SELECT stream FROM _zbz_stream_seq")}
        if c.tenants == [TA, TD] and not errs:
            zb.ok(f"following {c.tenants}: polls succeed (streams positioned: {sorted(s for s in streams if s.startswith('CDC_'))})")
        else:
            zb.bad(f"setup: tenants={c.tenants}, poll errors={errs[:2]}"); failed += 1

        # ── the throwaway tenant's stream deleted under the live tail ───────────
        zb.nats_cli("stream", "rm", STREAM, "-f")
        # psql prints the RETURNING value, then the command tag: keep the first line.
        uid = sql(f"INSERT INTO public.{TABLE} (lat, lng, note, tenant_id) VALUES (47.2, -1.55, 'dt-after-delete', '{TA}') RETURNING uid").splitlines()[0]
        arrived, poll_errors, slowest, named = False, [], 0.0, []
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            r, dt = timed_poll(c, 300)
            slowest = max(slowest, dt)
            if "error" in r:
                poll_errors.append(r["error"])
            if r.get("unreadable"):
                named = r["unreadable"]
            if c.q("SELECT 1 FROM pois WHERE uid = ?", [uid]):
                arrived = True
                if named:
                    break
        if arrived and not poll_errors:
            zb.ok(f"{STREAM} deleted under the tail: acme's row still arrived, no poll failed (slowest poll {slowest:.1f} s)")
        else:
            zb.bad(f"after the delete: arrived={arrived}, poll errors={poll_errors[:3]}"); failed += 1
        if slowest < 6.5:
            zb.ok(f"no poll stalled: the slowest took {slowest:.1f} s for a 0.3 s wait (one stream-info request at most)")
        else:
            zb.bad(f"a poll took {slowest:.1f} s — the dark stream is still costing the others"); failed += 1
        if named == [TD]:
            zb.ok(f"the poll report names the tenant that went dark: unreadable = {named}")
        else:
            zb.bad(f"unreadable = {named!r}"); failed += 1

        # ── the stream back: readable again on its own ──────────────────────────
        add_stream()
        back = False
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            r, _ = timed_poll(c, 500)
            if "error" not in r and not r.get("unreadable"):
                back = True
                break
        if back:
            zb.ok(f"{STREAM} recreated: the tenant is readable again after the backoff, nothing to do for the host")
        else:
            zb.bad(f"{STREAM} recreated but still reported unreadable"); failed += 1
    finally:
        cleanup(client)
    return failed


if __name__ == "__main__":
    sys.exit(main())

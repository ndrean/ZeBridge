#!/usr/bin/env python3
"""A running client picks up its first private assignment, through its original handle.

Enrollment always creates a membership (removing the last one is revocation, another
contract), so P starts with NO PRIVATE ASSIGNMENT: enrolled in the open tenant only, it
follows the public tables. Two throwaway tenants hold distinct probe rows: A (the one P is
assigned) and B (never P's). The first private membership is added beside the open one.

  1. The ORIGINAL clients (libzb, zb-client-ts) connect with P's credentials and receive a
     public CDC change; neither A's nor B's probe is visible.
  2. P is assigned A in PostgreSQL while the original clients keep running; the harness
     never closes or reopens them (an internal reconnect with renewed credentials is the
     clients' business).
  3. A CONTROL connection with credentials from /renew for the same principal and key reads
     A's probe and later change; B's probe stays out of its reach.
  4. Through the original handles: A's probe, then a later private change, never B's probe;
     and the handles stay healthy (a later public change still arrives).

Exit status (it starts in `known_red`):
  1  the known defect: the control passes, nothing leaked, the original handles are healthy,
     and they never receive A's seed by the deadline;
  0  both original handles receive A's seed and the later private change, nothing leaks;
  2  setup incomplete (enrollment, renewal, the public change before the assignment);
  3  anything else: a leak of B's probe, a failed control, an unhealthy handle, only one
     client seeding A, a crash.
Needs the compose dev stack (ZB_NATS_CONTAINER=zb-dev-nats): the probe bridge signs with the
stack's client key.
"""
import json, os, pathlib, re, subprocess, sys, tempfile, time, urllib.request, uuid
import zb
from clients import Lib, Node, fresh_sqlite

LOG = "/tmp/zb_late_tenant_bridge.log"
TMP = pathlib.Path(tempfile.mkdtemp(prefix="zb_late_tenant_"))
TEN, PUBT = "lt_rows", "lt_pub"
PUB = zb.publication()
OPEN = zb.TOPOLOGY.get("open_tenant", "_default")
TAG = os.urandom(3).hex()
P, HELPER = f"lt_{TAG}", f"lth_{TAG}"
A, B = f"lta{TAG}", f"ltb{TAG}"
DEADLINE_S = 60


def until(pred, budget, step=0.3):
    end = time.monotonic() + budget
    while time.monotonic() < end:
        if pred(): return True
        time.sleep(step)
    return False


def enrol(principal, tenant):
    code = os.urandom(16).hex()
    zb.psql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id, expires_at) "
            f"VALUES ('{code}', '{principal}', '{tenant}', now() + interval '5 minutes')")
    gen = subprocess.run([str(zb.BRIDGE), "--gen-nkey"], capture_output=True, text=True)
    pub = re.search(r"NATS_BRIDGE_NKEY_PUB=(U[A-Z0-9]+)", gen.stdout).group(1)
    seed = re.search(r"NATS_BRIDGE_NKEY_SEED=(SU[A-Z0-9]+)", gen.stdout).group(1)
    with urllib.request.urlopen(zb.http_base(probe=True) + f"/enroll?code={code}&user_pubkey={pub}", timeout=10) as r:
        jwt = json.loads(r.read().decode())["jwt"]
    return write_creds(principal, jwt, seed), pub, seed


def write_creds(principal, jwt, seed):
    path = TMP / f"{principal}-{int(time.time() * 1000)}.creds"
    path.write_text("-----BEGIN NATS USER JWT-----\n" + jwt + "\n------END NATS USER JWT------\n\n"
                    "-----BEGIN USER NKEY SEED-----\n" + seed + "\n------END USER NKEY SEED------\n")
    return path


def renew(principal, pub, seed):
    """GET /renew, signed with the principal's own key: a JWT minted from its current memberships."""
    import base64, nkeys
    ts = int(time.time())
    sig = base64.urlsafe_b64encode(nkeys.from_seed(seed.encode()).sign(f"zebridge-renew:{pub}:{ts}".encode())).decode().rstrip("=")
    with urllib.request.urlopen(zb.http_base(probe=True) + f"/renew?user_pubkey={pub}&ts={ts}&sig={sig}", timeout=10) as r:
        return write_creds(principal, json.loads(r.read().decode())["jwt"], seed)


def create_tables():
    zb.psql(f"CREATE TABLE public.{TEN} (uid uuid PRIMARY KEY, title text, tenant_id varchar(255) NOT NULL, "
            "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)")
    zb.psql(f"CREATE TABLE public.{PUBT} (uid uuid PRIMARY KEY, title text, updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)")
    for table, scope in ((TEN, "tenant_col => 'tenant_id'"), (PUBT, "public_reason => 'late_tenant probe'")):
        out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{table}', {scope}, "
                      f"version_col => 'updated_at', tombstone_col => 'deleted_at', generations => true, publication => '{PUB}', dry_run => false)")
        if "error" in out.lower(): raise RuntimeError(f"zebridge_enable({table}): {out}")


def teardown():
    for t in (TEN, PUBT):
        zb.forget_table(t)
        for sql in (f"DROP TABLE IF EXISTS public.{t}", f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{t}'",
                    f"DELETE FROM public.zebridge_generations WHERE tbl = '{t}'"):
            zb.psql(sql, quiet=True)
    for who in (P, HELPER):
        for sql in (f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{who}'",
                    f"DELETE FROM public.zebridge_principal_keys WHERE principal = '{who}'",
                    f"DELETE FROM public.zebridge_invites WHERE principal = '{who}'"):
            zb.psql(sql, quiet=True)
        zb.nats_cli("kv", "purge", zb.kv_bucket("tenants"), who, "-f")
    zb.drop_tenant_streams(A, B)


def titles(c):
    """The tenant table's titles on a replica (empty when the table is not followed)."""
    try: return {r[0] for r in c.q(f"SELECT title FROM {TEN}")}
    except RuntimeError: return set()


def public_seen(c):
    try: return any(r[0] == "public row" for r in c.q(f"SELECT title FROM {PUBT}"))
    except RuntimeError: return False


def main():
    if not os.environ.get("ZB_NATS_CONTAINER"):
        print("ZB_NATS_CONTAINER unset: this scenario needs the compose dev stack (its client signing key)"); return 2
    jwt = zb.jwt_stack()
    teardown()
    clients = {}
    results = {"leak": [], "setup": None}
    try:
        create_tables()
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5",
                       ZB_SIGNING_SEED=jwt["client_sk_file"].read_text().strip(), ZB_ACCOUNT_PUB=jwt["account_pub"]) as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                print("bridge never started its producer"); return 2
            # The two tenants' streams and probe rows exist before P is anywhere near them:
            # a helper principal is their member.
            for t in (A, B):
                zb.psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) VALUES ('{HELPER}', '{t}')")
            time.sleep(2)
            zb.psql(f"INSERT INTO public.{TEN} (uid, title, tenant_id) VALUES ('{uuid.uuid4()}', 'probe of A', '{A}'), ('{uuid.uuid4()}', 'probe of B', '{B}')")
            zb.psql(f"INSERT INTO public.{PUBT} (uid, title) VALUES ('{uuid.uuid4()}', 'public row')")
            original, pub, seed = enrol(P, OPEN)
            for kind in ("libzb", "ts"):
                db = str(TMP / f"original-{kind}.sqlite3")
                clients[kind] = Lib(db, [TEN, PUBT], f"py-lt-{kind}", principal=P, creds=original) if kind == "libzb" \
                    else Node(db, str(TMP / "node.log"), principal=P, creds=original)

            def poll():
                c = clients.get("libzb")
                if c: c.poll(200)
            def watch_leak():
                for kind, c in clients.items():
                    if "probe of B" in titles(c) and kind not in results["leak"]: results["leak"].append(kind)

            if not until(lambda: (poll(), all(public_seen(c) for c in clients.values()))[1], 90):
                results["setup"] = "the original clients never showed the public row"; return classify(results)
            # While unmapped, the original handles follow the public table live: a public CHANGE arrives.
            zb.psql(f"INSERT INTO public.{PUBT} (uid, title) VALUES ('{uuid.uuid4()}', 'public change while unmapped')")
            def public_change(c):
                try: return any(r[0] == "public change while unmapped" for r in c.q(f"SELECT title FROM {PUBT}"))
                except RuntimeError: return False
            if not until(lambda: (poll(), all(public_change(c) for c in clients.values()))[1], 60):
                results["setup"] = "the original clients never received a public change while unmapped"; return classify(results)
            before = {k: titles(c) for k, c in clients.items()}
            print(f"  · before the assignment, tenant rows seen: {json.dumps({k: sorted(v) for k, v in before.items()})}", flush=True)
            # 2. The assignment, with the original handles running.
            zb.psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) VALUES ('{P}', '{A}')")
            # 3. The control: credentials from /renew for the same principal and key read A,
            # and A's seed is there. (Renewed once the mapping has reached the database.)
            time.sleep(2)
            control_creds = renew(P, pub, seed)
            control = Lib(str(TMP / "control.sqlite3"), [TEN], "py-lt-control", principal=P, creds=control_creds)
            try:
                seeded = until(lambda: (control.poll(200), "probe of A" in titles(control))[1], 90)
                zb.psql(f"INSERT INTO public.{TEN} (uid, title, tenant_id) VALUES ('{uuid.uuid4()}', 'after the assignment', '{A}')")
                later = until(lambda: (control.poll(200), "after the assignment" in titles(control))[1], 60)
                results["control"] = {"seeded A": seeded, "later change": later, "B leaked": "probe of B" in titles(control)}
            finally:
                control.close()
            print(f"  · control (renewed credentials): {json.dumps(results['control'])}", flush=True)
            # 4. The original handles, until the deadline.
            end = time.monotonic() + DEADLINE_S
            while time.monotonic() < end:
                poll(); watch_leak()
                if all({"probe of A", "after the assignment"} <= titles(c) for c in clients.values()): break
                time.sleep(0.5)
            watch_leak()
            results["original"] = {k: sorted(titles(c)) for k, c in clients.items()}
            # Healthy: a later public change still reaches each original handle.
            zb.psql(f"INSERT INTO public.{PUBT} (uid, title) VALUES ('{uuid.uuid4()}', 'public change at the deadline')")
            def healthy(c):
                try: return any(r[0] == "public change at the deadline" for r in c.q(f"SELECT title FROM {PUBT}"))
                except RuntimeError: return False
            results["healthy"] = {k: until(lambda c=c: (poll(), healthy(c))[1], 60) for k, c in clients.items()}
            print(f"  · original handles after {DEADLINE_S} s: {json.dumps(results['original'])}", flush=True)
    finally:
        for c in clients.values():
            try: c.close()
            except Exception: pass
        teardown()
    return classify(results)


def classify(r):
    if r.get("setup"): print(f"  setup incomplete — {r['setup']}"); return 2
    ctl = r.get("control") or {}
    if not (ctl.get("seeded A") and ctl.get("later change")):
        print(f"  CONTROL FAILED — renewed credentials did not read A's seed and later change: {ctl}"); return 3
    if r["leak"] or ctl.get("B leaked"):
        print(f"  LEAK: tenant B's probe was visible to {r['leak'] or ['the control']}"); return 3
    if not all(r.get("healthy", {}).values()):
        print(f"  an original handle is not healthy (no public change at the deadline): {r.get('healthy')}"); return 3
    got = {k: {"probe of A", "after the assignment"} <= set(v) for k, v in r["original"].items()}
    print(f"  original handles: {got}", flush=True)
    if all(got.values()): return 0
    if not any("probe of A" in v for v in r["original"].values()):
        print("  known defect reproduced: the running clients never pick up their first private tenant")
        return 1
    return 3


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        import traceback; traceback.print_exc(); sys.exit(3)

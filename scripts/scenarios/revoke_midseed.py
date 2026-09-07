"""A principal revoked while its re-seed is in flight (NOTES §10dl, item 6).

Revocation has two rungs (§10cj, §10ck, §10cm), so two halves:

  SOFT  `bridge --revoke <p>`: the mapping goes, writes die now, tenant resolution dies
        at the next connect, reads live until the JWT expires. A re-seed caught by it
        must COMPLETE (the read door is open), the next write must be refused, the
        tenants key must be gone, and a reconnect must find no tenant.
  HARD  the same with OPERATOR_SEED and --conf: the account JWT's revocations map is
        rebuilt and re-signed, and on the server's reload the live session is KICKED.
        A re-seed caught mid-download must leave the table whole — the old rows or the
        complete new full, never a partial one — with the watermark and inbox intact,
        the cause named, and a reconnect refused. A witness on another principal seeds
        the same table to completion, on the same server, untouched.

Both clients (libzb polling, zb-client-ts in Node) on each probe principal, enrolled
through GET /enroll like any device. The dev server's config is amended in place by
the hammer and restored — and reloaded — at the end.
"""
import json, os, pathlib, re, shutil, subprocess, sys, threading, time, urllib.error, urllib.request, uuid
import zb
from clients import Lib, Node, both, fresh_sqlite

BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
SK_FILE = zb.ROOT / "scripts/native/nsc-store/data/nats/nsc/keys/keys/A/BG/ABGOGUGNFMBG47I2NTHPQFX4G3YUP23ENMNEM7ZUURFKIBGUK743WPND.nk"
OP_FILE = zb.ROOT / "scripts/native/nsc-store/data/nats/nsc/keys/keys/O/CZ/OCZZMCIKTYIW5RWDOOMNBFPXKPP4LMKQLUQSI2FFFHUYQ54WSEGGNR4P.nk"
ACCOUNT_PUB = "ABSDAXPW56ON5USFCTUQSWSQWTNLZL3DDDVZAPUH3244HU4NM7QFQD5L"
CONF = zb.ROOT / "scripts/native/nats-server-jwt.conf"
PID_FILE = zb.ROOT / "scripts/native/nats-server.pid"
ADMIN_URL = "postgres://postgres@127.0.0.1:5432/postgres"
LOG = "/tmp/zb_revoke_midseed_bridge.log"
T = "mig_rv"
TENANT = "kilo"
ROWS = 30000
PUB = zb.publication()
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")
TMP = pathlib.Path(os.environ.get("TMPDIR", "/tmp"))
# A revoked principal is dead for good — its retained ban would hang up any client that
# reuses the name (measured on the second run). Every run enrolls FRESH names.
RUN = os.urandom(3).hex()
SOFT, HARD = f"rv_soft_{RUN}", f"rv_hard_{RUN}"


def enroll(principal):
    """An invite row, a fresh nkey, one GET /enroll → a creds file (the jwt_expiry recipe)."""
    code = os.urandom(16).hex()
    zb.psql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id, role, expires_at) "
            f"VALUES ('{code}', '{principal}', '{TENANT}', 'client', now() + interval '10 minutes')")
    gen = subprocess.run([str(BRIDGE), "--gen-nkey"], capture_output=True, text=True)
    user_pub = re.search(r"NATS_BRIDGE_NKEY_PUB=(U[A-Z0-9]+)", gen.stdout).group(1)
    user_seed = re.search(r"NATS_BRIDGE_NKEY_SEED=(SU[A-Z0-9]+)", gen.stdout).group(1)
    with urllib.request.urlopen(zb.http_base(probe=True) + f"/enroll?code={code}&user_pubkey={user_pub}", timeout=10) as r:
        payload = json.loads(r.read().decode())
    # the mapping the enroll wrote rides the WAL to $KV.tenants: a client opened before
    # it lands resolves the OPEN tenant instead (§10ce's "resolution at the next connect")
    t0 = time.monotonic()
    while time.monotonic() - t0 < 20 and zb.kv_get("tenants", principal) != TENANT:
        time.sleep(0.3)
    path = TMP / f"zb_{principal}.creds"
    path.write_text("-----BEGIN NATS USER JWT-----\n" + payload["jwt"] + "\n------END NATS USER JWT------\n\n"
                    "-----BEGIN USER NKEY SEED-----\n" + user_seed + "\n------END USER NKEY SEED------\n")
    return path


def revoke(principal, hard):
    env = dict(os.environ, ADMIN_DATABASE_URL=ADMIN_URL); env.pop("OPERATOR_SEED", None)
    args = [str(BRIDGE), "--revoke", principal]
    if hard:
        env["OPERATOR_SEED"] = OP_FILE.read_text().strip(); env["ZB_ACCOUNT_PUB"] = ACCOUNT_PUB
        args += ["--conf", str(CONF)]
    r = subprocess.run(args, env=env, capture_output=True, text=True)
    out = r.stdout + r.stderr
    if hard and r.returncode == 0:
        os.kill(int(PID_FILE.read_text().strip()), 1)   # SIGHUP: the server reloads the amended JWT
    return r.returncode, out


def cleanup(principals):
    for p in principals:
        zb.psql(f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{p}'", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_invites WHERE principal = '{p}'", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_principal_keys WHERE principal = '{p}'", quiet=True)
        (TMP / f"zb_{p}.creds").unlink(missing_ok=True)
    zb.psql(f"DROP TABLE IF EXISTS public.{T}", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{T}'", quiet=True)
    zb.psql(f"DELETE FROM public.zebridge_generations WHERE tbl = '{T}'", quiet=True)


def wait_full(epoch, budget=40):
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        if zb.psql(f"SELECT count(*) FROM zebridge_generations WHERE tenant = '{TENANT}' AND tbl = '{T}' AND seed_epoch = {epoch} AND has_full") not in ("", "0"):
            return round(time.monotonic() - t0, 1)
        time.sleep(0.5)
    return None


def main():
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")
    if not (SK_FILE.exists() and OP_FILE.exists() and CONF.exists() and PID_FILE.exists()):
        sys.exit("the dev JWT stack (nsc store, conf, pid) is not where jwt-bootstrap.sh puts it")
    failed = 0
    def check(label, cond):
        nonlocal failed
        label = time.strftime("%H:%M:%S ") + label
        if cond: zb.ok(label)
        else: zb.bad(label); failed += 1
        sys.stdout.flush()

    conf_backup = CONF.read_text()
    cleanup([SOFT, HARD])
    zb.psql(f"CREATE TABLE public.{T} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), title text, {COMMON})")
    clients = []
    try:
        with zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5",
                       ZB_SIGNING_SEED=SK_FILE.read_text().strip(), ZB_ACCOUNT_PUB=ACCOUNT_PUB) as bridge:
            if not bridge.wait_for_log("enrollment endpoint armed", timeout=60) or not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never armed enrollment / started its producer"); print(bridge.text()[-1500:]); return 1
            out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{T}', {ENABLE})")
            check(f"zebridge_enable({T}) live: {out.strip()[:50]}…", "error" not in out.lower())
            zb.psql(f"INSERT INTO {T} (title, tenant_id) SELECT 'r' || g, '{TENANT}' FROM generate_series(1, {ROWS}) g")
            soft, hard = enroll(SOFT), enroll(HARD)
            check("§0 two probe principals enrolled through GET /enroll (invite → nkey → jwt), keys on record", soft.exists() and hard.exists()
                  and zb.psql(f"SELECT count(*) FROM zebridge_principal_keys WHERE principal IN ('{SOFT}','{HARD}')").strip() == "2")
            count = lambda c: c.q(f"SELECT count(*) FROM {T}")[0][0]
            epoch = lambda: int(zb.psql(f"SELECT seed_epoch FROM zebridge_catalogue WHERE tbl = '{T}'") or 0)
            wm_epoch = lambda c, tbl: (c.q(f"SELECT seed_epoch FROM {tbl} WHERE tbl = ?", [T]) or [[None]])[0][0]

            # ══ SOFT: the mapping rung, mid-seed ══════════════════════════════════
            for db in ("/tmp/zb-rv-soft-py.sqlite3", "/tmp/zb-rv-soft-node.sqlite3"): fresh_sqlite(db)
            py = Lib("/tmp/zb-rv-soft-py.sqlite3", [T], "py-rv-soft", principal=SOFT, creds=soft)
            nd = Node("/tmp/zb-rv-soft-node.sqlite3", "/tmp/zb_rv_soft_node.log", principal=SOFT, creds=soft)
            clients += [py, nd]
            dt = both(py, nd, lambda c: count(c) == ROWS, 300)
            check(f"§S0 both clients on rv_soft seeded {ROWS} rows ({dt} s)", dt is not None)
            zb.psql(f"INSERT INTO {T} (title, tenant_id) SELECT 's' || g, '{TENANT}' FROM generate_series(1, 5000) g")
            both(py, nd, lambda c: count(c) == ROWS + 5000, 60)
            e1 = epoch() + 1
            zb.psql(f"SELECT * FROM zebridge_reseed('{T}')", quiet=True)
            dtf = wait_full(e1)
            check(f"§S1 zebridge_reseed → the producer's full under epoch {e1} ({dtf} s)", dtf is not None)
            fired = {}
            def soft_hammer():
                time.sleep(1.0)
                fired["rc"], fired["out"] = revoke(SOFT, hard=False)
                fired["at"] = time.strftime("%H:%M:%S")
            threading.Thread(target=soft_hammer, daemon=True).start()
            # the ban (§10dm): the library hangs up on its own the moment it sees
            # mutation_ack.rv_soft.revoked — no operator key, no reload
            t0 = time.monotonic(); err = None
            while time.monotonic() - t0 < 30:
                r = py.poll()
                if "error" in r: err = r["error"]; break
            time.sleep(2)
            heard = open("/tmp/zb_rv_soft_node.log").read()
            check(f"§S2 mapping revoked at {fired.get('at')} while the seed ran (rc={fired.get('rc')}): libzb hung up on the ban after {round(time.monotonic() - t0, 1)} s → {err!r}; the Node client too: {'REVOKED by the operator' in heard}",
                  fired.get("rc") == 0 and err == "Revoked" and "REVOKED by the operator" in heard)
            n_py, n_nd = count(py), count(nd)
            check(f"§S3 the tables are whole on both (py {n_py}, node {n_nd} — {ROWS} or the complete new full {ROWS + 5000}); inboxes py {py.q('SELECT count(*) FROM _zbz_inbox')[0][0]}, node {nd.q('SELECT count(*) FROM _zebridge_inbox')[0][0]}",
                  n_py in (ROWS, ROWS + 5000) and n_nd in (ROWS, ROWS + 5000) and py.q("SELECT count(*) FROM _zbz_inbox")[0][0] == 0 and nd.q("SELECT count(*) FROM _zebridge_inbox")[0][0] == 0)
            f = py.flush(2000)
            check(f"§S3 a write attempt from the revoked client is answered {f.get('error')!r}, not sent", f.get("error") == "Revoked")
            check(f"§S4 $KV.tenants.<soft> is gone: {zb.kv_get('tenants', SOFT)!r}", zb.kv_get("tenants", SOFT) == "")
            py.close(); nd.close(); clients = []
            py2 = Lib("/tmp/zb-rv-soft-py.sqlite3", [T], "py-rv-soft-2", principal=SOFT, creds=soft)
            nd2 = Node("/tmp/zb-rv-soft-node.sqlite3", "/tmp/zb_rv_soft_node2.log", principal=SOFT, creds=soft)
            clients += [py2, nd2]
            zb.psql(f"INSERT INTO {T} (title, tenant_id) VALUES ('after-reconnect', '{TENANT}')")
            time.sleep(4)
            r = py2.poll()
            heard = open("/tmp/zb_rv_soft_node2.log").read()
            check(f"§S5 reconnected on the same files: the retained ban is found at once (libzb sync → {py2.sync_error!r}, poll → {r.get('error')!r}; Node connect → {nd2.connect_error!r}), the new row reached neither (py {count(py2)}, node {count(nd2)})",
                  py2.sync_error == "Revoked" and r.get("error") == "Revoked" and "revoked" in str(nd2.connect_error) and count(py2) == n_py and count(nd2) == n_nd)
            # the wipe is EXPLICIT: the library never deletes a device's rows on its own
            py2.wipe(); nd2.wipe(); clients = []
            left = [f for f in ("/tmp/zb-rv-soft-py.sqlite3", "/tmp/zb-rv-soft-node.sqlite3") if os.path.exists(f)]
            check(f"§S6 the application's explicit wipe (zb_client_wipe / zb.wipe()) removed both replica files; left: {left}", not left)
            # a revoked principal is dead for good: the operator creates a NEW one
            code = os.urandom(16).hex()
            zb.psql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id, role, expires_at) VALUES ('{code}', '{SOFT}', '{TENANT}', 'client', now() + interval '10 minutes')")
            gen = subprocess.run([str(BRIDGE), "--gen-nkey"], capture_output=True, text=True)
            pub2 = re.search(r"NATS_BRIDGE_NKEY_PUB=(U[A-Z0-9]+)", gen.stdout).group(1)
            try:
                with urllib.request.urlopen(zb.http_base(probe=True) + f"/enroll?code={code}&user_pubkey={pub2}", timeout=10) as r:
                    status = r.status
            except urllib.error.HTTPError as e:
                status = e.code
            check(f"§S7 re-enrolling the revoked name is refused (HTTP {status}) — a revoked principal is dead for good, the operator creates a new one", status >= 400)

            # ══ HARD: the hammer, mid-download ════════════════════════════════════
            for db in ("/tmp/zb-rv-hard-py.sqlite3", "/tmp/zb-rv-hard-node.sqlite3", "/tmp/zb-rv-witness.sqlite3"): fresh_sqlite(db)
            py = Lib("/tmp/zb-rv-hard-py.sqlite3", [T], "py-rv-hard", principal=HARD, creds=hard)
            nd = Node("/tmp/zb-rv-hard-node.sqlite3", "/tmp/zb_rv_hard_node.log", principal=HARD, creds=hard)
            wit = Lib("/tmp/zb-rv-witness.sqlite3", [T], "py-rv-witness")
            clients += [py, nd, wit]
            base = ROWS + 5001
            dt = both(py, nd, lambda c: count(c) == base and count(wit) == base, 300)
            check(f"§H0 both clients on rv_hard and a witness on omar hold {base} rows ({dt} s)", dt is not None)
            zb.psql(f"INSERT INTO {T} (title, tenant_id) SELECT 'h' || g, '{TENANT}' FROM generate_series(1, 5000) g")
            both(py, nd, lambda c: count(c) == base + 5000 and count(wit) == base + 5000, 60)
            e2 = epoch() + 1
            zb.psql(f"SELECT * FROM zebridge_reseed('{T}')", quiet=True)
            dtf = wait_full(e2)
            check(f"§H1 zebridge_reseed → the producer's full under epoch {e2} ({dtf} s)", dtf is not None)
            fired = {}
            def hard_hammer():
                time.sleep(1.0)
                fired["rc"], fired["out"] = revoke(HARD, hard=True)
                fired["at"] = time.strftime("%H:%M:%S.") + str(int(time.time() * 1000) % 1000)
            threading.Thread(target=hard_hammer, daemon=True).start()
            # poll libzb until its connection is gone (the seed runs inside these polls)
            t0 = time.monotonic(); err = None
            while time.monotonic() - t0 < 90:
                r = py.poll()
                if "error" in r: err = r["error"]; break
                wit.poll()
            for _ in range(20): wit.poll()
            # the hammer also deletes the mapping, so the ban may reach the client before the kick:
            # either name is the truth, and §H6 proves the enforcement regardless
            check(f"§H2 full revocation + reload at {fired.get('at')} (rc={fired.get('rc')}, {'FULL revocation written' in fired.get('out', '')}): libzb's poll names the cause after {round(time.monotonic() - t0, 1)} s → {err!r}",
                  fired.get("rc") == 0 and err in ("Revoked", "AuthorizationViolation", "AuthExpired", "AuthRevoked"))
            n_py = count(py); n_nd = count(nd)
            wm_py = wm_epoch(py, "_zbz_generations"); wm_nd = wm_epoch(nd, "_zebridge_generations")
            check(f"§H3 no partial table: py {n_py} rows (watermark epoch {wm_py}), node {n_nd} (epoch {wm_nd}) — each is the old count {base + 5000} or the complete new full; inboxes py {py.q('SELECT count(*) FROM _zbz_inbox')[0][0]}, node {nd.q('SELECT count(*) FROM _zebridge_inbox')[0][0]}",
                  n_py in (base + 5000,) and n_nd in (base + 5000,) and py.q("SELECT count(*) FROM _zbz_inbox")[0][0] == 0 and nd.q("SELECT count(*) FROM _zebridge_inbox")[0][0] == 0)
            time.sleep(2)
            heard = open("/tmp/zb_rv_hard_node.log").read()
            named = sorted({w for w in ("Authorization", "Authentication", "REVOKED by the operator", "closed by the server") if w in heard})
            check(f"§H4 the Node client heard the verdict by name: {named}", bool(named))
            dt = both(wit, wit, lambda c: wm_epoch(c, "_zbz_generations") == e2 and count(c) == base + 5000, 90)
            check(f"§H5 the witness on omar re-seeded to completion on the same server, untouched ({dt} s; {count(wit)} rows, epoch {wm_epoch(wit, '_zbz_generations')})", dt is not None)
            py.close(); nd.close(); clients = [wit]
            r = subprocess.run(["nats", "--server", zb.nats_server(), "--creds", str(hard), "pub", "x", "y"], capture_output=True, text=True, timeout=10)
            check(f"§H6 the revoked token cannot come back: {(r.stderr + r.stdout).strip()[:80]!r}", r.returncode != 0 and "Authorization" in r.stderr + r.stdout)
            wit.close(); clients = []
    finally:
        for c in clients:
            try: c.close()
            except Exception: pass
        # the dev server gets its pre-scenario account JWT back, and reloads it
        CONF.write_text(conf_backup)
        try: os.kill(int(PID_FILE.read_text().strip()), 1)
        except Exception: pass
        cleanup([SOFT, HARD])
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)

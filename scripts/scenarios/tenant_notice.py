#!/usr/bin/env python3
"""The bridge tells a principal its membership changed: `<mutation_ack>.<principal>.tenants`.

A principal with no private assignment (enrolled in the open tenant) subscribes to its own
notice subject with its own credentials, on ONE connection kept open for the whole run.

  A  a tenant added in PostgreSQL: the notice arrives, and by then the principal's own key
     (`$KV.tenants.<principal>`, read by direct get) holds the tenant.
  B  the bridge stops between the KV value and the notice (fault build,
     ZB_TEST_TENANT_NOTICE_EXIT, exit 70): the key holds the second tenant, no notice came.
     Only the bridge restarts. The flush that stopped never confirmed its LSN, so the slot
     replays the roster row and the notice arrives on the same connection. The connection
     must not have dropped: a reconnect-time read of the key would otherwise hide a missing
     notice.

  C  enroll after the boot roster snapshot, and enroll over a stale key after the
     purge decision. Each enrollment's acknowledged revision must still be directly
     readable with its own credentials before WAL replay can repair a missing key.

Exit 0 when every check passes, 1 when one fails, 2 when the setup is incomplete.
Needs the compose dev stack (ZB_NATS_CONTAINER: the probe bridge signs with the stack's
client key) and the fault build (zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults).
"""
import asyncio, json, os, pathlib, re, subprocess, sys, tempfile, time, urllib.request
import nats
import zb

ROOT = pathlib.Path(__file__).resolve().parents[2]
FAULT_BRIDGE = ROOT / "zig-out-faults" / "bin" / "bridge"
TMP = pathlib.Path(tempfile.mkdtemp(prefix="zb_tenant_notice_"))
TAG = os.urandom(3).hex()
P = f"tn_{TAG}"
FRESH, REPLACED, ORPHAN = (f"{P}_{suffix}" for suffix in ("fresh", "replaced", "orphan"))
A1, A2 = f"tna{TAG}", f"tnb{TAG}"
OPEN = zb.TOPOLOGY.get("open_tenant", "_default")
ACK = zb.TOPOLOGY["subjects"]["mutation_ack_prefix"]
BUCKET = zb.kv_bucket("tenants")
failed = 0
STARTS = []


def check(label, cond, detail=""):
    global failed
    (zb.ok if cond else zb.bad)(label + (f" — {detail}" if detail else ""))
    if not cond: failed += 1


def enrol(principal, tenant):
    code = os.urandom(16).hex()
    zb.psql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id, expires_at) VALUES ('{code}', '{principal}', '{tenant}', now() + interval '5 minutes')")
    gen = subprocess.run([str(zb.BRIDGE), "--gen-nkey"], capture_output=True, text=True)
    pub = re.search(r"NATS_BRIDGE_NKEY_PUB=(U[A-Z0-9]+)", gen.stdout).group(1)
    seed = re.search(r"NATS_BRIDGE_NKEY_SEED=(SU[A-Z0-9]+)", gen.stdout).group(1)
    with urllib.request.urlopen(zb.http_base(probe=True) + f"/enroll?code={code}&user_pubkey={pub}", timeout=10) as r:
        jwt = json.loads(r.read().decode())["jwt"]
    path = TMP / f"{principal}.creds"
    path.write_text("-----BEGIN NATS USER JWT-----\n" + jwt + "\n------END NATS USER JWT------\n\n"
                    "-----BEGIN USER NKEY SEED-----\n" + seed + "\n------END USER NKEY SEED------\n")
    return str(path)


def start_bridge(fault, **gates):
    """A probe bridge; the fault one is the fault build with its stop armed."""
    jwt = zb.jwt_stack()
    env = dict(ZB_SIGNING_SEED=jwt["client_sk_file"].read_text().strip(), ZB_ACCOUNT_PUB=jwt["account_pub"])
    if gates: env.update(gates)
    elif fault: env["ZB_TEST_TENANT_NOTICE_EXIT"] = P
    STARTS.append(fault)
    b = zb.Bridge(f"/tmp/zb_tenant_notice_bridge_{len(STARTS)}{'_fault' if fault else ''}.log", **env)
    normal, zb.BRIDGE = zb.BRIDGE, (FAULT_BRIDGE if fault else zb.BRIDGE)
    try: b.__enter__()
    finally: zb.BRIDGE = normal
    return b


def teardown():
    for principal in (P, FRESH, REPLACED):
        clean_principal(principal)
    zb.nats_cli("kv", "purge", BUCKET, ORPHAN, "-f")
    zb.drop_tenant_streams(A1, A2)


def clean_principal(principal):
    for sql in (f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{principal}'",
                f"DELETE FROM public.zebridge_principal_keys WHERE principal = '{principal}'",
                f"DELETE FROM public.zebridge_invites WHERE principal = '{principal}'"):
        zb.psql(sql, quiet=True)
    zb.nats_cli("kv", "purge", BUCKET, principal, "-f")


async def until(pred, budget):
    end = time.monotonic() + budget
    while time.monotonic() < end:
        if pred(): return True
        await asyncio.sleep(0.2)
    return pred()


async def startup_enrollment():
    after_snapshot, before_purge, before_replay = (TMP / name for name in ("snapshot", REPLACED, "replay"))
    gates = dict(ZB_TEST_TENANT_AFTER_SNAPSHOT=str(after_snapshot),
                 ZB_TEST_TENANT_BEFORE_PURGE=str(before_purge),
                 ZB_TEST_TENANT_BEFORE_REPLAY=str(before_replay))
    for path in (after_snapshot, before_purge, before_replay):
        path.touch()
    for principal in (REPLACED, ORPHAN):
        zb.nats_cli("kv", "put", BUCKET, principal, json.dumps([OPEN])).check_returncode()
    clients = []
    b = start_bridge(True, **gates)

    async def revision(nc, principal):
        msg = await nc.jetstream(domain="hub").get_msg(
            f"KV_{BUCKET}", subject=f"$KV.{BUCKET}.{principal}", direct=True)
        if not msg.seq or json.loads(msg.data) != [OPEN]:
            raise ValueError(f"unexpected membership for {principal}")
        return msg.seq

    async def enroll_at(gate, principal):
        if not b.wait_for_log(gate + ": holding", timeout=60):
            raise RuntimeError(f"bridge never reached {gate}")
        creds = enrol(principal, OPEN)
        nc = await nats.connect(zb.nats_server(), user_credentials=creds,
                                inbox_prefix=f"_INBOX.{principal}".encode())
        clients.append(nc)
        return nc, principal, await revision(nc, principal)

    try:
        fresh = await enroll_at("ZB_TEST_TENANT_AFTER_SNAPSHOT", FRESH)
        after_snapshot.unlink()
        replaced = await enroll_at("ZB_TEST_TENANT_BEFORE_PURGE", REPLACED)
        before_purge.unlink()
        if not b.wait_for_log("ZB_TEST_TENANT_BEFORE_REPLAY: holding", timeout=60):
            raise RuntimeError("boot did not finish tenant reconciliation")
        for nc, principal, first in (fresh, replaced):
            try:
                last = await revision(nc, principal)
            except Exception as err:
                last = str(err)
            check(f"C: {principal} still reads its enrollment revision before WAL replay",
                  last == first, f"{first} → {last}")
        # The same pass must still remove a genuinely stale mapping.
        admin = await zb.connect()
        clients.append(admin)
        kv = await admin.jetstream(domain="hub").key_value(BUCKET)
        try:
            await kv.get(ORPHAN)
        except nats.js.errors.KeyNotFoundError:
            check("C: the unclaimed stale key was purged", True)
        else:
            check("C: the unclaimed stale key was purged", False)
    finally:
        for path in (after_snapshot, before_purge, before_replay):
            path.unlink(missing_ok=True)
        for nc in clients:
            await nc.close()
        b.__exit__(None, None, None)


async def run():
    b = start_bridge(fault=False)
    try:
        # A and B exercise the WAL path; C holds the startup path open separately.
        if not b.wait_for_log("WAL replication resumed", timeout=60): print("bridge did not start"); return 2
        creds = enrol(P, OPEN)
        drops = []
        async def on_drop(): drops.append(time.monotonic())
        nc = await nats.connect(zb.nats_server(), user_credentials=creds, inbox_prefix=f"_INBOX.{P}".encode(),
                                disconnected_cb=on_drop)
        js = nc.jetstream(domain="hub")
        notices = []
        async def on_notice(m): notices.append(m.data.decode())
        await nc.subscribe(f"{ACK}.{P}.tenants", cb=on_notice)
        await nc.flush()

        async def key():
            try: return json.loads((await js.get_msg(f"KV_{BUCKET}", subject=f"$KV.{BUCKET}.{P}", direct=True)).data)
            except Exception as e: return f"{type(e).__name__}: {e}"

        # A: the notice for an assignment; the key holds it by then.
        zb.psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) VALUES ('{P}', '{A1}')")
        got = await until(lambda: len(notices) > 0, 20)
        k = await key()
        check(f"A: a notice on {ACK}.{P}.tenants", got, str(notices[:1]))
        check(f"A: by then the key holds {A1}", isinstance(k, list) and A1 in k, str(k))
        before = len(notices)
        # The slot confirms past A first: a replayed A row would otherwise meet the stop
        # before the row under test does.
        slot = zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1]
        lsn = zb.psql("SELECT pg_current_wal_lsn()").strip()
        if not await until(lambda: zb.psql(f"SELECT confirmed_flush_lsn >= '{lsn}' FROM pg_replication_slots WHERE slot_name = '{slot}'").strip() == "t", 30):
            print("the slot never confirmed past A"); return 2

        # B: stopped between the KV value and the notice; only the bridge restarts.
        b.__exit__(None, None, None)
        b = start_bridge(fault=True)
        if not b.wait_for_log("WAL replication resumed", timeout=60): print("fault bridge did not start"); return 2
        zb.psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) VALUES ('{P}', '{A2}')")
        code = b.wait_for_exit(timeout=30)
        await asyncio.sleep(1)
        k = await key()
        check("B: the bridge stopped after the KV value, before the notice (exit 70)", code == 70, f"exit {code}")
        check(f"B: the key already holds {A2}", isinstance(k, list) and A2 in k, str(k))
        check("B: no notice came for it", len(notices) == before, str(notices[before:]))
        b.__exit__(None, None, None)
        b = start_bridge(fault=False)
        got = await until(lambda: len(notices) > before, 60)
        check("B: after the restart, the notice arrives (the roster row replayed)", got, f"{len(notices) - before} notice(s)")
        check("B: on the same connection, never dropped", nc.is_connected and not drops, f"{len(drops)} drop(s)")
        await nc.close()
    finally:
        try: b.__exit__(None, None, None)
        except Exception: pass
    await startup_enrollment()
    return 0


def main():
    if not os.environ.get("ZB_NATS_CONTAINER"):
        print("ZB_NATS_CONTAINER unset: this scenario needs the compose dev stack"); return 2
    if not FAULT_BRIDGE.exists():
        print(f"{FAULT_BRIDGE} missing: zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults"); return 2
    teardown()
    try:
        rc = asyncio.run(run())
    except Exception as e:
        print(f"harness error: {type(e).__name__}: {e}"); rc = 2
    finally:
        teardown()
    return rc or (1 if failed else 0)


if __name__ == "__main__":
    sys.exit(main())

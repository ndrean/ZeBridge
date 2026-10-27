#!/usr/bin/env python3
"""The readings roles, proven on a real nats-server (examples/16-archive).

`bridge --init-nats operator` generates a scratch stack; the scenario boots it, makes
one readings stream per tenant (as the bridge does at boot), mints a sensor, an archiver
and a responder, and tries what each may and may not do:

  S  a SENSOR (its JWT alone, no key to sign with) publishes its own readings, over NATS
     and over MQTT; it cannot publish as another sensor, for another tenant, or subscribe
     to anything. Minted without a tenant, it publishes to _default;
  A  an ARCHIVER reads its tenant's stream and writes its tenant's manifest; another
     tenant's stream and manifest, and any stream admin, are refused;
  R  a RESPONDER reads its tenant's stream and manifest, and cannot write the manifest;
  P  the real examples/16-archive programs inside those grants: archiver.py with an
     archiver credential archives every reading to S3 (read by nats-py, P1, and by
     bridge_readings_fetch, P3), and responder.py with a responder credential answers the
     count from Parquet (needs local S3, boto3 and duckdb);
  U  an operator.store from before these roles: `--init-nats --update` adds the two keys,
     keeps every other line, and a sensor minted afterwards works.

Usage:  scripts/scenarios/.venv/bin/python scripts/scenarios/readings_roles.py
"""
import asyncio, json, pathlib, re, shutil, subprocess, sys, tempfile, time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
import nats  # noqa: E402
from nats.js.api import StreamConfig, KeyValueConfig  # noqa: E402

BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
PORT, MQTT = 14242, 11883
URL = f"nats://127.0.0.1:{PORT}"
NEW_LINES = tuple(f"SK_{role}_SEED" for role in ("ARCHIVER", "SENSOR"))   # what --update adds
failed = 0


def check(good, label):
    global failed
    if good:
        zb.ok(label)
    else:
        zb.bad(label)
        failed += 1


def mint(tmp, kind, name, tenant=None):
    cmd = [str(BRIDGE), f"--mint-{kind}", "--name", name, "--store", str(tmp / "zb-nats" / "operator.store")]
    if tenant:
        cmd += ["--tenant", tenant]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"--mint-{kind} failed: {r.stderr[-300:]}")
    path = tmp / f"{name}.creds"
    path.write_text(r.stdout)
    return path


def jwt_from(creds: pathlib.Path) -> str:
    """The creds file's first block: the user JWT."""
    return re.search(r"-----BEGIN NATS USER JWT-----\s*(\S+)\s*------END", creds.read_text()).group(1)


async def connect(creds=None, jwt_only=None, inbox=None):
    errors: list = []

    async def on_error(e):
        errors.append(str(e))
    kw = {"error_cb": on_error, "allow_reconnect": False, "connect_timeout": 3}
    if inbox:
        kw["inbox_prefix"] = inbox.encode()
    if jwt_only:   # the JWT alone, nothing signed
        kw["user_jwt_cb"] = lambda: jwt_only.encode()
        kw["signature_cb"] = lambda nonce: b""
    else:
        kw["user_credentials"] = str(creds)
    nc = await nats.connect(URL, **kw)
    return nc, errors


async def count(js, stream):
    return (await js.stream_info(stream)).state.messages


async def refused(coro, timeout=2.0):
    try:
        await asyncio.wait_for(coro, timeout)
        return False
    except Exception:
        return True


async def sub_refused(nc, errs, svc) -> bool:
    """What a sensor's subscription RECEIVES, not only what the server says: another
    principal publishes on the account's subjects, and nothing may arrive."""
    got: list = []

    async def take(m):
        got.append(m.subject)
    for subj in ("sensors.>", "cdc.>", "query.>", "_INBOX.>", ">"):
        await nc.subscribe(subj, cb=take)
    await nc.flush()
    await asyncio.sleep(0.3)
    for subj in ("sensors.acme.other.x", "cdc.acme.t.insert", "query.acme.count", "_INBOX.someone.1"):
        await svc.publish(subj, b"{}")
    await svc.flush()
    await asyncio.sleep(0.5)
    return not got


def mqtt_publish(jwt_text: str, topic: str) -> bool:
    """One MQTT reading, the JWT given as the MQTT password (any user name)."""
    import paho.mqtt.client as mqtt
    done = {"rc": None}
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"probe{int(time.time() * 1000)}")
    c.username_pw_set("sensor", jwt_text)
    c.on_connect = lambda cl, u, f, rc, p=None: done.__setitem__("rc", rc)
    try:
        c.connect("127.0.0.1", MQTT, 10)
    except Exception:
        return False
    c.loop_start()
    for _ in range(30):
        if done["rc"] is not None:
            break
        time.sleep(0.1)
    ok = done["rc"] is not None and not done["rc"].is_failure
    if ok:
        c.publish(topic, json.dumps({"value": 1.0, "ts_ms": int(time.time() * 1000)}), qos=1).wait_for_publish(3)
    time.sleep(0.3)
    c.loop_stop()
    c.disconnect()
    return ok


async def main(tmp: pathlib.Path):
    nsd = tmp / "zb-nats"
    svc, _ = await connect(creds=nsd / "creds" / "bridge.creds")
    js = svc.jetstream()
    # What the bridge does at boot: one stream per tenant, and the manifest bucket.
    for t in ("acme", "beta", "_default"):
        await js.add_stream(StreamConfig(name=f"READINGS_{t}", subjects=[f"sensors.{t}.>"], max_age=86400,
                                         max_bytes=64 * 1024 * 1024, discard="old"))
    await js.create_key_value(KeyValueConfig(bucket="time_manifests", history=10, direct=True))

    # ── S: the sensor ────────────────────────────────────────────────────────
    sensor = jwt_from(mint(tmp, "sensor", "boiler-12", "acme"))
    nc, errs = await connect(jwt_only=sensor)
    check(True, "S1 a sensor connects with its JWT alone, nothing signed")
    before = await count(js, "READINGS_acme")
    await nc.publish("sensors.acme.boiler-12.temperature", b'{"value": 21.5}')
    await nc.publish("sensors.acme.boiler-13.temperature", b'{"value": 99}')
    await nc.publish("sensors.beta.boiler-12.temperature", b'{"value": 99}')
    await nc.flush()
    await asyncio.sleep(0.3)
    n_acme, n_beta = await count(js, "READINGS_acme") - before, await count(js, "READINGS_beta")
    no_sub = await sub_refused(nc, errs, svc)   # publishes on the account: counted before
    await nc.close()
    check(n_acme == 1 and n_beta == 0, f"S2 it publishes as itself only: +{n_acme} in READINGS_acme (boiler-13 and beta refused), {n_beta} in READINGS_beta")
    check(no_sub, "S3 it reads nothing: subscriptions to sensors.>, cdc.>, query.>, _INBOX.>, > receive no message")

    before = await count(js, "READINGS_acme")
    mq_ok = mqtt_publish(sensor, "sensors/acme/boiler-12/humidity")
    mqtt_publish(sensor, "sensors/acme/boiler-13/humidity")
    await asyncio.sleep(0.5)
    n = await count(js, "READINGS_acme") - before
    check(mq_ok and n == 1, f"S4 over MQTT with the JWT: connected {mq_ok}, +{n} stored (its own topic only)")

    lamp = jwt_from(mint(tmp, "sensor", "lamp"))
    nc, _ = await connect(jwt_only=lamp)
    await nc.publish("sensors._default.lamp.lux", b'{"value": 300}')
    await nc.flush()
    await nc.close()
    await asyncio.sleep(0.3)
    check(await count(js, "READINGS__default") == 1, "S5 minted without a tenant: it publishes to _default")

    # ── A: the archiver ──────────────────────────────────────────────────────
    arch = mint(tmp, "archiver", "archiver-acme", "acme")
    nc, errs = await connect(creds=arch, inbox="_INBOX.archiver-acme")
    ajs = nc.jetstream()
    sub = await ajs.subscribe("sensors.acme.>", stream="READINGS_acme", ordered_consumer=True)
    got = await sub.next_msg(timeout=3)
    check(got.subject.startswith("sensors.acme."), f"A1 the archiver reads its tenant's stream ({got.subject})")
    check(await refused(ajs.subscribe("sensors.beta.>", stream="READINGS_beta", ordered_consumer=True)),
          "A2 another tenant's stream: refused")
    info = await ajs.stream_info("READINGS_acme")
    check(info.config.max_age == 86400, "A3 it reads its stream's limits (the retention check at start)")
    ack = await ajs.publish("$KV.time_manifests.acme.readings", b'{"covers_up_to": 1}')
    check(ack.seq > 0, "A4 it writes its tenant's manifest")
    check(await refused(ajs.publish("$KV.time_manifests.beta.readings", b'{"covers_up_to": 1}')),
          "A5 another tenant's manifest: refused")
    check(await refused(ajs.delete_stream("READINGS_acme")) and await refused(ajs.add_stream(StreamConfig(name="X", subjects=["x.>"]))),
          "A6 no stream admin: delete and create refused")
    await nc.close()

    # ── R: the responder ─────────────────────────────────────────────────────
    resp = mint(tmp, "responder", "readings-acme", "acme")
    nc, errs = await connect(creds=resp, inbox="_INBOX.readings-acme")
    rjs = nc.jetstream()
    sub = await rjs.subscribe("sensors.acme.>", stream="READINGS_acme", ordered_consumer=True)
    got = await sub.next_msg(timeout=3)
    check(got is not None, "R1 the responder reads its tenant's stream")
    check(await refused(rjs.subscribe("sensors.beta.>", stream="READINGS_beta", ordered_consumer=True)),
          "R2 another tenant's stream: refused")
    m = await nc.request("$JS.API.DIRECT.GET.KV_time_manifests.$KV.time_manifests.acme.readings", b"", timeout=3)
    check(b"covers_up_to" in m.data, "R3 it reads its tenant's manifest by direct get")
    check(await refused(rjs.publish("$KV.time_manifests.acme.readings", b'{"covers_up_to": 9}')),
          "R4 it cannot write the manifest")
    await nc.close()

    # ── P: the programs themselves, each with its own credential ─────────────
    await programs(tmp, js, arch, resp)
    await svc.close()


async def programs(tmp, js, arch, resp):
    import os
    ex = zb.ROOT / "examples" / "16-archive"
    sys.path.insert(0, str(ex))
    import common
    os.environ["NATS_URL"] = URL          # common.connect() reads it: the scratch server
    os.environ.pop("NATS_CREDS", None)
    # What the bridge makes at boot beside the streams: the buckets libzb reads at connect.
    for b in ("schemas", "tenants", "generations"):
        await js.create_key_value(KeyValueConfig(bucket=b, history=1, direct=True))
    await js.add_stream(StreamConfig(name="VERDICTS", subjects=["mutation_ack.>"], allow_direct=True))
    await common.reset_tenant(js, "acme")
    for i in range(200):
        await js.publish(f"sensors.acme.s{i % 4}.temperature", json.dumps({"value": i, "ts_ms": 1_700_000_000_000 + i}).encode())
    env = dict(os.environ, NATS_URL=URL, PYTHONPATH=str(zb.ROOT / "zb-python" / "src"))
    env.pop("NATS_CREDS", None)
    r = subprocess.run([sys.executable, str(ex / "archiver.py"), "--tenant", "acme", "--flush-s", "1", "--exit-when-idle", "3",
                        "--outage-s", "60"], env=dict(env, NATS_CREDS=str(arch)), capture_output=True, text=True, timeout=120)
    m = (await common.read_manifest(await common.connect(str(arch)), "acme"))[0]
    check(r.returncode == 0 and m and sum(f["rows"] for f in m["files"]) == 200,
          f"P1 archiver.py with the archiver credential: exit {r.returncode}, {sum(f['rows'] for f in m['files']) if m else 0} rows archived for 200")
    if r.returncode != 0:
        print(r.stdout[-800:], r.stderr[-800:])
    cache = tmp / "resp-cache"
    p = subprocess.Popen([sys.executable, str(ex / "responder.py"), "--tenant", "acme", "--creds", str(resp), "--cache", str(cache),
                          "--queue", "probe"], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    answer = {}
    try:
        # The asker is not under test here (clients asking is the serve scenario's): the
        # bridge's own credential asks, and the RESPONDER answers inside its grants.
        cl = await common.connect(str(tmp / "zb-nats" / "creds" / "bridge.creds"))
        for _ in range(40):
            try:
                answer = common.decode_answer((await cl.request("query.acme.count", b"{}", timeout=2)).data)
                break
            except Exception:
                await asyncio.sleep(1)
        await cl.close()
    finally:
        p.terminate()
        out = p.communicate(timeout=10)[0]
    check(answer.get("rows") == 200 and answer.get("hot") == 0,
          f"P2 responder.py with the responder credential answers from Parquet: {answer}")
    if answer.get("rows") != 200:
        print(out[-1500:])

    # P3: the same archive read by bridge_readings_fetch, inside the same grants. The native reader
    # makes its own pull consumer and needs its reply inbox to be the credential's own.
    fetch = zb.ROOT / "zig-out" / "bin" / "bridge_readings_fetch"
    if not fetch.exists():
        print(f"  · P3 skipped: {fetch} is missing (zig build)")
        return
    await common.reset_tenant(js, "acme")
    for i in range(200):
        await js.publish(f"sensors.acme.s{i % 4}.temperature", json.dumps({"value": i, "ts_ms": 1_700_000_000_000 + i}).encode())
    r = subprocess.run([sys.executable, str(ex / "archiver.py"), "--tenant", "acme", "--flush-s", "1", "--exit-when-idle", "3",
                        "--outage-s", "60", "--reader", "native", "--fetch-bin", str(fetch)],
                       env=dict(env, NATS_CREDS=str(arch)), capture_output=True, text=True, timeout=120)
    m = (await common.read_manifest(await common.connect(str(arch)), "acme"))[0]
    got = sum(f["rows"] for f in m["files"]) if m else 0
    check(r.returncode == 0 and got == 200,
          f"P3 archiver.py --reader native with the archiver credential: exit {r.returncode}, {got} rows archived for 200")
    if r.returncode != 0:
        print(r.stdout[-800:], r.stderr[-800:])


def update_old_store(tmp: pathlib.Path):
    """U: a store without the two keys, as a deployment made before them has."""
    nsd = tmp / "zb-nats"
    store = nsd / "operator.store"
    old = [l for l in store.read_text().splitlines(True) if not l.startswith(NEW_LINES)]
    store.write_text("".join(old))
    r = subprocess.run([str(BRIDGE), "--init-nats", "--update", "--dir", str(nsd), "--store", str(store)], capture_output=True, text=True)
    lines = store.read_text().splitlines(True)
    kept = all(l in lines for l in old)
    added = [l for l in lines if l.startswith(NEW_LINES)]
    check(r.returncode == 0 and kept and len(added) == 2 and oct(store.stat().st_mode & 0o777) == "0o600",
          f"U1 --update on an older store: {len(added)} line(s) added, every other line unchanged, the store still 0600")
    if r.returncode != 0:
        print(r.stdout[-500:], r.stderr[-500:])


async def after_update(tmp):
    sensor = jwt_from(mint(tmp, "sensor", "boiler-77", "acme"))
    svc, _ = await connect(creds=tmp / "zb-nats" / "creds" / "bridge.creds")
    js = svc.jetstream()
    before = await count(js, "READINGS_acme")
    nc, _ = await connect(jwt_only=sensor)
    await nc.publish("sensors.acme.boiler-77.temperature", b'{"value": 1}')
    await nc.flush()
    await nc.close()
    await asyncio.sleep(0.3)
    check(await count(js, "READINGS_acme") - before == 1, "U2 after the reload, a sensor minted from the new key publishes")
    await svc.close()


def run():
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb-readings-roles-"))
    ns = None
    try:
        r = subprocess.run([str(BRIDGE), "--init-nats", "operator", "--force", "--port", str(PORT),
                            "--http-port", "18242", "--ws-port", "18092"], cwd=tmp, capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"--init-nats operator failed: {r.stderr[-300:]}")
        conf = tmp / "zb-nats" / "nats-server.conf"
        conf.write_text(conf.read_text() + f"\nserver_name: readings_roles\nmqtt {{\n  port: {MQTT}\n}}\n")
        ns = subprocess.Popen(["nats-server", "-c", str(conf)], stdout=open(tmp / "ns.log", "w"), stderr=subprocess.STDOUT)
        for _ in range(50):
            if subprocess.run(["nc", "-z", "127.0.0.1", str(PORT)], capture_output=True).returncode == 0:
                break
            time.sleep(0.2)
        asyncio.run(main(tmp))
        update_old_store(tmp)
        ns.send_signal(1)   # SIGHUP: reload the conf the update re-signed
        time.sleep(1)
        asyncio.run(after_update(tmp))
    finally:
        if ns:
            ns.terminate()
            ns.wait(10)
        if failed and (tmp / "ns.log").exists():
            print((tmp / "ns.log").read_text()[-1500:])
        shutil.rmtree(tmp, ignore_errors=True)
    print("PASS" if not failed else f"FAIL ({failed})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(run())

#!/usr/bin/env python3
"""The native readings archiver (bridge_archiver, libzbarchive) against the Python one, on a real
nats-server and a local S3 (examples/16-archive, zb-archiver).

  A  parity: the same stream (readings, resends, payloads that are not readings, a skipped-only
     tail) archived by the Python archiver and by the native one gives the same files (keys,
     rows, sequences), the same coverage and the same readings in Parquet;
  B  the two are one archive: the native archiver continues a manifest the Python one wrote
     (and keeps a key it does not know), then the Python one continues the native one's;
  C  killed between the upload and the manifest (--crash-after-upload, exit 9), the manifest is
     unchanged and the next run covers the same readings again, once;
  D  two native archivers at once: one exits 0, the other 3 (the revision refused it);
  E  the stream pruned past the archive: exit 4, the manifest unchanged;
  E2 every reading after the archive purged, the stream left EMPTY (first_seq = last_seq + 1, no
     message): both archivers exit 4, rather than follow the stream from its next sequence and
     certify the missing readings as covered;
  E3 the same by expiry (max_age), the stream empty again: exit 4 from both;
  F  started before its stream exists: it says it waits, then archives;
  G  a stream that keeps readings less than 2 × flush + outage: exit 2;
  H  a wrong S3 secret: exit 1 on the upload (HTTP 403 from an S3 that checks signatures), the
     manifest unchanged;
  I  libzbarchive from a host (ctypes): the same archive, an unknown option refused by name, and
     no leak reported by a Debug build (zba_leaks() == 0);
  J  a Debug bridge_archiver reports no leak at exit (Zig's SafeAllocator prints any);
  L  the WebAssembly core (`zig build archive-core-wasm`, run by Node): its decoder and the
     Python one agree on readings_fetch.py's awkward and generated payloads, and its decisions
     are the native archiver's (skipped without node or the .wasm);
  M  times are UTC on a host that is not: with TZ=Europe/Paris, both archivers store each
     reading's sensor and stream time as the instant sent, and so does the responder's hot tail;
  K  the hosts of examples/16-archive/hosts — C, Go (cgo), Rust — each archive a tenant through
     libzbarchive (each is built here when its toolchain is on the PATH, skipped otherwise).

Needs nats-server on the PATH, a local S3 on 127.0.0.1:8333 (SeaweedFS; its keys in S3_KEYS or
AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY), libduckdb (ZB_DUCKDB_LIB, else the loader's search), nats-py,
boto3 and duckdb, and `zig build archiver readings-fetch archive-core-wasm` (zig-out/bin/bridge_archiver,
zig-out/lib/libzbarchive.so, zig-out/bin/bridge_readings_fetch; ZB_ARCHIVER, ZB_ARCHIVE_LIB and
ZB_READINGS_FETCH override them). Build Debug for the leak checks (I, J): they are skipped on a
release build, which checks nothing.

Usage:  python scripts/scenarios/archiver_native.py
"""
import asyncio, ctypes, json, os, pathlib, shutil, socket, subprocess, sys, tempfile, threading, time

import nats
from nats.js.api import KeyValueConfig, StreamConfig

import zb

REPO = pathlib.Path(__file__).resolve().parents[2]
EX = REPO / "examples" / "16-archive"
sys.path.insert(0, str(EX))
os.environ.setdefault("S3_ENDPOINT", "http://127.0.0.1:8333")
import common  # noqa: E402

NATIVE = pathlib.Path(os.environ.get("ZB_ARCHIVER", REPO / "zig-out" / "bin" / "bridge_archiver"))
LIB = pathlib.Path(os.environ.get("ZB_ARCHIVE_LIB", REPO / "zig-out" / "lib" / ("libzbarchive.dylib" if sys.platform == "darwin" else "libzbarchive.so")))
FETCH = pathlib.Path(os.environ.get("ZB_READINGS_FETCH", REPO / "zig-out" / "bin" / "bridge_readings_fetch"))
WASM = pathlib.Path(os.environ.get("ZB_ARCHIVE_WASM", REPO / "zig-out" / "wasm" / "zb_archive_core.wasm"))
PORT = 14337
URL = f"nats://127.0.0.1:{PORT}"
T0 = 1_700_000_000_000
failed = 0


def check(label, cond):
    global failed
    (zb.ok if cond else zb.bad)(f"{time.strftime('%H:%M:%S')} {label}")
    if not cond:
        failed += 1
    sys.stdout.flush()
    return bool(cond)


def env(**extra):
    e = dict(os.environ, NATS_URL=URL, ZB_READINGS_FETCH=str(FETCH))
    e.pop("NATS_CREDS", None)
    e.update(extra)
    return e


def python_cmd(tenant, *extra):
    return [sys.executable, str(EX / "archiver.py"), "--tenant", tenant, "--flush-s", "1", "--exit-when-idle", "3", "--outage-s", "60", *extra]


def python_archiver(tenant, *extra, wait=True):
    return run(python_cmd(tenant, *extra), env(), wait)


async def parquet_times(nc, tenant):
    """[[sensor ms, stream ms]] of every row of the archive, in stream order, read as instants."""
    import duckdb
    m, _ = await common.read_manifest(nc, tenant)
    out = []
    with tempfile.TemporaryDirectory() as d:
        for f in (m or {}).get("files", []):
            p = pathlib.Path(d) / "f.parquet"
            common.s3().download_file(common.BUCKET, f["key"], str(p))
            out += [list(r) for r in duckdb.sql(f"SELECT epoch_ms(sensor_ts), epoch_ms(received_at) FROM read_parquet('{p}') ORDER BY stream_seq").fetchall()]
    return out


async def stream_ms(js, tenant, seq):
    """The stream's own time of one message in whole ms, truncated (what received_at must hold).
    nats-py gives it as RFC 3339 with nanoseconds; read the digits, not a float."""
    import calendar, datetime, re
    t = (await js.get_msg(common.stream_for(tenant), seq)).time
    if isinstance(t, datetime.datetime):
        return calendar.timegm(t.utctimetuple()) * 1000 + t.microsecond // 1000
    m = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?Z", t)
    base = calendar.timegm(datetime.datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").timetuple())
    return base * 1000 + int((m.group(2) or "0").ljust(3, "0")[:3])


RESPONDER_PROBE = """
import json, pathlib, sys, tempfile
sys.path.insert(0, sys.argv[1])
import responder
r = responder.Readings(pathlib.Path(tempfile.mkdtemp()))
r.add([{"tenant": "zone", "sensor_id": "s1", "kind": "temp", "value": 1.0, "sensor_ms": 1700000000000, "received_ms": 5, "stream_seq": 1},
       {"tenant": "zone", "sensor_id": "s1", "kind": "temp", "value": 2.0, "sensor_ms": 253402300799999, "received_ms": 6, "stream_seq": 2}])
print(json.dumps(r.query("SELECT epoch_ms(sensor_ts), stream_seq FROM readings ORDER BY stream_seq")["rows"]))
"""


def responder_hot_times(tenant):
    """The responder's hot tail, filled in a child process with TZ=Europe/Paris (DuckDB takes TZ at connect)."""
    e = env(TZ="Europe/Paris", PYTHONPATH=str(REPO / "zb-python" / "src"))
    r = subprocess.run([sys.executable, "-c", RESPONDER_PROBE, str(EX)], env=e, capture_output=True, text=True, timeout=120)
    if r.returncode:
        return f"failed: {r.stderr.strip().splitlines()[-1] if r.stderr.strip() else r.returncode}"
    return json.loads(r.stdout)


def native_archiver(tenant, *extra, wait=True, **env_extra):
    cmd = [str(NATIVE), "--tenant", tenant, "--flush-s", "1", "--exit-when-idle", "3", "--outage-s", "60", *extra]
    return run(cmd, env(**env_extra), wait)


def run(cmd, e, wait):
    if not wait:
        return subprocess.Popen(cmd, env=e, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    r = subprocess.run(cmd, env=e, capture_output=True, text=True, timeout=300)
    return r.returncode, r.stdout, r.stderr


def leaked(stderr: str) -> bool:
    """Zig's SafeAllocator (Debug, ReleaseSafe) logs each leak as `error(gpa): memory address … leaked`."""
    return "leaked" in stderr


def debug_build(path: pathlib.Path) -> bool:
    """A Debug or ReleaseSafe Zig binary carries the safety panic strings; ReleaseFast strips them."""
    return b"reached unreachable code" in path.read_bytes()


async def publish_mix(nc, tenant, n, start_ms):
    """n readings over 7 sensors, one resend of the first, two payloads that are not readings."""
    for i in range(n):
        await nc.publish(f"sensors.{tenant}.s{i % 7}.temp", json.dumps({"value": i * 0.5, "ts_ms": start_ms + i}).encode())
    await nc.publish(f"sensors.{tenant}.s0.temp", json.dumps({"value": -1, "ts_ms": start_ms}).encode())
    await nc.publish(f"sensors.{tenant}.s0.temp", b'"21.5"')
    await nc.publish(f"sensors.{tenant}.s0.temp", b'{"value": 1}')
    await nc.flush()


async def settle(js, tenant, last):
    for _ in range(200):
        if (await js.stream_info(common.stream_for(tenant))).state.last_seq >= last:
            return
        await asyncio.sleep(0.05)


async def archive_rows(nc, tenant):
    """(manifest, the rows of every file in it) — rows as (sensor, kind, sensor ms, value, seq)."""
    import duckdb
    m, _ = await common.read_manifest(nc, tenant)
    if not m:
        return None, []
    s3 = common.s3()
    rows = []
    with tempfile.TemporaryDirectory() as d:
        for f in m["files"]:
            p = pathlib.Path(d) / "f.parquet"
            s3.download_file(common.BUCKET, f["key"], str(p))
            if p.stat().st_size != f["bytes"]:
                rows.append(("size differs", f["key"]))
            rows += duckdb.sql(f"SELECT sensor_id, kind, epoch_ms(sensor_ts), value, stream_seq FROM read_parquet('{p}') ORDER BY stream_seq").fetchall()
    return m, rows


def shape(m):
    return m and (m["covers_up_to"], [(f["key"], f["rows"], f["first_seq"], f["last_seq"]) for f in m["files"]])


async def reset_archive(js, tenant):
    """The manifest and the files, not the stream: what a second archiver of the same stream needs."""
    await js.publish(f"$KV.{common.KV}.{common.manifest_key(tenant)}", b"", headers={"KV-Operation": "PURGE", "Nats-Rollup": "sub"})
    s3 = common.s3()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=common.BUCKET, Prefix=f"{tenant}/{common.TABLE}/"):
        for o in page.get("Contents", []):
            s3.delete_object(Bucket=common.BUCKET, Key=o["Key"])


async def main(tmp):
    srv = subprocess.Popen(["nats-server", "-js", "-p", str(PORT), "-sd", str(tmp / "js")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):   # the port first: nats-py prints every refused connect
            try:
                socket.create_connection(("127.0.0.1", PORT), timeout=1).close()
                break
            except OSError:
                await asyncio.sleep(0.1)
        nc = await nats.connect(URL)
        js = nc.jetstream()
        try:
            await js.create_key_value(KeyValueConfig(bucket=common.KV, history=10, direct=True))
        except Exception:
            pass
        debug = debug_build(NATIVE)

        # A ── parity
        t = "parity"
        await common.ensure_for_tests(js, t)
        await common.reset_tenant(js, t)
        await publish_mix(nc, t, 1000, T0)
        await settle(js, t, 1003)
        code_p, out_p, err_p = python_archiver(t, "--batch", "300")
        m_py, rows_py = await archive_rows(nc, t)
        await reset_archive(js, t)
        code_n, out_n, err_n = native_archiver(t, "--batch", "300")
        m_na, rows_na = await archive_rows(nc, t)
        check(f"A both archivers exit 0 (python {code_p}, native {code_n})", code_p == 0 and code_n == 0)
        if code_n:
            print(out_n[-1500:], err_n[-1500:])
        check(f"A the same files, rows and coverage: {shape(m_na) and len(shape(m_na)[1])} file(s) to {m_na and m_na['covers_up_to']}",
              shape(m_py) == shape(m_na) and m_na["covers_up_to"] == 1003)
        check(f"A the same readings in Parquet ({len(rows_na)} rows, {len(set(r[:3] for r in rows_na))} distinct keys of 1000)",
              rows_py == rows_na and len(set(r[:3] for r in rows_na)) == 1000)
        check("J a Debug bridge_archiver reports no leak at exit" if debug else "J skipped: a release build checks no leak", not leaked(err_n))

        # B ── one archive, two implementations (each batch's resend falls in its own chunk: kept once)
        t = "interop"
        await common.ensure_for_tests(js, t)
        await common.reset_tenant(js, t)
        await publish_mix(nc, t, 200, T0)
        await settle(js, t, 203)
        python_archiver(t)
        m, rev = await common.read_manifest(nc, t)
        m["note"] = {"by": "ops"}
        await common.write_manifest(js, t, m, rev)
        await publish_mix(nc, t, 200, T0 + 10_000)
        await settle(js, t, 406)
        code, out, err = native_archiver(t)
        m2, _ = await common.read_manifest(nc, t)
        check(f"B the native archiver continues the Python one's manifest (exit {code}, covers {m2 and m2['covers_up_to']}, {m2 and len(m2['files'])} files) and keeps its unknown key",
              code == 0 and m2["covers_up_to"] == 406 and len(m2["files"]) == 2 and m2.get("note") == {"by": "ops"})
        await publish_mix(nc, t, 50, T0 + 20_000)
        await settle(js, t, 459)
        code, _, _ = python_archiver(t)
        m3, rows = await archive_rows(nc, t)
        check(f"B and the Python one continues the native one's (exit {code}, covers {m3 and m3['covers_up_to']}, {len(rows)} rows)",
              code == 0 and m3["covers_up_to"] == 459 and len(m3["files"]) == 3 and len(rows) == 450 and len(set(r[:3] for r in rows)) == 450)

        # C ── a kill between the upload and the manifest
        t = "crash"
        await common.ensure_for_tests(js, t)
        await common.reset_tenant(js, t)
        await publish_mix(nc, t, 100, T0)
        await settle(js, t, 103)
        code, out, _ = native_archiver(t, "--crash-after-upload")
        m, _ = await common.read_manifest(nc, t)
        check(f"C killed after the upload: exit {code}, the manifest not written", code == 9 and m is None and "flushed" in out)
        code, out, _ = native_archiver(t)
        m, rows = await archive_rows(nc, t)
        check(f"C the next run covers the same readings once: exit {code}, {len(rows)} rows, covers {m and m['covers_up_to']}",
              code == 0 and m["covers_up_to"] == 103 and len(m["files"]) == 1 and len(set(r[:3] for r in rows)) == 100 and len(rows) == 100)

        # D ── two at once
        t = "compete"
        await common.ensure_for_tests(js, t)
        await common.reset_tenant(js, t)
        await publish_mix(nc, t, 300, T0)
        await settle(js, t, 303)
        ps = [native_archiver(t, "--flush-s", "2", "--exit-when-idle", "6", "--batch", "50", wait=False) for _ in range(2)]
        outs = [p.communicate(timeout=120) for p in ps]
        codes = sorted(p.returncode for p in ps)
        m, rows = await archive_rows(nc, t)
        check(f"D two archivers: exits {codes} (3 = the revision refused it), {len(set(r[:3] for r in rows))} distinct readings of 300",
              codes == [0, 3] and len(set(r[:3] for r in rows)) == 300 and m["covers_up_to"] == 303)
        if codes != [0, 3]:
            for o in outs:
                print(o[0][-800:], o[1][-800:])

        # E ── the stream pruned past the archive
        await publish_mix(nc, t, 20, T0 + 6000)
        info = await js.stream_info(common.stream_for(t))
        await js.purge_stream(common.stream_for(t), seq=info.state.last_seq)
        before, rev_before = await common.read_manifest(nc, t)
        code, out, err = native_archiver(t)
        after, rev_after = await common.read_manifest(nc, t)
        check(f"E the stream pruned past the archive: exit {code} — {(out + err).strip().splitlines()[-1][:110] if (out + err).strip() else ''}",
              code == 4 and rev_after == rev_before)

        # E2 ── the whole unarchived suffix purged: an EMPTY stream past the archive
        # E3 ── the same by expiry
        for label, how in (("E2", "purge"), ("E3", "expiry")):
            t = f"empty{how}"
            try:
                await js.delete_stream(common.stream_for(t))
            except Exception:
                pass
            await js.add_stream(StreamConfig(name=common.stream_for(t), subjects=[common.subject_filter(t)],
                                             max_age=(2 if how == "expiry" else 24 * 3600)))
            await reset_archive(js, t)
            await publish_mix(nc, t, 4, T0)                       # 1..7: four readings, a resend, two skipped
            await settle(js, t, 7)
            opts = ("--flush-s", "0", "--outage-s", "0.5") if how == "expiry" else ()
            code, out, err = native_archiver(t, *opts)
            covered, rev = await common.read_manifest(nc, t)
            await publish_mix(nc, t, 6, T0 + 1000)                # 8..16, never archived
            await settle(js, t, 16)
            if how == "purge":
                await js.purge_stream(common.stream_for(t))
            else:
                await asyncio.sleep(3)                            # max_age 2 s: every message expires
            st = (await js.stream_info(common.stream_for(t))).state
            results = {}
            for name, run_one in (("native", native_archiver), ("python", python_archiver)):
                code, out, err = run_one(t, *opts)
                after, rev_after = await common.read_manifest(nc, t)
                results[name] = (code, rev_after == rev, (out + err).strip().splitlines()[-1][:90] if (out + err).strip() else "")
            check(f"{label} the stream {'purged' if how == 'purge' else 'expired'} past the archive ({covered and covered['covers_up_to']}), left empty (first {st.first_seq}, last {st.last_seq}, "
                  f"{st.messages} messages): {results}",
                  covered and covered["covers_up_to"] == 7 and st.messages == 0 and st.first_seq == 17
                  and all(r[0] == 4 and r[1] for r in results.values()))

        # F ── started before its stream
        t = "early"
        try:
            await js.delete_stream(common.stream_for(t))
        except Exception:
            pass
        p = native_archiver(t, "--exit-when-idle", "4", wait=False)
        await asyncio.sleep(3)
        await common.ensure_for_tests(js, t)
        await common.reset_tenant(js, t)
        await nc.publish(f"sensors.{t}.s1.temperature", b"21.5")
        await nc.flush()
        out, err = p.communicate(timeout=60)
        check(f"F started before its stream: waited, then archived (exit {p.returncode})",
              p.returncode == 0 and "waiting for" in out and "flushed 1 reading" in out)

        # G ── retention too short for 2 × flush + outage
        t = "brief"
        try:
            await js.delete_stream(common.stream_for(t))
        except Exception:
            pass
        await js.add_stream(StreamConfig(name=common.stream_for(t), subjects=[common.subject_filter(t)], max_age=100))
        code, out, err = native_archiver(t, "--flush-s", "30", "--outage-s", "60")
        check(f"G a stream that keeps readings 100 s for 2 × 30 + 60: exit {code} — {out.strip().splitlines()[-1][:100] if out.strip() else err[-100:]}",
              code == 2 and "needs 120 s" in out)

        # H ── a wrong secret on an S3 that checks signatures
        t = "badkey"
        await common.ensure_for_tests(js, t)
        await common.reset_tenant(js, t)
        await publish_mix(nc, t, 10, T0)
        await settle(js, t, 13)
        key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        if not key_id:
            keys = json.loads(pathlib.Path(os.environ.get("S3_KEYS", pathlib.Path.home() / ".config/zebridge/s3.json")).read_text())
            key_id = keys["identities"][0]["credentials"][0]["accessKey"] if "identities" in keys else keys["access_key_id"]
        code, out, err = native_archiver(t, AWS_ACCESS_KEY_ID=key_id, AWS_SECRET_ACCESS_KEY="not-the-secret")
        m, _ = await common.read_manifest(nc, t)
        check(f"H a wrong secret: exit {code}, the manifest unchanged — {out.strip().splitlines()[-1][:110] if out.strip() else ''}",
              code == 1 and m is None and "upload failed" in out)

        # I ── libzbarchive from a host
        t = "host"
        await common.ensure_for_tests(js, t)
        await common.reset_tenant(js, t)
        await publish_mix(nc, t, 120, T0)
        await settle(js, t, 123)
        code, lines, refused, leaks = await asyncio.to_thread(run_library, t)
        m, rows = await archive_rows(nc, t)
        check(f"I libzbarchive from ctypes: exit {code}, {len(lines)} line(s) through the callback, covers {m and m['covers_up_to']}",
              code == 0 and m["covers_up_to"] == 123 and len(set(r[:3] for r in rows)) == 120 and any("flushed" in l for l in lines))
        check(f"I an unknown option is refused by name: {refused[:80]}", "UnknownField" in refused and "keys: tenant" in refused)
        lib_debug = debug_build(LIB)
        check(f"I a Debug libzbarchive reports no leak (zba_leaks() = {leaks})" if lib_debug else "I leak check skipped: a release library checks nothing",
              leaks == 0)
        # M ── UTC times on a host that is not UTC
        t = "zone"
        await common.ensure_for_tests(js, t)
        await common.reset_tenant(js, t)
        await nc.publish(f"sensors.{t}.s1.temp", json.dumps({"value": 1, "ts_ms": T0}).encode())
        await nc.publish(f"sensors.{t}.s1.temp", json.dumps({"value": 2, "ts_ms": 253402300799999}).encode())
        await nc.flush()
        await settle(js, t, 2)
        times = {}
        for name, run_one in (("native", native_archiver), ("python", python_archiver)):
            await reset_archive(js, t)
            if name == "native":
                code, out, err = native_archiver(t, TZ="Europe/Paris")
            else:
                code, out, err = run(python_cmd(t), env(TZ="Europe/Paris"), True)
            times[name] = (code, await parquet_times(nc, t))
        recv = [await stream_ms(js, t, seq) for seq in (1, 2)]
        want = [[T0, recv[0]], [253402300799999, recv[1]]]
        check(f"M with TZ=Europe/Paris both archivers store the instants sent: {times} (want {want})",
              all(code == 0 and got == want for code, got in times.values()))
        hot = await asyncio.to_thread(responder_hot_times, t)
        check(f"M the responder's hot tail with TZ=Europe/Paris: {hot}", hot == [[T0, 1], [253402300799999, 2]])

        # L ── the WebAssembly core
        wasm_parity()

        # K ── C, Go and Rust hosts
        for name, binary in build_hosts(tmp):
            t = f"host{name}"
            await common.ensure_for_tests(js, t)
            await common.reset_tenant(js, t)
            await publish_mix(nc, t, 60, T0)
            await settle(js, t, 63)
            cfg = json.dumps({"tenant": t, "flush_s": 1, "exit_when_idle_s": 3, "outage_s": 60})
            r = subprocess.run([str(binary), cfg], env=env(LD_LIBRARY_PATH=str(LIB.parent), DYLD_LIBRARY_PATH=str(LIB.parent)),
                               capture_output=True, text=True, timeout=120)
            m, rows = await archive_rows(nc, t)
            check(f"K the {name} host archives through libzbarchive: exit {r.returncode}, covers {m and m['covers_up_to']}, {len(rows)} rows",
                  r.returncode == 0 and m["covers_up_to"] == 63 and len(set(x[:3] for x in rows)) == 60 and f"[{name} host] flushed" in r.stdout)
            if r.returncode:
                print(r.stdout[-800:], r.stderr[-800:])
        await nc.close()
    finally:
        srv.terminate()
        try:
            srv.wait(timeout=10)
        except subprocess.TimeoutExpired:
            srv.kill()


WASM_DRIVER = r"""
const fs = require('fs');
const w = new WebAssembly.Instance(new WebAssembly.Module(fs.readFileSync(process.argv[process.argv.length - 1])), {}).exports;
const put = (b) => { const p = w.zba_alloc(b.length || 1); new Uint8Array(w.memory.buffer, p, b.length).set(b); return [p, b.length]; };
const get = (r) => r ? Buffer.from(new Uint8Array(w.memory.buffer, Number(r >> 32n), Number(r & 0xffffffffn))).toString() : null;
const out = [];
for (const line of fs.readFileSync(0, 'utf8').split('\n')) {
  if (!line) continue;
  const c = JSON.parse(line);
  const [sp, sl] = put(Buffer.from(c.subject, 'utf8')), [dp, dl] = put(Buffer.from(c.data, 'base64'));
  const r = get(w.zba_decode(sp, sl, dp, dl, BigInt(c.received_ms)));
  out.push(r === null ? null : JSON.parse(r));
  w.zba_reset();
}
const d = {
  retention: [w.zba_retention_short(3719n * 1000000000n, 60, 3600), w.zba_retention_short(3720n * 1000000000n, 60, 3600), w.zba_retention_short(0n, 60, 3600)],
  start: [w.zba_start_at(1, 4n, 6n, 0), w.zba_start_at(1, 4n, 6n, 1), w.zba_start_at(0, 0n, 6n, 0), w.zba_start_at(1, 4n, 11n, 0)].map(Number),
  chunk: [w.zba_check_chunk(0n, 0n, 0n, 0n, 0n, 0), w.zba_check_chunk(1n, 1n, 5n, 5n, 0n, 0), w.zba_check_chunk(2n, 2n, 1n, 3n, 0n, 1),
          w.zba_check_chunk(2n, 1n, 1n, 2n, 0n, 0), w.zba_check_chunk(1n, 0n, 1n, 1n, 0n, 0)],
};
const [tp, tl] = put(Buffer.from('acme')); d.key = get(w.zba_file_key(tp, tl, 1700000000000n, 1n, 3n)); w.zba_reset();
console.log(JSON.stringify({ rows: out, decisions: d }));
"""


def wasm_parity():
    if not shutil.which("node") or not WASM.exists():
        print(f"  · L skipped: {'no node' if not shutil.which('node') else f'no {WASM} (zig build archive-core-wasm)'}")
        return
    import base64, random, types
    import readings_fetch as rf   # its payload sets: the ones the native reader is proven against
    payloads = list(rf.AWKWARD) + rf.generated(random.Random(7), 2000)
    cases, expected = [], []
    for i, (suffix, body) in enumerate(payloads):
        received_ms = T0 + i
        subject = f"sensors.acme.{suffix}"
        msg = types.SimpleNamespace(subject=subject, data=body, reply=f"$JS.ACK.READINGS_acme.c.1.{i + 1}.{i + 1}.{received_ms * 1_000_000}.0",
                                    metadata=types.SimpleNamespace(sequence=types.SimpleNamespace(stream=i + 1)))
        r = common.reading(msg)
        expected.append(r and {"tenant": r["tenant"], "sensor_id": r["sensor_id"], "kind": r["kind"], "value": r["value"], "sensor_ms": r["sensor_ms"]})
        cases.append(json.dumps({"subject": subject, "data": base64.b64encode(body).decode(), "received_ms": received_ms}))
    r = subprocess.run(["node", "-e", WASM_DRIVER, str(WASM)], input="\n".join(cases) + "\n", capture_output=True, text=True, timeout=120)
    if not check("L the wasm core runs under Node" + ("" if r.returncode == 0 else f": {r.stderr[-300:]}"), r.returncode == 0):
        return
    got = json.loads(r.stdout)
    # Zig prints a large double as its digits (1.2345678901234567e19 as 12345678901234567000),
    # which Python reads back as an exact int: the same double once read as one.
    for g in got["rows"]:
        if g is not None:
            g["value"] = float(g["value"])
    diffs = [(i, payloads[i], e, g) for i, (e, g) in enumerate(zip(expected, got["rows"])) if e != g]
    for d in diffs[:5]:
        print(f"      {d[1]!r}: python {d[2]}  wasm {d[3]}")
    check(f"L the wasm decoder and the Python one: {len(payloads):,} payloads, {sum(e is not None for e in expected):,} readings, {len(diffs)} differ", not diffs)
    d = got["decisions"]
    check(f"L the wasm decisions are the native archiver's: {d}",
          d["retention"] == [3720, 0, 0] and d["start"] == [0, 6, 6, 0] and d["chunk"] == [0, 1, 2, 3, 4]
          and d["key"] == "acme/readings/2023-11-14/000000000001-000000000003.parquet")


def build_hosts(tmp):
    """(name, executable) for each host whose toolchain is here; the header from include/."""
    hosts = EX / "hosts"
    inc, libdir = REPO / "include", LIB.parent
    out = []
    if shutil.which("cc"):
        exe = tmp / "zba-c"
        r = subprocess.run(["cc", "-Wall", "-I", str(inc), str(hosts / "c" / "archive.c"), "-L", str(libdir), "-lzbarchive", "-o", str(exe)], capture_output=True, text=True)
        check("K the C host builds" + ("" if r.returncode == 0 else f": {r.stderr[-200:]}"), r.returncode == 0) and out.append(("c", exe))
    else:
        print("  · K skipped for C: no cc")
    if shutil.which("go"):
        exe = tmp / "zba-go"
        r = subprocess.run(["go", "build", "-o", str(exe), "."], cwd=hosts / "go", capture_output=True, text=True,
                           env=dict(os.environ, CGO_CFLAGS=f"-I{inc}", CGO_LDFLAGS=f"-L{libdir}", GOFLAGS="-mod=mod"))
        check("K the Go host builds" + ("" if r.returncode == 0 else f": {r.stderr[-200:]}"), r.returncode == 0) and out.append(("go", exe))
    else:
        print("  · K skipped for Go: no go")
    if shutil.which("cargo"):
        r = subprocess.run(["cargo", "build", "-q", "--manifest-path", str(hosts / "rust" / "Cargo.toml"), "--target-dir", str(tmp / "rust")],
                           capture_output=True, text=True, env=dict(os.environ, ZBARCHIVE_LIB_DIR=str(libdir)))
        check("K the Rust host builds" + ("" if r.returncode == 0 else f": {r.stderr[-200:]}"), r.returncode == 0) and out.append(("rust", tmp / "rust" / "debug" / "zba-rust"))
    else:
        print("  · K skipped for Rust: no cargo")
    return out


def run_library(tenant):
    """zba_open / zba_run / zba_close through ctypes, the log lines collected by the callback."""
    lib = ctypes.CDLL(str(LIB))
    LOG = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_uint8, ctypes.c_char_p)
    lib.zba_open.restype = ctypes.c_void_p
    lib.zba_open.argtypes = [ctypes.c_char_p, LOG, ctypes.c_void_p]
    lib.zba_last_error.restype = ctypes.c_char_p
    lib.zba_run.argtypes = [ctypes.c_void_p]
    lib.zba_close.argtypes = [ctypes.c_void_p]
    lib.zba_leaks.restype = ctypes.c_uint32
    lines = []
    cb = LOG(lambda _ctx, _level, line: lines.append(line.decode()))
    assert not lib.zba_open(json.dumps({"tenant": tenant, "flush": 1}).encode(), cb, None)
    refused = lib.zba_last_error().decode()
    os.environ.update(NATS_URL=URL)
    os.environ.pop("NATS_CREDS", None)
    h = lib.zba_open(json.dumps({"tenant": tenant, "flush_s": 1, "exit_when_idle_s": 3, "outage_s": 60}).encode(), cb, None)
    assert h, lib.zba_last_error()
    code = lib.zba_run(h)
    lib.zba_close(h)
    return code, lines, refused, lib.zba_leaks()


def ready():
    missing = [str(p) for p in (NATIVE, LIB, FETCH) if not p.exists()]
    if missing:
        print(f"✗ missing {', '.join(missing)}: zig build archiver readings-fetch archive-core-wasm")
        return False
    if not shutil.which("nats-server"):
        print("✗ nats-server is not on the PATH")
        return False
    probe = socket.socket()
    probe.settimeout(1)
    try:
        probe.connect(("127.0.0.1", 8333))
    except OSError:
        print("✗ no S3 on 127.0.0.1:8333 (SeaweedFS: scripts/dev or scripts/linux)")
        return False
    finally:
        probe.close()
    return True


if __name__ == "__main__":
    if not ready():
        sys.exit(1)
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb-an-"))
    try:
        asyncio.run(main(tmp))
    except Exception as e:
        import traceback
        traceback.print_exc()
        check(f"raised: {type(e).__name__}: {str(e)[:300]}", False)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print()
    if failed:
        zb.bad(f"{failed} check(s) failed")
        sys.exit(1)
    zb.ok("the native archiver writes the Python archiver's archive, and keeps its recovery rules")

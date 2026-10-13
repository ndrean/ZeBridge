#!/usr/bin/env python3
"""Readings to Parquet, and a responder over Parquet + the live stream (examples/16-archive).

Sensors publish on `sensors.<tenant>.<sensor>.<kind>`; the stream READINGS_<tenant> keeps them. The
archiver writes them to Parquet in S3, then moves the manifest (NATS KV) forward. The
responder answers from the Parquet files the manifest names plus the stream after it.

  A  everything published is archived once: the Parquet rows equal the readings sent;
  B  an archiver killed between the upload and the manifest loses nothing and duplicates
     nothing: the next one covers the same readings again, and resent readings (same sensor,
     same time) are kept once;
  C  two archivers at once: the second one's manifest write fails on the revision, it stops;
  D  the stream pruned past the archive: the archiver refuses to start, rather than leave a
     hole nobody sees;
  E  the responder counts every reading once while the boundary moves: new readings show in
     the hot tail, and after the archiver's flush they are counted from Parquet instead;
  F  started before its tenant's stream exists, the archiver says it waits (no traceback),
     and archives as soon as the stream appears.

Needs: the dev stack (NATS), local S3 (SeaweedFS on 127.0.0.1:8333, keys in
~/.config/zebridge/s3.json), boto3 and duckdb in the venv.

Usage:  scripts/scenarios/.venv/bin/python scripts/scenarios/archive.py
"""
import asyncio, json, os, pathlib, subprocess, sys, time

import nats
import zb

REPO = pathlib.Path(__file__).resolve().parents[2]
EX = REPO / "examples" / "16-archive"
sys.path.insert(0, str(EX))
import common  # noqa: E402

TENANT = "_default"
NC: list = []
PY = sys.executable
ENV = dict(os.environ, NATS_URL=zb.nats_server(), NATS_CREDS=zb.creds_for("bridge"),
           PYTHONPATH=str(REPO / "zb-python" / "src"))


def archiver(*extra, wait=True):
    cmd = [PY, str(EX / "archiver.py"), "--tenant", TENANT, "--flush-s", "1", "--exit-when-idle", "3", *extra]
    if not wait:
        return subprocess.Popen(cmd, env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    r = subprocess.run(cmd, env=ENV, capture_output=True, text=True, timeout=120)
    return r.returncode, r.stdout + r.stderr


async def publish(js, n: int, start_ts: int, resend: int = 0) -> int:
    """n readings, one ms apart, over 10 sensors; then `resend` of them again (same ts)."""
    for i in range(n):
        await js.publish(f"sensors.{TENANT}.s{i % 10}.temperature",
                         json.dumps({"value": 20 + i % 7, "ts_ms": start_ts + i}).encode())
    for i in range(resend):
        await js.publish(f"sensors.{TENANT}.s{i % 10}.temperature",
                         json.dumps({"value": 20 + i % 7, "ts_ms": start_ts + i}).encode())
    return n


async def manifest() -> dict | None:
    return (await common.read_manifest(NC[0], TENANT))[0]


async def parquet_rows() -> int:
    m = await manifest()
    if not m:
        return 0
    files = common.fetch_files(m["files"], pathlib.Path("/private/tmp/claude-501/archive-check"))
    import duckdb
    return duckdb.sql(f"SELECT count(*) FROM read_parquet({[str(f) for f in files]!r})").fetchone()[0]


async def main() -> int:
    ok = True

    def check(good, label):
        nonlocal ok
        ok &= bool(good)
        print(f"{'✓' if good else '✗'} {label}", flush=True)

    nc = await nats.connect(zb.nats_server(), user_credentials=zb.creds_for("bridge"))
    js = nc.jetstream()
    await common.ensure_for_tests(js, TENANT)   # what the bridge makes at boot
    NC.append(nc)
    await common.reset_tenant(js, TENANT)   # a clean slate: manifest, files, stream subjects
    t0 = int(time.time() * 1000)
    sent = 0

    # A
    sent += await publish(js, 3000, t0)
    code, out = archiver()
    m = await manifest()
    rows = await parquet_rows()
    check(code == 0 and rows == sent, f"A {sent} readings sent, {rows} in Parquet ({len(m['files']) if m else 0} file(s)), archiver exit {code}")
    if code != 0:
        print(out[-2000:])

    # B
    sent += await publish(js, 500, t0 + 3000, resend=50)
    before = await manifest()
    code, out = archiver("--crash-after-upload")
    after = await manifest()
    check(code == 9 and after == before, f"B1 killed after the upload (exit {code}): the manifest did not move")
    code, out = archiver()
    rows = await parquet_rows()
    check(code == 0 and rows == sent, f"B2 the next archiver: {rows} rows in Parquet for {sent} distinct readings (50 resent, kept once)")

    # C
    a1 = archiver("--flush-s", "2", "--exit-when-idle", "6", wait=False)
    a2 = archiver("--flush-s", "2", "--exit-when-idle", "6", wait=False)
    await asyncio.sleep(2)
    sent += await publish(js, 200, t0 + 4000)
    codes = sorted([a1.wait(60), a2.wait(60)])
    rows = await parquet_rows()
    check(codes == [0, 3] and rows == sent, f"C two archivers: exits {codes} (3 = the revision refused it), {rows} rows for {sent}")

    # E (before D, which prunes the stream)
    resp = subprocess.Popen([PY, str(EX / "responder.py"), "--tenant", TENANT, "--queue", "archiveprobe",
                             "--creds", zb.creds_for("pois"), "--cache", "/private/tmp/claude-501/archive-resp"],
                            env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    asker = await nats.connect(zb.nats_server(), user_credentials=zb.creds_for("omar"), inbox_prefix=b"_INBOX.omar")

    async def count():
        for _ in range(30):
            try:
                r = await asker.request(f"query.{TENANT}.count", b"{}", timeout=2)
                return common.decode_answer(r.data)
            except (nats.errors.NoRespondersError, nats.errors.TimeoutError):
                await asyncio.sleep(1)
        return {}

    try:
        a = await count()
        check(a.get("rows") == sent and a.get("hot") == 0, f"E1 the responder from Parquet: {a}")
        sent += await publish(js, 100, t0 + 5000)
        await asyncio.sleep(1)
        a = await count()
        check(a.get("rows") == sent and a.get("hot") == 100, f"E2 100 new readings, in the hot tail: {a}")
        code, _ = archiver()
        for _ in range(20):
            a = await count()
            if a.get("hot") == 0:
                break
            await asyncio.sleep(0.5)
        check(code == 0 and a.get("rows") == sent and a.get("hot") == 0, f"E3 after the flush, from Parquet, counted once: {a}")
    finally:
        await asker.close()
        resp.terminate()
        out = resp.communicate(timeout=10)[0]
        if not ok:
            print(out[-3000:])

    # D
    await publish(js, 20, t0 + 6000)
    stream = common.stream_for(TENANT)
    info = await js.stream_info(stream)
    await js.purge_stream(stream, seq=info.state.last_seq)
    code, out = archiver()
    check(code == 4, f"D the stream pruned past the archive: archiver exit {code} — {out.strip().splitlines()[-1] if out.strip() else ''}")

    # F
    early = "zzwait"
    try:
        await js.delete_stream(common.stream_for(early))
    except Exception:
        pass
    p = archiver("--tenant", early, "--exit-when-idle", "4", wait=False)   # the last --tenant wins
    await asyncio.sleep(3)
    await common.ensure_for_tests(js, early)
    await common.reset_tenant(js, early)
    await js.publish(f"sensors.{early}.s1.temperature", b"21.5")
    out = p.communicate(timeout=60)[0]
    check(p.returncode == 0 and "waiting for" in out and "Traceback" not in out and "flushed 1 reading" in out,
          f"F started before its stream: waited, then archived (exit {p.returncode})")
    if p.returncode != 0 or "Traceback" in out:
        print(out[-1500:])
    await common.reset_tenant(js, early)
    await js.delete_stream(common.stream_for(early))

    await common.reset_tenant(js, TENANT)
    await nc.close()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

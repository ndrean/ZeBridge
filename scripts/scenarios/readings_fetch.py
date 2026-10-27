#!/usr/bin/env python3
"""The native reader of a readings stream (bridge_readings_fetch) against the Python one.

The archiver reads its tenant's stream with `bridge_readings_fetch` (Zig, an order of magnitude
faster than nats-py) and turns the CSV into Parquet with DuckDB. The Python side still decodes
the responder's hot tail with `common.reading()`, so the two must agree on what a reading is,
message by message, or the same reading is archived but not counted (or counted but not archived).

  A  awkward payloads: every message is a row in BOTH or in NEITHER, and the rows are equal
     (value bit for bit, sensor time, received time, stream sequence);
  B  a few thousand generated payloads (number formats, junk, nesting) the same way;
  C  a sensor id or kind with a comma or a quote survives the CSV;
  D  chunks: `--max` pieces cover the stream with no gap and no overlap, and `last_seq`
     moves past messages that were not readings;
  E  an empty range is an answer, not an error; so is a start beyond the end;
  F  a stream that does not exist exits 2;
  G  a stream pruned past `--from` reports `first_seq` above it (the archiver refuses to go on);
     a hole inside a chunk, including across pull batches, fails instead of certifying coverage;
  H  the dedupe done in SQL keeps what the archiver's Python dict kept (the latest arrival).

  I  the whole native archiver with two chunk sizes leaves the same logical readings in Parquet (needs a local S3, SeaweedFS on 127.0.0.1:8333, and boto3; skipped
     with a note without one).

Needs nats-server on the PATH, nats-py and duckdb, and `zig build` (zig-out/bin/bridge_readings_fetch,
or ZB_READINGS_FETCH). No PostgreSQL.

Usage:  python scripts/scenarios/readings_fetch.py
"""
import asyncio, csv, json, os, pathlib, random, shutil, subprocess, sys, tempfile, time

import nats
from nats.js.api import ConsumerConfig, DeliverPolicy, StreamConfig

import zb

REPO = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples" / "16-archive"))
os.environ.setdefault("S3_ENDPOINT", "http://127.0.0.1:1")
import common  # noqa: E402

FETCH = pathlib.Path(os.environ.get("ZB_READINGS_FETCH", REPO / "zig-out" / "bin" / "bridge_readings_fetch"))
TENANT = "acme"
STREAM = common.stream_for(TENANT)
failed = 0


def check(label, cond):
    global failed
    (zb.ok if cond else zb.bad)(f"{time.strftime('%H:%M:%S')} {label}")
    if not cond: failed += 1
    sys.stdout.flush()
    return bool(cond)


def fetch(url, tenant, start, out, mx=100_000, extra=()):
    r = subprocess.run([str(FETCH), "--tenant", tenant, "--from", str(start), "--out", str(out), "--max", str(mx), "--idle-ms", "300", *extra],
                       env=dict(os.environ, NATS_URL=url), capture_output=True, text=True, errors="replace", timeout=120)
    return r.returncode, (json.loads(r.stdout) if r.returncode == 0 and r.stdout.strip() else None), r.stderr


def read_csv(path):
    with open(path, newline="") as f:
        return [dict(zip(("tenant", "sensor_id", "kind", "value", "sensor_ms", "received_ms", "stream_seq"), row)) for row in csv.reader(f)]


async def reference(js, upto, tenant=TENANT):
    """What common.reading() makes of every message 1..upto, keyed by stream sequence."""
    sub = await js.subscribe(common.subject_filter(tenant), stream=common.stream_for(tenant), ordered_consumer=True,
                             config=ConsumerConfig(deliver_policy=DeliverPolicy.BY_START_SEQUENCE, opt_start_seq=1))
    ref, seen = {}, 0
    while seen < upto:
        m = await sub.next_msg(timeout=10)
        seen = m.metadata.sequence.stream
        ref[seen] = common.reading(m)
    await sub.unsubscribe()
    return ref


def same(row, r):
    """A CSV row against the reader's dict."""
    return (row["tenant"] == r["tenant"] and row["sensor_id"] == r["sensor_id"] and row["kind"] == r["kind"]
            and float(row["value"]) == r["value"] and int(row["sensor_ms"]) == r["sensor_ms"]
            and int(row["received_ms"]) == r["received_ms"] and int(row["stream_seq"]) == r["stream_seq"])


def compare(label, rows, ref):
    by_seq = {int(r["stream_seq"]): r for r in rows}
    diffs = []
    for seq, r in ref.items():
        row = by_seq.get(seq)
        if (r is None) != (row is None) or (r is not None and not same(row, r)):
            diffs.append((seq, r, row))
    if diffs:
        for seq, r, row in diffs[:4]:
            print(f"      seq {seq}: python {r}  native {row}")
    return check(f"{label}: {len(ref):,} messages, {sum(r is not None for r in ref.values()):,} readings, {len(diffs)} differ", not diffs)


AWKWARD = [
    ("s1.temp", b"21.5"), ("s1.temp", b"7"), ("s1.temp", b" -3e2 "), ("s1.temp", b"0"), ("s1.temp", b"-0"), ("s1.temp", b"-0.0"),
    ("s1.temp", b"1e308"), ("s1.temp", b"1.7976931348623157e308"), ("s1.temp", b"5e-324"), ("s1.temp", b"1e-400"),
    ("s1.temp", b"12345678901234567890"), ("s1.temp", b"-9223372036854775808"), ("s1.temp", b"9007199254740993"),
    ("s1.temp", b"0.1"), ("s1.temp", b"123456789.123456789"), ("s1.temp", b"1E5"), ("s1.temp", b"1e+5"),
    ("s1.temp", b'{"value": 1.5, "ts_ms": 1700000000000}'), ("s1.temp", b'{"ts_ms": 99, "value": 2, "extra": {"value": 5, "ts_ms": 6}}'),
    ("s1.temp", b'{"value": 1, "ts_ms": null}'), ("s1.temp", b'{"value": 1, "ts_ms": 1700000000000.9}'), ("s1.temp", b'{"value": 1, "ts_ms": -5.9}'),
    ("s1.temp", b'{"value": 1, "ts_ms": 0}'), ("s1.temp", b'{"value": 1, "ts_ms": 9223372036854775807}'), ("s1.temp", b'{"value": 1, "ts_ms": -9223372036854775808}'),
    ("s1.temp", b'{"value": 1, "ts_ms": 9223372036854775808}'), ("s1.temp", b'{"value": 1, "ts_ms": 1e30}'), ("s1.temp", b'{"value": 1, "ts_ms": 9.1e18}'),
    ("s1.temp", b'{"value": 1, "ts_ms": 253402300799999}'), ("s1.temp", b'{"value": 1, "ts_ms": 253402300800000}'), ("s1.temp", b'{"value": 1, "ts_ms": -1}'), ("s1.temp", b'{"value": 1, "ts_ms": -0.5}'),
    ("s1.temp", b'{"value": 1, "value": 2, "ts_ms": 3, "ts_ms": 4}'), ("s1.temp", b'{"value":1,"ts_ms":2}  \n'),
    ("s1.temp", b'"21.5"'), ("s1.temp", b"true"), ("s1.temp", b"false"), ("s1.temp", b"null"), ("s1.temp", b"[1]"), ("s1.temp", b"[]"), ("s1.temp", b"{}"),
    ("s1.temp", b"NaN"), ("s1.temp", b"Infinity"), ("s1.temp", b"-Infinity"), ("s1.temp", b"1e999"), ("s1.temp", b"-1e999"),
    ("s1.temp", b'{"value": NaN, "ts_ms": 1}'), ("s1.temp", b'{"value": 1e999, "ts_ms": 1}'), ("s1.temp", b'{"value": 1, "ts_ms": 1e999}'),
    ("s1.temp", b'{"value": 1}'), ("s1.temp", b'{"ts_ms": 1}'), ("s1.temp", b'{"value": "1", "ts_ms": 1}'), ("s1.temp", b'{"value": true, "ts_ms": 1}'),
    ("s1.temp", b'{"value": null, "ts_ms": 1}'), ("s1.temp", b'{"value": [1], "ts_ms": 1}'), ("s1.temp", b'{"value": 1, "ts_ms": "1"}'),
    ("s1.temp", b'{"value": 1, "ts_ms": true}'), ("s1.temp", b'{"value": 1, "ts_ms": [1]}'),
    ("s1.temp", b""), ("s1.temp", b"not json"), ("s1.temp", b"{"), ("s1.temp", b"1 2"), ("s1.temp", b"1,"), ("s1.temp", b"01"), ("s1.temp", b"+1"), ("s1.temp", b".5"), ("s1.temp", b"5."),
    ("s1.temp", b"0x10"), ("s1.temp", b"1_000"), ("s1.temp", b"\xff\xfe"), ("s1.temp", b'{"value": 1, "ts_ms": 2, "note": "\xff"}'),
    ("s1.temp", b'{"value": 1, "ts_ms": 2, "note": "caf\xc3\xa9"}'), ("s1.temp", b"\xef\xbb\xbf1"),
    ("s1.temp", b"[" * 60 + b"]" * 60), ("s1.temp", b'{"a":' * 40 + b"1" + b"}" * 40),
    ("s1.temp", b'{"value": 3, "ts_ms": 4, "value": "x"}'),
    ("s1", b"1"), ("s1.temp.extra", b"1"), ("s1.temp.extra.more", b"1"),
    ("s-1_x.temp_c", b"2"), ("sé.té", b"3"),
]


def generated(rnd, n):
    out = []
    atoms = [lambda: str(rnd.randint(-10**6, 10**6)), lambda: repr(rnd.uniform(-1e6, 1e6)), lambda: f"{rnd.uniform(-9, 9):.{rnd.randint(0, 17)}f}",
             lambda: f"{rnd.randint(1, 9)}e{rnd.randint(-330, 330)}", lambda: f"{rnd.uniform(1, 9):.5f}E{rnd.choice('+-')}{rnd.randint(0, 320)}",
             lambda: str(rnd.randint(-2**70, 2**70)), lambda: rnd.choice(["null", "true", "false", '"1"', "[]", "{}", "NaN", "Infinity", "-0", "0", "1e999"])]
    for _ in range(n):
        v = rnd.choice(atoms)()
        kind = rnd.random()
        if kind < 0.35:
            body = v
        elif kind < 0.9:
            ts = rnd.choice([str(rnd.randint(-10**3, 2 * 10**12)), repr(rnd.uniform(0, 2e12)), "null", str(rnd.randint(-2**65, 2**65)), '"5"', "true", "1e30"])
            parts = [f'"value": {v}', f'"ts_ms": {ts}']
            if rnd.random() < 0.15: parts.pop(rnd.randrange(2))
            rnd.shuffle(parts)
            body = "{" + ", ".join(parts) + "}"
        else:
            body = rnd.choice(["", "x", "{", "[", '{"value":', "1 1", "é", "0.", ".", "-", "e5"])
        out.append((f"g{rnd.randint(0, 40)}.{rnd.choice(['temp', 'hum', 'p'])}", body.encode("utf-8")))
    return out


async def archive_e2e(nc, js, url, tmp):
    import socket
    probe = socket.socket()
    probe.settimeout(1)
    try:
        probe.connect(("127.0.0.1", 8333))
    except OSError:
        print("  · I skipped: no S3 on 127.0.0.1:8333 (SeaweedFS: examples/16-archive/README.md)")
        return
    finally:
        probe.close()
    os.environ["S3_ENDPOINT"] = "http://127.0.0.1:8333"
    from nats.js.api import KeyValueConfig
    try:
        await js.create_key_value(KeyValueConfig(bucket=common.KV, history=1, direct=True))
    except Exception:
        pass
    rnd = random.Random(11)
    base_ms = 1_700_000_000_000
    items = []
    for i in range(600):
        k = rnd.random()
        sensor = f"s{rnd.randint(0, 9)}"
        if k < 0.55:
            items.append((f"{sensor}.temp", json.dumps({"value": round(rnd.uniform(-20, 40), 2), "ts_ms": base_ms + rnd.randint(0, 300)}).encode()))
        elif k < 0.7:
            items.append((f"{sensor}.temp", json.dumps({"value": i, "ts_ms": base_ms + (i // 7)}).encode()))   # resent: the same key again
        elif k < 0.8:
            # A bare value uses THIS stream's receipt time. Distinct test streams
            # can group same-sensor arrivals into different milliseconds, causing
            # different legitimate dedupe results. Keep these sensor ids unique;
            # the explicit-time readings above still exercise resent readings.
            items.append((f"bare{i}.hum", str(rnd.randint(0, 100)).encode()))
        elif k < 0.9:
            items.append((f"{sensor}.hum", rnd.choice([b"junk", b'{"value": 1}', b'"3"', b"NaN", b"[]"])))
        else:
            items.append((f"{rnd.choice(['a,b', 'c\"d'])}.k", json.dumps({"value": i, "ts_ms": base_ms + i}).encode()))
    items.append(("s1.temp", b'{"value": 99, "ts_ms": 1700000000999}'))   # the last message is a reading
    results = {}
    for t, batch_size in (("ea", 100), ("eb", 5000)):
        label = f"batch {batch_size}"
        await js.add_stream(StreamConfig(name=common.stream_for(t), subjects=[common.subject_filter(t)], max_age=86400))
        await common.reset_tenant(js, t)
        for suffix, body in items:
            await nc.publish(f"sensors.{t}.{suffix}", body)
        await nc.flush()
        for _ in range(200):
            if (await js.stream_info(common.stream_for(t))).state.last_seq >= len(items): break
            await asyncio.sleep(0.05)
        cmd = [sys.executable, str(REPO / "examples" / "16-archive" / "archiver.py"), "--tenant", t, "--flush-s", "1", "--batch", str(batch_size),
               "--exit-when-idle", "3", "--outage-s", "60", "--fetch-bin", str(FETCH)]
        env = dict(os.environ, NATS_URL=url)
        env.pop("NATS_CREDS", None)
        r = subprocess.run(cmd, env=env, capture_output=True, text=True, errors="replace", timeout=180)
        check(f"I. archiver.py ({label}) exits 0", r.returncode == 0)
        if r.returncode:
            print(r.stdout[-600:], r.stderr[-600:])
        manifest, _ = await common.read_manifest(nc, t)
        results[t] = manifest
        # A trailing chunk with no readings still has durable coverage. Reopening
        # must not re-read it, and no empty Parquet object belongs in the manifest.
        files_before = list(manifest["files"])
        for payload in (b"junk", b'{"value": 1}'):
            await js.publish(f"sensors.{t}.s1.temp", payload)
        again = subprocess.run(cmd, env=env, capture_output=True, text=True, errors="replace", timeout=180)
        skipped_manifest, _ = await common.read_manifest(nc, t)
        check(f"I. {label} commits a skipped-only tail without a Parquet file",
              again.returncode == 0 and skipped_manifest["covers_up_to"] == len(items) + 2
              and skipped_manifest["files"] == files_before)
        reopened = subprocess.run(cmd, env=env, capture_output=True, text=True, errors="replace", timeout=180)
        after_reopen, _ = await common.read_manifest(nc, t)
        check(f"I. {label} reopens beyond the skipped tail with the manifest unchanged",
              reopened.returncode == 0 and after_reopen == skipped_manifest)
    import duckdb
    rows = {}
    for t, m in results.items():
        files = common.fetch_files(m["files"], tmp / f"cache-{t}")
        q = ", ".join("'" + str(f) + "'" for f in files)
        got = duckdb.sql(f"SELECT sensor_id, kind, value, sensor_ts::VARCHAR, stream_seq FROM read_parquet([{q}])").fetchall()
        # across chunks a resent reading may sit in two files: the latest arrival is the reading
        latest = {}
        for sid, kind, value, ts, seq in got:
            key = (sid, kind, ts)
            if key not in latest or seq > latest[key][4]:
                latest[key] = (sid, kind, value, ts, seq)
        rows[t] = (sorted(latest.values()), len(got), m)
    ref = await reference(js, len(items), "eb")
    want = {}
    for seq in sorted(ref):
        r = ref[seq]
        if r is not None:
            want[(r["sensor_id"], r["kind"], r["sensor_ms"])] = seq
    # The sensor time of a bare number is the stream's own, which differs between the two streams.
    strip = lambda rs: [(sid, kind, value, seq) for sid, kind, value, ts, seq in rs]
    same_readings = strip(rows["ea"][0]) == strip(rows["eb"][0])
    check(f"I. both archives hold the same readings: {len(rows['ea'][0])} (batch 100) and {len(rows['eb'][0])} (batch 5000), "
          f"{len(want)} distinct keys sent", same_readings and len(rows["eb"][0]) == len(want))
    check("I. each covers the stream to its last message", rows["ea"][2]["covers_up_to"] == len(items) == rows["eb"][2]["covers_up_to"])
    check(f"I. files written: {len(rows['ea'][2]['files'])} (batch 100), {len(rows['eb'][2]['files'])} (batch 5000), rows in them {rows['ea'][1]} and {rows['eb'][1]}",
          len(rows["eb"][2]["files"]) >= 1)


async def main(tmp):
    srv = subprocess.Popen(["nats-server", "-js", "-p", "14335", "-sd", str(tmp / "js")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = "nats://127.0.0.1:14335"
    try:
        await asyncio.sleep(1.5)
        nc = await nats.connect(url)
        js = nc.jetstream()
        await js.add_stream(StreamConfig(name=STREAM, subjects=[common.subject_filter(TENANT)], max_age=86400))

        async def publish(items):
            for suffix, body in items:
                await nc.publish(f"sensors.{TENANT}.{suffix}", body)
            await nc.flush()

        async def wait_for(n):
            for _ in range(200):
                if (await js.stream_info(STREAM)).state.last_seq >= n: return
                await asyncio.sleep(0.05)
            raise RuntimeError("stream did not take the messages")

        # A
        await publish(AWKWARD)
        n = len(AWKWARD)
        await wait_for(n)
        out = tmp / "a.csv"
        rc, summary, err = fetch(url, TENANT, 1, out)
        check("A. the fetch ran", rc == 0 and summary is not None)
        if rc:
            print(err); return
        ref = await reference(js, n)
        compare("A. awkward payloads", read_csv(out), ref)
        check("A. the summary counts agree", summary["messages"] == n and summary["rows"] == sum(r is not None for r in ref.values())
              and summary["skipped"] == n - summary["rows"] and summary["last_seq"] == n and summary["first_seq"] == 1)

        # B
        rnd = random.Random(7)
        gen = generated(rnd, 4000)
        await publish(gen)
        total = n + len(gen)
        await wait_for(total)
        out = tmp / "b.csv"
        rc, summary, err = fetch(url, TENANT, 1, out)
        ref = await reference(js, total)
        compare("B. generated payloads", read_csv(out), ref)

        # C
        base = total
        weird = [("a,b.k\"1", b"1"), ("x\"y.z,w", b"2"), ("\"q\".\"r\"", b"3"), ("plain.k", b"4")]
        await publish(weird)
        total += len(weird)
        await wait_for(total)
        out = tmp / "c.csv"
        rc, summary, err = fetch(url, TENANT, base + 1, out)
        rows = read_csv(out)
        ref = await reference(js, total)
        ok = len(rows) == 4 and [(r["sensor_id"], r["kind"]) for r in rows] == [("a,b", "k\"1"), ("x\"y", "z,w"), ("\"q\"", "\"r\""), ("plain", "k")]
        check("C. a comma or a quote in a sensor id or kind survives the CSV", ok)
        if not ok: print("      ", [(r["sensor_id"], r["kind"]) for r in rows])

        # D: chunks cover the stream
        parts, pos, calls, prev_last = [], 1, 0, 0
        while True:
            out = tmp / f"d{calls}.csv"
            rc, s, err = fetch(url, TENANT, pos, out, mx=700)
            calls += 1
            if rc or s["messages"] == 0: break
            assert s["first_seq"] == pos, (s, pos)
            assert s["messages"] == s["last_seq"] - s["first_seq"] + 1, s
            parts += read_csv(out)
            pos = s["last_seq"] + 1
        full_rc, full, _ = fetch(url, TENANT, 1, tmp / "full.csv")
        whole = read_csv(tmp / "full.csv")
        check(f"D. {calls - 1} chunks of at most 700 messages cover the stream: same rows, same order, no overlap",
              parts == whole and len({r['stream_seq'] for r in parts}) == len(parts) and pos == total + 1)
        check("D. last_seq moves past messages that were not readings (the next chunk starts at last_seq + 1)", pos == total + 1)

        # E
        out = tmp / "e.csv"
        rc, s, err = fetch(url, TENANT, total + 1, out)
        check("E. an empty range is an answer: 0 messages, exit 0, an empty file", rc == 0 and s["messages"] == 0 and out.stat().st_size == 0)
        rc, s, err = fetch(url, TENANT, total + 500, out)
        check("E. a start beyond the end is the same", rc == 0 and s["messages"] == 0)

        # F
        rc, s, err = fetch(url, "nobody", 1, tmp / "f.csv")
        check("F. a stream that does not exist exits 2 and says so", rc == 2 and "does not exist" in err)

        # H (before the purge): the dedupe in SQL keeps the latest arrival, like the archiver's dict
        dup = [(f"dd.k", json.dumps({"value": i, "ts_ms": 5000 + (i % 3)}).encode()) for i in range(12)] + [("dd.k", b"junk"), ("dd2.k", b'{"value": 9, "ts_ms": 5000}')]
        await publish(dup)
        base, total = total, total + len(dup)
        await wait_for(total)
        out = tmp / "h.csv"
        rc, s, err = fetch(url, TENANT, base + 1, out)
        ref = await reference(js, total)
        py = {}
        for seq in sorted(ref):
            r = ref[seq]
            if r is not None and seq > base:
                py[(r["sensor_id"], r["kind"], r["sensor_ms"])] = r
        import duckdb
        sql = duckdb.sql(f"SELECT stream_seq FROM read_csv('{out}', header=false, allow_quoted_nulls=false, columns={{tenant:'VARCHAR', sensor_id:'VARCHAR', kind:'VARCHAR', value:'DOUBLE', sensor_ms:'BIGINT', received_ms:'BIGINT', stream_seq:'BIGINT'}}) "
                         f"QUALIFY row_number() OVER (PARTITION BY sensor_id, kind, sensor_ms ORDER BY stream_seq DESC) = 1").fetchall()
        check(f"H. the SQL dedupe keeps the rows the dict kept: {len(py)} of {s['rows']}", sorted(x[0] for x in sql) == sorted(r["stream_seq"] for r in py.values()))

        # G: pruned past --from
        cut = total // 2
        await js.purge_stream(STREAM, seq=cut)
        out = tmp / "g.csv"
        rc, s, err = fetch(url, TENANT, 1, out)
        check(f"G. a stream pruned below the start reports first_seq {s and s['first_seq']} above --from 1 (the archiver refuses to go on)",
              rc == 0 and s["first_seq"] == cut and s["first_seq"] > 1)
        # A prefix check cannot see an internal hole. Use a separate stream so both a
        # single pull and a later pull in the same fetch must detect the missing event.
        gap_tenant = "gap"
        gap_stream = common.stream_for(gap_tenant)
        await js.add_stream(StreamConfig(name=gap_stream, subjects=[common.subject_filter(gap_tenant)]))
        for i in range(3):
            await js.publish(f"sensors.{gap_tenant}.s1.temp", json.dumps({"value": i, "ts_ms": 1000 + i}).encode())
        await js.delete_msg(gap_stream, 2)
        for batch in (1, 5000):
            rc, s, err = fetch(url, gap_tenant, 1, tmp / f"gap-{batch}.csv", extra=("--batch", str(batch)))
            check(f"G. an internal sequence hole fails (pull batch {batch}); no successful coverage summary",
                  rc != 0 and s is None and "SequenceGap" in err and "after 1, received 3" in err)
        await archive_e2e(nc, js, url, tmp)
        await nc.close()
    finally:
        srv.terminate()
        try: srv.wait(timeout=10)
        except subprocess.TimeoutExpired: srv.kill()


def build_hint():
    if not FETCH.exists():
        print(f"✗ {FETCH} is missing: zig build   (or zig build readings-fetch)")
        return False
    return True


if __name__ == "__main__":
    if not build_hint():
        sys.exit(1)
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="zb-rf-"))
    try:
        asyncio.run(main(tmp))
    except Exception as e:
        import traceback; traceback.print_exc()
        check(f"raised: {type(e).__name__}: {str(e)[:300]}", False)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print()
    if failed:
        zb.bad(f"{failed} check(s) failed")
        sys.exit(1)
    zb.ok("the native reader and common.reading() agree on every payload, and the chunks, the empty range and the pruned stream behave")

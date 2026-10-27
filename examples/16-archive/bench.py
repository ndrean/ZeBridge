#!/usr/bin/env python3
"""Native archive stages compared with the former Python path (S3 left out). Needs nats-server on PATH, nats-py and duckdb.

    python3 examples/16-archive/bench.py [readings, default 500000]


  1. ingest : former Python baseline: ordered consumer, next_msg(), common.reading(), dict insert
  2. flush  : former Python baseline: JSON lines on disk -> DuckDB read_json -> COPY ... parquet zstd
"""
import asyncio, json, os, subprocess, sys, tempfile, time, pathlib, shutil, random

ARCH = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ARCH))
os.environ.setdefault("S3_ENDPOINT", "http://127.0.0.1:1")
import nats
from nats.js.api import ConsumerConfig, DeliverPolicy, StreamConfig
import common

N = int(sys.argv[1]) if len(sys.argv) > 1 else 500_000
PORT = 14333
TENANT = "bench"


async def publish(js_nc, n):
    nc = js_nc
    t0 = time.monotonic()
    rnd = random.Random(1)
    base = int(time.time() * 1000)
    for i in range(n):
        s = f"sensors.{TENANT}.s{i % 500}.temp"
        await nc.publish(s, json.dumps({"value": rnd.random() * 100, "ts_ms": base + i}).encode())
        if i % 5000 == 0:
            await nc.flush()
    await nc.flush()
    return time.monotonic() - t0


async def main():
    tmp = tempfile.mkdtemp(prefix="zb-bench-")
    srv = subprocess.Popen(["nats-server", "-js", "-p", str(PORT), "-sd", tmp + "/js"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        await asyncio.sleep(1.5)
        nc = await nats.connect(f"nats://127.0.0.1:{PORT}")
        js = nc.jetstream()
        await js.add_stream(StreamConfig(name=common.stream_for(TENANT), subjects=[common.subject_filter(TENANT)], max_age=86400))
        t = await publish(nc, N)
        print(f"published {N:,} readings in {t:.1f}s ({N / t:,.0f}/s, the producer side, not what is measured)")
        info = await js.stream_info(common.stream_for(TENANT))
        print(f"stream holds {info.state.messages:,} messages, {info.state.bytes / 1e6:.1f} MB")

        # 1. former Python ingest baseline
        sub = await js.subscribe(common.subject_filter(TENANT), stream=common.stream_for(TENANT), ordered_consumer=True,
                                 config=ConsumerConfig(deliver_policy=DeliverPolicy.BY_START_SEQUENCE, opt_start_seq=1))
        rows, got = {}, 0
        t0 = time.monotonic()
        while got < N:
            msg = await sub.next_msg(timeout=5)
            r = common.reading(msg)
            got += 1
            if r is not None:
                rows[(r["sensor_id"], r["kind"], r["sensor_ms"])] = r
        t_ing = time.monotonic() - t0
        print(f"1. ingest : {got:,} msgs in {t_ing:.2f}s = {got / t_ing:,.0f} msg/s  (next_msg + reading() + dict)")
        # 1b. same stream, where the time goes: next_msg alone, reading() alone, and a batched pull
        sub2 = await js.subscribe(common.subject_filter(TENANT), stream=common.stream_for(TENANT), ordered_consumer=True,
                                  config=ConsumerConfig(deliver_policy=DeliverPolicy.BY_START_SEQUENCE, opt_start_seq=1))
        msgs, t0 = [], time.monotonic()
        while len(msgs) < N:
            msgs.append(await sub2.next_msg(timeout=5))
        t_nm = time.monotonic() - t0
        print(f"1b. next_msg only      : {N / t_nm:>10,.0f} msg/s  ({t_nm / N * 1e6:.1f} us/msg)")
        t0 = time.monotonic()
        for m in msgs: common.reading(m)
        t_rd = time.monotonic() - t0
        print(f"1b. reading() only     : {N / t_rd:>10,.0f} msg/s  ({t_rd / N * 1e6:.1f} us/msg: subject split, json.loads, metadata, dict)")
        psub = await js.pull_subscribe(common.subject_filter(TENANT), stream=common.stream_for(TENANT),
                                       config=ConsumerConfig(deliver_policy=DeliverPolicy.BY_START_SEQUENCE, opt_start_seq=1, ack_policy="none"))
        got2, t0 = 0, time.monotonic()
        while got2 < N:
            batch = await psub.fetch(5000, timeout=5)
            got2 += len(batch)
        t_pull = time.monotonic() - t0
        print(f"1c. pull fetch(5000) only: {got2 / t_pull:>8,.0f} msg/s  (no decoding)")
        await nc.close()

        # 1d. the native reader (bridge_readings_fetch), as archiver.py runs it
        fetch = pathlib.Path(os.environ.get("ZB_READINGS_FETCH", ARCH.parents[1] / "zig-out" / "bin" / "bridge_readings_fetch"))
        csv_out = tmp + "/native.csv"
        if fetch.exists():
            for run in range(3):
                t0 = time.monotonic()
                r = subprocess.run([str(fetch), "--tenant", TENANT, "--from", "1", "--max", str(N), "--out", csv_out],
                                   env=dict(os.environ, NATS_URL=f"nats://127.0.0.1:{PORT}"), capture_output=True, text=True)
                t = time.monotonic() - t0
                summary = json.loads(r.stdout) if r.returncode == 0 else r.stderr[-200:]
                print(f"1d. native reader, run {run + 1}/3: {N / t:>10,.0f} msg/s  ({t:.2f}s with the process start; a ReleaseFast build) {summary if run == 0 else ''}")
        else:
            print(f"1d. native reader: skipped, {fetch} is missing (zig build -Doptimize=ReleaseFast)")

        # 2. former JSON-lines flush baseline (without S3)
        import duckdb
        lst = list(rows.values())
        for batch in (100_000, len(lst)):
            part = lst[:batch]
            for rep in range(3):   # the archiver is long-lived: report the warm run
                with tempfile.TemporaryDirectory() as d:
                    src, out = f"{d}/rows.json", f"{d}/out.parquet"
                    t0 = time.monotonic()
                    with open(src, "w") as f:
                        for r in part:
                            f.write(json.dumps(r) + "\n")
                    t_json = time.monotonic() - t0
                    t0 = time.monotonic()
                    duckdb.sql(f"COPY ({common.SELECT_ROWS} FROM read_json('{src}', format='newline_delimited', "
                               f"columns={{tenant:'VARCHAR', sensor_id:'VARCHAR', kind:'VARCHAR', value:'DOUBLE', sensor_ms:'BIGINT', received_ms:'BIGINT', stream_seq:'BIGINT'}}) "
                               f"ORDER BY sensor_ts) TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
                    t_copy = time.monotonic() - t0
                    size = os.path.getsize(out)
                print(f"2. flush  : {len(part):>9,} rows, run {rep + 1}/3: json lines {t_json:.2f}s + duckdb COPY {t_copy:.2f}s = {len(part) / (t_json + t_copy):,.0f} rows/s ({size / 1e6:.1f} MB parquet)")

        if fetch.exists():
            for rep in range(3):
                pq = tmp + "/native.parquet"
                t0 = time.monotonic()
                duckdb.sql(f"COPY ({common.SELECT_ROWS} FROM read_csv('{csv_out}', header=false, allow_quoted_nulls=false, "
                           f"columns={{tenant:'VARCHAR', sensor_id:'VARCHAR', kind:'VARCHAR', value:'DOUBLE', sensor_ms:'BIGINT', received_ms:'BIGINT', stream_seq:'BIGINT'}}) "
                           f"QUALIFY row_number() OVER (PARTITION BY sensor_id, kind, sensor_ms ORDER BY stream_seq DESC) = 1 "
                           f"ORDER BY sensor_ts) TO '{pq}' (FORMAT parquet, COMPRESSION zstd)")
                t = time.monotonic() - t0
                print(f"2b. flush from the native CSV, run {rep + 1}/3: {N / t:>9,.0f} rows/s ({t:.2f}s: DuckDB CSV -> dedupe in SQL -> Parquet)")
    finally:
        srv.terminate()
        shutil.rmtree(tmp, ignore_errors=True)


asyncio.run(main())

#!/usr/bin/env python3
"""The archiver: one tenant's readings, from the stream into Parquet files in S3.

    stream READINGS_<tenant>  →  batch  →  Parquet  →  S3  →  manifest (NATS KV)

Its position is the manifest's `covers_up_to`: it keeps no consumer of its own. At start it
reads the manifest and follows the stream from the next sequence. Each flush:

  1. drops resent readings (the same sensor, kind and sensor time: the latest one stays);
  2. writes one Parquet file with DuckDB and uploads it;
  3. checks the object is there, with the right size;
  4. only then writes the manifest, on the revision it read: a second archiver fails here.

Killed between 2 and 4, the file is an orphan nobody reads, and the next archiver covers the
same readings again. It refuses to start when the stream no longer holds `covers_up_to + 1`
(readings lost before any file held them), or when the stream keeps readings for less time
than two flushes plus the S3 outage it should survive.

    NATS_URL=… NATS_CREDS=archiver-acme.creds python3 examples/16-archive/archiver.py --tenant acme

Its credential: `bridge --mint-archiver --name archiver-acme --tenant acme` — it may read
READINGS_acme and write acme's manifests, nothing else.

Exit codes: 0 stopped, 2 retention too short, 3 another archiver moved the manifest,
4 readings lost from the stream, 9 killed on purpose (--crash-after-upload, tests).
"""
import argparse, asyncio, datetime, json, os, signal, sys, tempfile, time

import nats
from nats.js.api import ConsumerConfig, DeliverPolicy

import common


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tenant", required=True)
    ap.add_argument("--flush-s", type=float, default=60, help="flush at least this often while readings arrive")
    ap.add_argument("--batch", type=int, default=100_000, help="flush when this many readings are waiting")
    ap.add_argument("--outage-s", type=float, default=3600, help="the S3 outage the stream must outlast")
    ap.add_argument("--exit-when-idle", type=float, default=0, help="stop after this long with nothing to flush (tests)")
    ap.add_argument("--accept-loss", action="store_true", help="start even if the stream lost readings")
    ap.add_argument("--crash-after-upload", action="store_true", help=argparse.SUPPRESS)
    a = ap.parse_args()

    nc = await common.connect()
    js = nc.jetstream()
    stream = common.stream_for(a.tenant)

    # The stream may not exist yet: the bridge makes it when the tenant has its first row in
    # zebridge_user_tenants. Waiting, rather than exiting, starts the archive the moment it
    # does, under Docker or systemd alike, with one line said instead of a restart loop.
    info = await stream_when_there(js, stream, a.tenant)

    # The manifest AFTER the wait: its revision is what the first write must name, and a
    # revision read before a long wait may no longer be the latest.
    manifest, rev = await common.read_manifest(nc, a.tenant)
    archived = manifest is not None   # a tenant with no archive yet has nothing to lose
    manifest = manifest or {"covers_up_to": 0, "files": [], "schema": common.SCHEMA}
    covers = manifest["covers_up_to"]

    # The retention inequality: a reading must outlive two flushes and an S3 outage.
    max_age = info.config.max_age or 0
    need = 2 * a.flush_s + a.outage_s
    if max_age and max_age < need:
        print(f"🔴 {stream} keeps readings {max_age:.0f} s; this archiver needs {need:.0f} s "
              f"(2 × flush {a.flush_s:.0f} s + outage {a.outage_s:.0f} s): raise the stream's max_age", flush=True)
        return 2

    # Nothing may be missing between the archive and the stream.
    first = info.state.first_seq
    if archived and info.state.messages > 0 and first > covers + 1 and not a.accept_loss:
        print(f"🔴 the stream starts at {first}, the archive covers up to {covers}: readings "
              f"{covers + 1}..{first - 1} are gone and no file holds them (--accept-loss to go on)", flush=True)
        return 4
    start = max(covers + 1, first)
    print(f"archiver {a.tenant}: archive covers up to {covers} ({len(manifest['files'])} file(s)), "
          f"following {common.subject_filter(a.tenant)} from {start}", flush=True)

    sub = await js.subscribe(common.subject_filter(a.tenant), stream=stream, ordered_consumer=True,
                             config=ConsumerConfig(deliver_policy=DeliverPolicy.BY_START_SEQUENCE, opt_start_seq=start))
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)

    s3 = common.s3()
    rows: dict = {}          # (sensor, kind, sensor time) → the latest reading
    last_seq = covers
    first_at = last_msg_at = time.monotonic()
    code = 0
    while not stop.is_set():
        try:
            msg = await sub.next_msg(timeout=0.25)
            r = common.reading(msg)
            last_seq = msg.metadata.sequence.stream
            last_msg_at = time.monotonic()
            if r is not None:
                if not rows:
                    first_at = last_msg_at
                rows[(r["sensor_id"], r["kind"], r["sensor_ms"])] = r
        except nats.errors.TimeoutError:
            pass
        now = time.monotonic()
        if rows and (len(rows) >= a.batch or now - first_at >= a.flush_s):
            code = flush(a, s3, manifest, list(rows.values()), last_seq)
            if code:
                break
            # Awaited outside flush(): the manifest write is the one async step.
            code, rev = await commit(js, a.tenant, manifest, rev)
            if code:
                break
            rows.clear()
        elif a.exit_when_idle and not rows and now - last_msg_at >= a.exit_when_idle:
            break
    await nc.close()
    return code


async def stream_when_there(js, stream, tenant, every_s=10):
    from nats.js.errors import NotFoundError
    said = False
    while True:
        try:
            return await js.stream_info(stream)
        except NotFoundError:
            if not said:
                print(f"⏳ waiting for the stream {stream}: the bridge makes it when the tenant has a row in "
                      f"zebridge_user_tenants (INSERT INTO zebridge_user_tenants (principal, tenant_id) "
                      f"VALUES ('<principal>', '{tenant}')). Checking every {every_s} s.", flush=True)
                said = True
            await asyncio.sleep(every_s)


def flush(a, s3, manifest, rows, last_seq):
    """Write and upload one file, and add it to `manifest` (not yet written)."""
    import duckdb
    first_seq = min(r["stream_seq"] for r in rows)
    day = datetime.datetime.fromtimestamp(rows[0]["sensor_ms"] / 1000, datetime.UTC).date().isoformat()
    key = f"{a.tenant}/{common.TABLE}/{day}/{first_seq:012d}-{last_seq:012d}.parquet"
    with tempfile.TemporaryDirectory() as d:
        src, out = f"{d}/rows.json", f"{d}/out.parquet"
        with open(src, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        duckdb.sql(f"COPY ({common.SELECT_ROWS} FROM read_json('{src}', format='newline_delimited', "
                   f"columns={{tenant:'VARCHAR', sensor_id:'VARCHAR', kind:'VARCHAR', value:'DOUBLE', sensor_ms:'BIGINT', received_ms:'BIGINT', stream_seq:'BIGINT'}}) "
                   f"ORDER BY sensor_ts) TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
        size = os.path.getsize(out)
        s3.upload_file(out, common.BUCKET, key)
    head = s3.head_object(Bucket=common.BUCKET, Key=key)
    if head["ContentLength"] != size:
        print(f"🔴 {key}: {head['ContentLength']} bytes in S3, {size} written; the manifest stays", flush=True)
        return 5
    print(f"flushed {len(rows)} reading(s) to {key} ({size} bytes)", flush=True)
    if a.crash_after_upload:
        os._exit(9)
    manifest["files"].append({"key": key, "rows": len(rows), "bytes": size, "first_seq": first_seq, "last_seq": last_seq})
    manifest["covers_up_to"] = last_seq
    return 0


async def commit(js, tenant, manifest, rev):
    """Files first, manifest second, on the revision read: one writer per tenant."""
    try:
        rev = await common.write_manifest(js, tenant, manifest, rev)
    except nats.js.errors.APIError as e:
        print(f"🔴 the manifest moved under this archiver (another one runs?): {e}", flush=True)
        return 3, rev
    print(f"manifest: covers up to {manifest['covers_up_to']}, {len(manifest['files'])} file(s)", flush=True)
    return 0, rev


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

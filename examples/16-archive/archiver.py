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

`bridge_readings_fetch` reads each chunk into CSV. DuckDB deduplicates it and writes
Parquet; Python handles S3 uploads and manifest commits. The fetcher is required.

Exit codes: 0 stopped, 1 a reader failed, 2 retention too short, 3 another archiver moved the
manifest, 4 readings lost from the stream, 9 killed on purpose (--crash-after-upload, tests).
"""
import argparse, asyncio, datetime, json, os, shutil, signal, sys, tempfile, time

import nats

import common


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tenant", required=True)
    ap.add_argument("--flush-s", type=float, default=60, help="flush at least this often while readings arrive")
    ap.add_argument("--batch", type=int, default=100_000, help="flush when this many readings are waiting")
    ap.add_argument("--outage-s", type=float, default=3600, help="the S3 outage the stream must outlast")
    ap.add_argument("--exit-when-idle", type=float, default=0, help="stop after this long with nothing to flush (tests)")
    ap.add_argument("--accept-loss", action="store_true", help="start even if the stream lost readings")
    ap.add_argument("--fetch-bin", default=os.environ.get("ZB_READINGS_FETCH"), help="bridge_readings_fetch executable (default: on the PATH)")
    ap.add_argument("--crash-after-upload", action="store_true", help=argparse.SUPPRESS)
    a = ap.parse_args()

    fetch = resolve_fetch(a)
    if fetch is None:
        return 1

    nc = await common.connect()
    try:
        js = nc.jetstream()
        stream = common.stream_for(a.tenant)

        # The stream may not exist yet: the bridge makes it when the tenant has its first row in
        # zebridge_user_tenants. Waiting, rather than exiting, starts the archive the moment it
        # does, under Docker or systemd alike, with one line said instead of a restart loop.
        info = await common.stream_when_there(js, a.tenant)

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

        return await archive_loop(a, nc, js, stream, manifest, rev, start, fetch)
    finally:
        await nc.close()


def upload(a, s3, manifest, out, key, size, nrows, first_seq, last_seq):
    """The file to S3, its size checked there, and its entry in `manifest` (not yet written)."""
    s3.upload_file(out, common.BUCKET, key)
    head = s3.head_object(Bucket=common.BUCKET, Key=key)
    if head["ContentLength"] != size:
        print(f"🔴 {key}: {head['ContentLength']} bytes in S3, {size} written; the manifest stays", flush=True)
        return 5
    print(f"flushed {nrows} reading(s) to {key} ({size} bytes)", flush=True)
    if a.crash_after_upload:
        os._exit(9)
    manifest["files"].append({"key": key, "rows": nrows, "bytes": size, "first_seq": first_seq, "last_seq": last_seq})
    manifest["covers_up_to"] = last_seq
    return 0


def resolve_fetch(a):
    path = a.fetch_bin or shutil.which("bridge_readings_fetch")
    if not path or not os.path.exists(path):
        print("🔴 the archiver needs bridge_readings_fetch (built with the bridge: zig build): put it on the PATH, "
              "or pass --fetch-bin / set ZB_READINGS_FETCH", flush=True)
        return None
    return path


async def fetch_chunk(fetch, a, pos, out):
    """Read up to --batch messages from sequence `pos` into the CSV `out`. (summary, None) or (None, why)."""
    cmd = [fetch, "--tenant", a.tenant, "--from", str(pos), "--max", str(a.batch), "--out", out]
    name = common.principal_of(os.environ.get("NATS_CREDS"))
    if name:   # an archiver's credential may subscribe to its own inbox only
        cmd += ["--inbox-prefix", f"_INBOX.{name}"]
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        return None, f"exit {proc.returncode}: {stderr.decode(errors='replace').strip()[-400:]}"
    return json.loads(stdout), None


CSV_TYPES = ("{tenant:'VARCHAR', sensor_id:'VARCHAR', kind:'VARCHAR', value:'DOUBLE', "
             "sensor_ms:'BIGINT', received_ms:'BIGINT', stream_seq:'BIGINT'}")


def flush_csv(a, s3, manifest, csv_path, summary):
    """Write a chunk as Parquet, keeping the latest arrival per sensor, kind and sensor time."""
    import duckdb
    first_seq, last_seq = summary["first_row_seq"], summary["last_seq"]
    day = datetime.datetime.fromtimestamp(summary["first_ms"] / 1000, datetime.UTC).date().isoformat()
    key = f"{a.tenant}/{common.TABLE}/{day}/{first_seq:012d}-{last_seq:012d}.parquet"
    with tempfile.TemporaryDirectory() as d:
        out = f"{d}/out.parquet"
        nrows = duckdb.execute(
            f"COPY ({common.SELECT_ROWS} FROM read_csv('{csv_path}', header=false, allow_quoted_nulls=false, columns={CSV_TYPES}) "
            f"QUALIFY row_number() OVER (PARTITION BY sensor_id, kind, sensor_ms ORDER BY stream_seq DESC) = 1 "
            f"ORDER BY sensor_ts) TO '{out}' (FORMAT parquet, COMPRESSION zstd)").fetchone()[0]
        size = os.path.getsize(out)
        return upload(a, s3, manifest, out, key, size, nrows, first_seq, last_seq)


async def archive_loop(a, nc, js, stream, manifest, rev, start, fetch):
    """Read chunks with bridge_readings_fetch and commit their archive coverage.

    A cycle starts when a batch is waiting or --flush-s has passed since the first waiting reading,
    and reads until the stream has nothing more: one file and one manifest write per chunk."""
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    s3 = common.s3()
    pos = start - 1            # the last stream sequence this archiver has read
    waiting_since = None
    idle_since = time.monotonic()
    with tempfile.TemporaryDirectory() as tmp:
        while not stop.is_set():
            info = await js.stream_info(stream)
            pending = info.state.last_seq - pos
            now = time.monotonic()
            if pending > 0 and waiting_since is None:
                waiting_since = now
            if pending <= 0:
                waiting_since = None
                if a.exit_when_idle and now - idle_since >= a.exit_when_idle:
                    break
            elif pending >= a.batch or now - waiting_since >= a.flush_s:
                idle_since = now
                while not stop.is_set():
                    csv_path = f"{tmp}/chunk.csv"
                    summary, why = await fetch_chunk(fetch, a, pos + 1, csv_path)
                    if summary is None:
                        print(f"🔴 bridge_readings_fetch failed ({why})", flush=True)
                        return 1
                    if summary["messages"] == 0:
                        break
                    if summary["first_seq"] != pos + 1 and not a.accept_loss:
                        print(f"🔴 the stream now starts at {summary['first_seq']}, the archive covers up to {pos}: readings "
                              f"{pos + 1}..{summary['first_seq'] - 1} are gone and no file holds them (--accept-loss to go on)", flush=True)
                        return 4
                    if summary["messages"] != summary["last_seq"] - summary["first_seq"] + 1:
                        print(f"🔴 the reader returned a gap inside {summary['first_seq']}..{summary['last_seq']}; "
                              "the archive manifest stays unchanged", flush=True)
                        return 4
                    if summary["rows"] > 0:
                        code = flush_csv(a, s3, manifest, csv_path, summary)
                        if code:
                            return code
                    else:
                        manifest["covers_up_to"] = summary["last_seq"]
                    code, rev = await commit(js, a.tenant, manifest, rev)
                    if code:
                        return code
                    pos = summary["last_seq"]
                    if summary["messages"] < a.batch:
                        break
                waiting_since = None
                continue
            await asyncio.sleep(0.25)
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

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

`bridge_readings_fetch` reads each chunk into CSV. DuckDB deduplicates it and writes
Parquet; Python handles S3 uploads and manifest commits. The fetcher is required.

    from zb_archiver import ArchiverConfig, run
    code = await run(ArchiverConfig(tenant="acme", flush_s=60))

`run` returns an `ExitCode`; it never raises for an outcome it can name.
"""
from __future__ import annotations

import asyncio, dataclasses, datetime, enum, json, os, shutil, tempfile, time
from typing import Callable

from . import env, layout


class ExitCode(enum.IntEnum):
    STOPPED = 0           # stopped (a signal, or --exit-when-idle)
    FAILED = 1            # the fetcher is missing or failed
    RETENTION_SHORT = 2   # the stream keeps readings for less than 2 × flush + outage
    MANIFEST_MOVED = 3    # another archiver wrote the manifest: one writer per tenant
    READINGS_LOST = 4     # readings left the stream before a file held them
    UPLOAD_MISMATCH = 5   # the object in S3 is not the size written; the manifest stays
    CRASHED = 9           # killed on purpose (crash_after_upload, tests)


@dataclasses.dataclass
class ArchiverConfig:
    tenant: str
    flush_s: float = 60            # flush at least this often while readings arrive
    batch: int = 100_000           # flush when this many readings are waiting; messages per chunk
    outage_s: float = 3600         # the S3 outage the stream must outlast
    exit_when_idle: float = 0      # stop after this long with nothing to flush (0: never)
    accept_loss: bool = False      # go on even if the stream lost readings
    fetch_bin: str | None = None   # bridge_readings_fetch; else ZB_READINGS_FETCH, else the PATH
    nats_url: str | None = None    # else NATS_URL
    creds: str | None = None       # else NATS_CREDS
    s3_endpoint: str | None = None # else S3_ENDPOINT
    bucket: str | None = None      # else S3_BUCKET
    crash_after_upload: bool = False
    say: Callable[..., None] = print


def resolve_fetch(cfg: ArchiverConfig) -> str | None:
    path = cfg.fetch_bin or os.environ.get("ZB_READINGS_FETCH") or shutil.which("bridge_readings_fetch")
    if not path or not os.path.exists(path):
        cfg.say("🔴 the archiver needs bridge_readings_fetch (built with the bridge: zig build): put it on the PATH, "
                "or pass --fetch-bin / set ZB_READINGS_FETCH", flush=True)
        return None
    return path


async def run(cfg: ArchiverConfig, stop: asyncio.Event | None = None) -> ExitCode:
    """Archive `cfg.tenant` until `stop` is set, idle for `cfg.exit_when_idle`, or an exit code."""
    fetch = resolve_fetch(cfg)
    if fetch is None:
        return ExitCode.FAILED
    stop = stop or asyncio.Event()
    say = cfg.say

    nc = await env.connect(cfg.creds, cfg.nats_url)
    try:
        js = nc.jetstream()
        stream = layout.stream_for(cfg.tenant)

        # The stream may not exist yet: the bridge makes it when the tenant has its first row in
        # zebridge_user_tenants. Waiting, rather than exiting, starts the archive the moment it
        # does, under Docker or systemd alike, with one line said instead of a restart loop.
        info = await env.stream_when_there(js, cfg.tenant, say=say)

        # The manifest AFTER the wait: its revision is what the first write must name, and a
        # revision read before a long wait may no longer be the latest.
        manifest, rev = await layout.read_manifest(nc, cfg.tenant)
        archived = manifest is not None   # a tenant with no archive yet has nothing to lose
        manifest = manifest or layout.empty_manifest()
        covers = manifest["covers_up_to"]

        # The retention inequality: a reading must outlive two flushes and an S3 outage.
        max_age = info.config.max_age or 0
        need = 2 * cfg.flush_s + cfg.outage_s
        if max_age and max_age < need:
            say(f"🔴 {stream} keeps readings {max_age:.0f} s; this archiver needs {need:.0f} s "
                f"(2 × flush {cfg.flush_s:.0f} s + outage {cfg.outage_s:.0f} s): raise the stream's max_age", flush=True)
            return ExitCode.RETENTION_SHORT

        # Nothing may be missing between the archive and the stream.
        first = info.state.first_seq
        if archived and info.state.messages > 0 and first > covers + 1 and not cfg.accept_loss:
            say(f"🔴 the stream starts at {first}, the archive covers up to {covers}: readings "
                f"{covers + 1}..{first - 1} are gone and no file holds them (--accept-loss to go on)", flush=True)
            return ExitCode.READINGS_LOST
        start = max(covers + 1, first)
        say(f"archiver {cfg.tenant}: archive covers up to {covers} ({len(manifest['files'])} file(s)), "
            f"following {layout.subject_filter(cfg.tenant)} from {start}", flush=True)

        return await archive_loop(cfg, nc, js, stream, manifest, rev, start, fetch, stop)
    finally:
        await nc.close()


def upload(cfg, s3, manifest, out, key, size, nrows, first_seq, last_seq) -> ExitCode:
    """The file to S3, its size checked there, and its entry in `manifest` (not yet written)."""
    bucket = cfg.bucket or env.bucket()
    s3.upload_file(out, bucket, key)
    head = s3.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != size:
        cfg.say(f"🔴 {key}: {head['ContentLength']} bytes in S3, {size} written; the manifest stays", flush=True)
        return ExitCode.UPLOAD_MISMATCH
    cfg.say(f"flushed {nrows} reading(s) to {key} ({size} bytes)", flush=True)
    if cfg.crash_after_upload:
        os._exit(ExitCode.CRASHED)
    manifest["files"].append({"key": key, "rows": nrows, "bytes": size, "first_seq": first_seq, "last_seq": last_seq})
    manifest["covers_up_to"] = last_seq
    return ExitCode.STOPPED


async def fetch_chunk(fetch, cfg, pos, out):
    """Read up to `cfg.batch` messages from sequence `pos` into the CSV `out`. (summary, None) or (None, why)."""
    cmd = [fetch, "--tenant", cfg.tenant, "--from", str(pos), "--max", str(cfg.batch), "--out", out]
    creds = cfg.creds or os.environ.get("NATS_CREDS")
    name = env.principal_of(creds)
    if name:   # an archiver's credential may subscribe to its own inbox only
        cmd += ["--inbox-prefix", f"_INBOX.{name}"]
    child_env = dict(os.environ)   # the fetcher connects from the environment, like the bridge
    if cfg.nats_url:
        child_env["NATS_URL"] = cfg.nats_url
    if cfg.creds:
        child_env["NATS_CREDS"] = cfg.creds
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                                                env=child_env)
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        return None, f"exit {proc.returncode}: {stderr.decode(errors='replace').strip()[-400:]}"
    return json.loads(stdout), None


CSV_TYPES = ("{tenant:'VARCHAR', sensor_id:'VARCHAR', kind:'VARCHAR', value:'DOUBLE', "
             "sensor_ms:'BIGINT', received_ms:'BIGINT', stream_seq:'BIGINT'}")


def flush_csv(cfg, s3, manifest, csv_path, summary) -> ExitCode:
    """Write a chunk as Parquet, keeping the latest arrival per sensor, kind and sensor time."""
    import duckdb
    first_seq, last_seq = summary["first_row_seq"], summary["last_seq"]
    day = datetime.datetime.fromtimestamp(summary["first_ms"] / 1000, datetime.timezone.utc).date().isoformat()
    key = layout.file_key(cfg.tenant, day, first_seq, last_seq)
    with tempfile.TemporaryDirectory() as d:
        out = f"{d}/out.parquet"
        nrows = duckdb.execute(
            f"COPY ({layout.SELECT_ROWS} FROM read_csv('{csv_path}', header=false, allow_quoted_nulls=false, columns={CSV_TYPES}) "
            f"QUALIFY row_number() OVER (PARTITION BY sensor_id, kind, sensor_ms ORDER BY stream_seq DESC) = 1 "
            f"ORDER BY sensor_ts) TO '{out}' (FORMAT parquet, COMPRESSION zstd)").fetchone()[0]
        size = os.path.getsize(out)
        return upload(cfg, s3, manifest, out, key, size, nrows, first_seq, last_seq)


async def archive_loop(cfg, nc, js, stream, manifest, rev, start, fetch, stop: asyncio.Event) -> ExitCode:
    """Read chunks with bridge_readings_fetch and commit their archive coverage.

    A cycle starts when a batch is waiting or `flush_s` has passed since the first waiting reading,
    and reads until the stream has nothing more: one file and one manifest write per chunk."""
    say = cfg.say
    s3 = env.s3(cfg.s3_endpoint)
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
                if cfg.exit_when_idle and now - idle_since >= cfg.exit_when_idle:
                    break
            elif pending >= cfg.batch or now - waiting_since >= cfg.flush_s:
                idle_since = now
                while not stop.is_set():
                    csv_path = f"{tmp}/chunk.csv"
                    summary, why = await fetch_chunk(fetch, cfg, pos + 1, csv_path)
                    if summary is None:
                        say(f"🔴 bridge_readings_fetch failed ({why})", flush=True)
                        return ExitCode.FAILED
                    if summary["messages"] == 0:
                        break
                    if summary["first_seq"] != pos + 1 and not cfg.accept_loss:
                        say(f"🔴 the stream now starts at {summary['first_seq']}, the archive covers up to {pos}: readings "
                            f"{pos + 1}..{summary['first_seq'] - 1} are gone and no file holds them (--accept-loss to go on)", flush=True)
                        return ExitCode.READINGS_LOST
                    if summary["messages"] != summary["last_seq"] - summary["first_seq"] + 1:
                        say(f"🔴 the reader returned a gap inside {summary['first_seq']}..{summary['last_seq']}; "
                            "the archive manifest stays unchanged", flush=True)
                        return ExitCode.READINGS_LOST
                    if summary["rows"] > 0:
                        code = flush_csv(cfg, s3, manifest, csv_path, summary)
                        if code:
                            return code
                    else:
                        manifest["covers_up_to"] = summary["last_seq"]
                    code, rev = await commit(js, cfg, manifest, rev)
                    if code:
                        return code
                    pos = summary["last_seq"]
                    if summary["messages"] < cfg.batch:
                        break
                waiting_since = None
                continue
            await asyncio.sleep(0.25)
    return ExitCode.STOPPED


async def commit(js, cfg, manifest, rev):
    """Files first, manifest second, on the revision read: one writer per tenant."""
    import nats.js.errors
    try:
        rev = await layout.write_manifest(js, cfg.tenant, manifest, rev)
    except nats.js.errors.APIError as e:
        cfg.say(f"🔴 the manifest moved under this archiver (another one runs?): {e}", flush=True)
        return ExitCode.MANIFEST_MOVED, rev
    cfg.say(f"manifest: covers up to {manifest['covers_up_to']}, {len(manifest['files'])} file(s)", flush=True)
    return ExitCode.STOPPED, rev

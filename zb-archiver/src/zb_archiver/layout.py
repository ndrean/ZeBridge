"""The archive's contract: the stream, the manifest, the files, and what a reading is.

    stream   READINGS_<tenant> on sensors.<tenant>.<sensor>.<kind>, payload {"value", "ts_ms"}
             or a bare number. The bridge makes one per tenant at boot.
    manifest NATS KV time_manifests, key <tenant>.readings:
             {"covers_up_to": <stream seq>, "files": [{"key", "rows", "bytes", "first_seq", "last_seq"}], "schema": 1}
    files    s3://<bucket>/<tenant>/readings/<yyyy-mm-dd>/<first_seq>-<last_seq>.parquet

The manifest is read by DIRECT GET of its exact key and written as a publish on its KV
subject, with the revision expected in a header. Nothing binds the bucket: binding asks
for the bucket's stream info, which the archiver and responder grants refuse (it could
list every tenant's keys).

The writer (the archiver) and every reader (examples/16-archive/responder.py) import these
names from here, so the two sides cannot drift apart.
"""
from __future__ import annotations

import calendar, json, math

STREAM_PREFIX = "READINGS_"
KV = "time_manifests"
TABLE = "readings"
SCHEMA = 1
COLUMNS = "tenant VARCHAR, sensor_id VARCHAR, kind VARCHAR, value DOUBLE, sensor_ts TIMESTAMPTZ, received_at TIMESTAMPTZ, stream_seq BIGINT"


def utc_ts(ms_sql: str) -> str:
    """The DuckDB expression for a TIMESTAMPTZ from milliseconds since 1970 in UTC.

    `make_timestamptz(µs)` takes the instant as it is. The former
    `make_timestamp(µs)::TIMESTAMPTZ` built a TIMESTAMP with no zone and cast it in the
    session's TimeZone: with TZ=Europe/Paris, 1700000000000 was stored as 1699996400000. The
    native archiver (src/archive_duckdb.zig) writes the same expression."""
    return f"make_timestamptz(({ms_sql}) * 1000)"


SELECT_ROWS = (f"SELECT tenant, sensor_id, kind, value, {utc_ts('sensor_ms')} AS sensor_ts, "
               f"{utc_ts('received_ms')} AS received_at, stream_seq")

# 9999-12-31T23:59:59.999Z, the last day datetime can name (readings_decode.zig, max_sensor_ms)
MAX_SENSOR_MS = 253_402_300_799_999


def stream_for(tenant: str) -> str:
    return f"{STREAM_PREFIX}{tenant}"


def subject_filter(tenant: str) -> str:
    return f"sensors.{tenant}.>"


def manifest_key(tenant: str) -> str:
    return f"{tenant}.{TABLE}"


def file_key(tenant: str, day: str, first_seq: int, last_seq: int) -> str:
    """The object key of one Parquet file: `day` is the sensor date of its first row, ISO."""
    return f"{tenant}/{TABLE}/{day}/{first_seq:012d}-{last_seq:012d}.parquet"


def empty_manifest() -> dict:
    return {"covers_up_to": 0, "files": [], "schema": SCHEMA}


async def read_manifest(nc, tenant: str):
    """(manifest, revision): (None, None) when the key never existed, (None, rev) when it
    was deleted or purged — the next write must name that revision."""
    m = await nc.request(f"$JS.API.DIRECT.GET.KV_{KV}.$KV.{KV}.{manifest_key(tenant)}", b"", timeout=5)
    h = m.headers or {}
    if h.get("Status") == "404" or not h.get("Nats-Sequence"):
        return None, None
    rev = int(h["Nats-Sequence"])
    if h.get("KV-Operation") in ("DEL", "PURGE") or not m.data:
        return None, rev
    return json.loads(m.data), rev


async def write_manifest(js, tenant: str, manifest: dict, rev: int | None) -> int:
    """The manifest's next revision, written only if `rev` is still the latest: a second
    writer's publish is refused (wrong last sequence) and raises."""
    ack = await js.publish(f"$KV.{KV}.{manifest_key(tenant)}", json.dumps(manifest).encode(),
                           headers={"Nats-Expected-Last-Subject-Sequence": str(rev or 0)})
    return ack.seq


def _reject_constant(name):
    raise ValueError(name)   # NaN, Infinity, -Infinity: not JSON, and not a reading


def _object(pairs):
    """A JSON object, but a repeated key is not a reading: RFC 8259 leaves the value unpredictable."""
    d = dict(pairs)
    if len(d) != len(pairs):
        raise ValueError("duplicate key")
    return d


def stream_ms(msg) -> int:
    """The stream's timestamp in whole milliseconds, truncated — the native reader's rule exactly.
    The ack subject ends `…<timestamp in ns>.<pending>`; nats-py turns the nanoseconds into a
    datetime through a float and is off by a millisecond often enough to matter, so read them."""
    try:
        return int(msg.reply.split(".")[-2]) // 1_000_000
    except (AttributeError, ValueError, IndexError):
        t = msg.metadata.timestamp
        return calendar.timegm(t.utctimetuple()) * 1000 + t.microsecond // 1000


def reading(msg) -> dict | None:
    """One stream message as a row; None for a subject or payload that is not a reading.

    The payload contract (the same rules as src/readings_decode.zig, which the native reader
    applies; scripts/scenarios/readings_fetch.py feeds both the same payloads):
    a JSON number, or {"value": <number>, "ts_ms": <integer, float or null>} with BOTH keys,
    the time between 1970 and the year 9999.
    A quoted number, a boolean, NaN/Infinity, a number that overflows to infinity and an
    object without ts_ms are not readings. Skipped, never a crash: a message the archiver
    cannot read must not stop the archive of the ones behind it."""
    parts = msg.subject.split(".")
    if len(parts) != 4:
        return None
    try:
        # Decoded as UTF-8 here, not by json.loads(bytes): that one sniffs UTF-16/32 and a BOM.
        body = json.loads(msg.data.decode("utf-8"), parse_constant=_reject_constant, object_pairs_hook=_object)
    except (ValueError, RecursionError):   # UnicodeDecodeError and JSONDecodeError are ValueErrors
        return None
    if isinstance(body, dict):
        if "value" not in body or "ts_ms" not in body:
            return None
        value, ts_ms = body["value"], body["ts_ms"]
        if ts_ms is not None:
            if isinstance(ts_ms, bool) or not isinstance(ts_ms, (int, float)):
                return None
            if isinstance(ts_ms, int):
                if not -2**63 <= ts_ms < 2**63:
                    return None
            elif not (math.isfinite(ts_ms) and abs(ts_ms) < 9.2e18):
                return None
    else:
        value, ts_ms = body, None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        value = float(value)
    except OverflowError:
        return None
    if not math.isfinite(value):
        return None
    meta = msg.metadata
    received_ms = stream_ms(msg)
    sensor_ms = int(ts_ms) if ts_ms is not None else received_ms
    if not 0 <= sensor_ms <= MAX_SENSOR_MS:   # no day to name a file after: not a time
        return None
    return {"tenant": parts[1], "sensor_id": parts[2], "kind": parts[3], "value": value,
            "sensor_ms": sensor_ms, "received_ms": received_ms, "stream_seq": meta.sequence.stream}

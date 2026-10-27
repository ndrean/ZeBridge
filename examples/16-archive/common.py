"""What the archiver and the responder share: the stream, the manifest, the files.

    stream   READINGS_<tenant> on sensors.<tenant>.<sensor>.<kind>, payload {"value", "ts_ms"}
             or a bare number. The bridge makes one per tenant at boot.
    manifest NATS KV time_manifests, key <tenant>.readings:
             {"covers_up_to": <stream seq>, "files": [{"key", "rows", "bytes", "first_seq", "last_seq"}], "schema": 1}
    files    s3://<bucket>/<tenant>/readings/<yyyy-mm-dd>/<first_seq>-<last_seq>.parquet

The manifest is read by DIRECT GET of its exact key and written as a publish on its KV
subject, with the revision expected in a header. Nothing binds the bucket: binding asks
for the bucket's stream info, which the archiver and responder grants refuse (it could
list every tenant's keys).

NATS: NATS_URL, NATS_CREDS, and NATS_TLS_CA for a server whose certificate a private CA signed.

S3 is any S3 API: SeaweedFS locally, Cloudflare R2 on a server. Settings from the environment:
S3_ENDPOINT (http://127.0.0.1:8333), S3_BUCKET (zb-events), S3_REGION (auto), and the keys from
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, or else the file S3_KEYS (~/.config/zebridge/s3.json:
SeaweedFS's identity file, or {"access_key_id", "secret_access_key"}).
"""
from __future__ import annotations

import base64, calendar, json, math, os, pathlib

STREAM_PREFIX = "READINGS_"
KV = "time_manifests"
TABLE = "readings"
SCHEMA = 1
COLUMNS = "tenant VARCHAR, sensor_id VARCHAR, kind VARCHAR, value DOUBLE, sensor_ts TIMESTAMPTZ, received_at TIMESTAMPTZ, stream_seq BIGINT"
BUCKET = os.environ.get("S3_BUCKET", "zb-events")


def stream_for(tenant: str) -> str:
    return f"{STREAM_PREFIX}{tenant}"


def subject_filter(tenant: str) -> str:
    return f"sensors.{tenant}.>"


def manifest_key(tenant: str) -> str:
    return f"{tenant}.{TABLE}"


def principal_of(creds_path: str | None) -> str | None:
    """The user name in a creds file's JWT: its own inbox is `_INBOX.<name>.>`."""
    if not creds_path:
        return None
    text = pathlib.Path(creds_path).read_text()
    jwt = text.split("-----BEGIN NATS USER JWT-----", 1)[1].split("------END", 1)[0].strip()
    body = jwt.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))).get("name")


def nats_url() -> str:
    """NATS_URL; unset, the dev server. The images set it EMPTY: a container must name its server."""
    url = os.environ.get("NATS_URL")
    if url == "":
        raise SystemExit("🔴 NATS_URL is not set: pass the NATS server, e.g. -e NATS_URL=tls://nats.example.com:4222")
    return url or "nats://127.0.0.1:4222"


async def connect(creds: str | None = None):
    """NATS with the given creds (or NATS_CREDS), and the principal's own inbox prefix."""
    import nats
    url = nats_url()
    creds = creds or os.environ.get("NATS_CREDS")
    if creds and not os.path.exists(creds):
        raise SystemExit(f"🔴 no creds file at {creds}: mount the archiver's (bridge --mint-archiver), e.g. -v archiver-acme.creds:{creds}:ro")
    name = principal_of(creds)
    kw = {"user_credentials": creds} if creds else {}
    if name:
        kw["inbox_prefix"] = f"_INBOX.{name}".encode()
    if os.environ.get("NATS_TLS_CA"):   # a server whose certificate a private CA signed
        import ssl
        kw["tls"] = ssl.create_default_context(cafile=os.environ["NATS_TLS_CA"])
    return await nats.connect(url, **kw)


def s3():
    import boto3
    from botocore.config import Config
    endpoint = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:8333")
    local = any(h in endpoint for h in ("://127.0.0.1", "://localhost", "://[::1]"))
    kw = {}
    if not os.environ.get("AWS_ACCESS_KEY_ID") and not local:
        # The dev keys file is for the local S3 only: a misspelled variable once sent them to
        # R2, which answered "400 Bad Request" with no word of why.
        raise SystemExit(f"🔴 AWS_ACCESS_KEY_ID is not set for {endpoint}: set AWS_ACCESS_KEY_ID and "
                         "AWS_SECRET_ACCESS_KEY (the S3 key pair; with R2, the API token's access key)")
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        path = pathlib.Path(os.environ.get("S3_KEYS", pathlib.Path.home() / ".config" / "zebridge" / "s3.json"))
        if path.exists():
            d = json.loads(path.read_text())
            c = d["identities"][0]["credentials"][0] if "identities" in d else d
            kw = {"aws_access_key_id": c.get("accessKey") or c["access_key_id"],
                  "aws_secret_access_key": c.get("secretKey") or c["secret_access_key"]}
    return boto3.client("s3", endpoint_url=endpoint,
                        region_name=os.environ.get("S3_REGION", "auto"),
                        config=Config(s3={"addressing_style": "path"}), **kw)


async def stream_when_there(js, tenant: str, every_s: float = 10):
    """The tenant's stream, waited for: the bridge makes it when the tenant has its first row
    in zebridge_user_tenants. Said once, then checked every `every_s` seconds."""
    from nats.js.errors import NotFoundError
    stream, said = stream_for(tenant), False
    while True:
        try:
            return await js.stream_info(stream)
        except NotFoundError:
            if not said:
                print(f"⏳ waiting for the stream {stream}: the bridge makes it when the tenant has a row in "
                      f"zebridge_user_tenants (INSERT INTO zebridge_user_tenants (principal, tenant_id) "
                      f"VALUES ('<principal>', '{tenant}')). Checking every {every_s:g} s.", flush=True)
                said = True
            import asyncio
            await asyncio.sleep(every_s)


# 9999-12-31T23:59:59.999Z, the last day datetime can name (readings_decode.zig, max_sensor_ms)
MAX_SENSOR_MS = 253_402_300_799_999


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


SELECT_ROWS = ("SELECT tenant, sensor_id, kind, value, make_timestamp(sensor_ms * 1000)::TIMESTAMPTZ AS sensor_ts, "
               "make_timestamp(received_ms * 1000)::TIMESTAMPTZ AS received_at, stream_seq")


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


def fetch_files(files: list[dict], cache: pathlib.Path) -> list[pathlib.Path]:
    """Each file once: Parquet never changes, so a file in the cache is the file."""
    out, client = [], None
    for f in files:
        local = cache / f["key"]
        if not local.exists() or local.stat().st_size != f["bytes"]:
            client = client or s3()
            local.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(BUCKET, f["key"], str(local) + ".part")
            os.replace(str(local) + ".part", local)
        out.append(local)
    return out


def decode_answer(data: bytes):
    """An answer as a raw NATS client sees it: zstd when it was worth compressing."""
    if data[:4] == b"\x28\xb5\x2f\xfd":
        from compression import zstd
        data = zstd.decompress(data)
    return json.loads(data)


async def ensure_for_tests(js, tenant: str):
    """Tests only, with the bridge's creds: the stream and bucket the bridge makes at boot,
    for a stack whose bridge predates them."""
    from nats.js.api import StreamConfig, KeyValueConfig
    from nats.js.errors import NotFoundError
    try:
        await js.stream_info(stream_for(tenant))
    except NotFoundError:
        await js.add_stream(StreamConfig(name=stream_for(tenant), subjects=[subject_filter(tenant)],
                                         max_age=24 * 3600, max_bytes=256 << 20, discard="old"))
    try:
        info = await js.stream_info(f"KV_{KV}")
        if not info.config.allow_direct:   # a bucket made before the bridge made it: reads by direct get
            info.config.allow_direct = True
            await js.update_stream(info.config)
    except NotFoundError:
        await js.create_key_value(KeyValueConfig(bucket=KV, history=10, direct=True))


async def reset_tenant(js, tenant: str):
    """Tests only: forget a tenant's archive — manifest, files, and its readings."""
    try:
        await js.publish(f"$KV.{KV}.{manifest_key(tenant)}", b"", headers={"KV-Operation": "PURGE", "Nats-Rollup": "sub"})
    except Exception:
        pass
    client = s3()
    try:
        client.head_bucket(Bucket=BUCKET)
    except Exception:
        client.create_bucket(Bucket=BUCKET)
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{tenant}/{TABLE}/"):
        for o in page.get("Contents", []):
            client.delete_object(Bucket=BUCKET, Key=o["Key"])
    await js.purge_stream(stream_for(tenant))

"""What the archiver and the responder share: the stream, the manifest, the files.

    stream   READINGS on sensors.<tenant>.<sensor>.<kind>, payload {"value", "ts_ms"} or a bare number
    manifest NATS KV time_manifests, key <tenant>.readings:
             {"covers_up_to": <stream seq>, "files": [{"key", "rows", "bytes", "first_seq", "last_seq"}], "schema": 1}
    files    s3://<bucket>/<tenant>/readings/<yyyy-mm-dd>/<first_seq>-<last_seq>.parquet

S3 is any S3 API: SeaweedFS locally, Cloudflare R2 on a server. Settings from the environment:
S3_ENDPOINT (http://127.0.0.1:8333), S3_BUCKET (zb-events), S3_REGION (auto), and the keys from
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, or else the file S3_KEYS (~/.config/zebridge/s3.json:
SeaweedFS's identity file, or {"access_key_id", "secret_access_key"}).
"""
from __future__ import annotations

import json, os, pathlib

STREAM = "READINGS"
KV = "time_manifests"
TABLE = "readings"
SCHEMA = 1
COLUMNS = "tenant VARCHAR, sensor_id VARCHAR, kind VARCHAR, value DOUBLE, sensor_ts TIMESTAMPTZ, received_at TIMESTAMPTZ, stream_seq BIGINT"
BUCKET = os.environ.get("S3_BUCKET", "zb-events")


def subject_filter(tenant: str) -> str:
    return f"sensors.{tenant}.>"


def manifest_key(tenant: str) -> str:
    return f"{tenant}.{TABLE}"


def s3():
    import boto3
    from botocore.config import Config
    kw = {}
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        path = pathlib.Path(os.environ.get("S3_KEYS", pathlib.Path.home() / ".config" / "zebridge" / "s3.json"))
        if path.exists():
            d = json.loads(path.read_text())
            c = d["identities"][0]["credentials"][0] if "identities" in d else d
            kw = {"aws_access_key_id": c.get("accessKey") or c["access_key_id"],
                  "aws_secret_access_key": c.get("secretKey") or c["secret_access_key"]}
    return boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT", "http://127.0.0.1:8333"),
                        region_name=os.environ.get("S3_REGION", "auto"),
                        config=Config(s3={"addressing_style": "path"}), **kw)


def reading(msg) -> dict | None:
    """One stream message as a row; None for a subject or payload that is not a reading."""
    parts = msg.subject.split(".")
    if len(parts) != 4:
        return None
    try:
        body = json.loads(msg.data)
        value, ts_ms = (body["value"], body["ts_ms"]) if isinstance(body, dict) else (body, None)
        value = float(value)
    except (ValueError, KeyError, TypeError):
        return None
    meta = msg.metadata
    received_ms = int(meta.timestamp.timestamp() * 1000)
    return {"tenant": parts[1], "sensor_id": parts[2], "kind": parts[3], "value": value,
            "sensor_ms": int(ts_ms) if ts_ms is not None else received_ms,
            "received_ms": received_ms, "stream_seq": meta.sequence.stream}


SELECT_ROWS = ("SELECT tenant, sensor_id, kind, value, make_timestamp(sensor_ms * 1000)::TIMESTAMPTZ AS sensor_ts, "
               "make_timestamp(received_ms * 1000)::TIMESTAMPTZ AS received_at, stream_seq")


async def provision(js, max_age_s: int = 24 * 3600):
    """The stream and the manifest bucket, created when missing (idempotent)."""
    from nats.js.api import StreamConfig, KeyValueConfig
    from nats.js.errors import NotFoundError
    try:
        await js.stream_info(STREAM)
    except NotFoundError:
        await js.add_stream(StreamConfig(name=STREAM, subjects=["sensors.>"], max_age=max_age_s))
    try:
        await js.key_value(KV)
    except Exception:
        await js.create_key_value(KeyValueConfig(bucket=KV, history=10))


async def read_manifest(kv, tenant: str):
    """(manifest, revision), or (None, None) when the tenant has none yet."""
    from nats.js.errors import KeyNotFoundError
    try:
        e = await kv.get(manifest_key(tenant))
    except KeyNotFoundError:
        return None, None
    if e.value is None:   # deleted
        return None, e.revision
    return json.loads(e.value), e.revision


def manifest_sync(tenant: str) -> dict | None:
    """The current manifest, from a script that is not async."""
    import asyncio, nats

    async def go():
        nc = await nats.connect(os.environ.get("NATS_URL", "nats://127.0.0.1:4222"),
                                user_credentials=os.environ.get("NATS_CREDS"))
        try:
            return (await read_manifest(await nc.jetstream().key_value(KV), tenant))[0]
        finally:
            await nc.close()
    return asyncio.run(go())


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


async def reset_tenant(js, tenant: str):
    """Tests only: forget a tenant's archive — manifest, files, and its readings in the stream."""
    kv = await js.key_value(KV)
    try:
        await kv.purge(manifest_key(tenant))
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
    await js.purge_stream(STREAM, subject=subject_filter(tenant))

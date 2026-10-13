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

S3 is any S3 API: SeaweedFS locally, Cloudflare R2 on a server. Settings from the environment:
S3_ENDPOINT (http://127.0.0.1:8333), S3_BUCKET (zb-events), S3_REGION (auto), and the keys from
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, or else the file S3_KEYS (~/.config/zebridge/s3.json:
SeaweedFS's identity file, or {"access_key_id", "secret_access_key"}).
"""
from __future__ import annotations

import base64, json, os, pathlib

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


async def connect(creds: str | None = None):
    """NATS with the given creds (or NATS_CREDS), and the principal's own inbox prefix."""
    import nats
    creds = creds or os.environ.get("NATS_CREDS")
    name = principal_of(creds)
    kw = {"user_credentials": creds} if creds else {}
    if name:
        kw["inbox_prefix"] = f"_INBOX.{name}".encode()
    return await nats.connect(os.environ.get("NATS_URL", "nats://127.0.0.1:4222"), **kw)


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

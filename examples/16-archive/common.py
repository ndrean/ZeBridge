"""What the example's programs share. The archive's contract and connections are the
zb-archiver library (zb-archiver/, `pip install zb-archiver`); this module re-exports them
under the names the responder and the scenarios use, and adds what only they need.

Without the package installed, the library is taken from the repository's zb-archiver/src.
"""
from __future__ import annotations

import json, pathlib, sys

try:
    import zb_archiver
except ImportError:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "zb-archiver" / "src"))
    import zb_archiver

from zb_archiver.env import connect, fetch_files, nats_url, principal_of, s3, stream_when_there, bucket
from zb_archiver.layout import (COLUMNS, KV, MAX_SENSOR_MS, SCHEMA, SELECT_ROWS, STREAM_PREFIX, TABLE,
                                manifest_key, read_manifest, reading, stream_for, stream_ms, subject_filter,
                                utc_ts, write_manifest)

BUCKET = bucket()


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

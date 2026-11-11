#!/usr/bin/env python3
"""Test a built native archiver image on disposable Docker services.

scripts/scenarios/.venv/bin/python deploy/test-archiver-image.py IMAGE
Needs Docker, nats-py, boto3 and duckdb. No dev stack or real credentials are used.
"""
import asyncio
import json
import pathlib
import subprocess
import sys
import tempfile
import time
import uuid

import boto3
import duckdb
import nats
from botocore.config import Config
from nats.js.api import KeyValueConfig, StreamConfig


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True, timeout=180)


async def main(image):
    tag = "zb-archive-test-" + uuid.uuid4().hex[:10]
    names = [tag + suffix for suffix in ("-nats", "-s3", "-run", "-wait")]
    network = tag
    nc = None
    with tempfile.TemporaryDirectory(prefix=tag) as tmp:
        tmp = pathlib.Path(tmp)
        keys = tmp / "s3.json"
        keys.write_text(json.dumps({"identities": [{"name": "test", "credentials": [{
            "accessKey": "archive-test", "secretKey": "archive-test-secret"}],
            "actions": ["Admin", "Read", "List", "Write"]}]}))
        docker("network", "create", network)
        try:
            missing = docker("run", "--rm", image, "--tenant", "test", check=False)
            assert missing.returncode == 1 and "NATS_URL is not set" in missing.stderr, missing
            docker("run", "--rm", "--entrypoint", "/bin/sh", image, "-ec",
                   'test "$(id -u)" != 0; '
                   'if command -v python3; then exit 1; fi; '
                   'if ldd /usr/local/bin/bridge_archiver /usr/local/lib/libduckdb.so | grep "not found"; then exit 1; fi')
            print("PASS native image: non-root, no Python, dependencies present", flush=True)
            docker("run", "-d", "--name", names[0], "--network", network, "--network-alias", "nats",
                   "-p", "127.0.0.1::4222", "nats:2.15.0-alpine", "-js")
            docker("run", "-d", "--name", names[1], "--network", network, "--network-alias", "s3",
                   "-p", "127.0.0.1::8333", "-v", f"{keys}:/etc/s3.json:ro",
                   "chrislusf/seaweedfs:latest", "server", "-s3", "-s3.config=/etc/s3.json", "-dir=/data")
            port = lambda name, p: docker("port", name, p).stdout.strip().rsplit(":", 1)[1]
            nats_port, s3_port = port(names[0], "4222/tcp"), port(names[1], "8333/tcp")
            s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{s3_port}", region_name="auto",
                              aws_access_key_id="archive-test", aws_secret_access_key="archive-test-secret",
                              config=Config(connect_timeout=1, read_timeout=1, retries={"max_attempts": 0},
                                            s3={"addressing_style": "path"}))
            for _ in range(100):
                try:
                    s3.create_bucket(Bucket="archive-test")
                    break
                except Exception:
                    await asyncio.sleep(.2)
            else:
                raise RuntimeError(docker("logs", names[1]).stdout)
            nc = await nats.connect(f"nats://127.0.0.1:{nats_port}", max_reconnect_attempts=1)
            js = nc.jetstream()
            kv = await js.create_key_value(KeyValueConfig(bucket="time_manifests", history=10, direct=True))
            await js.add_stream(StreamConfig(name="READINGS_test", subjects=["sensors.test.>"], max_age=86400))
            for i in range(20):
                await js.publish("sensors.test.s.temp", json.dumps({"value": i, "ts_ms": 1700000000000 + i}).encode())
            await js.publish("sensors.test.s.temp", b'{"value":999,"ts_ms":1700000000000}')
            await js.publish("sensors.test.s.temp", b'{"value":"invalid"}')
            settings = ["--network", network, "-e", "NATS_URL=nats://nats:4222", "-e", "NATS_CREDS=",
                        "-e", "S3_ENDPOINT=http://s3:8333", "-e", "S3_BUCKET=archive-test",
                        "-e", "AWS_ACCESS_KEY_ID=archive-test", "-e", "AWS_SECRET_ACCESS_KEY=archive-test-secret",
                        "-e", "TZ=Europe/Paris"]
            args = ["--tenant", "test", "--flush-s", "0", "--batch", "100", "--exit-when-idle", "1"]
            first = docker("run", "--rm", *settings, image, *args)
            print(first.stdout, end="", flush=True)
            entry = await kv.get("test.readings")
            manifest = json.loads(entry.value)
            assert manifest["covers_up_to"] == 22 and len(manifest["files"]) == 1, manifest
            f = manifest["files"][0]
            obj = s3.get_object(Bucket="archive-test", Key=f["key"])["Body"].read()
            assert len(obj) == f["bytes"]
            parquet = tmp / "readings.parquet"
            parquet.write_bytes(obj)
            rows = duckdb.sql(f"SELECT epoch_ms(sensor_ts), value FROM read_parquet('{parquet}') ORDER BY sensor_ts").fetchall()
            assert rows == [(1700000000000 + i, 999.0 if i == 0 else float(i)) for i in range(20)], rows
            print("PASS S3 Parquet: exact rows, dedupe, UTC timestamps and skipped-message coverage", flush=True)
            docker("run", "--rm", *settings, image, *args)
            assert (await kv.get("test.readings")).revision == entry.revision
            print("PASS restart: no duplicate file or manifest write", flush=True)
            await js.publish("sensors.test.s.temp", b'1')
            await js.purge_stream("READINGS_test")
            lost = docker("run", "--rm", *settings, image, *args, check=False)
            assert lost.returncode == 4, lost
            assert (await kv.get("test.readings")).revision == entry.revision
            print("PASS empty pruned stream: exit 4, manifest unchanged", flush=True)
            docker("run", "-d", "--name", names[3], *settings, image, "--tenant", "missing")
            for _ in range(100):
                if "waiting for the stream" in docker("logs", names[3]).stdout:
                    break
                await asyncio.sleep(.1)
            else:
                raise RuntimeError("archiver never reached the missing-stream wait")
            started = time.monotonic()
            docker("stop", "--time", "5", names[3])
            assert docker("inspect", "-f", "{{.State.ExitCode}}", names[3]).stdout.strip() == "0"
            assert time.monotonic() - started < 5
            print("PASS SIGTERM: native process stops cleanly while waiting", flush=True)
        finally:
            if nc:
                await nc.close()
            for name in names:
                docker("rm", "-f", "-v", name, check=False)
            docker("network", "rm", network, check=False)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))

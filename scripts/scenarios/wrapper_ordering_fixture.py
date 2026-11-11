"""Run a wrapper's ordering probe against a disposable JetStream with no verdict producer.

Run with the scenarios Python: wrapper_ordering_fixture.py [--port PORT] COMMAND [ARGS ...].
The command receives the broker address in ZB_ORDERING_URL. On Android use adb reverse.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

import nats
from nats.js.api import KeyValueConfig, StreamConfig


async def provision(url):
    nc = await nats.connect(url)
    try:
        js = nc.jetstream()
        for name in ("tenants", "schemas", "generations"):
            await js.create_key_value(KeyValueConfig(bucket=name, storage="memory", direct=True))
        await (await js.key_value("tenants")).put("tester", b'["_default"]')
        await (await js.key_value("schemas")).put("wr_rows", json.dumps({
            "lsn": 1, "pk_columns": ["uid"], "writable": True, "ordering": 1,
            "sqlite": {"columns": [{"name": "uid", "type": "TEXT"}]},
        }).encode())
        for name, subject in (("CDC_PUBLIC", "cdc._default.>"), ("MUTATIONS", "mutation.>"),
                              ("VERDICTS", "mutation_ack.>")):
            await js.add_stream(StreamConfig(name=name, subjects=[subject], storage="memory", allow_direct=True))
    finally:
        await nc.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.command:
        parser.error("supply the command to run against the fixture")
    with socket.socket() as s:
        s.bind(("127.0.0.1", args.port))
        port = s.getsockname()[1]
    url = f"nats://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory(prefix="zb-wrapper-") as tmp:
        with Path(tmp, "server.log").open("w+") as log:
            server = subprocess.Popen(["nats-server", "-a", "127.0.0.1", "-p", str(port),
                                       "-js", "-sd", tmp], stdout=log, stderr=log)
            try:
                for _ in range(100):
                    if server.poll() is not None:
                        log.seek(0)
                        raise RuntimeError(log.read())
                    try:
                        socket.create_connection(("127.0.0.1", port), timeout=.1).close()
                        break
                    except OSError:
                        time.sleep(.05)
                else:
                    raise RuntimeError("private NATS did not start")
                asyncio.run(provision(url))
                return subprocess.run(args.command, env={**os.environ, "ZB_ORDERING_URL": url}).returncode
            finally:
                server.terminate()
                server.wait(timeout=10)


if __name__ == "__main__":
    raise SystemExit(main())

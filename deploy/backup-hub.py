#!/usr/bin/env python3
"""Plan a hub recovery checkpoint; --apply pauses admission, drains and dumps PostgreSQL.

Uses NATS_URL/NATS_CREDS/NATS_JS_DOMAIN, DATABASE_READER_URL/DATABASE_WRITER_URL
(for a rendered role/schema bootstrap) and ADMIN_DATABASE_URL (pg_dump only).
The checkpoint contains secrets and application data: its directory is 0700 and
files are 0600. Keep the offline operator store and deployment vault separately.
Encrypt and copy the completed checkpoint to the operator's backup destination.
"""
import argparse
import asyncio
import copy
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import time
import urllib.parse
import uuid

import nats

ROOT = pathlib.Path(__file__).resolve().parents[1]


def postgres_environment(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in ("postgres", "postgresql") or not parsed.hostname or not parsed.path.strip("/"):
        raise ValueError("ADMIN_DATABASE_URL must name a database")
    env = dict(os.environ, PGHOST=parsed.hostname, PGPORT=str(parsed.port or 5432),
               PGDATABASE=urllib.parse.unquote(parsed.path.lstrip("/")),
               PGUSER=urllib.parse.unquote(parsed.username or "postgres"),
               PGPASSWORD=urllib.parse.unquote(parsed.password or ""))
    for name, value in urllib.parse.parse_qsl(parsed.query):
        if name in ("sslmode", "sslrootcert", "sslcert", "sslkey"):
            env["PG" + name.upper()] = value
    return env


def copy_secret(source, destination):
    with open(source, "rb") as original, open(destination, "xb") as target:
        os.chmod(destination, 0o600)
        shutil.copyfileobj(original, target)
        target.flush()
        os.fsync(target.fileno())


async def checkpoint(nc, destination, config, credentials, dump, bootstrap, *, domain=None, apply=False, timeout=60):
    js = nc.jetstream(domain=domain)
    info = await js.stream_info("MUTATIONS")
    if info.config.subjects != ["$ZB.mutation.>"]:
        raise RuntimeError("checkpoint requires the private admission queue; migrate the legacy queue first")
    print(json.dumps({"action": "checkpoint", "apply": apply, "queued_writes": info.state.messages,
                      "destination": str(destination), "recovery": "restore PostgreSQL and rebuild a fresh JetStream feed"}))
    if not apply:
        return
    destination = pathlib.Path(destination)
    destination.mkdir(mode=0o700)  # Refuse overwriting a previous recovery point.
    original = copy.deepcopy(info.config)
    paused = copy.deepcopy(original)
    paused.subjects = ["$ZB.BACKUP_PAUSED." + uuid.uuid4().hex]
    # Persist the recovery instruction BEFORE pausing. If this process or host
    # dies, an operator has the exact stream configuration needed to resume.
    restore = destination / "resume-mutations.json"
    restore.write_text(json.dumps(original.as_dict()))
    restore.chmod(0o600)
    paused_capture = False
    try:
        await js.update_stream(config=paused)
        paused_capture = True
        end = time.monotonic() + timeout
        while (await js.stream_info("MUTATIONS")).state.messages:
            if time.monotonic() >= end:
                raise RuntimeError("accepted writes did not drain; no completed checkpoint was produced")
            await asyncio.sleep(.1)
        # No public publish can receive a successful durable PubAck while capture
        # is paused. Every pre-pause accepted write has committed before its ACK.
        await asyncio.to_thread(dump, destination / "database.dump")
        bootstrap(destination / "bootstrap.sql")
        copy_secret(config, destination / "nats-server.conf")
        copy_secret(credentials, destination / "bridge.creds")
        manifest = {"format": 1, "created_at": time.time(), "mutation_messages": 0,
                    "recovery": "fresh JetStream feed; generation metadata must be cleared before the first bridge starts",
                    "files": {}}
        for filename in ("database.dump", "bootstrap.sql", "nats-server.conf", "bridge.creds", "resume-mutations.json"):
            path = destination / filename
            path.chmod(0o600)
            with path.open("rb") as source:
                manifest["files"][filename] = hashlib.file_digest(source, "sha256").hexdigest()
        path = destination / "manifest.json"
        path.write_text(json.dumps(manifest, indent=2) + "\n")
        path.chmod(0o600)
    finally:
        if paused_capture:
            await js.update_stream(config=original)
    print("Checkpoint complete; mutation capture resumed. Encrypt and copy it off the hub, then verify a restore.")


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=pathlib.Path)
    parser.add_argument("--config", required=True, type=pathlib.Path, help="the hub NATS config, including current account revocations")
    parser.add_argument("--bridge", default=os.environ.get("ZB_BRIDGE_BIN", str(ROOT / "zig-out/bin/bridge")))
    parser.add_argument("--pg-dump", default="pg_dump")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--drain-timeout", type=float, default=60)
    args = parser.parse_args()
    credentials = pathlib.Path(os.environ["NATS_CREDS"])
    def dump(path):
        subprocess.run([args.pg_dump, "-Fc", "-f", str(path)], env=postgres_environment(os.environ["ADMIN_DATABASE_URL"]), check=True)
    def bootstrap(path):
        with open(path, "xb") as output:
            os.chmod(path, 0o600)
            subprocess.run([args.bridge, "--init-sql"], stdout=output, check=True)
    nc = await nats.connect(os.environ["NATS_URL"], user_credentials=str(credentials))
    try:
        await checkpoint(nc, args.destination, args.config, credentials, dump, bootstrap,
                         domain=os.environ.get("NATS_JS_DOMAIN") or None, apply=args.apply, timeout=args.drain_timeout)
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())

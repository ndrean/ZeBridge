#!/usr/bin/env python3
"""Two NATS faults the snapshot listener used to handle badly.

**A — the REQUESTS stream is missing.** The listener retried forever, one log line every
2s, while everything else looked healthy: `/health` green, CDC flowing, publisher
connected on its own connection. No client could ever bootstrap. A listener that has
never connected is describing a configuration fault, not an outage, so the first
connection is now bounded and the bridge stops.

**B — a JetStream status message.** A pull consumer also receives control frames on its
own reply inbox (`<uuid>.<sid>`), and those carry no reply-to. Acking one publishes to
`msg.letter.ReplyTo().?` in the vendored client — a null unwrap that panicked the
snapshot thread, right after logging
`📸 Invalid snapshot request subject: 04b01ecd-…-e734dca6ad10.2196`. Deleting the
consumer out from under a running bridge makes the server send exactly that frame.

Both phases mutate the broker and put it back: A saves the stream config and recreates
it byte-identically, B lets the listener recreate its own durable consumer.

Usage:  python scripts/scenarios/faults.py
"""

import json
import pathlib
import sys

import zb

STREAM = zb.TOPOLOGY["streams"]["requests"]
CONSUMER = "bridge_snapshot_worker"
TMP = pathlib.Path("/tmp")


def phase_a() -> int:
    print(f"\nA. {STREAM} missing → the bridge must stop, not spin")

    saved = zb.nats_cli("stream", "info", STREAM, "--json")
    if saved.returncode != 0:
        zb.bad(f"cannot read {STREAM}: {saved.stderr.strip()}")
        return 1
    config = json.loads(saved.stdout)["config"]
    cfg_file = TMP / "zb_requests.cfg.json"
    cfg_file.write_text(json.dumps(config, indent=2))

    rm = zb.nats_cli("stream", "rm", STREAM, "-f")
    if rm.returncode != 0:
        zb.bad(f"cannot delete {STREAM}: {rm.stderr.strip()}")
        return 1
    print(f"  {STREAM} deleted (config saved to {cfg_file})")

    failed = 0
    try:
        with zb.Bridge("/tmp/zb_faults_a.log") as br:
            code = br.wait_for_exit(timeout=60)
            text = br.text()

        if code is None:
            zb.bad("still running after 60s — it is spinning on a connection it will never get")
            failed = 1
        elif "FATAL" not in text or "never reached NATS" not in text:
            zb.bad(f"exited {code} but without the FATAL diagnosis — check /tmp/zb_faults_a.log")
            failed = 1
        elif code == 0:
            # A supervisor reads the exit code, not the log: 0 means "job done", so
            # `restart: on-failure` would leave a misconfigured bridge stopped and quiet.
            zb.bad("stopped and diagnosed, but exited 0 — a misconfiguration must be a failure")
            failed = 1
        else:
            zb.ok(f"stopped after the boot budget, naming the cause, exit {code}")
    finally:
        failed += restore(config, cfg_file)

    return failed


def restore(config, cfg_file) -> int:
    """Put REQUESTS back exactly as it was, and prove it."""
    add = zb.nats_cli("stream", "add", STREAM, "--config", str(cfg_file))
    if add.returncode != 0:
        zb.bad(f"COULD NOT RESTORE {STREAM}: {add.stderr.strip()}")
        print(f"    restore by hand: nats stream add {STREAM} --config {cfg_file}")
        return 1
    back = json.loads(zb.nats_cli("stream", "info", STREAM, "--json").stdout)["config"]
    drift = {k: (config.get(k), back.get(k)) for k in set(config) | set(back)
             if config.get(k) != back.get(k)}
    if drift:
        zb.bad(f"{STREAM} restored with different settings: {drift}")
        return 1
    zb.ok(f"{STREAM} restored byte-identically")
    return 0


def phase_b() -> int:
    print(f"\nB. {CONSUMER} deleted under a running bridge → status frame, not a panic")

    with zb.Bridge("/tmp/zb_faults_b.log", LOG_LEVEL="debug") as br:
        if not br.wait_for_log("Snapshot listener: ✅ Consuming", timeout=40):
            zb.bad("the listener never started consuming — check /tmp/zb_faults_b.log")
            return 1

        rm = zb.nats_cli("consumer", "rm", STREAM, CONSUMER, "-f")
        if rm.returncode != 0:
            zb.bad(f"cannot delete the consumer: {rm.stderr.strip()}")
            return 1
        print("  consumer deleted")

        import time

        time.sleep(10)
        alive = br.proc.poll() is None
        text = br.text()

    failed = 0
    if not alive:
        zb.bad("the bridge died — the status frame is still reaching ACK")
        failed = 1
    else:
        zb.ok("the bridge survived")

    if "status message" in text:
        zb.ok("the frame was recognised and recycled")
    elif "Invalid snapshot request subject" in text:
        zb.bad("the frame was parsed as a request — the reply-to guard is not in front of the ACK")
        failed = 1
    else:
        print("  ⓘ  no status frame observed; the server may have answered differently")

    return failed


async def main():
    if not zb.NKEY_SEED:
        sys.exit("NATS_NKEY_SEED is not set.\n  set -a && . ./.env && set +a")
    return phase_a() + phase_b()


zb.run(main)

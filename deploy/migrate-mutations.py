#!/usr/bin/env python3
"""Review an empty legacy MUTATIONS queue cutover; --apply performs it.

Needs nats-py (scripts/scenarios/.venv), NATS_URL and NATS_CREDS from the operator's
runtime environment, and a newly built bridge (--bridge overrides its path).
No stream is deleted or purged. A non-empty queue is refused before modification.
"""
import argparse
import asyncio
import copy
import json
import os
import pathlib
import subprocess

import nats
from nats.js.errors import NotFoundError

ROOT = pathlib.Path(__file__).resolve().parents[1]


async def migrate(nc, wanted, apply=False, domain=None):
    js = nc.jetstream(domain=domain)
    stream = wanted["name"]
    try:
        current = await js.stream_info(stream)
    except NotFoundError as err:
        if err.err_code != 10059:  # Only an absent stream, never an unrelated API failure.
            raise
        print(json.dumps({"stream": stream, "status": "absent", "apply": apply, "changed": False}))
        print(f"{stream} does not exist: no legacy queue to migrate; no changes made. "
              "Start admission (--mutation-ingress) first; it creates the private stream. "
              "Then start the bridge with MUTATION_ADMISSION=1.")
        return
    secure_subjects = wanted["subjects"]
    print(json.dumps({"stream": stream, "messages": current.state.messages,
                      "subjects_before": current.config.subjects, "subjects_after": secure_subjects,
                      "max_messages_per_principal": wanted["max_msgs_per_subject"],
                      "max_message_bytes": wanted["max_msg_size"], "apply": apply}))
    if current.config.subjects == secure_subjects:
        if (current.config.no_ack or current.config.max_age != 0 or current.config.max_msgs_per_subject <= 0 or
            current.config.max_msgs_per_subject > wanted["max_msgs_per_subject"] or
            current.config.max_msg_size <= 0 or current.config.max_msg_size > wanted["max_msg_size"] or
            current.config.max_bytes <= 0 or current.config.max_bytes > wanted["max_bytes"] or
            current.config.retention != "workqueue" or
            current.config.discard != "new" or not current.config.discard_new_per_subject):
            raise RuntimeError("private capture exists with unsafe limits; inspect before proceeding")
        print("Already uses private capture and bounded limits; no changes made.")
        return
    if not apply:
        return
    if current.state.messages:
        raise RuntimeError("MUTATIONS is not empty: drain accepted writes and archive dead letters first; nothing changed")
    previous = copy.deepcopy(current.config)
    paused = copy.deepcopy(previous)
    paused.subjects = secure_subjects
    # Pause public capture without changing any retention limit. A final in-flight
    # request can still arrive between the first read and this update; check again.
    await js.update_stream(config=paused)
    try:
        if (await js.stream_info(stream)).state.messages:
            raise RuntimeError("a write arrived during cutover; restoring public capture without changing limits")
        updated = copy.deepcopy(paused)
        for key in ("retention", "max_age", "max_bytes", "max_msg_size", "max_msgs_per_subject",
                    "discard", "discard_new_per_subject", "no_ack"):
            value = wanted.get(key)
            # Zig's API renders max_age in nanoseconds; nats-py represents seconds.
            if key == "max_age":
                value /= 1_000_000_000
            setattr(updated, key, value)
        await js.update_stream(config=updated)
    except Exception:
        await js.update_stream(config=previous)
        raise
    # An empty legacy durable has the old public filter. Delete only this idle
    # consumer, leaving stream identity, sequence history and queued data intact.
    for consumer in await js.consumers_info(stream):
        if consumer.config.filter_subject and consumer.config.filter_subject not in secure_subjects:
            if consumer.num_pending or consumer.num_ack_pending:
                raise RuntimeError("legacy consumer is not idle; stream retained, inspect before starting the new worker")
            await js.delete_consumer(stream, consumer.name)
    print("Private capture and admission limits installed. Start admission, then restart the bridge with MUTATION_ADMISSION=1.")


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform the reviewed empty-queue cutover")
    parser.add_argument("--bridge", default=os.environ.get("ZB_BRIDGE_BIN", str(ROOT / "zig-out/bin/bridge")))
    args = parser.parse_args()
    plan = subprocess.run([args.bridge, "--mutation-ingress", "--print-config"], capture_output=True, text=True, check=True)
    wanted = json.loads(plan.stdout)
    nc = await nats.connect(os.environ["NATS_URL"], user_credentials=os.environ["NATS_CREDS"])
    try:
        await migrate(nc, wanted, args.apply, domain=os.environ.get("NATS_JS_DOMAIN") or None)
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())

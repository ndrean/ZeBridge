#!/usr/bin/env python3
"""The bridge must not be able to connect as the admin.

`PG_HOST`/`PG_PORT`/`PG_USER`/`PG_PASSWORD` are the **superuser** credentials
`bridge-init` interpolates into `init.sql` to create the bridge's roles. They used to be
a fallback for the bridge's own connection, sitting in the same environment as
everything else — so a `DATABASE_READER_URL` that was missing, misspelled, or dropped by a
deploy meant connecting as `postgres` instead of `bridge_reader`, with a log that looked
entirely healthy.

That matters because the split is the security model: `bridge_reader` holds SELECT +
REPLICATION and is *physically* unable to write, and `bridge_writer` has no table
privileges until a DBA opens one. Neither guarantee survives a process that can quietly
connect as someone else.

Three phases, all with a live admin account in the environment:

  A  no DATABASE_READER_URL, full admin credentials present → must refuse to start
  B  DATABASE_READER_URL set, admin credentials also present → must warn and ignore them
  C  no DATABASE_WRITER_URL, POSTGRES_WRITER_USER present → ingress off, not fallen back

then two on the NATS side, where the principal is the JWT's user name:

  D  as a CLIENT principal: may publish under its own name only, cannot forge verdicts
  E  as the BRIDGE: a principal that is not a legal NATS token never lands a write

⚠️ Owns the only bridge (phases A–C start one). run.py runs it in the "bridge" role,
i.e. NATS_CREDS=scripts/native/creds/bridge.creds; phase D picks a client principal
itself (ZB_PRINCIPAL, else omar — the same default run.py gives client scenarios).

Usage:  python scripts/scenarios/credentials.py   (set -a && . ./.env.bridge && set +a)
"""

import asyncio
import os
import pathlib
import re
import sys
import uuid
from datetime import datetime, timezone

import msgpack

import zb

TMP = pathlib.Path(os.environ.get("TMPDIR", "/tmp"))

# A plausible admin environment: whatever the operator's .env.admin holds, with
# defaults matching the NATIVE stack (127.0.0.1:5432, postgres) so this runs without one.
ADMIN = {
    "PG_HOST": os.environ.get("PG_HOST", "127.0.0.1"),
    "PG_PORT": os.environ.get("PG_PORT", "5432"),
    "PG_USER": os.environ.get("PG_USER", "postgres"),
    "PG_PASSWORD": os.environ.get("PG_PASSWORD", ""),
    "PG_DB": os.environ.get("PG_DB", "postgres"),
}


def writer_parts() -> tuple[str, str]:
    """(user, password) from DATABASE_WRITER_URL — never a hardcoded password."""
    m = re.match(r"postgres(?:ql)?://([^:]+):([^@]+)@", os.environ.get("DATABASE_WRITER_URL", ""))
    if not m:
        sys.exit("DATABASE_WRITER_URL is not set or not parseable — phase C needs the writer's "
                 "user/password to offer as POSTGRES_WRITER_*: set -a && . ./.env.bridge && set +a")
    return m.group(1), m.group(2)


def drop_probe_slot():
    zb.psql("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='zb_probe' AND NOT active) "
            "THEN PERFORM pg_drop_replication_slot('zb_probe'); END IF; END $$", quiet=True)


def run(name, log, drop=(), add=None, expect_exit=False, timeout=45):
    env = dict(os.environ)
    env.update(ADMIN)
    env.update(add or {})
    for key in drop:
        env.pop(key, None)

    br = zb.Bridge(log)
    br.env = env
    # The "ignored" notices are debug-level: absent variables must not warn on every
    # start, but they have to be findable when someone hits the old layout.
    br.env["LOG_LEVEL"] = "debug"
    with br:
        if expect_exit:
            code = br.wait_for_exit(timeout=20)
            return code, br.text()
        br.wait_for_log("Mutation listener: ✅ Ready", timeout=timeout) or br.wait_for_log(
            "Replication started successfully", timeout=5
        )
        return br.proc.poll(), br.text()


def main_sync() -> int:
    failed = 0

    print("\nA. DATABASE_READER_URL absent, admin credentials present")
    code, text = run("A", str(TMP / "zb_cred_a.log"), drop=("DATABASE_READER_URL",), expect_exit=True)
    if code is None:
        zb.bad("still running — it found a way to connect without DATABASE_READER_URL")
        failed = 1
    elif "DATABASE_READER_URL is required" in text and code != 0:
        zb.ok(f"refused to start, exit {code}, and said why")
    else:
        zb.bad(f"exited {code} without the DATABASE_READER_URL diagnosis")
        failed = 1
    if ADMIN["PG_USER"] in text and "ignored" not in text and "required" not in text:
        zb.bad(f"the log mentions '{ADMIN['PG_USER']}' — check it did not connect as admin")
        failed = 1

    print("\nB. DATABASE_READER_URL present, admin credentials also present")
    _, text = run("B", str(TMP / "zb_cred_b.log"))
    if "are set but ignored" in text:
        zb.ok("admin variables noted as ignored (debug)")
    else:
        zb.bad("nothing in the log connects the ignored admin variables to DATABASE_READER_URL")
        failed = 1
    connected = [l for l in text.splitlines() if "connected to PostgreSQL" in l]
    for line in connected:
        print(f"  {line.split('): ')[-1]}")
    if any(f"as '{ADMIN['PG_USER']}'" in l for l in connected):
        zb.bad(f"connected as '{ADMIN['PG_USER']}' — the admin fallback is still live")
        failed = 1
    elif connected:
        zb.ok("every connection used a role from a URL, not the admin account")

    print("\nC. DATABASE_WRITER_URL absent, POSTGRES_WRITER_USER present")
    wuser, wpass = writer_parts()
    _, text = run(
        "C",
        str(TMP / "zb_cred_c.log"),
        drop=("DATABASE_WRITER_URL",),
        add={"POSTGRES_WRITER_USER": wuser, "POSTGRES_WRITER_PASSWORD": wpass},
    )
    if "ingress (mutation) path disabled" in text:
        zb.ok("ingress off — it did not assemble a writer connection from parts")
    else:
        zb.bad("ingress did not report itself disabled")
        failed = 1
    if "Mutation listener: connected" in text:
        zb.bad("the mutation listener connected anyway")
        failed = 1

    return failed


async def principal_is_enforced() -> int:
    """The subject's principal must be the authenticated user, and nothing else.

    PROTOCOL.md §7.1 rests entirely on this: the principal is trustworthy *because* NATS
    authorises subjects, so a client granted `mutation.alice.>` cannot write as anyone
    else. That is a claim about the server's permission block, and it was only ever
    checked by hand.

    ⚠️ The failure mode is the reason this is worth a test rather than a paragraph. A
    denied JetStream publish is never acked, so it surfaces as **`nats: timeout`** — not
    as a permission error. A real scenario (`mutate.py`) hardcoded a principal that did
    not match its credential and hung for ten seconds, which reads like a broker fault
    rather than the guard working exactly as designed.
    """
    # A CLIENT principal, connected with ITS creds. run.py gives this scenario the
    # bridge's creds (it owns a bridge), and as the bridge every check below passes
    # while proving nothing — so the client identity is chosen here: ZB_PRINCIPAL,
    # else omar (run.py's own default for client scenarios).
    me = os.environ.get("ZB_PRINCIPAL") or zb.principal()
    if not me or me == "bridge":
        me = "omar"
    nc = await zb.connect_as(me)
    js = nc.jetstream()

    body = msgpack.packb({"key": {"uid": "x"}, "version": "2026-01-01T00:00:00.000000Z"})
    failed = 0

    try:
        await js.publish(f"mutation.{me}.test_types.insert", body, timeout=3)
        zb.ok(f"'{me}' may publish under its own principal")
    except Exception as exc:  # noqa: BLE001
        zb.bad(f"'{me}' could not publish as itself: {exc}")
        failed += 1

    for impostor in ("bob", f"{me}x", "admin"):
        try:
            await js.publish(f"mutation.{impostor}.test_types.insert", body, timeout=3)
            zb.bad(f"'{me}' published as '{impostor}' — the principal is not enforced")
            failed += 1
        except Exception:  # noqa: BLE001
            # Timeout or permissions violation — both mean the server refused it. The
            # distinction does not matter here; the write not landing does.
            zb.ok(f"'{me}' refused when claiming to be '{impostor}'")

    # ── the reply channel cannot be forged ─────────────────────────────────────
    #
    # ⚠️ This is the assumption PROTOCOL.md §7.1's outbox rules rest on: a verdict can
    # only have come from the bridge. Without it a client could fabricate its own
    # `accepted` and pop an outbox entry for a write that never reached PostgreSQL — or
    # publish a verdict on *another* principal's subject and lie to their client.
    #
    # The dead-letter space is included for the same reason: it is the operator's record
    # of refused writes, and a client that could write to it could hide its own.
    for subject, what in (
        (f"mutation_ack.{me}.forged", "a verdict to itself"),
        ("mutation_ack.bob.forged", "a verdict to another principal"),
        ("mutation_error.test_types", "a dead letter"),
    ):
        try:
            await js.publish(subject, b"forged", timeout=3)
            zb.bad(f"'{me}' forged {what} on '{subject}' — the reply channel is not trustworthy")
            failed += 1
        except Exception:  # noqa: BLE001
            # No PubAck. The violation itself arrives asynchronously, which is why a
            # JetStream publish is required: a core publish here is dropped in silence.
            zb.ok(f"'{me}' cannot forge {what}")

    await nc.close()
    return failed


async def malformed_principal_is_visible():
    """A principal that is not a legal NATS token breaks the REPLY channel, not just the write.

    ⚠️ This is a **provisioning** rule, and that is what makes it dangerous. A client cannot
    choose its principal — the broker's allow-list pins it — so a bad value is baked in when
    the account is created, and every symptom appears at the far end where nobody is
    watching. PROTOCOL.md §7.1.

    Published as the BRIDGE (NATS_CREDS=bridge.creds, which run.py provides for this
    scenario), because a correctly-confined client *cannot* reach these subjects —
    which is exactly why the mistake survives to production.
    """
    creds = os.environ.get("NATS_CREDS", "")
    if pathlib.Path(creds).stem != "bridge":
        print(f"  (skipped: NATS_CREDS={creds or 'unset'} is not bridge.creds, and only the bridge "
              "identity can publish under an arbitrary principal)")
        return 0

    nc = await zb.connect()
    js = nc.jetstream()
    v = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
    failed = 0

    def body():
        u = str(uuid.uuid4())
        return msgpack.packb({
            "key": {"uid": u},
            "data": {"uid": u, "some_text": "badprincipal", "updated_at": v,
                     "inserted_at": v, "tenant_id": "acme"},
            "version": v, "client_id": "c-bad",
        })

    for principal, note in (("a.b", "a dot"), ("a*", "a wildcard"), ("a>", "a `>`")):
        try:
            await asyncio.wait_for(
                js.publish(f"mutation.{principal}.test_types.insert", body(),
                           headers={"Nats-Msg-Id": f"bad-{uuid.uuid4().hex[:8]}"}),
                timeout=4,
            )
        except Exception:  # noqa: BLE001
            pass  # refused at publish is the *good* outcome; the assertion is below

    await asyncio.sleep(4)
    # The one thing that must never happen: a write under a principal whose reply subject
    # cannot be addressed must not land. Silently applying it would leave the client
    # retrying forever against a row that already exists.
    landed = zb.psql(
        "SELECT count(*) FROM public.test_types WHERE some_text = 'badprincipal'"
    ).strip()
    if landed == "0":
        zb.ok("a principal that is not a legal NATS token never lands a write")
    else:
        zb.bad(
            f"{landed} row(s) were written under a principal whose verdict subject is "
            "unusable — the client can never be told, and §7.1 tells it to retry forever"
        )
        failed += 1
        zb.psql("DELETE FROM public.test_types WHERE some_text = 'badprincipal'", quiet=True)

    await nc.close()
    return failed


async def main():
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge (phases A–C start one)")
    drop_probe_slot()
    try:
        rc = main_sync() or 0
    finally:
        drop_probe_slot()
    print("\nD. the subject's principal is the authenticated user")
    rc += await principal_is_enforced()
    print("\nE. a principal that is not a legal NATS token")
    return rc + await malformed_principal_is_visible()


zb.run(main)

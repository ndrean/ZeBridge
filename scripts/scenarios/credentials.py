#!/usr/bin/env python3
"""The bridge must not be able to connect as the admin.

`PG_HOST`/`PG_PORT`/`PG_USER`/`PG_PASSWORD` are the **superuser** credentials
`bridge-init` interpolates into `init.sql` to create the bridge's roles. They used to be
a fallback for the bridge's own connection, sitting in the same environment as
everything else — so a `DATABASE_URL` that was missing, misspelled, or dropped by a
deploy meant connecting as `postgres` instead of `bridge_reader`, with a log that looked
entirely healthy.

That matters because the split is the security model: `bridge_reader` holds SELECT +
REPLICATION and is *physically* unable to write, and `bridge_writer` has no table
privileges until a DBA opens one. Neither guarantee survives a process that can quietly
connect as someone else.

Three phases, all with a live admin account in the environment:

  A  no DATABASE_URL, full admin credentials present → must refuse to start
  B  DATABASE_URL set, admin credentials also present → must warn and ignore them
  C  no DATABASE_WRITER_URL, POSTGRES_WRITER_USER present → ingress off, not fallen back

Usage:  python scripts/scenarios/credentials.py
"""

import os

import zb

# A plausible admin environment: whatever the operator's .env.admin holds, with
# defaults matching the compose stack so this runs without one.
ADMIN = {
    "PG_HOST": os.environ.get("PG_HOST", "127.0.0.1"),
    "PG_PORT": os.environ.get("PG_PORT", "55432"),
    "PG_USER": os.environ.get("PG_USER", "postgres"),
    "PG_PASSWORD": os.environ.get("PG_PASSWORD", "postgres_password"),
    "PG_DB": os.environ.get("PG_DB", "postgres"),
}


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
        br.wait_for_log("Mutation listener", timeout=timeout) or br.wait_for_log(
            "WAL replication stream started", timeout=5
        )
        return br.proc.poll(), br.text()


def main_sync() -> int:
    failed = 0

    print("\nA. DATABASE_URL absent, admin credentials present")
    code, text = run("A", "/tmp/zb_cred_a.log", drop=("DATABASE_URL",), expect_exit=True)
    if code is None:
        zb.bad("still running — it found a way to connect without DATABASE_URL")
        failed = 1
    elif "DATABASE_URL is required" in text and code != 0:
        zb.ok(f"refused to start, exit {code}, and said why")
    else:
        zb.bad(f"exited {code} without the DATABASE_URL diagnosis")
        failed = 1
    if ADMIN["PG_USER"] in text and "ignored" not in text and "required" not in text:
        zb.bad(f"the log mentions '{ADMIN['PG_USER']}' — check it did not connect as admin")
        failed = 1

    print("\nB. DATABASE_URL present, admin credentials also present")
    _, text = run("B", "/tmp/zb_cred_b.log")
    if "are set but ignored" in text:
        zb.ok("admin variables noted as ignored (debug)")
    else:
        zb.bad("nothing in the log connects the ignored admin variables to DATABASE_URL")
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
    _, text = run(
        "C",
        "/tmp/zb_cred_c.log",
        drop=("DATABASE_WRITER_URL",),
        add={"POSTGRES_WRITER_USER": "bridge_writer", "POSTGRES_WRITER_PASSWORD": "writer_password_changeme"},
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
    import msgpack

    nc = await zb.connect()
    js = nc.jetstream()
    me = zb.NATS_URL.split("://", 1)[-1].rsplit("@", 1)[0].split(":", 1)[0] if "@" in zb.NATS_URL else None
    if not me:
        print("  (skipped: NATS_URL carries no user, so there is no principal to compare)")
        await nc.close()
        return 0

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

    await nc.close()
    return failed


async def main():
    rc = main_sync() or 0
    print("\nD. the subject's principal is the authenticated user")
    return rc + await principal_is_enforced()


zb.run(main)

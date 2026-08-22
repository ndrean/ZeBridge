#!/usr/bin/env python3
"""Tenant scoping end to end: identity ($KV.tenants), audience (INIT_<TENANT> streams),
and contents (RLS via zb.tenant) — the whole of NOTES.md §1.12 in one replayable script.

PROTOCOL.md "The Connection Flow", Step 0: a fresh consumer holding only its principal ID
asks NATS for its own tenant instead of guessing or hardcoding one. This is the behavioural
counterpart to that design (NOTES.md §1.12 part 3) — a client's own kv.get() against its own
key, another principal's key, and what happens when zebridge_user_tenants changes while the
bridge is running. The final section extends this past identity into the read path itself:
does a tenant-scoped snapshot request actually reach only the right stream, and does it
actually contain only the right tenant's rows?

Four things this exists to keep caught, each measured once while building the feature and
each a way this could silently regress:

  1. The exact-key grant is not the bare key. Direct Get's scopable subject is
     `$JS.API.DIRECT.GET.KV_tenants.$KV.tenants.<key>` — the bucket's own internal
     `$KV.<bucket>.<key>` subject as the suffix, not `$KV_tenants.<key>` as first
     guessed. Wrong once; `nats-server.conf.template` carries the fix, and check 2
     below is what would catch a regression of it.

  2. `zebridge_user_tenants` must never leak as ordinary CDC (`cdc.zebridge_user_tenants.*`)
     — the whole reason this bucket exists is to avoid handing every client the full
     principal→tenant roster, and publishing the mapping table's own rows as CDC would
     do exactly that through a different door. See the matching exemption in
     `zebridge_publication_guard()` (init.core.template.sql) and the bridge's
     relation-name special-case in its WAL loop (bridge.zig).

  3. A revoked/reassigned principal takes effect on the *next connect*, with no NATS
     reload and no bridge restart — the whole point of a live KV bucket over a static
     config. Tested here with a throwaway principal, not a fixture.

  4. A tenant-scoped snapshot must be scoped in BOTH senses, and either one missing is
     a real leak: `bob` must not be able to reach `acme`'s dump (audience — the stream
     and the `$KV.snapshots` descriptor), and even alice's own dump of a table holding
     both tenants' rows must contain ONLY her tenant's (contents — `zb.tenant` + the
     `zebridge_scope_reads_by_tenant()` RLS policy, PROTOCOL.md "The Connection Flow"
     §2, NOTES.md §1.12 part 1). Audience without contents is a stream a client can't
     reach holding the wrong rows anyway; contents without audience is correctly
     filtered data anyone can still read. Neither alone is the property that matters.

⚠️ NOT covered here: the `@nats-io/kv` client's `Kvm.open()` defaulting to the
unscopable `$JS.API.STREAM.MSG.GET.KV_tenants` path unless `{allow_direct: true}` is
passed explicitly. This scenario drives the `nats-py` client, which stays on Direct Get
regardless of that option — it cannot reproduce a regression in `web-consumer`'s own
`resolveTenant()` call site. See that function's own comment in App.tsx.

Usage:
    scripts/scenarios/.venv/bin/python scripts/scenarios/tenant_kv.py

Needs alice/bob/john/mary declared in nats-server.conf (dev password `s3cret`) and mapped
in `zebridge_user_tenants` — the same fixtures this project's own dev stack already
carries. Reads and writes `zebridge_user_tenants` for a throwaway principal
(`tw_kv_probe`) to test live propagation, and cleans it up whether the checks pass or
fail. Inserts two throwaway rows into `test_types` (marked `tw_kv_acme_row`/
`tw_kv_globex_row`) to test snapshot contents, hard-deleted at the end as `zb_sweeper` —
a plain `DELETE` would only tombstone them (the soft-delete guard intercepts every
writer alike), and the sweeper identity is what that trigger's own bypass exists for.
The live-propagation and snapshot checks need a running bridge and are skipped — not
failed — if no response arrives within their timeout; every other check needs only NATS.
"""

import asyncio
import time

import nats
import nats.js.errors

import zb

# principal -> (password, expected tenant). `john` is mapped to OPEN_TENANT
# (`_default`) rather than to a private tenant — a deliberate membership (SECURITY.md
# §1.2, "'Open' is a mapping, not a default"), not an absence of one, so he resolves a
# real value here too.
PRINCIPALS = {
    "alice": ("s3cret", "acme"),
    "bob": ("s3cret", "globex"),
    "mary": ("s3cret", "globex"),
    "john": ("s3cret", "_default"),
}

# principal -> a key it must NOT be able to read
DENIED = {"alice": "bob", "bob": "alice", "mary": "alice"}

PROBE = "tw_kv_probe"
BUCKET = zb.TOPOLOGY["kv"]["tenants"]

# For check 4 (snapshot audience + contents). `test_types` is the project's own
# tenant-scoped writable fixture (TENANT_RULES=test_types:tenant_id in .env.bridge) —
# not a throwaway table, so only these two marked rows are ours to add and remove.
SNAP_TABLE = "test_types"
SNAP_ACME_TEXT = "tw_kv_acme_row"
SNAP_GLOBEX_TEXT = "tw_kv_globex_row"

failed = 0


def check(label: str, cond: bool, detail: str):
    global failed
    if cond:
        zb.ok(f"{label}: {detail}")
    else:
        zb.bad(f"{label}: {detail}")
        failed += 1


async def _silent_error(_e):
    # A denied Direct Get is the expected outcome for half of check 2 below — nats-py's
    # default error callback prints every one of them to stderr, which would make a
    # passing run look like it hit a wall of errors. The check functions below already
    # report the outcome; this just stops nats-py editorializing on top of it.
    pass


async def connect_as(principal: str, password: str) -> nats.NATS:
    return await nats.connect(zb.NATS_URL, user=principal, password=password, error_cb=_silent_error)


async def own_key(nc: nats.NATS, key: str):
    """Direct Get on `key`. Returns (value_or_None, 'not_found' | error class name | None)."""
    js = nc.jetstream()
    kv = await js.key_value(BUCKET)
    try:
        entry = await kv.get(key)
        return entry.value.decode(), None
    except nats.js.errors.KeyNotFoundError:
        return None, "not_found"
    except Exception as e:
        return None, type(e).__name__


async def main():
    print()

    # ── 1. each principal resolves its own tenant correctly ────────────────────
    for principal, (password, expected) in PRINCIPALS.items():
        nc = await connect_as(principal, password)
        value, err = await own_key(nc, principal)
        await nc.close()
        check(
            f"{principal} resolves own tenant",
            value == expected,
            f"'{value}'" + ("" if value == expected else f" — expected '{expected}', err={err!r}"),
        )

    # ── 1b. a genuinely unmapped key gets a clean 'not found', never a violation ─
    #
    # None of the four fixtures above is actually unmapped any more (john is mapped
    # to the open tenant on purpose — see PRINCIPALS above), so this exercises the
    # same code path directly against the admin connection instead of pretending a
    # real principal has no mapping. It is the same claim the exact-key grant makes
    # for john's own connection when he genuinely has none: absence is a clean miss,
    # not a Permissions Violation, because he is still granted his own exact key.
    admin_probe = await zb.connect()
    admin_probe_js = admin_probe.jetstream()
    admin_probe_kv = await admin_probe_js.key_value(BUCKET)
    try:
        await admin_probe_kv.get("tw_kv_never_mapped")
        check("unmapped key resolves cleanly", False, "WRONGLY found a value for a key that was never set")
    except nats.js.errors.KeyNotFoundError:
        check("unmapped key resolves cleanly", True, "'not found', not an error — the bucket has no entry to hide")
    except Exception as e:
        check("unmapped key resolves cleanly", False, f"unexpected {type(e).__name__}, not KeyNotFoundError")
    await admin_probe.close()

    # ── 2. cross-principal denial — the exact-key grant, not a wildcard ────────
    for principal, victim in DENIED.items():
        password = PRINCIPALS[principal][0]
        nc = await connect_as(principal, password)
        try:
            js = nc.jetstream()
            kv = await js.key_value(BUCKET)
            await asyncio.wait_for(kv.get(victim), timeout=3)
            check(
                f"{principal} denied on {victim}'s key",
                False,
                f"WRONGLY READABLE — {principal} read {victim}'s tenant mapping",
            )
        except asyncio.TimeoutError:
            check(f"{principal} denied on {victim}'s key", True, "timed out (Permissions Violation)")
        except Exception as e:
            check(f"{principal} denied on {victim}'s key", True, f"refused ({type(e).__name__})")
        finally:
            await nc.close()

    # ── 3. live propagation: INSERT, then UPDATE, with NO bridge restart ───────
    zb.psql(f"DELETE FROM zebridge_user_tenants WHERE principal = '{PROBE}'", quiet=True)
    admin = await zb.connect()
    admin_js = admin.jetstream()
    admin_kv = await admin_js.key_value(BUCKET)

    async def poll_for(expected: str, timeout: float = 15) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                entry = await admin_kv.get(PROBE)
                if entry.value.decode() == expected:
                    return True
            except Exception:
                pass
            await asyncio.sleep(0.5)
        return False

    zb.psql(f"INSERT INTO zebridge_user_tenants (principal, tenant_id) VALUES ('{PROBE}', 'acme')")
    if await poll_for("acme"):
        check("live INSERT propagates", True, f"$KV.{BUCKET}.{PROBE} = 'acme', no bridge restart")

        zb.psql(f"UPDATE zebridge_user_tenants SET tenant_id = 'globex' WHERE principal = '{PROBE}'")
        check(
            "live UPDATE propagates",
            await poll_for("globex"),
            f"$KV.{BUCKET}.{PROBE} = 'globex' after reassignment, no bridge restart",
        )
    else:
        print(
            "  ⓘ  no bridge reachable within 15s — live-propagation checks skipped, not "
            "failed (checks 1 and 2 above still ran against whatever was already in the bucket)"
        )

    zb.psql(f"DELETE FROM zebridge_user_tenants WHERE principal = '{PROBE}'", quiet=True)

    # ── 4. snapshot audience AND contents — the read path, not just identity ───
    #
    # Two throwaway rows in the real tenant-scoped fixture, one per tenant, written as
    # alice (acme) and bob (globex) — the mapped fixture principals, not new ones. A
    # snapshot request as alice must come back with ONLY her row: that is contents,
    # `zb.tenant` + `zebridge_scope_reads_by_tenant()`'s RLS policy actually filtering
    # the underlying SELECT/COPY, not just the bridge choosing where to publish it.
    # Denying bob's reach into alice's stream and descriptor is audience, already
    # covered by check 2's grant shape — exercised again here against a table that
    # actually holds both tenants' rows, since a permission that happens to work on an
    # empty probe proves less than one that works once there is something to leak.
    def bridge_write(principal: str, tenant: str, text: str):
        stmt = (
            f"BEGIN; SET LOCAL ROLE bridge_writer; "
            f"SELECT set_config('zb.principal', '{principal}', true); "
            f"INSERT INTO {SNAP_TABLE} (uid, some_text, tenant_id, updated_at, inserted_at) "
            f"VALUES (gen_random_uuid(), '{text}', '{tenant}', now(), now()); COMMIT;"
        )
        zb.psql(stmt, quiet=True)

    def hard_delete_snap_rows():
        # A plain DELETE only tombstones (the soft-delete guard intercepts every writer
        # alike, including psql running as the superuser) — zb_sweeper is the identity
        # that trigger's own bypass exists for.
        stmt = (
            "SELECT set_config('zb.principal', 'zb_sweeper', false); "
            f"DELETE FROM {SNAP_TABLE} WHERE some_text IN ('{SNAP_ACME_TEXT}', '{SNAP_GLOBEX_TEXT}');"
        )
        zb.psql(stmt, quiet=True)

    hard_delete_snap_rows()  # clean slate from any earlier run
    bridge_write("alice", "acme", SNAP_ACME_TEXT)
    bridge_write("bob", "globex", SNAP_GLOBEX_TEXT)

    snapshots_bucket = zb.TOPOLOGY["kv"]["snapshots"]
    snap_kv = await admin_js.key_value(snapshots_bucket)
    init_prefix = zb.TOPOLOGY["subjects"]["init_prefix"]
    acme_stream = f"{(zb.TOPOLOGY.get('init_streams') or {}).get('tenant_prefix', 'INIT_')}ACME"

    async def acme_descriptor():
        try:
            entry = await snap_kv.get(f"acme.{SNAP_TABLE}")
            return zb.decode(entry.value)
        except Exception:
            return None

    before = await acme_descriptor()
    before_id = before.get("snapshot_id") if before else None

    # REQUESTS carries `max_msgs_per_subject: 1` (a deliberate one-pending-request-per-
    # table throttle, NOT retention-on-ack — a message survives its own `max_age` window
    # whether or not the worker has already consumed it) — a rerun of this scenario
    # within that window would have its publish silently discarded, and a bridge that is
    # working perfectly would look identical to one that is not reachable at all. Purge
    # this scenario's own slot first so every run gets a fresh request regardless of
    # timing; this does not touch any other table's or tenant's pending request.
    requests_stream = zb.TOPOLOGY["streams"]["requests"]
    try:
        await admin_js.purge_stream(requests_stream, subject=f"snapshot.request.acme.{SNAP_TABLE}")
    except Exception:
        pass

    await admin.publish(f"snapshot.request.acme.{SNAP_TABLE}", b"")

    desc = None
    deadline = time.time() + 20
    while time.time() < deadline:
        d = await acme_descriptor()
        if d and d.get("snapshot_id") != before_id:
            desc = d
            break
        await asyncio.sleep(0.5)

    if desc is None:
        print(
            "  ⓘ  no bridge reachable within 20s — snapshot audience/contents checks "
            "skipped, not failed"
        )
    else:
        snap_id = desc["snapshot_id"]

        async def fetch_one(subject: str):
            sub = await admin_js.pull_subscribe(subject, durable=None, stream=acme_stream)
            msgs = await sub.fetch(1, timeout=5)
            for m in msgs:
                await m.ack()
            return zb.decode(msgs[0].data) if msgs else None

        schema_payload = await fetch_one(f"{init_prefix}.snap.acme.schema.{SNAP_TABLE}.{snap_id}")
        columns = (schema_payload or {}).get("schema") if isinstance(schema_payload, dict) else None

        data_sub = await admin_js.pull_subscribe(
            f"{init_prefix}.snap.acme.{SNAP_TABLE}.{snap_id}.>", durable=None, stream=acme_stream
        )
        data_msgs = await data_sub.fetch(10, timeout=5)
        rows = []
        for m in data_msgs:
            decoded = zb.decode(m.data)
            if isinstance(decoded, list):
                rows.extend(decoded)
            await m.ack()

        if columns and "some_text" in columns:
            idx = columns.index("some_text")
            texts = [r[idx] for r in rows]
            check(
                "acme snapshot contains only acme's row",
                SNAP_ACME_TEXT in texts and SNAP_GLOBEX_TEXT not in texts,
                f"got {texts}",
            )
        else:
            check("acme snapshot contains only acme's row", False,
                  f"could not resolve column order from schema message: {schema_payload!r}")

        bob_nc = await connect_as("bob", PRINCIPALS["bob"][0])
        try:
            bob_js = bob_nc.jetstream()
            bob_kv = await bob_js.key_value(snapshots_bucket)
            try:
                await asyncio.wait_for(bob_kv.get(f"acme.{SNAP_TABLE}"), timeout=3)
                check("bob denied alice's snapshot descriptor", False, "WRONGLY READABLE")
            except asyncio.TimeoutError:
                check("bob denied alice's snapshot descriptor", True, "timed out (Permissions Violation)")
            except Exception as e:
                check("bob denied alice's snapshot descriptor", True, f"refused ({type(e).__name__})")

            try:
                await asyncio.wait_for(bob_js.stream_info(acme_stream), timeout=3)
                check(f"bob denied {acme_stream} stream info", False, "WRONGLY READABLE")
            except asyncio.TimeoutError:
                check(f"bob denied {acme_stream} stream info", True, "timed out (Permissions Violation)")
            except Exception as e:
                check(f"bob denied {acme_stream} stream info", True, f"refused ({type(e).__name__})")
        finally:
            await bob_nc.close()

    hard_delete_snap_rows()

    # ── 5. zebridge_user_tenants never reaches CDC ─────────────────────────────
    prefix = (zb.TOPOLOGY.get("cdc_streams") or {}).get("tenant_prefix", "CDC_")
    public_stream = (zb.TOPOLOGY.get("cdc_streams") or {}).get("public", "CDC_PUBLIC")
    tenant_streams = [f"{prefix}{t.upper()}" for t in (zb.TOPOLOGY.get("tenants") or [])]
    for stream in [*tenant_streams, public_stream]:
        try:
            info = await admin_js.stream_info(stream)
            subjects = info.config.subjects or []
            leaked = any("zebridge_user_tenants" in s for s in subjects)
            check(
                f"{stream} does not carry zebridge_user_tenants",
                not leaked,
                "not in the stream's subject list" if not leaked else f"SUBJECT PRESENT: {subjects}",
            )
        except Exception:
            print(f"  ⓘ  stream {stream} not reachable here — skipped")

    await admin.close()

    print()
    if failed:
        print(f"\033[31m{failed} check(s) failed\033[0m\n")
        return 1
    print("\033[32mall checks passed\033[0m\n")
    return 0


zb.run(main)

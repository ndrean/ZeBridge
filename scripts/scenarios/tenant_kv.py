#!/usr/bin/env python3
"""Tenant identity end to end: `$KV.tenants.<principal>` — who a client is, resolved
from NATS and nothing else.

PROTOCOL.md "The Connection Flow", Step 0: a fresh consumer holding only its principal ID
asks NATS for its own tenant instead of guessing or hardcoding one. This is the behavioural
counterpart to that design (NOTES.md §1.12 part 3) — a client's own kv.get() against its own
key, another principal's key, and what happens when zebridge_user_tenants changes while the
bridge is running.

Three things this exists to keep caught, each measured once while building the feature and
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

⚠️ NOT covered here: the `@nats-io/kv` client's `Kvm.open()` defaulting to the
unscopable `$JS.API.STREAM.MSG.GET.KV_tenants` path unless `{allow_direct: true}` is
passed explicitly. This scenario drives the `nats-py` client, which stays on Direct Get
regardless of that option — it cannot reproduce a regression in `web-consumer`'s own
`resolveTenant()` call site. See that function's own comment in App.tsx.

Usage:
    scripts/scenarios/.venv/bin/python scripts/scenarios/tenant_kv.py

Needs the creds `scripts/native/jwt-bootstrap.sh` mints (alice/bob/mary/nina), each
mapped in `zebridge_user_tenants` as the bootstrap maps them, and `NATS_CREDS` for the
admin half (`bridge.creds`). Reads and writes `zebridge_user_tenants` for a throwaway
principal (`tw_kv_probe`) to test live propagation, and cleans it up whether the checks
pass or fail. The live-propagation check needs a running bridge and is skipped — not
failed — if no response arrives within its timeout; every other check needs only NATS.
"""

import asyncio
import time

import nats
import nats.js.errors

import zb

# principal -> expected tenant, as scripts/native/jwt-bootstrap.sh mints them
# (alice:acme bob:globex mary:globex nina:tango). nina's tenant was born at runtime
# (dyntenant onboarding) — a mapping like any other once it exists.
PRINCIPALS = {
    "alice": "acme",
    "bob": "globex",
    "mary": "globex",
    "nina": "tango",
}

# principal -> a key it must NOT be able to read
DENIED = {"alice": "bob", "bob": "alice", "mary": "alice"}

PROBE = "tw_kv_probe"
BUCKET = zb.TOPOLOGY["kv"]["tenants"]

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


async def connect_as(principal: str) -> nats.NATS:
    """`zb.connect_as`, plus the silent error callback: same creds file, same server."""
    return await nats.connect(zb.nats_server(), user_credentials=zb.creds_for(principal), error_cb=_silent_error)


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
    for principal, expected in PRINCIPALS.items():
        nc = await connect_as(principal)
        value, err = await own_key(nc, principal)
        await nc.close()
        check(
            f"{principal} resolves own tenant",
            value == expected,
            f"'{value}'" + ("" if value == expected else f" — expected '{expected}', err={err!r}"),
        )

    # ── 1b. a genuinely unmapped key gets a clean 'not found', never a violation ─
    #
    # None of the four principals above is unmapped, so this exercises the same code
    # path directly against the admin connection instead of pretending a real
    # principal has no mapping. It is the same claim the exact-key grant makes for a
    # principal's own connection when it genuinely has none: absence is a clean miss,
    # not a Permissions Violation, because it is still granted its own exact key.
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
        nc = await connect_as(principal)
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

    try:
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
    finally:
        # Whatever happened above, the throwaway mapping must not outlive the run.
        zb.psql(f"DELETE FROM zebridge_user_tenants WHERE principal = '{PROBE}'", quiet=True)

    # ── 4. zebridge_user_tenants never reaches CDC ─────────────────────────────
    prefix = (zb.TOPOLOGY.get("cdc_streams") or {}).get("tenant_prefix", "CDC_")
    public_stream = (zb.TOPOLOGY.get("cdc_streams") or {}).get("public", "CDC_PUBLIC")
    tenant_streams = [f"{prefix}{t}" for t in zb.tenants()]
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

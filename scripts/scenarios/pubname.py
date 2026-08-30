#!/usr/bin/env python3
"""Where the publication name comes from — and what happens when nobody says.

Two halves of one rule, added 2026-08-28 (NOTES §10ad/§10ae): **the publication is
named, never defaulted**, on both sides of the boundary.

  * The BRIDGE used to fall back to a compiled `cdc_pub` — so `bridge` with no
    flags and no env vars started on a publication nobody had chosen. On a host
    where that name exists it replicates the wrong table set, cleanly, with every
    check green. `--pub` given as the last argument with nothing after it fell into
    the same default, which made a typo look exactly like a working command.
  * `zebridge_enable` used to default to `${BRIDGE_CDC_PUBLICATION}` rendered into
    the template, which made that name a SECOND spelling of the bridge's own
    `--pub` — two values that had to agree with nothing checking that they did.
    The templates now name no publication at all; `zebridge_create_publication`
    makes one, and it is the only supported way, because it also attaches the three
    internal tables (ddl_events, gc_watermark, user_tenants, catalogue) that a hand-made
    `CREATE PUBLICATION` silently left out — a bridge that replicates user tables
    but never learns a schema changed and cannot resolve a tenant.

What is asserted here, in the order the operator meets it:

  1. nothing said            -> refusal naming both channels, no boot
  2. flag with no value      -> refusal naming the flag (NOT the compiled default)
  3. flag with an empty value-> refused at the existence check
  4. flag only               -> that name is what PostgreSQL is asked for
  5. env only                -> ditto; this is the flagless .env.bridge boot
  6. flag over env           -> the flag wins
  7. a name PostgreSQL does not have -> fatal, before any streaming
  8. the SQL half: no publication after install, created on request with its three
     internal tables, in either install order, and enable refuses to guess

Cases 4-6 point at names that deliberately do NOT exist: the "not found" error
names the publication the bridge resolved, so one run proves both which channel
won and that a wrong name stops the boot. Case 5b boots for real against the live
publication to show the flagless path all the way through.

Needs: a built bridge, the live stack (case 5b), and ADMIN_DATABASE_URL for the
SQL half.

Usage:  scripts/scenarios/.venv/bin/python scripts/scenarios/pubname.py
"""

import os
import re
import subprocess
import sys
import tempfile
import time

import zb

BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
ADMIN_URL = os.environ.get(
    "ADMIN_DATABASE_URL", "postgres://postgres@127.0.0.1:5432/postgres"
)
SCRATCH = "zb_pubname_scratch"
PROBE_SLOT = "zb_pubname_probe"
PORT = "9097"


def admin(sql: str, db: str | None = None, stop: bool = True) -> subprocess.CompletedProcess:
    url = ADMIN_URL if db is None else re.sub(r"/[^/]+$", f"/{db}", ADMIN_URL)
    cmd = ["psql", url, "-tA"] + (["-v", "ON_ERROR_STOP=1"] if stop else [])
    return subprocess.run(cmd + ["-c", sql], capture_output=True, text=True)


def boot(argv: list[str], env_over: dict, seconds: float = 6.0) -> str:
    """Start a bridge, give it `seconds` to say something, return its stderr.

    Killed either way: these cases are about the first hundred milliseconds of a
    boot, and case 5b is the only one meant to reach replication.
    """
    env = dict(os.environ)
    # Cleared, not overwritten: the whole point of case 1 is an environment that
    # says nothing, and `dict(os.environ)` inherits whatever sourced .env.bridge.
    for k in ("BRIDGE_CDC_PUBLICATION", "BRIDGE_CDC_SLOT"):
        env.pop(k, None)
    env.update({k: v for k, v in env_over.items() if v is not None})
    env.setdefault("LOG_LEVEL", "info")
    with tempfile.NamedTemporaryFile("w+", suffix=".log", delete=False) as log:
        proc = subprocess.Popen(
            [str(BRIDGE), "--port", PORT, *argv],
            env=env, stdout=subprocess.DEVNULL, stderr=log,
        )
        deadline = time.time() + seconds
        while time.time() < deadline and proc.poll() is None:
            time.sleep(0.25)
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        log.flush()
        with open(log.name, errors="replace") as fh:
            text = fh.read()
    os.unlink(log.name)
    return text


async def main():
    failed = 0

    def expect(label: str, text: str, needle: str, absent: str | None = None):
        nonlocal failed
        if needle not in text:
            zb.bad(f"{label}: expected {needle!r}\n      got: {text.strip()[-300:]}")
            failed += 1
            return
        if absent and absent in text:
            zb.bad(f"{label}: {absent!r} should not appear — it did")
            failed += 1
            return
        zb.ok(label)

    if not BRIDGE.exists():
        sys.exit(f"{BRIDGE} not found — run `zig build`")

    print("\n─── the bridge: which name, and from where ───")

    # 1. Nothing said at all. The slot is reported first because it is checked
    #    first; both refusals name their flag AND their env var.
    t = boot([], {})
    expect("1a. no flag, no env -> refuses, naming --slot and BRIDGE_CDC_SLOT",
           t, "no replication slot named", absent="cdc_slot")
    t = boot([], {"BRIDGE_CDC_SLOT": PROBE_SLOT})
    expect("1b. slot known, publication not -> refuses, naming --pub and its env var",
           t, "no publication named", absent="cdc_pub")

    # 2. The dangerous one: a flag at the end of the line with nothing after it.
    t = boot(["--slot", PROBE_SLOT, "--pub"], {})
    expect("2. --pub with no value -> refused, not silently defaulted",
           t, "--pub requires a value", absent="Publication name")

    # 3. An empty value is a value, and it is refused where names are checked.
    t = boot(["--slot", PROBE_SLOT, "--pub", ""], {})
    expect("3. --pub '' -> refused at the existence check", t, "Publication '' not found")

    # 4-6. Which channel wins. Every name here is absent from PostgreSQL on
    #      purpose, so the "not found" line reports what the bridge resolved.
    t = boot(["--slot", PROBE_SLOT, "--pub", "zb_ghost_flag"], {})
    expect("4. flag only -> the flag's name reaches PostgreSQL",
           t, "Publication 'zb_ghost_flag' not found")

    t = boot(["--slot", PROBE_SLOT], {"BRIDGE_CDC_PUBLICATION": "zb_ghost_env"})
    expect("5a. env only -> the env var's name reaches PostgreSQL",
           t, "Publication 'zb_ghost_env' not found")

    t = boot(["--slot", PROBE_SLOT, "--pub", "zb_ghost_flag"],
             {"BRIDGE_CDC_PUBLICATION": "zb_ghost_env"})
    expect("6. flag over env -> the flag wins", t,
           "Publication 'zb_ghost_flag' not found", absent="zb_ghost_env' not found")

    # 7 == 4/5/6's mechanism, stated on its own: a name PostgreSQL does not have
    #     is fatal at boot, not a bridge that streams nothing.
    expect("7. an unknown publication is FATAL, not a quiet no-op",
           t, "🔴")

    # 5b. The flagless boot, for real, on the live publication — the path
    #     .env.bridge exists to serve. Its own slot, dropped below.
    live = os.environ.get("BRIDGE_CDC_PUBLICATION")
    if not live:
        zb.bad("5b skipped: BRIDGE_CDC_PUBLICATION is not set (source .env.bridge)")
        failed += 1
    else:
        t = boot([], {"BRIDGE_CDC_PUBLICATION": live, "BRIDGE_CDC_SLOT": PROBE_SLOT}, seconds=12)
        expect(f"5b. flagless boot from the environment reaches replication on {live!r}",
               t, "Replication started successfully")
        admin(f"SELECT pg_drop_replication_slot('{PROBE_SLOT}') "
              f"WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='{PROBE_SLOT}')",
              stop=False)

    print("\n─── the SQL: nothing is created behind your back ───")

    admin(f"DROP DATABASE IF EXISTS {SCRATCH}", stop=False)
    if admin(f"CREATE DATABASE {SCRATCH}").returncode != 0:
        zb.bad(f"could not create {SCRATCH} — is ADMIN_DATABASE_URL right?")
        return 1
    try:
        env = dict(os.environ, TARGET_DB=SCRATCH)
        for template in ("init.core.template.sql", "init.write.template.sql"):
            sql = subprocess.run(["envsubst"], stdin=open(zb.ROOT / template),
                                 capture_output=True, text=True, env=env).stdout
            # The core half alone first: that is the readonly profile, and it is
            # where zebridge_user_tenants does not exist yet.
            if template.endswith("core.template.sql"):
                core_sql = sql
            else:
                write_sql = sql
        r = subprocess.run(["psql", re.sub(r"/[^/]+$", f"/{SCRATCH}", ADMIN_URL),
                            "-v", "ON_ERROR_STOP=1", "-q"], input=core_sql,
                           capture_output=True, text=True)
        if r.returncode != 0:
            zb.bad(f"init.core did not apply: {r.stderr.strip()[-300:]}")
            return 1

        pubs = admin("SELECT coalesce(string_agg(pubname, ','), '(none)') FROM pg_publication",
                     db=SCRATCH).stdout.strip()
        if pubs == "(none)":
            zb.ok("8a. the templates create NO publication — the name left them entirely")
        else:
            zb.bad(f"8a. a publication appeared from the template: {pubs}")
            failed += 1

        out = admin("SELECT status FROM public.zebridge_create_publication('p_probe')",
                    db=SCRATCH).stdout.split()
        # created + ddl_events + gc_watermark + user_tenants(ABSENT, write half not applied)
        # + zebridge_catalogue (internal since §10bj: the bridge reloads on its rows)
        if out[:1] == ["created"] and out.count("added") == 3 and "ABSENT" in out:
            zb.ok("8b. create_publication: made it, attached the three internal tables that "
                  "exist, and SAID SO about the third")
        else:
            zb.bad(f"8b. unexpected create_publication report: {out}")
            failed += 1

        r = subprocess.run(["psql", re.sub(r"/[^/]+$", f"/{SCRATCH}", ADMIN_URL),
                            "-v", "ON_ERROR_STOP=1", "-q"], input=write_sql,
                           capture_output=True, text=True)
        carried = admin("SELECT string_agg(tablename, ',' ORDER BY tablename) "
                        "FROM pg_publication_tables WHERE pubname='p_probe'",
                        db=SCRATCH).stdout.strip()
        want = "zebridge_catalogue,zebridge_ddl_events,zebridge_gc_watermark,zebridge_user_tenants"
        if carried == want:
            zb.ok("8c. init.write applied AFTER the publication existed still attached "
                  "zebridge_user_tenants — install order does not matter")
        else:
            zb.bad(f"8c. p_probe carries {carried!r}, wanted {want!r}")
            failed += 1

        admin("CREATE TABLE public.widgets (id uuid PRIMARY KEY, updated_at timestamptz NOT NULL)",
              db=SCRATCH)
        base = ("SELECT * FROM zebridge_enable('public.widgets'::regclass, "
                "public_reason => 'pubname probe'")

        r = admin(base + ", dry_run => false)", db=SCRATCH, stop=False)
        expect("8d. enable with no publication -> raises, and names both channels",
               r.stderr, "no publication named")

        r = admin(base + ", publication => 'p_typo', dry_run => false)", db=SCRATCH, stop=False)
        expect("8e. enable on an unknown publication -> raises rather than creating it",
               r.stderr, "does not exist")

        r = admin(base + ", publication => 'p_typo', create_publication => true, "
                         "dry_run => false)", db=SCRATCH, stop=False)
        carried = admin("SELECT string_agg(tablename, ',' ORDER BY tablename) "
                        "FROM pg_publication_tables WHERE pubname='p_typo'",
                        db=SCRATCH).stdout.strip()
        want = ("widgets,zebridge_catalogue,zebridge_ddl_events,zebridge_gc_watermark,zebridge_user_tenants")
        if carried == want:
            zb.ok("8f. create_publication => true makes one that is COMPLETE — the three "
                  "internal tables, not just the user table")
        else:
            zb.bad(f"8f. p_typo carries {carried!r}, wanted {want!r}")
            failed += 1
    finally:
        admin(f"DROP DATABASE IF EXISTS {SCRATCH}", stop=False)

    return 1 if failed else 0


zb.run(main)

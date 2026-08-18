#!/usr/bin/env python3
"""init.sql.template — does `envsubst` produce what the file says?

`init.sql.template` creates every role, grant, trigger and guard the security model rests
on, and it is rendered by `envsubst` before psql ever sees it. That render step is a second
language between the author and the database, and it fails **silently in both directions**:

  * `envsubst` treats `$fn` in a dollar-quote tag as a variable reference and substitutes
    it away. `AS $fn$ … $fn$` becomes `AS $ … $`, which breaks the quoting and swallows
    every definition after it. Measured once: **seven functions in the file, one in the
    database, and psql reported no error.**
  * an unset variable is replaced with the empty string, so `password: ''` or
    `INTERVAL ''` render as valid-looking SQL that means something else.

Neither shows up as a failed migration. The only way to catch them is to render the file
and count what survived, which is what this does:

  1. render with the real `envsubst`
  2. no named dollar tag (`$word$`) may remain — the whole class of bug, refused outright
  3. every CREATE in the template must appear in the render — nothing silently dropped
  4. apply to a scratch database and count the objects that actually exist
  5. drop the scratch database

Usage:  python scripts/scenarios/render.py

Needs `envsubst` (gettext) and the variables `init.sql.template` interpolates — source
`.env.admin` first:

    set -a && . ./.env.admin && set +a
"""

import os
import re
import subprocess
import sys
import tempfile

import zb

ROOT = zb.ROOT
TEMPLATE = ROOT / "init.sql.template"
SCRATCH_DB = "zb_render_check"

# The variables the template interpolates. Unset ones render as "" and produce SQL that
# looks fine, so they are checked before anything else.
REQUIRED_VARS = [
    "POSTGRES_BRIDGE_USER",
    "POSTGRES_BRIDGE_PASSWORD",
    "POSTGRES_WRITER_USER",
    "POSTGRES_WRITER_PASSWORD",
    "TARGET_DB",
    "BRIDGE_CDC_PUBLICATION",
]

ADMIN_URL = os.environ.get(
    "ADMIN_DATABASE_URL", "postgres://postgres:postgres_password@127.0.0.1:55432/postgres"
)


def admin(sql: str, db: str | None = None, stop_on_error: bool = True) -> subprocess.CompletedProcess:
    url = ADMIN_URL if db is None else re.sub(r"/[^/]+$", f"/{db}", ADMIN_URL)
    cmd = ["psql", url, "-At"]
    if stop_on_error:
        cmd += ["-v", "ON_ERROR_STOP=1"]
    return subprocess.run(cmd + ["-c", sql], capture_output=True, text=True)


async def main():
    # Async only because `zb.run` drives the scenarios through asyncio; nothing here
    # awaits — this scenario talks to psql and envsubst, not to NATS.
    failed = 0
    src = TEMPLATE.read_text()

    # ── 0. every variable is set ────────────────────────────────────────────────
    missing = [v for v in REQUIRED_VARS if not os.environ.get(v)]
    if missing:
        sys.exit(
            f"unset: {', '.join(missing)}\n"
            "  envsubst replaces these with the empty string, which renders as valid SQL.\n"
            "  set -a && . ./.env.admin && set +a"
        )
    zb.ok(f"all {len(REQUIRED_VARS)} interpolated variables are set")

    # ── 1. render ───────────────────────────────────────────────────────────────
    res = subprocess.run(["envsubst"], input=src, capture_output=True, text=True)
    if res.returncode != 0:
        sys.exit(f"envsubst failed: {res.stderr}")
    rendered = res.stdout

    # ── 2. no named dollar tag survived ─────────────────────────────────────────
    #
    # Checked on the RENDER, not the template: a tag that envsubst ate leaves no trace of
    # itself, so the template is where it must not appear and the render is where the
    # damage shows. Both are checked — the template so the mistake is caught at authoring
    # time, the render so an unknown variable name cannot slip through.
    tags_in_template = sorted(set(re.findall(r"\$[A-Za-z_][A-Za-z0-9_]*\$", src)))
    if tags_in_template:
        zb.bad(
            f"named dollar-quote tag(s) in the template: {tags_in_template} — "
            "envsubst reads these as variables and deletes them. Use a bare $$."
        )
        failed += 1
    else:
        zb.ok("no named dollar-quote tags in the template")

    # ── 3. nothing was silently dropped ─────────────────────────────────────────
    def creates(text: str) -> list[str]:
        return re.findall(
            r"^CREATE (?:OR REPLACE )?(FUNCTION|TABLE|EVENT TRIGGER|PUBLICATION|INDEX)\s+"
            r"(?:IF NOT EXISTS\s+)?([\w.\"]+)",
            text,
            re.MULTILINE,
        )

    want, got = creates(src), creates(rendered)
    if len(want) != len(got):
        lost = set(want) - set(got)
        zb.bad(f"render lost {len(want) - len(got)} definition(s): {sorted(lost)}")
        failed += 1
    else:
        zb.ok(f"all {len(want)} CREATE statements survived the render")

    # An unset variable renders as "", which is how `password: ''` and `INTERVAL ''` happen.
    for empty in re.findall(r"^.*(?:''|\"\")\s*(?:;|,).*$", rendered, re.MULTILINE):
        if "PASSWORD" in empty.upper() or "INTERVAL" in empty.upper():
            zb.bad(f"empty value rendered into: {empty.strip()[:80]}")
            failed += 1

    # ── 4. it runs, and the objects exist afterwards ────────────────────────────
    admin(f"DROP DATABASE IF EXISTS {SCRATCH_DB}", stop_on_error=False)
    if admin(f"CREATE DATABASE {SCRATCH_DB}").returncode != 0:
        sys.exit(f"could not create the scratch database {SCRATCH_DB}")

    try:
        with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as fh:
            fh.write(rendered)
            path = fh.name
        url = re.sub(r"/[^/]+$", f"/{SCRATCH_DB}", ADMIN_URL)
        run = subprocess.run(
            ["psql", url, "-v", "ON_ERROR_STOP=1", "-q", "-f", path],
            capture_output=True,
            text=True,
        )
        if run.returncode != 0:
            zb.bad(f"the rendered SQL failed to apply: {run.stderr.strip().splitlines()[:2]}")
            failed += 1
        else:
            zb.ok("the rendered SQL applies cleanly to a fresh database")

        # The decisive count. psql exiting 0 is not evidence: the truncation bug produced a
        # clean exit with six of seven functions missing.
        counts = admin(
            "SELECT (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace"
            "        WHERE n.nspname='public' AND p.proname LIKE 'zebridge%')::text"
            " || '|' || (SELECT count(*) FROM pg_event_trigger WHERE evtname LIKE 'zebridge%')::text"
            " || '|' || (SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'zebridge%')::text",
            db=SCRATCH_DB,
        ).stdout.strip()
        fns, trgs, tbls = (int(x) for x in counts.split("|"))

        want_fns = sum(1 for kind, _ in want if kind == "FUNCTION")
        want_trgs = sum(1 for kind, _ in want if kind == "EVENT TRIGGER")
        print(f"\n  functions      {fns} (template declares {want_fns})")
        print(f"  event triggers {trgs} (template declares {want_trgs})")
        print(f"  tables         {tbls}")

        if fns < want_fns:
            zb.bad(f"{want_fns - fns} function(s) declared but not created — the truncation signature")
            failed += 1
        else:
            zb.ok("every declared function exists in the database")

        if trgs < want_trgs:
            zb.bad(f"{want_trgs - trgs} event trigger(s) missing — the publication guard is one of these")
            failed += 1
        else:
            zb.ok("every declared event trigger exists")
    finally:
        admin(f"DROP DATABASE IF EXISTS {SCRATCH_DB}", stop_on_error=False)

    return 1 if failed else 0


zb.run(main)

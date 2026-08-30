#!/usr/bin/env python3
"""Cross-check the two env files, since nothing else can.

`.env.admin` and `.env.bridge` are deliberately separate — admin credentials must not be
in the shell that launches the bridge — but the split leaves values that have to agree
across the boundary and no component able to notice when they do not:

    native stack   Postgres on 127.0.0.1:5432 (scripts/native/up.sh; ZB_PG_PORT to move it)
    .env.bridge    DATABASE_READER_URL=…@127.0.0.1:5432/…   the port the bridge dials

Change one and the bridge simply fails to connect, with an error naming a port that
looks correct in whichever file you happen to open. The bridge cannot check this itself:
it has no idea where the stack was brought up, and asking it to read `.env.admin` would
undo the separation.

This also catches the things a split file layout invites: a variable that moved but was
left behind in the old file, admin credentials creeping back into the bridge's, and a
`NATS_CREDS` that names a file nobody minted.

Runs offline — no bridge, no broker, no database.

Usage:  python scripts/scenarios/envcheck.py
"""

import os
import pathlib
import re
import sys
from urllib.parse import urlparse

import zb

ADMIN = zb.ROOT / ".env.admin"
BRIDGE = zb.ROOT / ".env.bridge"

# Read by the DBA's tooling (init.sql, zb-derive-env.py), never by the bridge. `bridge
# --help` is the other half of this list. The per-role POSTGRES_* pairs are derived
# from the bridge's URLs now and belong in NEITHER file.
ADMIN_ONLY = [
    "PG_HOST", "PG_PORT", "PG_USER", "PG_PASSWORD", "PG_DB", "TARGET_DB",
    "POSTGRES_READER_USER", "POSTGRES_READER_PASSWORD",
    "POSTGRES_WRITER_USER", "POSTGRES_WRITER_PASSWORD",
    "NATS_BRIDGE_NKEY_PUB",
]


def parse(path: pathlib.Path) -> dict:
    if not path.exists():
        sys.exit(f"{path.name} not found")
    out = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        # Trailing `# comment` on a value line, as .env files allow.
        out[key.strip()] = re.split(r"\s+#", value.strip(), maxsplit=1)[0].strip()
    return out


async def main():
    admin, bridge = parse(ADMIN), parse(BRIDGE)
    failed = 0

    # ── the bridge's URLs against the native Postgres ──────────────────────────
    native_port = os.environ.get("ZB_PG_PORT", "5432")
    for name in ("DATABASE_READER_URL", "DATABASE_WRITER_URL"):
        url = bridge.get(name)
        if not url:
            if name == "DATABASE_READER_URL":
                zb.bad("DATABASE_READER_URL is missing from .env.bridge — the bridge will refuse to start")
                failed = 1
            else:
                print(f"  ⓘ  {name} unset: ingress disabled")
            continue
        port = str(urlparse(url).port or 5432)
        if port != native_port:
            zb.bad(f"{name} dials :{port} but the native Postgres listens on :{native_port} "
                   "(ZB_PG_PORT if you moved it)")
            failed = 1
        else:
            zb.ok(f"{name} dials :{port}, the native Postgres port")

    # ── the credential the shell names must exist ──────────────────────────────
    creds = os.environ.get("NATS_CREDS") or bridge.get("NATS_CREDS")
    if creds:
        path = pathlib.Path(creds)
        if not path.is_absolute():
            path = zb.ROOT / path
        if path.exists():
            zb.ok(f"NATS_CREDS={creds} exists")
        else:
            zb.bad(f"NATS_CREDS={creds} does not exist — scripts/native/jwt-bootstrap.sh mints the creds")
            failed = 1
    else:
        print("  ⓘ  NATS_CREDS unset in the shell and .env.bridge — the bridge needs one (bridge.creds)")

    strays = [k for k in ADMIN_ONLY if k in bridge]
    if strays:
        zb.bad(f".env.bridge still defines admin variables: {', '.join(strays)}")
        print("     the bridge ignores them, so they are silent misinformation")
        failed = 1
    else:
        zb.ok("no admin variable leaks into .env.bridge")

    # Name collisions across the two files. This is the general form of the hazard: a
    # shell that has sourced both ends up with whichever was read last, and neither
    # file shows which one won.
    #
    # `DATABASE_READER_URL` is the sharp case. The admin legitimately needs a superuser
    # connection (init.sql, the Elixir emitter), but naming it `DATABASE_READER_URL` means
    # sourcing .env.admin hands the *bridge's* variable a superuser — the exact fallback
    # that was deliberately removed from the code. Give the admin's its own name.
    BRIDGE_OWNED = ("DATABASE_READER_URL", "DATABASE_WRITER_URL", "NATS_URL", "NATS_CREDS", "BRIDGE_PORT")
    for name in sorted(set(admin) & set(bridge)):
        same = admin[name] == bridge[name]
        if name in BRIDGE_OWNED:
            zb.bad(f"{name} is defined in BOTH files — it is the bridge's name; "
                   f"rename the admin one (ADMIN_{name})")
            print(f"     .env.admin  {admin[name]}")
            print(f"     .env.bridge {bridge[name]}")
            failed = 1
        elif not same:
            zb.bad(f"{name} is defined in both files with different values")
            failed = 1
        else:
            print(f"  ⓘ  {name} duplicated in both files (same value) — pick one home")

    # ── BASE_BUF against what the broker will accept ───────────────────────────
    #
    # ⚠️ This gap is what made a client-reachable denial of service possible. NATS accepts
    # up to `max_payload` (1 MiB by default); CDC packs a row into `2^BASE_BUF` (16 KiB by
    # default). Between the two sits every row a client may legally write but the change
    # feed cannot carry — and before the ingress size check existed, one such write
    # suspended the table for **every** client while its sender was told `accepted`.
    #
    # The ingress check now refuses those writes, so this is no longer a hazard — it is a
    # capability limit, and the point of saying it out loud is that an operator cannot see
    # it anywhere else: `BASE_BUF` lives in .env.bridge and `max_payload` in the NATS
    # server config, and nothing compares them.
    base_buf = int(bridge.get("BASE_BUF") or 14)
    row_limit = 1 << base_buf
    nats_max = 1024 * 1024  # nats-server.conf default; the bridge reads the advertised value
    if row_limit < nats_max:
        print(
            f"  ⓘ  BASE_BUF={base_buf} caps a row at {row_limit:,} bytes while NATS accepts "
            f"{nats_max:,}. Rows between the two are refused at ingress (RowTooLargeToReplicate) —\n"
            f"     intended, but it means clients cannot write rows larger than {row_limit:,} bytes. "
            "Published to them as `max_row_bytes` in the schema."
        )
    else:
        zb.ok(f"BASE_BUF={base_buf} covers everything NATS will accept ({row_limit:,} bytes)")

    for name in BRIDGE_OWNED:
        if name in admin and name not in bridge:
            zb.bad(f".env.admin defines {name}, which is the bridge's variable — "
                   f"rename it (ADMIN_{name}) so sourcing .env.admin cannot reconfigure the bridge")
            failed = 1

    return failed


zb.run(main)

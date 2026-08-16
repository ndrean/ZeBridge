#!/usr/bin/env python3
"""Cross-check the two env files, since nothing else can.

`.env.admin` and `.env.bridge` are deliberately separate — admin credentials must not be
in the shell that launches the bridge — but the split leaves one value that has to agree
across the boundary and no component able to notice when it does not:

    .env.admin   PG_PUBLISH_PORT=55432        the port compose publishes Postgres on
    .env.bridge  DATABASE_URL=…@127.0.0.1:55432/…   the port the bridge dials

Change one and the bridge simply fails to connect, with an error naming a port that
looks correct in whichever file you happen to open. The bridge cannot check this itself:
it has no idea what compose published, and asking it to read `.env.admin` would undo
the separation.

This also catches the things a split file layout invites: a variable that moved but was
left behind in the old file, and admin credentials creeping back into the bridge's.

Runs offline — no bridge, no broker, no database.

Usage:  python scripts/scenarios/envcheck.py
"""

import pathlib
import re
import sys
from urllib.parse import urlparse

import zb

ADMIN = zb.ROOT / ".env.admin"
BRIDGE = zb.ROOT / ".env.bridge"

# Read by compose or by the init containers, never by the bridge. `bridge --help` is the
# other half of this list.
ADMIN_ONLY = [
    "PG_HOST", "PG_PORT", "PG_USER", "PG_PASSWORD", "PG_DB", "TARGET_DB",
    "POSTGRES_BRIDGE_USER", "POSTGRES_BRIDGE_PASSWORD",
    "POSTGRES_WRITER_USER", "POSTGRES_WRITER_PASSWORD",
    "PG_PUBLISH_PORT", "SNAP_RET_SECONDS", "NATS_BRIDGE_NKEY_PUB",
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

    published = admin.get("PG_PUBLISH_PORT")
    if not published:
        zb.bad("PG_PUBLISH_PORT is not in .env.admin — compose falls back to 55432 silently")
        failed = 1

    for name in ("DATABASE_URL", "DATABASE_WRITER_URL"):
        url = bridge.get(name)
        if not url:
            if name == "DATABASE_URL":
                zb.bad("DATABASE_URL is missing from .env.bridge — the bridge will refuse to start")
                failed = 1
            else:
                print(f"  ⓘ  {name} unset: ingress disabled")
            continue
        port = str(urlparse(url).port or 5432)
        if published and port != published:
            zb.bad(f"{name} dials :{port} but compose publishes :{published}")
            failed = 1
        elif published:
            zb.ok(f"{name} dials :{port}, the port compose publishes")

    strays = [k for k in ADMIN_ONLY if k in bridge]
    if strays:
        zb.bad(f".env.bridge still defines admin variables: {', '.join(strays)}")
        print("     the bridge ignores them, so they are silent misinformation")
        failed = 1
    else:
        zb.ok("no admin variable leaks into .env.bridge")

    missing = [k for k in ADMIN_ONLY if k not in admin]
    if missing:
        print(f"  ⓘ  not set in .env.admin: {', '.join(missing)}")

    for name in ("DATABASE_URL", "DATABASE_WRITER_URL", "NATS_URL"):
        if name in admin:
            zb.bad(f".env.admin defines {name} — that is the bridge's, and compose hands "
                   "this file to containers that have no business holding it")
            failed = 1

    return failed


zb.run(main)

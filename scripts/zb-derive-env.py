#!/usr/bin/env python3
"""Derive what init.sql needs from the values that already exist elsewhere.

One value, one home. `init.{core,write}.template.sql` interpolates seven
variables, and four of them are the bridge's own role credentials — the same
secret `.env.bridge` already carries inside `DATABASE_READER_URL` /
`DATABASE_WRITER_URL`, only in parts instead of as a URL. Storing them twice is
how a password drifts: change it in one file and nothing complains until the
next bridge restart fails to authenticate, naming the ROLE rather than the file
that went stale.

So: `.env.bridge` owns the role credentials and the publication name (they are
the bridge's identity), `grammar.json` owns the wire names, and `.env.admin`
shrinks to what only the DBA needs — the superuser connection and the target
database.

    eval "$(python3 scripts/zb-derive-env.py)"      # shell
    import zb_derive_env; zb_derive_env.derive_into(os.environ)   # python

⚠️ A password containing `@` or `:` must be percent-encoded in the URL, exactly
as libpq requires — this parser splits on the last `@` and the first `:`, which
is the same rule the database itself applies.
"""
import json
import os
import pathlib
import shlex
import sys
from urllib.parse import unquote

ROOT = pathlib.Path(__file__).resolve().parent.parent


def _split(url: str) -> tuple[str, str]:
    """postgres://user:pass@host:port/db -> (user, pass), percent-decoded."""
    rest = url.split("://", 1)[-1]
    creds = rest.rsplit("@", 1)[0] if "@" in rest else ""
    user, _, password = creds.partition(":")
    return unquote(user), unquote(password)


def derive_into(env) -> dict:
    """Fill the template's variables from the canonical sources. Never
    overwrites something already set — an explicit value always wins."""
    out = {}
    for role, var in (("READER", "DATABASE_READER_URL"), ("WRITER", "DATABASE_WRITER_URL")):
        url = env.get(var)
        if not url:
            continue
        user, password = _split(url)
        for suffix, value in (("USER", user), ("PASSWORD", password)):
            key = f"POSTGRES_{role}_{suffix}"
            if value and not env.get(key):
                env[key] = value
                out[key] = value
    # The wire grammar owns the open tenant; .env.admin used to carry a copy,
    # and check.py exists partly to catch the two disagreeing.
    if not env.get("OPEN_TENANT"):
        try:
            grammar = json.loads((ROOT / "grammar.json").read_text())
            if grammar.get("open_tenant"):
                env["OPEN_TENANT"] = grammar["open_tenant"]
                out["OPEN_TENANT"] = grammar["open_tenant"]
        except (OSError, json.JSONDecodeError):
            pass
    return out


if __name__ == "__main__":
    derived = derive_into(os.environ)
    for k, v in derived.items():
        print(f"export {k}={shlex.quote(v)}")
    if not derived:
        print("# nothing to derive — every variable was already set", file=sys.stderr)

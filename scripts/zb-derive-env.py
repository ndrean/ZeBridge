#!/usr/bin/env python3
"""Derive what init.sql needs from the values that already exist elsewhere.

One value, one home. `init.{core,write}.template.sql` interpolates the template
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
import subprocess
import sys
from urllib.parse import unquote

ROOT = pathlib.Path(__file__).resolve().parent.parent


def _split(url: str) -> tuple[str, str]:
    """postgres://user:pass@host:port/db -> (user, pass), percent-decoded."""
    rest = url.split("://", 1)[-1]
    creds = rest.rsplit("@", 1)[0] if "@" in rest else ""
    user, _, password = creds.partition(":")
    return unquote(user), unquote(password)


def _tenant_limit(env) -> str:
    """`bridge --limits` -> tenant_bytes. It exits before reading any configuration."""
    binary = env.get("ZB_BRIDGE_BIN") or str(ROOT / "zig-out" / "bin" / "bridge")
    try:
        res = subprocess.run([binary, "--limits"], capture_output=True, text=True, timeout=10, check=True)
        return str(int(json.loads(res.stdout)["tenant_bytes"]))
    except (OSError, subprocess.SubprocessError, ValueError, KeyError) as e:
        raise ValueError(
            f"MAX_TENANT_BYTES: `{binary} --limits` failed ({e}) — build the bridge (zig build), "
            "or set ZB_BRIDGE_BIN, or set MAX_TENANT_BYTES yourself"
        ) from e


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
            grammar = json.loads((ROOT / "src" / "grammar.json").read_text())
            if grammar.get("open_tenant"):
                env["OPEN_TENANT"] = grammar["open_tenant"]
                out["OPEN_TENANT"] = grammar["open_tenant"]
        except (OSError, json.JSONDecodeError):
            pass
    if not env.get("LEAF_PREFIX"):
        grammar = json.loads((ROOT / "src" / "grammar.json").read_text())
        env["LEAF_PREFIX"] = grammar.get("leaf_prefix", "lf-")
        out["LEAF_PREFIX"] = env["LEAF_PREFIX"]
    # The tenant guard's length limit is the build's (src/limits.zig: 128 with the pinned
    # client library, 46 without), so it is read from the bridge, not copied here. Empty
    # renders `octet_length(x) <= ` — init.write.template.sql does not apply.
    if not env.get("MAX_TENANT_BYTES"):
        env["MAX_TENANT_BYTES"] = out["MAX_TENANT_BYTES"] = _tenant_limit(env)
    # Legacy envsubst installs enable the guard by default. An explicit override
    # can set false; bridge --init-sql uses its own build option instead.
    if not env.get("RECEIPT_COMPLETION_GUARD"):
        env["RECEIPT_COMPLETION_GUARD"] = "true"
        out["RECEIPT_COMPLETION_GUARD"] = "true"
    if env["RECEIPT_COMPLETION_GUARD"] not in ("true", "false"):
        raise ValueError("RECEIPT_COMPLETION_GUARD must be true or false")
    return out


if __name__ == "__main__":
    derived = derive_into(os.environ)
    for k, v in derived.items():
        print(f"export {k}={shlex.quote(v)}")
    if not derived:
        print("# nothing to derive — every variable was already set", file=sys.stderr)

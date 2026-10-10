#!/usr/bin/env python3
"""Generate include/zb.h from the C ABI in src/capi.zig.

The signatures come from the code, every time: each `export fn` is read and its Zig
types are mapped to C. The descriptions are written here, for a C reader. An export
without a description, or a type this map does not know, stops the generator.

    python3 scripts/gen-header.py           # write include/zb.h
    python3 scripts/gen-header.py --check   # fail if include/zb.h is not up to date
"""
import json
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent.parent
CAPI = HERE / "src" / "capi.zig"
ABI = HERE / "abi.json"
OUT = HERE / "include" / "zb.h"

TYPES = {
    "c_int": "int",
    "u64": "uint64_t",
    "void": "void",
    "?[*:0]const u8": "const char *",
    "?[*:0]u8": "char *",
}

# One or two sentences each. "Returns JSON" means a string the caller frees with
# zb_free; the generator adds that line from the return type.
DOCS = {
    "zb_abi_version": "The version of this C ABI. A host checks it against the version its own declarations were written for.",
    "zb_free": "Frees a string this library returned. Every `char *` result goes back through here, once.",
    "zb_call": "Runs a pure rule of the library's core, with no client: `fn_name` names it (\"mergeRegisters\"), `args_json` carries its arguments. Returns JSON.",
    "zb_client_connect": "Opens the local replica and the NATS connection. `opts_json` holds the options (dbPath, tables, natsUrl and creds, or bridgeUrl and invite). Returns a handle, or 0 on failure: the reason is in zb_last_error().",
    "zb_last_error": "Why the last zb_client_connect on this thread returned 0. NULL once a connect succeeds.",
    "zb_client_close": "Closes the connection and the replica. Returns 0, or 1 for an unknown handle.",
    "zb_client_wipe": "Closes the client and deletes its replica files. Returns 0, or 1 for an unknown handle.",
    "zb_grammar_hash": "The SHA-256 (hex) of the wire grammar this library was built with, to compare with the bridge's.",
    "zb_grammar_json": "The wire grammar this library was built with, as JSON.",
    "zb_creds_file_text": "Builds the text of a `.creds` file from the JWT that /enroll returned and the seed from zb_create_user.",
    "zb_client_wake": "Ends a zb_client_poll that is waiting, at once. The only call allowed from a thread that does not own the handle. Returns 0, or 1 for an unknown handle.",
    "zb_client_stamp": "A register stamp on the bridge's clock, for cooperative editing (the `t` beside a value).",
    "zb_client_revoked": "1 if the principal was revoked, 0 if not, -1 for an unknown handle.",
    "zb_client_live": "How many client handles are open in this process. For leak checks in tests.",
    "zb_create_user": "Generates the key pair a device enrolls with: {\"publicKey\", \"seed\"}. The seed is the private half: keep it on the device.",
    "zb_client_join": "Follows one more tenant: its stream, and its rows in the same local tables. Returns {\"tenants\": [...]}.",
    "zb_client_leave": "Stops following a tenant: its rows leave the local tables. Returns {\"tenants\": [...]}.",
    "zb_client_sync": "The first call after connect: resolves the tenant, applies the schemas, seeds the tables, catches up on the streams. Returns {\"principal\", \"tenant\", \"tenants\", \"first\", \"unseeded\"}.",
    "zb_client_query": "Reads the local replica. `params_json` is a JSON array of the `?` parameters. Returns {\"columns\", \"rows\"}.",
    "zb_client_mutate": "One write: applied locally at once, then sent. `op` is INSERT, UPDATE or DELETE; `key_json` and `values_json` are JSON objects. Returns {\"msgId\"}: the poll report's outcomes name it.",
    "zb_client_mutate_at": "zb_client_mutate with the caller's own version stamp (RFC 3339, UTC).",
    "zb_client_serve": "Answers questions on this client's connection. `opts_json`: {\"tenants\", \"queries\"}. The questions arrive in the poll report's `requests`.",
    "zb_client_reply": "Answers one question from the poll report's `requests`. Returns {\"replied\"}.",
    "zb_client_poll": "Waits up to `wait_ms` for changes, applies them and collects verdicts. Blocks the calling thread. Returns {\"applied\", \"settled\", \"changed_tables\", \"seeded\", \"outcomes\", \"pending\"}.",
    "zb_client_request": "Asks a service (`subject` is query.<tenant>.<name>) and waits up to `timeout_ms` for its answer.",
    "zb_client_ingest": "Writes a service's answer ({\"columns\", \"rows\"}) into a local on-demand table. `scope_json` is optional.",
    "zb_client_flush_outbox": "Sends the queued writes and waits up to `wait_ms` for their verdicts. Returns {\"sent\", \"settled\", \"verdicts\"}.",
}

ORDER_NOTE = """\
 * Every string goes in and out as UTF-8 JSON (or a plain C string where noted).
 * Strings you pass are read during the call and never kept.
 * A `char *` result is yours: free it with zb_free(). A `const char *` result belongs
 * to the library: do not free it.
 * A call that fails returns {"error": "<Name>"} (plus "detail"), except
 * zb_client_connect, which returns 0 and leaves the reason in zb_last_error().
 * One thread owns a handle; only zb_client_wake may be called from another."""


def exports():
    text = CAPI.read_text()
    found = []
    for m in re.finditer(r"^export fn (\w+)\(([^)]*)\)\s*([^{]+?)\s*\{", text, re.M):
        name, params, ret = m.group(1), m.group(2).strip(), m.group(3).strip()
        args = []
        if params:
            for p in params.split(","):
                pname, ptype = (s.strip() for s in p.split(":", 1))
                args.append((pname, ptype))
        found.append((name, args, ret))
    return found


def ctype(t, where):
    if t not in TYPES:
        sys.exit(f"gen-header: {where}: no C type for `{t}` — add it to TYPES")
    return TYPES[t]


def render():
    abi = json.loads(ABI.read_text())
    lines = [
        "/* zb.h: libzb's C ABI. Generated by scripts/gen-header.py from src/capi.zig.",
        " * Do not edit: change the code, then run the script.",
        " *",
        ORDER_NOTE,
        " */",
        "#ifndef ZB_H",
        "#define ZB_H",
        "",
        "#include <stdint.h>",
        "",
        f"#define ZB_ABI_VERSION {abi['version']}",
        "",
        "#ifdef __cplusplus",
        'extern "C" {',
        "#endif",
        "",
    ]
    names = []
    for name, args, ret in exports():
        names.append(name)
        if name not in DOCS:
            sys.exit(f"gen-header: {name} has no description — add it to DOCS")
        doc = DOCS[name]
        cret = ctype(ret, name)
        if cret == "char *" and name != "zb_free":
            doc += " Free the result with zb_free()."
        elif cret == "const char *":
            doc += " The library owns the string: do not free it."
        cargs = ", ".join(f"{ctype(t, name + '.' + n)}{'' if ctype(t, name).endswith('*') else ' '}{n}" for n, t in args) or "void"
        lines.append(f"/* {doc} */")
        lines.append(f"{cret}{'' if cret.endswith('*') else ' '}{name}({cargs});")
        lines.append("")
    stale = sorted(set(DOCS) - set(names))
    if stale:
        sys.exit(f"gen-header: described but not exported: {', '.join(stale)}")
    missing = sorted(set(abi["functions"]) ^ set(names))
    if missing:
        sys.exit(f"gen-header: abi.json and capi.zig disagree on: {', '.join(missing)}")
    lines += ["#ifdef __cplusplus", "}", "#endif", "", "#endif /* ZB_H */", ""]
    return "\n".join(lines)


if __name__ == "__main__":
    header = render()
    if "--check" in sys.argv:
        if not OUT.exists() or OUT.read_text() != header:
            sys.exit("gen-header: include/zb.h is out of date — run scripts/gen-header.py")
        print("include/zb.h is up to date")
    else:
        OUT.parent.mkdir(exist_ok=True)
        OUT.write_text(header)
        print(f"wrote {OUT.relative_to(HERE)} ({header.count(');')} functions)")

#!/usr/bin/env python3
"""libzb/abi.json against the code: the exported `zb_*` functions, the option keys
`openBox` reads from `zb_client_connect`'s JSON, and the version — capi.zig's
`abi_version` and every host that pins it (zb-react-native, zb-android, zb-python, zb-dart). Any difference fails and
says what to do: a host embedding a COPY of libzb (an xcframework, an .so) finds out it
is stale by comparing versions, so a change that is not a bump is a stale copy nobody
can detect (NOTES §10ja: an app's libzb, built before `creds`, sat in connect)."""
import json, pathlib, re, sys

LIBZB = pathlib.Path(__file__).resolve().parents[1]
REPO = LIBZB.parent
abi = json.loads((LIBZB / "abi.json").read_text())
src = (LIBZB / "src" / "capi.zig").read_text()

fns = sorted(set(re.findall(r"^export fn (zb_\w+)\(", src, re.M)))
a = src.index("fn openBox(")
b = a + 10 + re.search(r"^(export )?fn ", src[a + 10:], re.M).start()
keys = sorted({k for pair in re.findall(r'\(o, "([a-zA-Z]+)"|get\("([a-zA-Z]+)"\)', src[a:b]) for k in pair if k})
m = re.search(r"pub const abi_version: c_int = (\d+);", src)
code_version = int(m.group(1)) if m else None

problems = []
if fns != abi["functions"]:
    problems.append(f"exported functions changed: +{sorted(set(fns) - set(abi['functions']))} -{sorted(set(abi['functions']) - set(fns))}")
if keys != abi["connectOptions"]:
    problems.append(f"connect options changed: +{sorted(set(keys) - set(abi['connectOptions']))} -{sorted(set(abi['connectOptions']) - set(keys))}")
if code_version != abi["version"]:
    problems.append(f"capi.zig abi_version is {code_version}, abi.json says {abi['version']}")
for pin in (REPO / "zb-react-native" / "src" / "abi.ts", REPO / "zb-android" / "src" / "main" / "kotlin" / "dev" / "zebridge" / "ZeBridge.kt", REPO / "zb-python" / "src" / "zebridge" / "__init__.py", REPO / "zb-dart" / "lib" / "src" / "native.dart"):
    if pin.exists():
        pm = re.search(r"(?:ZB_ABI|zbAbi) = (\d+)", pin.read_text())
        if not pm or int(pm.group(1)) != abi["version"]:
            problems.append(f"{pin.relative_to(REPO)} pins {pm.group(1) if pm else '?'}, abi.json says {abi['version']}")

if problems:
    for p in problems:
        print(f"✗ {p}")
    print("→ bump `version` in libzb/abi.json, `abi_version` in capi.zig and every pin, and update the lists")
    sys.exit(1)
print(f"✓ libzb ABI {abi['version']}: {len(fns)} functions, {len(keys)} connect options, pins agree")

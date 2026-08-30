#!/usr/bin/env python3
"""The conformance runner, Python edition — consumer #3 of the fixtures.

Loads the Zig-built C library (zig-out/lib/libzbcore.*) and runs
zb-client-ts/fixtures/core-fixtures.json against it: the SAME file the
TypeScript core is pinned by. A section the library does not implement — no
dispatcher here, or "unknown fn" from the library — is a FAILURE: silence is how
ports rot. `--allow-skip` downgrades that to a loud SKIP for exploratory runs.

    cd libzb && zig build && python3 python/runner.py [--allow-skip]
"""
import argparse
import ctypes
import json
import sys
from pathlib import Path

from _env import load_lib

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("--allow-skip", action="store_true",
                help="report an unported section as SKIP instead of failing (exploratory use)")
ALLOW_SKIP = ap.parse_args().allow_skip

HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent.parent / "zb-client-ts" / "fixtures" / "core-fixtures.json"
if not FIXTURES.exists():
    sys.exit(f"fixtures missing: {FIXTURES}")

lib = load_lib()
lib.zb_call.restype = ctypes.c_void_p
lib.zb_call.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
lib.zb_free.argtypes = [ctypes.c_void_p]

def call(fn: str, args: dict):
    p = lib.zb_call(fn.encode(), json.dumps(args).encode())
    if not p:
        raise RuntimeError("zb_call returned NULL")
    try:
        return json.loads(ctypes.string_at(p).decode())
    finally:
        lib.zb_free(p)

fx = json.loads(FIXTURES.read_text())

# section -> (input builder, expected extractor). The input is the fixture
# case's own fields; the expected value is what the TS runner asserts.
SECTIONS = {
    "seedGate":        (lambda c: {"ev": c["ev"], "anchor": c["anchor"]},          lambda c: c["drops"]),
    "update":          (lambda c: {"table": c["table"], "pkCols": c["pkCols"], "data": c["data"]}, lambda c: c["plan"]),
    "exists":          (lambda c: {"table": c["table"], "pkCols": c["pkCols"], "data": c["data"]}, lambda c: c["plan"]),
    "pgArrayLiteral":  (lambda c: {"in": c["in"]},                                 lambda c: c["out"]),
    "tombstoned":      (lambda c: {"tombstoneColumn": c["tombstoneColumn"], "data": c["data"]}, lambda c: c["drops"]),
    "position":        (lambda c: {"stored": c["stored"], "batch": c["batch"]},    lambda c: c["next"]),
    "fkKind":          (lambda c: {"message": c["message"]},                       lambda c: c["kind"]),
    "pgTsToWire":      (lambda c: {"in": c["in"]},                                 lambda c: c["out"]),
    "lsnToNumber":     (lambda c: {"in": c["in"]},                                 lambda c: c["out"]),
    "normalizeVersion":(lambda c: {"in": c["in"]},                                 lambda c: c["out"]),
    "nextVersion":     (lambda c: {"now": c["now"], "last": c["last"]},            lambda c: c["out"]),
    "hlcVersion":      (lambda c: {"now": c["now"], "last": c["last"], "floor": c["floor"]}, lambda c: c["out"]),
    "subjectSafe":     (lambda c: {"in": c["in"]},                                 lambda c: c["out"]),
    "envelope":        (lambda c: c["args"],                                       lambda c: c["out"]),
    "keyChange":       (lambda c: {"table": c["table"], "pkCols": c["pkCols"], "data": c["data"]}, lambda c: c["step"]),
    "upsert":          (lambda c: {"table": c["table"], "pkCols": c["pkCols"], "data": c["data"]}, lambda c: c["step"]),
    "delete":          (lambda c: {"table": c["table"], "pkCols": c["pkCols"], "data": c["data"]}, lambda c: c["step"]),
    "chainUpsert":     (lambda c: {"table": c["table"], "cols": c["cols"], "pkCols": c["pkCols"], "versionCol": c["versionCol"]}, lambda c: c["sql"]),
    "chainRowParams":  (lambda c: {"row": c["row"]},                               lambda c: c["params"]),
    "chainPlan":       (lambda c: {"manifest": c["manifest"], "watermark": c["watermark"]}, lambda c: c["plan"]),
    "fullPredates":    (lambda c: {"manifest": c["manifest"], "plan": c["plan"], "storedSeq": c["storedSeq"]}, lambda c: c["predates"]),
    "outboxWatermark": (lambda c: {"entries": c["entries"], "watermark": c["watermark"]}, lambda c: {"send": c["send"], "refuse": c["refuse"]}),
    "scope":           (lambda c: {"streams": c["streams"], "tables": c["tables"]}, lambda c: {"gapped": sorted(c["gapped"]), "tablesToSeed": sorted(c["tablesToSeed"])}),
    "columnDdl":       (lambda c: {"col": c["col"], "pkCols": c["pkCols"]},          lambda c: c["ddl"]),
    "fkClauses":       (lambda c: {"fks": c["fks"]},                                 lambda c: c["text"]),
    "createTable":     (lambda c: {"table": c["table"], "cols": c["cols"], "pkCols": c["pkCols"], "fks": c["fks"]}, lambda c: c["steps"]),
    "rebuildSteps":    (lambda c: {"table": c["table"], "cols": c["cols"], "pkCols": c["pkCols"], "fks": c["fks"], "existing": c["existing"]}, lambda c: c["steps"]),
    "diffColumns":     (lambda c: {"existing": c["existing"], "wanted": c["wanted"], "renamed": c["renamed"]}, lambda c: c["out"]),
    "fkDiffer":        (lambda c: {"ddl": c["ddl"], "want": c["want"]},              lambda c: c["differs"]),
    "viewSteps":       (lambda c: {"table": c["table"], "names": c["names"]},        lambda c: c["steps"]),
    "indexPlan":       (lambda c: {"table": c["table"], "have": c["have"], "want": c["want"]}, lambda c: {"drops": c["drops"], "creates": c["creates"]}),
}

# order-insensitive sections: sort list fields before comparing
def _norm(section, v):
    if section == "scope" and isinstance(v, dict):
        return {k: sorted(x) if isinstance(x, list) else x for k, x in v.items()}
    return v

passed = failed = 0
skipped: list[str] = []


def unported(label: str) -> None:
    """An unported section fails the run unless --allow-skip asked for a SKIP."""
    global failed
    skipped.append(label)
    if not ALLOW_SKIP:
        failed += 1
        print(f"✗ not ported: {label}")


for section, cases in fx.items():
    if section.startswith("_"):
        continue
    if section not in SECTIONS:
        unported(f"{section} ({len(cases)}) [no dispatcher in runner.py]")
        continue
    build, expect = SECTIONS[section]
    for c in cases:
        got = call(section, build(c))
        if isinstance(got, dict) and got.get("error") == "unknown fn":
            unported(f"{section} ({len(cases)}) [unknown fn in the library]")
            break
        want = expect(c)
        got = _norm(section, got)
        want = _norm(section, want)
        if got == want:
            passed += 1
        else:
            failed += 1
            print(f"✗ {section}: {c['name']}")
            print(f"    want: {json.dumps(want)}")
            print(f"    got : {json.dumps(got)}")

print(f"\n# pass {passed}")
print(f"# fail {failed}")
if skipped:
    tag = "SKIP (--allow-skip)" if ALLOW_SKIP else "FAILED as not ported"
    print(f"# {tag}: {', '.join(skipped)}")
sys.exit(1 if failed else 0)

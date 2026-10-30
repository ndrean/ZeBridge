#!/usr/bin/env python3
"""One version for every ZeBridge package: the bridge, libzb, and each client package
that embeds libzb carry the release's number, so `v0.2.0` means the same code everywhere.

    scripts/version.py show            every version line, and whether they agree
    scripts/version.py set 0.2.0       write it everywhere
    scripts/version.py check 0.2.0     exit 1 when any line differs (release.yml, at the tag)

A package published from a release whose code did not change is republished all the same:
the libzb inside it is the release's.
"""
import pathlib, re, sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
V = r"\d+\.\d+\.\d+"

# (file, pattern whose group 2 is the version, how many lines must match)
PLACES = [
    ("build.zig.zon", rf'^(\s*\.version = ")({V})(",)', 1),
    ("libzb/build.zig.zon", rf'^(\s*\.version = ")({V})(",)', 1),
    ("zb-python/pyproject.toml", rf'^(version = ")({V})(")', 1),
    ("zb-client-ts/package.json", rf'^(  "version": ")({V})(",)', 1),
    ("zb-react-native/package.json", rf'^(  "version": ")({V})(",)', 1),
    ("zb-react-native/ios/ZbNative.podspec", rf"^(\s*s\.version\s*= ')({V})(')", 1),
    ("zb-dart/pubspec.yaml", rf"^(version: )({V})()$", 1),
    ("zb-android/build.gradle.kts", rf'(coordinates\("eu\.zebridge", "zebridge-android", ")({V})("\))', 1),
    # The docs' install lines.
    ("zb-android/README.md", rf'(eu\.zebridge:zebridge-android:)({V})()', 1),
    # The images a deployment runs by default: the release's own.
    ("docker-compose.quickstart.yml", rf"(ZB_VERSION(?::-|=))({V})()", 4),
    ("deploy/compose/compose.yml", rf"(ZB_VERSION:-)({V})()", 1),
    ("deploy/compose/.env.example", rf"^(ZB_VERSION=)({V})()$", 1),
    ("deploy/airports-responder.Dockerfile", rf"(ZB_VERSION=)({V})()", 2),
    ("deploy/compose/README.md", rf"^(   V=)({V})()$", 1),   # the tag its two files come from
    # Apps on the published packages: the phone check by hand (smoke/README.md) runs this
    # release, and the smoke client's editor view follows it.
    ("examples/10-airports/flutter/pubspec.yaml", rf"^(  zebridge: )({V})()(?=\s)", 1),
    ("smoke/clients/dart/pubspec.yaml", rf"^(  zebridge: \^)({V})()$", 1),
]


def found():
    for path, pat, n in PLACES:
        text = (ROOT / path).read_text()
        hits = [m.group(2) for m in re.finditer(pat, text, re.M)]
        yield path, hits, n


def show() -> int:
    seen = set()
    for path, hits, n in found():
        seen.update(hits)
        note = "" if len(hits) == n else f"   ⚠️ {len(hits)} line(s), {n} expected"
        print(f"{', '.join(hits) or '—':10} {path}{note}")
    print("✅ one version" if len(seen) == 1 else f"⚠️ {len(seen)} versions: {', '.join(sorted(seen))}")
    return 0


def check(want: str) -> int:
    bad = [(p, h, n) for p, h, n in found() if len(h) != n or any(x != want for x in h)]
    for path, hits, n in bad:
        print(f"🔴 {path}: {', '.join(hits) or 'no version line'} (want {want} × {n})")
    if not bad:
        print(f"✅ every package says {want}")
    return 1 if bad else 0


def set_(want: str) -> int:
    for path, pat, n in PLACES:
        f = ROOT / path
        text, k = re.subn(pat, lambda m: m.group(1) + want + m.group(3), f.read_text(), flags=re.M)
        if k != n:
            print(f"🔴 {path}: {k} line(s) changed, {n} expected — fix PLACES")
            return 1
        f.write_text(text)
    return check(want)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "show"
    if cmd == "show":
        sys.exit(show())
    if cmd in ("set", "check") and len(sys.argv) == 3 and re.fullmatch(V, sys.argv[2]):
        sys.exit(set_(sys.argv[2]) if cmd == "set" else check(sys.argv[2]))
    sys.exit(__doc__)

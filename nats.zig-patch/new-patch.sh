#!/usr/bin/env bash
# Cut the NEXT patch of the series: everything the submodule's working tree holds beyond
# upstream + the existing series, as one numbered file. Edit nats.zig/src (and tests/),
# run this with a topic name, then write the ledger entry. Never cut a patch from
# `git -C nats.zig diff` and filter hunks by hand: that is how patches came to carry
# each other's lines (NATS_ZIG_NOTES §17).
#   usage: nats.zig-patch/new-patch.sh <topic>        e.g. new-patch.sh jetstream-domain
set -euo pipefail
[ $# -eq 1 ] || { echo "usage: $0 <topic>"; exit 2; }
here="$(cd "$(dirname "$0")" && pwd)"
sub="$here/../nats.zig"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
git -C "$sub" archive HEAD | tar -x -C "$tmp"
for p in "$here"/[0-9][0-9]-nats.zig-*.patch; do git -C "$tmp" apply "$p"; done
last="$(ls "$here"/[0-9][0-9]-nats.zig-*.patch | tail -1 | xargs basename | cut -c1-2)"
next="$(printf '%02d' $((10#$last + 1)))"
out="$here/$next-nats.zig-$1.patch"
# a/ b/ prefixes relative to the submodule root, as `git -C nats.zig diff` would print them
if git -C "$tmp" diff --no-index --src-prefix="a/" --dst-prefix="b/" -- "$tmp/src" "$sub/src" > "$out.src" 2>/dev/null; then :; fi
if git -C "$tmp" diff --no-index --src-prefix="a/" --dst-prefix="b/" -- "$tmp/tests" "$sub/tests" > "$out.tests" 2>/dev/null; then :; fi
cat "$out.src" "$out.tests" | sed -e "s#a$tmp/#a/#g" -e "s#b$sub/#b/#g" -e "s#$tmp/#src/#; s#$sub/##" > "$out"
rm -f "$out.src" "$out.tests"
if [ ! -s "$out" ]; then rm -f "$out"; echo "nothing to cut: the working tree equals upstream + the series"; exit 0; fi
echo "wrote $(basename "$out") ($(grep -c '^@@' "$out") hunks, $(grep -c '^+++ b/' "$out") files)"
"$here/check-series.sh"

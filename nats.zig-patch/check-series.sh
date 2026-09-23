#!/usr/bin/env bash
# Proves the series: clean upstream (the commit the parent records) + every patch in
# numeric order must equal the submodule's working tree, src/ and tests/, byte for byte.
# `git apply -R --check` on one patch proves less than it seems — it passes for a patch
# whose lines a later patch also carries. This is the check that means "up to date".
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
sub="$here/../nats.zig"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
git -C "$sub" archive HEAD | tar -x -C "$tmp"
n=0
for p in "$here"/[0-9][0-9]-nats.zig-*.patch; do
  git -C "$tmp" apply "$p" || { echo "✗ $(basename "$p") does not apply on top of the previous ones"; exit 1; }
  n=$((n+1))
done
if diff -r "$tmp/src" "$sub/src" >/dev/null && diff -r "$tmp/tests" "$sub/tests" >/dev/null; then
  echo "✅ $n patches: upstream $(git -C "$sub" rev-parse --short HEAD) + series == working tree (src, tests)"
else
  echo "✗ the series does not reproduce the working tree:"; diff -rq "$tmp/src" "$sub/src" || true; diff -rq "$tmp/tests" "$sub/tests" || true; exit 1
fi

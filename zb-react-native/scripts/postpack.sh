#!/bin/sh
# After packing: drop the copy prepack.sh made, so this repository's examples keep reading
# zb-android's files in place (android/build.gradle prefers the copy when it exists).
here=$(cd "$(dirname "$0")/.." && pwd)
rm -rf "$here/android/zb-android" "$here/LICENSE"

#!/bin/sh
# The Zig standard library an iOS build of libzb compiles against: prints the path of a
# patched copy when zig-std-patch/ holds a fix for this Zig's version, nothing otherwise.
#
#   zl=$(scripts/zig-lib-dir.sh); [ -z "$zl" ] || export ZIG_LIB_DIR="$zl"
#
# Zig 0.17.0 cannot build std.Io.Threaded for iOS (NullFile has no fd there); master's fix
# is three lines (zig-std-patch/0.17.0-ios-threaded.patch). The copy is made once per Zig
# version and patch set, in Zig's own cache. Delete zig-std-patch/<version>-*.patch when a
# release carries the fix: this script then prints nothing and builds use Zig's own std.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
v=$(zig version)
set -- "$here"/zig-std-patch/"$v"-*.patch
[ -e "$1" ] || exit 0
lib=$(zig env | sed -n 's/^ *\.lib_dir = "\(.*\)",$/\1/p')
[ -d "$lib/std" ] || { echo "zig-lib-dir.sh: no std under '$lib'" >&2; exit 1; }
sum=$(cat "$@" | shasum | cut -c1-12)
cache=${ZIG_GLOBAL_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/zig}
dir="$cache/zebridge-lib/$v-$sum"
if [ ! -d "$dir" ]; then
  tmp="$dir.tmp.$$"
  rm -rf "$tmp"; mkdir -p "$(dirname "$dir")"
  cp -R "$lib" "$tmp"
  for p in "$@"; do patch -s -p1 -d "$tmp" < "$p"; done
  mv "$tmp" "$dir" 2>/dev/null || rm -rf "$tmp"   # another build made it first: keep theirs
fi
echo "$dir"

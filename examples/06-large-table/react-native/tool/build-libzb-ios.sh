#!/bin/sh
# libzb for the React Native module (modules/zb-native): the Flutter app's build, then
# each slice PRELINKED into one object that exports only `zb_*`.
#
# ⚠️ The prelink is the point. libzb carries its own SQLite, and so does expo-sqlite
# (it compiles sqlite3.c into the app). Two copies of the same global symbols in one
# link either fail as duplicates or, worse, resolve libzb's calls into expo's SQLite,
# built with other options. `ld -r -exported_symbol '_zb_*'` makes everything else
# private to libzb's object: its SQLite, zstd, nats and compiler_rt stay its own.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
libzb=$(cd "$here/../../../libzb" && pwd)

"$here/../flutter/tool/build-libzb-ios.sh"   # zig build lib + repack, both slices

for slice in ios ios-sim; do
  case $slice in
    ios) platform=ios ;;
    ios-sim) platform=ios-simulator ;;
  esac
  dir="$libzb/zig-out/$slice"
  rm -rf "$dir/prelink"; mkdir -p "$dir/prelink"
  ld -r -arch arm64 -platform_version $platform 15.1 18.0 -exported_symbol '_zb_*' \
     "$dir"/repack/*.o -o "$dir/prelink/zb.o"
  libtool -static -o "$dir/prelink/libzb.a" "$dir/prelink/zb.o"
  if nm -gU "$dir/prelink/zb.o" | grep -q ' _sqlite3_'; then
    echo "SQLite is still exported from the $slice slice" >&2; exit 1
  fi
done

out="$here/modules/zb-native/ios/ZbCore.xcframework"
rm -rf "$out"
xcodebuild -create-xcframework \
  -library "$libzb/zig-out/ios/prelink/libzb.a" \
  -library "$libzb/zig-out/ios-sim/prelink/libzb.a" \
  -output "$out"
echo "ok: $out"

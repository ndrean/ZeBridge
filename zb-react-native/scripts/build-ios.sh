#!/bin/sh
# libzb for zb-react-native: both iOS slices (device, simulator) built by Zig, each
# PRELINKED into one object that exports only `zb_*`, as ios/ZbCore.xcframework.
#
# ⚠️ The prelink is the point. libzb carries its own SQLite, and so does expo-sqlite
# (it compiles sqlite3.c into the app). Two copies of the same global symbols in one
# link either fail as duplicates or, worse, resolve libzb's calls into expo's SQLite,
# built with other options. `ld -r -exported_symbol '_zb_*'` makes everything else
# private to libzb's object: its SQLite, zstd, nats and compiler_rt stay its own.
#
# ⚠️ The xcframework is a COPY of libzb: after any libzb change, run this again and
# rebuild the app. src/index.ts refuses a copy whose ABI is not the code's (abi.json).
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
libzb=$(cd "$here/../libzb" && pwd)

python3 "$libzb/python/abi_check.py"   # the ABI this copy will report is the one the code pins

for slice in ios ios-sim; do
  case $slice in
    ios) target=aarch64-ios; sdk=iphoneos; platform=ios ;;
    ios-sim) target=aarch64-ios-simulator; sdk=iphonesimulator; platform=ios-simulator ;;
  esac
  out="$libzb/zig-out/$slice"
  (cd "$libzb" && zig build lib -Dtarget=$target -Dvendor=true -Dlibpq=false \
      --sysroot "$(xcrun --sdk $sdk --show-sdk-path)" -Doptimize=ReleaseFast -p "zig-out/$slice")
  # Zig's archive → its objects (Apple's ld wants its own archive index).
  rm -rf "$out/repack"; mkdir -p "$out/repack"
  (cd "$out/repack" && ar x ../lib/libzbcore.a && chmod 644 ./*.o)
  rm -rf "$out/prelink"; mkdir -p "$out/prelink"
  ld -r -arch arm64 -platform_version $platform 15.1 18.0 -exported_symbol '_zb_*' \
     "$out"/repack/*.o -o "$out/prelink/zb.o"
  libtool -static -o "$out/prelink/libzb.a" "$out/prelink/zb.o"
  if nm -gU "$out/prelink/zb.o" | grep -q ' _sqlite3_'; then
    echo "SQLite is still exported from the $slice slice" >&2; exit 1
  fi
done

dest="$here/ios/ZbCore.xcframework"
rm -rf "$dest"
xcodebuild -create-xcframework \
  -library "$libzb/zig-out/ios/prelink/libzb.a" \
  -library "$libzb/zig-out/ios-sim/prelink/libzb.a" \
  -output "$dest"
echo "ok: $dest (libzb ABI $(python3 -c "import json;print(json.load(open('$libzb/abi.json'))['version'])"))"

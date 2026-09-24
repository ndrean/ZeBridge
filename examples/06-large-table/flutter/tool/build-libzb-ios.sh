#!/bin/sh
# libzb for iOS: both slices, repacked for Apple's linker, as one xcframework.
#
# Four things that each cost a failed link (NOTES §10iy):
#   * `zig build lib` — the library alone; the three developer executables cannot link
#     for iOS (Zig's stack-trace code wants a dyld symbol the SDK does not export);
#   * `-Dvendor=true -Dlibpq=false` — sqlite and zstd from the pinned sources, no libpq;
#   * Zig's archiver writes members Apple's ld rejects ("not 8-byte aligned"), and with
#     mode 000 — extract, chmod, repack with Apple's libtool;
#   * the slices must carry the SAME file name for the xcframework to keep it, and the
#     Runner's xcconfig force-loads that name.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
libzb=$(cd "$here/../../../libzb" && pwd)
for slice in ios ios-sim; do
  case $slice in
    ios) target=aarch64-ios; sdk=iphoneos ;;
    ios-sim) target=aarch64-ios-simulator; sdk=iphonesimulator ;;
  esac
  (cd "$libzb" && zig build lib -Dtarget=$target -Dvendor=true -Dlibpq=false \
      --sysroot "$(xcrun --sdk $sdk --show-sdk-path)" -Doptimize=ReleaseFast -p "zig-out/$slice")
  work="$libzb/zig-out/$slice/repack"; rm -rf "$work"; mkdir -p "$work" "$libzb/zig-out/$slice/apple"
  (cd "$work" && ar x ../lib/libzbcore.a && chmod 644 ./*.o && libtool -static -o ../apple/libzbcore.a ./*.o 2>&1 | grep -v 'has no symbols' || true)
done
rm -rf "$here/ios/libzb/libzb.xcframework"
xcodebuild -create-xcframework \
  -library "$libzb/zig-out/ios/apple/libzbcore.a" \
  -library "$libzb/zig-out/ios-sim/apple/libzbcore.a" \
  -output "$here/ios/libzb/libzb.xcframework"
echo "ok: $here/ios/libzb/libzb.xcframework"

#!/bin/sh
# libzb for the pub.dev package: one dynamic library per target, in prebuilt/<target>/,
# which hook/build.dart hands to the Dart or Flutter build that asks for it.
#
#   android_arm64 android_arm android_x64        libzbcore.so    (NDK clang links Zig's archive)
#   ios_arm64 ios_sim_arm64 ios_sim_x64          libzbcore.dylib (iOS 13+)
#   macos_arm64 macos_x64                        libzbcore.dylib (macOS 13+)
#   linux_x64 linux_arm64                        libzbcore.so    (glibc 2.35+)
#
# libzb carries its own SQLite and zstd: each library needs only the system's C library.
#
#   zb-dart/scripts/build-prebuilt.sh
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
libzb=$(cd "$here/../libzb" && pwd)
python3 "$libzb/python/abi_check.py"   # the ABI the package pins is the one these carry

ndk=$(ls -d "$HOME/Library/Android/sdk/ndk"/* | sort -V | tail -1)
tc="$ndk/toolchains/llvm/prebuilt/darwin-x86_64"
api=29
rm -rf "$here/prebuilt"
cp "$here/../LICENSE" "$here/LICENSE"   # pub.dev wants it in the package

zigbuild() { # <out dir name> <zig target> [extra args...]
  out=$1; target=$2; shift 2
  (cd "$libzb" && zig build lib -Dtarget="$target" -Dvendor=true -Doptimize=ReleaseFast -p "zig-out/dart-$out" "$@")
}

android() { # <name> <zig target> <clang target> [zig cpu]
  zigbuild "$1" "$2" -Dandroid-api=$api --sysroot "$tc/sysroot" ${4:+-Dcpu=$4}
  mkdir -p "$here/prebuilt/$1"
  "$tc/bin/clang" --target="$3" -shared -o "$here/prebuilt/$1/libzbcore.so" \
    -Wl,--whole-archive "$libzb/zig-out/dart-$1/lib/libzbcore.a" -Wl,--no-whole-archive -lm -llog
}

apple() { # <name> <zig target> <sdk>
  if [ "$3" = macosx ]; then zigbuild "$1" "$2"; else zigbuild "$1" "$2" -Dstatic=false --sysroot "$(xcrun --sdk "$3" --show-sdk-path)"; fi
  mkdir -p "$here/prebuilt/$1"
  cp "$libzb/zig-out/dart-$1/lib/libzbcore.dylib" "$here/prebuilt/$1/"
}

linux() { # <name> <zig target>
  zigbuild "$1" "$2"
  mkdir -p "$here/prebuilt/$1"
  cp "$libzb/zig-out/dart-$1/lib/libzbcore.so" "$here/prebuilt/$1/"
}

android android_arm64 aarch64-linux-android aarch64-linux-android$api
android android_arm arm-linux-androideabi armv7a-linux-androideabi$api cortex_a7
android android_x64 x86_64-linux-android x86_64-linux-android$api
apple ios_arm64 aarch64-ios.13.0 iphoneos
apple ios_sim_arm64 aarch64-ios.13.0-simulator iphonesimulator
apple ios_sim_x64 x86_64-ios.13.0-simulator iphonesimulator
apple macos_arm64 aarch64-macos.13.0 macosx
apple macos_x64 x86_64-macos.13.0 macosx
linux linux_x64 x86_64-linux-gnu.2.35
linux linux_arm64 aarch64-linux-gnu.2.35

# Android and Linux keep their debug sections otherwise (~30 MB each): pub.dev caps a
# package, and Zig's ReleaseFast panics carry no trace a user could read anyway.
for f in "$here"/prebuilt/android_*/libzbcore.so "$here"/prebuilt/linux_*/libzbcore.so; do
  "$tc/bin/llvm-strip" --strip-unneeded "$f"
done

du -sh "$here/prebuilt"/*

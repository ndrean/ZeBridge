#!/bin/sh
# libzb for Android: the static archive from Zig, linked into a shared library by the
# NDK's clang — Zig cannot synthesise Android's libc (NOTES §10ir), and dart:ffi loads
# a .so. Lands in android/app/src/main/jniLibs/arm64-v8a/ (git-ignored).
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
libzb=$(cd "$here/../../../libzb" && pwd)
ndk=$(ls -d "$HOME/Library/Android/sdk/ndk"/* | tail -1)
tc="$ndk/toolchains/llvm/prebuilt/darwin-x86_64"
api=29
(cd "$libzb" && zig build lib -Dtarget=aarch64-linux-android -Dvendor=true -Dlibpq=false -Dandroid-api=$api \
    --sysroot "$tc/sysroot" -Doptimize=ReleaseFast -p zig-out/android)
mkdir -p "$libzb/zig-out/android/shared" "$here/android/app/src/main/jniLibs/arm64-v8a"
"$tc/bin/clang" --target=aarch64-linux-android$api -shared -o "$libzb/zig-out/android/shared/libzbcore.so" \
  -Wl,--whole-archive "$libzb/zig-out/android/lib/libzbcore.a" -Wl,--no-whole-archive -lm -llog
cp "$libzb/zig-out/android/shared/libzbcore.so" "$here/android/app/src/main/jniLibs/arm64-v8a/"
echo "ok: $here/android/app/src/main/jniLibs/arm64-v8a/libzbcore.so"

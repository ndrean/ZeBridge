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
# Both ABIs: a 2 GB Android Go phone (the moto e20 at hand) may run a 32-bit userspace,
# and Android then loads the armeabi-v7a Flutter engine — and finds no library.
build() { # <zig target> <clang target> <jniLibs abi>
  (cd "$libzb" && zig build lib -Dtarget="$1" -Dvendor=true -Dlibpq=false -Dandroid-api=$api \
      --sysroot "$tc/sysroot" -Doptimize=ReleaseFast -p "zig-out/android-$3")
  mkdir -p "$libzb/zig-out/android-$3/shared" "$here/android/app/src/main/jniLibs/$3"
  "$tc/bin/clang" --target="$2" -shared -o "$libzb/zig-out/android-$3/shared/libzbcore.so" \
    -Wl,--whole-archive "$libzb/zig-out/android-$3/lib/libzbcore.a" -Wl,--no-whole-archive -lm -llog
  cp "$libzb/zig-out/android-$3/shared/libzbcore.so" "$here/android/app/src/main/jniLibs/$3/"
  echo "ok: $here/android/app/src/main/jniLibs/$3/libzbcore.so"
}
build aarch64-linux-android aarch64-linux-android$api arm64-v8a
# armeabi-v7a is NOT built: libzb and nats.zig use 64-bit atomics (std.atomic.Value(u64)
# has no 32-bit ARM lowering in Zig's std) and nuid.zig mixes usize with u64 — six
# compile errors on 2026-09-24. A 32-bit userspace (Android Go on 2 GB phones) needs a
# 32-bit-clean libzb first; until then the app there starts its 32-bit Flutter engine
# and finds no library. `adb shell getprop ro.product.cpu.abilist` says which a phone is.
# build arm-linux-androideabi armv7a-linux-androideabi$api armeabi-v7a

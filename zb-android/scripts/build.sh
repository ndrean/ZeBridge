#!/bin/sh
# Build the Android AAR: libzb for each CPU, the JNI layer linked into it, then Gradle.
#
#   zb-android/scripts/build.sh      → zb-android/build/outputs/aar/zb-android-release.aar
#
# Needs: zig, the Android NDK (newest under ~/Library/Android/sdk/ndk, or $ANDROID_NDK),
# the SDK (sdk.dir in local.properties, written here from $ANDROID_HOME), and a JDK
# ($JAVA_HOME, else Android Studio's own).
#
# Per CPU: `zig build lib` makes libzb's static archive (SQLite and zstd compiled in,
# DISTRIBUTION.md), the NDK's clang compiles src/main/cpp/zb_jni.c and links both into
# ONE libzb.so. `--exclude-libs,ALL` keeps libzb's own symbols private to that file, so
# only the JNI functions are exported and the linker drops what nothing calls.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
libzb=$(cd "$here/../libzb" && pwd)
sdk=${ANDROID_HOME:-$HOME/Library/Android/sdk}
ndk=${ANDROID_NDK:-$(ls -d "$sdk/ndk"/* | tail -1)}
tc="$ndk/toolchains/llvm/prebuilt/darwin-x86_64"
api=29

python3 "$libzb/python/abi_check.py"   # the ABI this copy reports is the one Kotlin pins

build() { # <zig target> <clang target> <android abi> [zig cpu]
  out="$libzb/zig-out/android-$3"
  (cd "$libzb" && zig build lib -Dtarget="$1" ${4:+-Dcpu=$4} -Dvendor=true -Dandroid-api=$api \
      --sysroot "$tc/sysroot" -Doptimize=ReleaseFast -p "zig-out/android-$3")
  mkdir -p "$here/src/main/jniLibs/$3"
  "$tc/bin/clang" --target="$2" -shared -fPIC -O2 -o "$here/src/main/jniLibs/$3/libzb.so" \
    "$here/src/main/cpp/zb_jni.c" "$out/lib/libzbcore.a" \
    -Wl,--exclude-libs,ALL -Wl,--gc-sections -Wl,-z,max-page-size=16384 -s -lm -llog -ldl
  echo "ok: $3 ($(wc -c < "$here/src/main/jniLibs/$3/libzb.so" | tr -d ' ') bytes)"
}
build aarch64-linux-android aarch64-linux-android$api arm64-v8a
build arm-linux-androideabi armv7a-linux-androideabi$api armeabi-v7a cortex_a7
build x86_64-linux-android x86_64-linux-android$api x86_64

echo "sdk.dir=$sdk" > "$here/local.properties"
export JAVA_HOME="${JAVA_HOME:-/Applications/Android Studio.app/Contents/jbr/Contents/Home}"
(cd "$here" && gradle --quiet assembleRelease)
echo "ok: $here/build/outputs/aar/zb-android-release.aar"

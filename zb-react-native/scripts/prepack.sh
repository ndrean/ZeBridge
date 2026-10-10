#!/bin/sh
# Before `pnpm pack` / `pnpm publish`: the package carries everything an app needs.
#
# In this repository, android/build.gradle reads zb-android's Kotlin and libzb.so files in
# place, outside this folder. A published package has only its own folder, so they are
# copied into android/zb-android/ here (build.gradle prefers that copy when it exists) and
# removed again after packing (scripts/postpack.sh), so the examples keep reading the
# repository's own.
#
# Nothing is built here: run scripts/build-ios.sh and scripts/build-android.sh first.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
src="$here/../zb-android/src/main"

missing=""
[ -d "$here/ios/ZbCore.xcframework" ] || missing="$missing ios/ZbCore.xcframework (scripts/build-ios.sh)"
[ -f "$here/ios/zb.h" ] || missing="$missing ios/zb.h (scripts/build-ios.sh)"
for abi in arm64-v8a armeabi-v7a x86_64; do
  [ -f "$src/jniLibs/$abi/libzb.so" ] || missing="$missing jniLibs/$abi/libzb.so (scripts/build-android.sh)"
done
[ -z "$missing" ] || { echo "prepack: missing:$missing" >&2; exit 1; }

rm -rf "$here/android/zb-android"
mkdir -p "$here/android/zb-android"
cp -R "$src/kotlin" "$src/jniLibs" "$here/android/zb-android/"
cp "$here/../LICENSE" "$here/LICENSE"
echo "prepack: android/zb-android/ (Kotlin + libzb.so for 3 CPUs), ios/ZbCore.xcframework, LICENSE"

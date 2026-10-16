#!/bin/sh
# The Android NDK every CI build of libzb uses, installed when the runner lacks it; prints
# its path:   export ANDROID_NDK="$(scripts/android-ndk.sh)"
#
# Pinned to the release machine's NDK, not the runner's newest: a release must not change
# compiler when GitHub updates its images. NDK 30 refuses an Android target that carries no
# API level ("Unversioned target triples are not supported!"), and Zig's target does not
# pass one to Clang yet; moving past 28 waits for that.
set -eu
v=28.2.13676358
sdk=${ANDROID_HOME:-${ANDROID_SDK_ROOT:?neither ANDROID_HOME nor ANDROID_SDK_ROOT is set}}
d="$sdk/ndk/$v"
if [ ! -d "$d" ]; then
  yes | "$sdk/cmdline-tools/latest/bin/sdkmanager" --install "ndk;$v" >/dev/null
fi
[ -d "$d/toolchains/llvm/prebuilt" ] || { echo "NDK $v not found in $d" >&2; exit 1; }
echo "$d"

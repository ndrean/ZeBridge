#!/bin/sh
# libzb for zb-react-native on Android: zb-android's binding, its libzb.so per CPU, which
# android/build.gradle takes from zb-android/src/main/jniLibs. libzb carries its own TLS roots.
#
# ⚠️ The .so files are a COPY of libzb: after any libzb change, run this again and rebuild
# the app. src/index.ts refuses a copy whose ABI is not the code's (abi.json).
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
"$here/../zb-android/scripts/build.sh"

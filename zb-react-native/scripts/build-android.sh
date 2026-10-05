#!/bin/sh
# libzb for zb-react-native on Android: zb-android's binding (its libzb.so per CPU, which
# android/build.gradle takes from zb-android/src/main/jniLibs), and the TLS roots libzb checks
# https:// and tls:// against — Zig reads no trust store on Android. The roots are this Mac's
# system roots, Apple's, a set Android's own store matches for the public CAs.
#
# ⚠️ The .so files are a COPY of libzb: after any libzb change, run this again and rebuild
# the app. src/index.ts refuses a copy whose ABI is not the code's (abi.json).
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
"$here/../zb-android/scripts/build.sh"
mkdir -p "$here/android/src/main/assets"
security find-certificate -a -p /System/Library/Keychains/SystemRootCertificates.keychain > "$here/android/src/main/assets/zb-roots.pem"
echo "ok: $(grep -c 'BEGIN CERTIFICATE' "$here/android/src/main/assets/zb-roots.pem") roots in android/src/main/assets/zb-roots.pem"

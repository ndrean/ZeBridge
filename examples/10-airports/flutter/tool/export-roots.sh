#!/bin/sh
# The trusted root certificates the app ships as `caFile`: Zig reads no trust store on
# iOS, so libzb verifies https:// and tls:// against this file. Exported from this
# Mac's system roots, which are Apple's, the same set iOS trusts.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
security find-certificate -a -p /System/Library/Keychains/SystemRootCertificates.keychain > "$here/assets/roots.pem"
echo "ok: $(grep -c 'BEGIN CERTIFICATE' "$here/assets/roots.pem") roots in $here/assets/roots.pem"

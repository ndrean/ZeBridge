#!/bin/sh
# Install the release on clean Debian and Ubuntu containers and check it runs:
#   apt installs both .deb (and their dependencies), every binary prints its version,
#   no library is missing, Python loads libzbcore.so, a C program builds against zb.h,
#   and the tarball works on its own.
#
#   deploy/test-linux.sh                 # every image, both architectures
#   deploy/test-linux.sh arm64           # one architecture
#   IMAGES="debian:12" deploy/test-linux.sh amd64
#
# amd64 runs under emulation on an Apple Silicon Mac: slower, same result.
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
version=$(sed -n 's/^ *\.version = "\(.*\)",/\1/p' "$root/build.zig.zon")
dist="$root/dist/$version"
archs=${1:-"arm64 amd64"}
images=${IMAGES:-"debian:12 debian:13 ubuntu:22.04 ubuntu:24.04"}
[ -f "$dist/SHA256SUMS" ] || { echo "no $dist/SHA256SUMS: run deploy/package-linux.sh first" >&2; exit 1; }
(cd "$dist" && shasum -a 256 -c SHA256SUMS >/dev/null) || { echo "checksums do not match" >&2; exit 1; }

failed=0
for a in $archs; do
  case $a in amd64) arch=x86_64 ;; arm64) arch=aarch64 ;; *) echo "arch: amd64 or arm64" >&2; exit 1 ;; esac
  for img in $images; do
    printf '%-14s %-6s ' "$img" "$a"
    if out=$(docker run --rm --platform "linux/$a" -v "$dist":/dist:ro "$img" sh -eu -c "
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -qq >/dev/null
      apt-get install -y -qq /dist/zebridge_${version}_$a.deb /dist/libzb_${version}_$a.deb python3 gcc libc6-dev >/dev/null 2>&1
      test \"\$(bridge --version)\" = 'bridge $version'
      test \"\$(bridge_sweeper --version)\" = 'bridge_sweeper $version'
      test \"\$(zb-respond --version)\" = 'zb-respond $version'
      for f in /usr/bin/bridge /usr/bin/bridge_sweeper /usr/bin/zb-respond /usr/lib/$arch-linux-gnu/libzbcore.so; do
        if ldd \$f | grep -q 'not found'; then echo \"missing library for \$f\"; ldd \$f; exit 1; fi
      done
      bridge --help | grep -q -- '--diagnose'
      python3 -c '
import ctypes, json
z = ctypes.CDLL(\"libzbcore.so\")
assert z.zb_abi_version() == 6, z.zb_abi_version()
z.zb_call.restype = ctypes.c_void_p
z.zb_call.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
z.zb_free.argtypes = [ctypes.c_void_p]
a = {\"x\": {\"v\": 1, \"t\": \"2026-01-01T00:00:00Z\", \"w\": \"a\"}}
b = {\"x\": {\"v\": 2, \"t\": \"2026-01-02T00:00:00Z\", \"w\": \"b\"}}
p = z.zb_call(b\"mergeRegisters\", json.dumps({\"a\": a, \"b\": b}).encode())
out = json.loads(ctypes.string_at(p)); z.zb_free(p)
assert out[\"x\"][\"v\"] == 2, out
'
      cat > /tmp/smoke.c <<'EOF'
#include <stdio.h>
#include <zb.h>
int main(void) {
    if (zb_abi_version() != ZB_ABI_VERSION) return 1;
    char *h = zb_grammar_hash(); if (!h) return 2; zb_free(h);
    return 0;
}
EOF
      gcc -Wall -Wextra -o /tmp/smoke /tmp/smoke.c -lzbcore && /tmp/smoke
      # the tarball, on its own
      mkdir /tmp/t && cd /tmp/t
      tar -xzf /dist/zebridge-$version-linux-$arch.tar.gz && tar -xzf /dist/libzb-$version-linux-$arch.tar.gz
      test \"\$(./zebridge-$version-linux-$arch/bin/bridge --version)\" = 'bridge $version'
      gcc -o smoke2 /tmp/smoke.c -Ilibzb-$version-linux-$arch/include -Llibzb-$version-linux-$arch/lib -lzbcore -Wl,-rpath,\"\\\$ORIGIN/libzb-$version-linux-$arch/lib\" && ./smoke2
      cat /etc/os-release | sed -n 's/^PRETTY_NAME=//p' | tr -d '\"'
    " 2>&1); then
      echo "ok: $(echo "$out" | tail -1)"
    else
      failed=$((failed + 1)); echo "FAILED"; echo "$out" | tail -15 | sed 's/^/    /'
    fi
  done
done
exit $failed

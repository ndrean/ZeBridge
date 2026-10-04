#!/bin/sh
# The Linux builds a server needs, from a Mac (or any Docker host):
#   zig-out/linux-<arch>/bin/{bridge,bridge_sweeper}   — link the system's libpq and zstd
#   libzb/zig-out/linux-<arch>/lib/libzbcore.so        — SQLite and zstd compiled in
#
#   deploy/build-linux.sh            # x86_64 (default)
#   deploy/build-linux.sh aarch64    # an ARM server (Ampere)
#
# The bridge links libpq and zstd: the container supplies their headers and link names, for
# the target CPU, from Debian's packages (multiarch, one mirror for every architecture).
# The binary's own floor is Zig's target, glibc 2.35: it runs on Ubuntu 22.04+ and Debian
# 12+, with the server's libpq5 (14 or later: pipeline mode) and libzstd1. The container is
# arm64 and cross-compiles: an amd64 container under Rosetta crashes Zig's translate-c
# ("bss_size overflow"). libzb vendors its C, so it cross-compiles with no container.
set -eu
arch=${1:-x86_64}
case $arch in
  x86_64) deb=amd64 ;;
  aarch64) deb=arm64 ;;
  *) echo "arch: x86_64 or aarch64" >&2; exit 1 ;;
esac
root=$(cd "$(dirname "$0")/.." && pwd)
cache=${ZB_LINUX_CACHE:-$HOME/.cache/zebridge-linux-build}
mkdir -p "$cache"

docker run --rm --platform linux/arm64 -v "$root":/src -v "$cache":/cache debian:13-slim sh -eu -c "
export DEBIAN_FRONTEND=noninteractive
dpkg --add-architecture $deb
apt-get update -qq >/dev/null
apt-get install -y -qq curl xz-utils ca-certificates libpq-dev:$deb libzstd-dev:$deb >/dev/null
ln -sf /usr/lib/$arch-linux-gnu/libpq.so /usr/lib/libpq.so
ln -sf /usr/lib/$arch-linux-gnu/libzstd.so /usr/lib/libzstd.so
if [ ! -x /cache/zig/zig ]; then
  curl -sL https://ziglang.org/download/0.16.0/zig-aarch64-linux-0.16.0.tar.xz | tar -xJ -C /cache
  mv /cache/zig-aarch64-linux-0.16.0 /cache/zig
fi
cd /src
/cache/zig/zig build bridge sweeper -Dtarget=$arch-linux-gnu.2.35 -Doptimize=ReleaseSafe \
  -p /src/zig-out/linux-$arch --cache-dir /cache/local-$arch --global-cache-dir /cache/global
"

(cd "$root/libzb" && zig build lib -Dtarget="$arch-linux-gnu.2.35" -Dvendor=true -Doptimize=ReleaseFast -p "zig-out/linux-$arch")

ls -la "$root/zig-out/linux-$arch/bin" "$root/libzb/zig-out/linux-$arch/lib"

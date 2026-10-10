#!/bin/sh
# The Linux release, Debian and Ubuntu, from a Mac (or any Docker host):
#
#   dist/<version>/zebridge-<version>-linux-<arch>.tar.gz   bridge, bridge_sweeper, example units
#   dist/<version>/libzb-<version>-linux-<arch>.tar.gz      libzbcore.so, zb.h, zb-respond
#   dist/<version>/zebridge_<version>_<debarch>.deb         the same, installed under /usr
#   dist/<version>/libzb_<version>_<debarch>.deb
#   dist/<version>/SHA256SUMS
#
#   deploy/package-linux.sh            # x86_64 (default)
#   deploy/package-linux.sh aarch64    # an ARM server
#   deploy/package-linux.sh x86_64 --no-build   # package what deploy/build-linux.sh left
#
# The version is build.zig.zon's. The binaries need glibc 2.35 (Ubuntu 22.04+, Debian 12+);
# the bridge and the sweeper also need libpq 14 or later and libzstd, which the .deb
# declares and apt installs.
set -eu
arch=${1:-x86_64}
case $arch in
  x86_64) deb=amd64 ;;
  aarch64) deb=arm64 ;;
  *) echo "arch: x86_64 or aarch64" >&2; exit 1 ;;
esac
root=$(cd "$(dirname "$0")/.." && pwd)
version=$(sed -n 's/^ *\.version = "\(.*\)",/\1/p' "$root/build.zig.zon")
libzb_version=$(sed -n 's/^ *\.version = "\(.*\)",/\1/p' "$root/libzb/build.zig.zon")
[ "$version" = "$libzb_version" ] || { echo "build.zig.zon says $version, libzb/build.zig.zon $libzb_version" >&2; exit 1; }

[ "${2:-}" = "--no-build" ] || "$root/deploy/build-linux.sh" "$arch"
python3 "$root/libzb/scripts/gen-header.py" --check

bin="$root/zig-out/linux-$arch/bin"
lib="$root/libzb/zig-out/linux-$arch"
out="$root/dist/$version"
stage="$out/stage-$arch"
rm -rf "$stage"
mkdir -p "$stage" "$out"

unit() { # name, binary, after, restart seconds
  cat <<EOF
[Unit]
Description=ZeBridge: $1
After=$3
Wants=network-online.target

[Service]
User=zebridge
# --init-nats writes .env.nats; the database URLs, slot and publication go in .env.bridge.
EnvironmentFile=/etc/zebridge/.env.nats
EnvironmentFile=/etc/zebridge/.env.bridge
ExecStart=$2
Restart=on-failure
RestartSec=$4

[Install]
WantedBy=multi-user.target
EOF
}

# ── zebridge: the bridge and the sweeper ──────────────────────────────────────────
z="$stage/zebridge-$version-linux-$arch"
mkdir -p "$z/bin" "$z/examples"
cp "$bin/bridge" "$bin/bridge_sweeper" "$z/bin/"
cp "$root/LICENSE" "$z/"
unit "PostgreSQL to NATS" /usr/bin/bridge "network-online.target nats.service" 5 > "$z/examples/zebridge.service"
unit "tombstone sweeper" /usr/bin/bridge_sweeper "zebridge.service" 30 > "$z/examples/zebridge-sweeper.service"
cat > "$z/README.txt" <<EOF
ZeBridge $version for Linux ($arch): the bridge and the sweeper.

Needs glibc 2.35 or later (Ubuntu 22.04+, Debian 12+), libpq 14 or later and libzstd:
    sudo apt install libpq5 libzstd1
The .deb package installs these for you.

    bin/bridge --version
    bin/bridge --help

Setting up a server: https://github.com/ndrean/zebridge/blob/main/DEPLOYMENT.md
The examples/ units expect the binaries in /usr/bin (where the .deb puts them), a
"zebridge" user, and the two env files in /etc/zebridge.
EOF

# ── libzb: the client library, its header, the native responder ─────────────────
l="$stage/libzb-$version-linux-$arch"
mkdir -p "$l/lib" "$l/include" "$l/bin"
cp "$lib/lib/libzbcore.so" "$l/lib/"
cp "$root/libzb/include/zb.h" "$l/include/"
cp "$lib/bin/zb-respond" "$l/bin/"
cp "$root/LICENSE" "$l/"
cat > "$l/README.txt" <<EOF
libzb $version for Linux ($arch): the ZeBridge client as a C library (ABI 6).

    lib/libzbcore.so    SQLite and zstd compiled in; needs only glibc 2.35+
    include/zb.h        the C declarations; every call takes and returns JSON
    bin/zb-respond      the native responder (zb-respond config.json)

Optional engines load at run time if installed: libpq (PostgreSQL replica), libduckdb.

    cc app.c -Iinclude -Llib -lzbcore -Wl,-rpath,'\$ORIGIN/lib'

From Python: ctypes.CDLL("lib/libzbcore.so"). The API: https://github.com/ndrean/zebridge/blob/main/CLIENTS.md
EOF

# ── tarballs ─────────────────────────────────────────────────────────────────────
for d in "$z" "$l"; do
  tar -C "$stage" -czf "$out/$(basename "$d").tar.gz" "$(basename "$d")"
done

# ── .deb, built with dpkg-deb in a Debian container ──────────────────────────────
triplet="$arch-linux-gnu"
zd="$stage/deb-zebridge"
mkdir -p "$zd/DEBIAN" "$zd/usr/bin" "$zd/usr/share/doc/zebridge/examples"
cp "$z/bin/bridge" "$z/bin/bridge_sweeper" "$zd/usr/bin/"
cp "$z/examples/"*.service "$zd/usr/share/doc/zebridge/examples/"
cp "$root/LICENSE" "$zd/usr/share/doc/zebridge/copyright"
cp "$z/README.txt" "$zd/usr/share/doc/zebridge/"
cat > "$zd/DEBIAN/control" <<EOF
Package: zebridge
Version: $version
Architecture: $deb
Maintainer: ndrean <dreanneven@gmail.com>
Depends: libc6 (>= 2.35), libpq5 (>= 14), libzstd1
Section: database
Priority: optional
Homepage: https://github.com/ndrean/zebridge
Description: PostgreSQL to NATS JetStream bridge, for offline-first replicas
 The bridge streams PostgreSQL logical replication to NATS JetStream and applies
 the clients' writes back to PostgreSQL. bridge_sweeper reaps old tombstones.
 Example systemd units are in /usr/share/doc/zebridge/examples.
EOF

ld="$stage/deb-libzb"
mkdir -p "$ld/DEBIAN" "$ld/usr/lib/$triplet" "$ld/usr/include" "$ld/usr/bin" "$ld/usr/share/doc/libzb"
cp "$l/lib/libzbcore.so" "$ld/usr/lib/$triplet/"
cp "$l/include/zb.h" "$ld/usr/include/"
cp "$l/bin/zb-respond" "$ld/usr/bin/"
cp "$root/LICENSE" "$ld/usr/share/doc/libzb/copyright"
cp "$l/README.txt" "$ld/usr/share/doc/libzb/"
cat > "$ld/DEBIAN/control" <<EOF
Package: libzb
Version: $version
Architecture: $deb
Maintainer: ndrean <dreanneven@gmail.com>
Depends: libc6 (>= 2.35)
Suggests: libpq5
Section: libs
Priority: optional
Homepage: https://github.com/ndrean/zebridge
Description: ZeBridge client library: an offline-first replica over NATS
 libzbcore.so keeps a local SQLite replica of PostgreSQL tables in sync over NATS
 and sends writes back. C header in /usr/include/zb.h; the native responder is
 zb-respond.
EOF
printf 'activate-noawait ldconfig\n' > "$ld/DEBIAN/triggers"

docker run --rm --platform linux/arm64 -v "$out":/out debian:13-slim sh -eu -c "
  dpkg-deb --root-owner-group -Zxz --build /out/stage-$arch/deb-zebridge /out/zebridge_${version}_$deb.deb >/dev/null
  dpkg-deb --root-owner-group -Zxz --build /out/stage-$arch/deb-libzb /out/libzb_${version}_$deb.deb >/dev/null
"
rm -rf "$stage"

(cd "$out" && shasum -a 256 ./*.tar.gz ./*.deb | sed 's| \./| |' > SHA256SUMS)
ls -la "$out"

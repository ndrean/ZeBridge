#!/bin/sh
# libzb for macOS, as release downloads next to the Linux ones (deploy/package-linux.sh):
#
#   dist/<version>/libzb-<version>-macos-<arch>.tar.gz    libzbcore.dylib (macOS 13+), zb.h
#
# Apple silicon (arm64) and Intel (x86_64). Then SHA256SUMS is rewritten over every file
# in dist/<version>/.
#
#   deploy/package-macos.sh
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
version=$(sed -n 's/^ *\.version = "\(.*\)",/\1/p' "$root/build.zig.zon")
out="$root/dist/$version"
stage="$out/stage-macos"
python3 "$root/libzb/scripts/gen-header.py" --check
rm -rf "$stage"; mkdir -p "$stage" "$out"

for spec in "aarch64 arm64" "x86_64 x86_64"; do
  set -- $spec
  zarch=$1; arch=$2
  (cd "$root/libzb" && zig build lib -Dtarget="$zarch-macos.13.0" -Dvendor=true -Doptimize=ReleaseFast -p "zig-out/macos-$arch")
  l="$stage/libzb-$version-macos-$arch"
  mkdir -p "$l/lib" "$l/include"
  cp "$root/libzb/zig-out/macos-$arch/lib/libzbcore.dylib" "$l/lib/"
  cp "$root/libzb/include/zb.h" "$l/include/"
  cp "$root/LICENSE" "$l/"
  cat > "$l/README.txt" <<EOF
libzb $version for macOS ($arch, macOS 13+): the ZeBridge client as a C library (ABI 6).

    lib/libzbcore.dylib  SQLite and zstd compiled in; needs nothing else
    include/zb.h         the C declarations; every call takes and returns JSON

Optional engines load at run time if installed: libpq (PostgreSQL replica), libduckdb.

    cc app.c -Iinclude -Llib -lzbcore -Wl,-rpath,@loader_path/lib

From any language with a C interface: load lib/libzbcore.dylib and call the functions in
zb.h. The API: https://github.com/ndrean/zebridge/blob/main/CLIENTS.md
EOF
  tar -C "$stage" -czf "$out/libzb-$version-macos-$arch.tar.gz" "libzb-$version-macos-$arch"
done
rm -rf "$stage"

(cd "$out" && shasum -a 256 ./*.tar.gz ./*.deb | sed 's| \./| |' > SHA256SUMS)
ls -la "$out"

#!/bin/sh
# The PyPI wheels, one per platform, each carrying its own libzb in zebridge/lib/:
#
#   zb-python/dist/zebridge-<version>-py3-none-<platform>.whl
#
# Linux x86_64 and aarch64 (glibc 2.35+, manylinux_2_35) and macOS arm64 and x86_64
# (13+). Zig cross-compiles all four from a Mac; libzb carries its own SQLite and zstd, so
# each library needs only the C library. No Windows wheel yet, and no source distribution:
# without libzb, the package cannot load.
#
#   zb-python/scripts/build-wheels.sh
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
libzb=$(cd "$here/../libzb" && pwd)

python3 "$libzb/python/abi_check.py"   # the ABI the wheels carry is the one the package pins

rm -rf "$here/dist"
cp "$here/../LICENSE" "$here/LICENSE"
for spec in \
  "x86_64-linux-gnu.2.35 manylinux_2_35_x86_64 libzbcore.so" \
  "aarch64-linux-gnu.2.35 manylinux_2_35_aarch64 libzbcore.so" \
  "aarch64-macos.13.0 macosx_13_0_arm64 libzbcore.dylib" \
  "x86_64-macos.13.0 macosx_13_0_x86_64 libzbcore.dylib"; do
  set -- $spec
  target=$1; tag=$2; lib=$3
  out="zig-out/wheel-$tag"
  (cd "$libzb" && zig build lib -Dtarget="$target" -Dvendor=true -Doptimize=ReleaseFast -p "$out")
  rm -rf "$here/src/zebridge/lib" "$here/build" "$here/src/zebridge.egg-info"
  mkdir -p "$here/src/zebridge/lib"
  cp "$libzb/$out/lib/$lib" "$here/src/zebridge/lib/"
  (cd "$here" && ZB_WHEEL_PLATFORM="$tag" uv build --wheel --out-dir dist >/dev/null)
  echo "built $tag"
done
rm -rf "$here/src/zebridge/lib" "$here/build" "$here/src/zebridge.egg-info" "$here/LICENSE"
ls -la "$here/dist"

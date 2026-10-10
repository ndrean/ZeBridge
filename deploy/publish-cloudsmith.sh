#!/bin/sh
# Publish dist/<version>/'s .deb packages to a Cloudsmith Debian repository.
#
#   deploy/publish-cloudsmith.sh <workspace> [repository]    # repository: zebridge
#
# Needs the Cloudsmith CLI (`uv tool install cloudsmith-cli`) and an API key in
# ~/.cloudsmith/credentials.ini or CLOUDSMITH_API_KEY. One upload per file serves every
# Debian and Ubuntu release (any-distro/any-version): the packages declare
# libc6 (>= 2.35), so apt refuses them where the binaries cannot run.
set -eu
ws=${1:?usage: deploy/publish-cloudsmith.sh <workspace> [repository]}
repo=${2:-zebridge}
root=$(cd "$(dirname "$0")/.." && pwd)
version=$(sed -n 's/^ *\.version = "\(.*\)",/\1/p' "$root/build.zig.zon")
dist="$root/dist/$version"

[ -f "$dist/SHA256SUMS" ] || { echo "no $dist/SHA256SUMS: run deploy/package-linux.sh first" >&2; exit 1; }
(cd "$dist" && shasum -a 256 -c SHA256SUMS >/dev/null) || { echo "checksums do not match" >&2; exit 1; }
cloudsmith whoami >/dev/null 2>&1 || { echo "the Cloudsmith CLI is not authenticated: see this script's header" >&2; exit 1; }

for deb in "$dist"/*.deb; do
  echo "→ $(basename "$deb")"
  cloudsmith push deb "$ws/$repo/any-distro/any-version" "$deb"
done

cat <<EOF

Published $version to $ws/$repo. On a Debian or Ubuntu machine:

  curl -sLf 'https://dl.cloudsmith.io/public/$ws/$repo/cfg/setup/bash.deb.sh' | sudo bash
  sudo apt install zebridge libzb
EOF

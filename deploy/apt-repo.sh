#!/bin/sh
# The APT repository as static files, in dist/apt/, for any static host. packages.zebridge.eu
# is built and deployed by .github/workflows/apt.yml when a release is published; this script
# is what that workflow runs, and the manual way on a release machine.
#
#   deploy/apt-repo.sh                      # adds dist/<version>/'s .deb, re-indexes, signs
#   ZB_APT_KEY=<key id> deploy/apt-repo.sh  # another signing key
#   ZB_APT_URL=https://… deploy/apt-repo.sh # the address written in index.html
#
# For CI (apt.yml):
#   ZB_APT_POOL_READY=1    dist/apt/pool/main already holds every release's .deb: no copy
#   ZB_APT_VERSION=x.y.z   the "latest" index.html names (default: build.zig.zon's)
#   ZB_APT_GPG_OPTS=…      extra gpg options, e.g. a passphrase file for a non-interactive sign
#   ZB_APT_DIR=…           where the repository is built (default dist/apt)
#
# Every release adds its packages to pool/, so apt can still install an older one. The
# indexes are made by apt-ftparchive (directly when installed, else in a Debian container)
# and signed with the key named "ZeBridge packages": InRelease and Release.gpg, plus the
# public key, zebridge.gpg, which users install once.
set -eu
key=${ZB_APT_KEY:-"ZeBridge packages"}
url=${ZB_APT_URL:-https://packages.zebridge.eu}
root=$(cd "$(dirname "$0")/.." && pwd)
version=${ZB_APT_VERSION:-$(sed -n 's/^ *\.version = "\(.*\)",/\1/p' "$root/build.zig.zon")}
dist="$root/dist/$version"
apt=${ZB_APT_DIR:-$root/dist/apt}
gpg_opts=${ZB_APT_GPG_OPTS:-}

gpg --list-secret-keys "$key" >/dev/null 2>&1 || { echo "no secret key \"$key\": create it (see DISTRIBUTION.md)" >&2; exit 1; }
if [ "${ZB_APT_POOL_READY:-}" != 1 ]; then
  # The release's own files (built by .github/workflows/release.yml), so apt serves exactly
  # what the GitHub release and the image hold. Start from an EMPTY dist/<version>: gh does
  # not overwrite, and a leftover local build would be signed instead.
  [ -f "$dist/SHA256SUMS" ] || { echo "no $dist/SHA256SUMS: rm -rf dist/$version; gh release download v$version --dir dist/$version (or deploy/package-linux.sh for a local build)" >&2; exit 1; }
  (cd "$dist" && shasum -a 256 -c SHA256SUMS >/dev/null) || { echo "checksums do not match" >&2; exit 1; }
  mkdir -p "$apt/pool/main"
  cp "$dist"/*.deb "$apt/pool/main/"
fi
ls "$apt"/pool/main/*.deb >/dev/null 2>&1 || { echo "no .deb in $apt/pool/main" >&2; exit 1; }

# The indexes: one Packages file per architecture, and the Release file over them.
index='
  for arch in amd64 arm64; do
    mkdir -p dists/stable/main/binary-$arch
    apt-ftparchive --arch $arch packages pool > dists/stable/main/binary-$arch/Packages
    gzip -9kf dists/stable/main/binary-$arch/Packages
  done
  apt-ftparchive \
    -o APT::FTPArchive::Release::Origin=ZeBridge \
    -o APT::FTPArchive::Release::Label=ZeBridge \
    -o APT::FTPArchive::Release::Suite=stable \
    -o APT::FTPArchive::Release::Codename=stable \
    -o "APT::FTPArchive::Release::Architectures=amd64 arm64" \
    -o APT::FTPArchive::Release::Components=main \
    -o "APT::FTPArchive::Release::Description=ZeBridge: the bridge and libzb" \
    release dists/stable > Release.tmp
  mv Release.tmp dists/stable/Release
'
if command -v apt-ftparchive >/dev/null 2>&1; then
  (cd "$apt" && sh -eu -c "$index")
else
  docker run --rm --platform linux/arm64 -v "$apt":/repo debian:13-slim sh -eu -c "
    apt-get update -qq >/dev/null && apt-get install -y -qq apt-utils >/dev/null 2>&1
    cd /repo
    $index"
fi

# The signatures: on the release machine, or in CI with the key from its secrets.
rm -f "$apt/dists/stable/InRelease" "$apt/dists/stable/Release.gpg"
# shellcheck disable=SC2086
gpg --batch --yes $gpg_opts --local-user "$key" --clearsign -o "$apt/dists/stable/InRelease" "$apt/dists/stable/Release"
# shellcheck disable=SC2086
gpg --batch --yes $gpg_opts --local-user "$key" --armor --detach-sign -o "$apt/dists/stable/Release.gpg" "$apt/dists/stable/Release"
gpg --export "$key" > "$apt/zebridge.gpg"
fingerprint=$(gpg --with-colons --fingerprint "$key" | awk -F: '/^fpr/ {print $10; exit}')

cat > "$apt/index.html" <<EOF
<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>ZeBridge packages</title>
<style>
  :root { --bg: #fff; --fg: #1d1d1f; --muted: #6e6e73; --code: #f5f5f7; }
  @media (prefers-color-scheme: dark) { :root { --bg: #111; --fg: #f5f5f7; --muted: #a1a1a6; --code: #1d1d1f; } }
  body { background: var(--bg); color: var(--fg); font: 16px/1.5 system-ui, sans-serif; max-width: 46rem; margin: 3rem auto; padding: 0 1rem; }
  pre { background: var(--code); padding: 1rem; overflow-x: auto; border-radius: 6px; }
  small { color: var(--muted); }
</style></head>
<body>
<h1>ZeBridge packages</h1>
<p>Debian 12+ and Ubuntu 22.04+, amd64 and arm64. Latest: $version.</p>
<pre>curl -fsSL $url/zebridge.gpg | sudo tee /usr/share/keyrings/zebridge.gpg &gt;/dev/null
echo "deb [signed-by=/usr/share/keyrings/zebridge.gpg] $url stable main" \\
  | sudo tee /etc/apt/sources.list.d/zebridge.list
sudo apt update
sudo apt install zebridge libzb</pre>
<p><code>zebridge</code>: the bridge and the sweeper. <code>libzb</code>: the client library, its C header and <code>zb-respond</code>.</p>
<p><small>Signing key fingerprint: $fingerprint</small></p>
<p><a href="https://github.com/ndrean/zebridge">github.com/ndrean/zebridge</a></p>
</body>
</html>
EOF

echo "dist/apt/ is ready: upload the whole folder. Packages:"
grep -h '^Package:\|^Version:\|^Architecture:' "$apt"/dists/stable/main/binary-*/Packages | paste - - - | sort -u

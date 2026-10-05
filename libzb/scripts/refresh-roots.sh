#!/bin/sh
# The trusted roots libzb's iOS and Android builds carry (src/roots.zig): Mozilla's CA
# bundle, as curl publishes it. Zig reads no trust store on iOS or Android, so libzb checks
# https:// and tls:// against these when the app names no `caFile`.
#
# Run by hand before a release; the build warns when the bundle is over six months old.
# Downloaded, checked against curl's published SHA-256, and dated (Mozilla's own date, from
# the file's header) for that warning. Review the diff and commit it like any change.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
dir="$here/src/roots"
mkdir -p "$dir"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

curl -fsSL https://curl.se/ca/cacert.pem -o "$tmp/bundle"
curl -fsSL https://curl.se/ca/cacert.pem.sha256 -o "$tmp/sha"
want=$(cut -d' ' -f1 "$tmp/sha")
got=$(shasum -a 256 "$tmp/bundle" | cut -d' ' -f1)
if [ "$want" != "$got" ]; then
  echo "the bundle does not match curl's published SHA-256 ($got, expected $want)" >&2
  exit 1
fi

asof=$(sed -n 's/^## Certificate data from Mozilla as of: //p' "$tmp/bundle" | head -1)
secs=$(date -j -u -f '%a %b %e %T %Y %Z' "$asof" +%s 2>/dev/null || date -u -d "$asof" +%s)
mv "$tmp/bundle" "$dir/cacert.pem"
echo "$secs" > "$dir/cacert.date"
echo "ok: $(grep -c 'BEGIN CERTIFICATE' "$dir/cacert.pem") roots, Mozilla's data of $asof"

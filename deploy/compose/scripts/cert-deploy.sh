#!/bin/sh
# After each issue or renewal: the copies NATS (cert + key) and HAProxy (one file) read.
# Their run scripts notice the new file and reload; no container restarts.
set -eu
live=${RENEWED_LINEAGE:-/etc/letsencrypt/live/zebridge}
umask 077
cp "$live/fullchain.pem" /certs/fullchain.pem.new
cp "$live/privkey.pem" /certs/privkey.pem.new
cat "$live/fullchain.pem" "$live/privkey.pem" > /certs/haproxy.pem.new
# HAProxy runs as its own user: the files are readable inside this volume only.
chmod 644 /certs/*.new
for f in fullchain.pem privkey.pem haproxy.pem; do mv "/certs/$f.new" "/certs/$f"; done
echo "certificate installed for: $(ls /etc/letsencrypt/live | grep -v README | tr '\n' ' ')"

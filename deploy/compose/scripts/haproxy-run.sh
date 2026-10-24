#!/bin/sh
# HAProxy in master-worker mode, reloaded (SIGUSR2: no connection dropped) when the
# certificate changes.
cfg=/conf/haproxy.cfg
cert=/certs/haproxy.pem
# The certbot service puts a certificate in place first (a temporary one, then Let's Encrypt's).
until [ -f "$cert" ]; do sleep 2; done
haproxy -W -db -f "$cfg" &
pid=$!
trap 'kill -TERM $pid; wait $pid; exit 0' TERM INT
# The content, not the time: two writes within one second share a modification time. Every
# 2 s while the temporary certificate is in place (certbot-run.sh marks it), so Let's
# Encrypt's is served within seconds of its issue; every 60 s after, for the renewals. The
# comparison follows the mark's check: cert-deploy.sh writes the certificate before it
# removes the mark, so a mark seen gone means the new certificate is there to compare.
seen=$(cksum < "$cert")
while kill -0 "$pid" 2>/dev/null; do
  if [ -f /certs/temporary ]; then every=2; else every=60; fi
  now=$(cksum < "$cert" 2>/dev/null || echo "$seen")
  if [ "$now" != "$seen" ]; then
    seen=$now
    kill -USR2 "$pid" && echo "certificate changed: haproxy reloaded"
  fi
  sleep "$every" & wait $!
done
wait "$pid"

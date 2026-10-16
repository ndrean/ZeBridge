#!/bin/sh
# nats-server, reloaded (SIGHUP: no connection dropped) when the certificate changes.
conf=/etc/zebridge/nats-server.conf
cert=/certs/fullchain.pem
# The certbot service puts a certificate in place first (a temporary one, then Let's Encrypt's).
until [ -f "$cert" ]; do sleep 2; done
nats-server -c "$conf" &
pid=$!
trap 'kill -TERM $pid; wait $pid; exit 0' TERM INT
seen=$(stat -c %Y "$cert")
while kill -0 "$pid" 2>/dev/null; do
  sleep 60 & wait $!
  now=$(stat -c %Y "$cert" 2>/dev/null || echo "$seen")
  if [ "$now" != "$seen" ]; then
    seen=$now
    kill -HUP "$pid" && echo "certificate changed: nats-server reloaded"
  fi
done
wait "$pid"

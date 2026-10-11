#!/bin/sh
# HAProxy in master-worker mode, reloaded (SIGUSR2: no connection dropped) when the
# certificate changes.
cfg=/usr/local/etc/haproxy/haproxy.cfg
cert=/certs/haproxy.pem
haproxy -W -db -f "$cfg" &
pid=$!
trap 'kill -TERM $pid; wait $pid; exit 0' TERM INT
seen=$(stat -c %Y "$cert")
while kill -0 "$pid" 2>/dev/null; do
  sleep 600 & wait $!
  now=$(stat -c %Y "$cert" 2>/dev/null || echo "$seen")
  if [ "$now" != "$seen" ]; then
    seen=$now
    kill -USR2 "$pid" && echo "certificate changed: haproxy reloaded"
  fi
done
wait "$pid"

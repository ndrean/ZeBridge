#!/bin/sh
# nats-server, started on Let's Encrypt's certificate and reloaded (SIGHUP: no connection
# dropped) when a renewal replaces it.
conf=/etc/zebridge/nats-server.conf
cert=/certs/fullchain.pem
# Not on the temporary certificate (certbot-run.sh marks it /certs/temporary): only HAProxy
# needs one before Let's Encrypt's, to listen on 443 while port 80 answers the challenge.
# Admission and the clients check the name, so a NATS on the temporary one refuses them
# all, and `docker compose up` gave up on the bridge waiting for admission. The healthcheck
# (compose.yml) turns healthy once the server runs: until then, the services after it wait.
said=""
until [ -f "$cert" ] && [ ! -f /certs/temporary ]; do
  [ -n "$said" ] || { echo "waiting for Let's Encrypt's certificate (certbot's log says how it goes)"; said=1; }
  sleep 2
done
nats-server -c "$conf" &
pid=$!
trap 'kill -TERM $pid; wait $pid; exit 0' TERM INT
# The content, not the time: two writes within one second share a modification time.
seen=$(cksum < "$cert")
while kill -0 "$pid" 2>/dev/null; do
  sleep 60 & wait $!
  now=$(cksum < "$cert" 2>/dev/null || echo "$seen")
  if [ "$now" != "$seen" ]; then
    seen=$now
    kill -HUP "$pid" && echo "certificate changed: nats-server reloaded"
  fi
done
wait "$pid"

#!/bin/sh
# The NATS stack, ONCE: operator, accounts, the bridge's creds, the server's conf. A second
# run would make a new operator and lock every enrolled device out, so an existing conf
# is kept as it is.
set -eu
d=/etc/zebridge
if [ -f "$d/nats-server.conf" ]; then
  echo "NATS configuration present: kept"
  exit 0
fi
bridge --init-nats operator --dir "$d" > /dev/null
# TLS on the client port: native clients connect from the internet. The websocket (8080)
# stays plain inside the network; HAProxy terminates its TLS on 443.
sed -i 's|^port: 4222$|port: 4222\ntls {\n  cert_file: "/certs/fullchain.pem"\n  key_file: "/certs/privkey.pem"\n}\nping_interval: "1m"|' "$d/nats-server.conf"
# The bridge dials the certificate's name; inside the network it is the nats container.
sed -i "s|^NATS_URL=.*|NATS_URL=tls://$NATS_HOST:4222|" "$d/.env.nats"
chown zebridge:zebridge "$d/.env.nats" "$d/creds/bridge.creds"
chmod 600 "$d/.env.nats" "$d/creds/bridge.creds" "$d/operator.store"
echo "NATS configuration generated in the zb-etc volume. Copy operator.store off the server."

#!/bin/sh
# The certificates, all through HAProxy: it alone holds port 80 and passes Let's Encrypt's
# challenge to this container (certbot:8888). A one-shot holding port 80 for the first
# certificate collided with HAProxy at every later `up` (compose reruns exited one-shots).
#
#   1. no certificate yet: a temporary self-signed one, so HAProxy and NATS can start;
#   2. Let's Encrypt's, retried until it is issued (the names must point at this server);
#   3. renewals twice a day.
# cert-deploy.sh installs each one; HAProxy and NATS notice the new file and reload.
set -u
live=/etc/letsencrypt/live/zebridge

if [ ! -f /certs/haproxy.pem ]; then
  t=$(mktemp -d)
  openssl req -x509 -newkey rsa:2048 -nodes -days 7 -subj "/CN=temporary" \
    -keyout "$t/key" -out "$t/crt" 2>/dev/null
  cp "$t/crt" /certs/fullchain.pem
  cp "$t/key" /certs/privkey.pem
  cat "$t/crt" "$t/key" > /certs/haproxy.pem
  chmod 644 /certs/fullchain.pem /certs/privkey.pem /certs/haproxy.pem   # this volume only
  rm -rf "$t"
  echo "temporary self-signed certificate in place, until Let's Encrypt's"
fi

# Let's Encrypt's expiry notices go to ACME_EMAIL. Empty, compose refuses to start; a
# placeholder or a typo is refused here, with the temporary certificate already in place
# so HAProxy and NATS run meanwhile, and certbot shows as Restarting in `docker compose ps`.
email_ok() {
  case "$1" in
    *[!A-Za-z0-9._%+@-]* | *@*@*) return 1 ;;   # spaces, <, >, quotes; two @
    ?*@?*.?*) ;;
    *) return 1 ;;
  esac
  case "${1##*@}" in
    example.com | example.org | example.net | *.example.com | *.test | *.invalid | *.localhost) return 1 ;;
  esac
}
if ! email_ok "$ACME_EMAIL"; then
  echo "ACME_EMAIL in .env is not an e-mail address: '$ACME_EMAIL'. Let's Encrypt needs a real one;"
  echo "set it, then: docker compose up -d certbot"
  exit 1
fi

# The challenge reaches us through HAProxy: wait until it listens.
until python3 -c 'import socket; socket.create_connection(("haproxy", 80), 2)' 2>/dev/null; do sleep 2; done

args=""
for d in $DOMAINS; do args="$args -d $d"; done
while :; do
  if [ -d "$live" ]; then
    certbot renew --http-01-port 8888 --deploy-hook /conf/scripts/cert-deploy.sh
    sleep 12h & wait $!
  else
    # shellcheck disable=SC2086
    if certbot certonly --standalone --http-01-port 8888 --non-interactive --agree-tos \
         -m "$ACME_EMAIL" --cert-name zebridge --deploy-hook /conf/scripts/cert-deploy.sh $args; then
      continue
    fi
    echo "no certificate yet: do the three names point at this server, and is port 80 open? Next try in 10 minutes"
    sleep 600 & wait $!
  fi
done

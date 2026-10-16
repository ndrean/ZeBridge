#!/bin/sh
# The first certificate, before HAProxy holds port 80: certbot answers Let's Encrypt's
# HTTP-01 challenge itself. Skipped once a certificate exists (renewals: the certbot service).
set -eu
if [ -f /certs/haproxy.pem ]; then
  echo "certificate present: nothing to do"
  exit 0
fi
args=""
for d in $DOMAINS; do args="$args -d $d"; done
# shellcheck disable=SC2086
certbot certonly --standalone --non-interactive --agree-tos -m "$ACME_EMAIL" \
  --cert-name zebridge --deploy-hook /conf/scripts/cert-deploy.sh $args

#!/bin/sh
set -eu
if [ -z "${NATS_URL:-}" ]; then
    echo '🔴 NATS_URL is not set: pass the NATS server, e.g. -e NATS_URL=tls://nats.example.com:4222' >&2
    exit 1
fi
exec /usr/local/bin/bridge_archiver "$@"

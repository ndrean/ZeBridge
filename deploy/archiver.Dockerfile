# The native readings archiver (bridge_archiver, examples/16-archive): one tenant's readings, from
# its stream READINGS_<tenant> to Parquet files in S3 (Cloudflare R2, or any S3). One
# container per tenant, next to the NATS server that holds the stream (the hub).
#
# Build from the repository's root with the current release's .deb in dist/<version>/:
#
#   docker build -f deploy/archiver.Dockerfile --build-arg VERSION=<version> -t zb-archiver .
#   scripts/scenarios/.venv/bin/python deploy/test-archiver-image.py zb-archiver
#
# Run it with an archiver's creds (bridge --mint-archiver --tenant acme) mounted, the NATS
# server and the S3 settings. It holds no state: its position is the manifest in NATS KV, so
# a restart, a move or a rebuild loses nothing. With nats-server running natively on the
# host, join it through the host's network, by the name its certificate carries:
#
#   docker run -d --name archiver-acme --restart on-failure --network host \
#     -v /etc/zebridge/creds/archiver-acme.creds:/creds:ro \
#     -e NATS_URL=tls://nats.example.com:4222 \
#     -e S3_ENDPOINT=https://<account>.r2.cloudflarestorage.com -e S3_BUCKET=zb-events \
#     -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
#     ghcr.io/ndrean/zebridge-archiver --tenant acme --flush-s 60
#
# `--restart on-failure` restarts it after a crash; exits 2, 3 and 4 mean a person must look
# (the stream keeps readings too briefly, another archiver runs for this tenant, readings were
# lost) — Docker restarts those too, so watch bridge_archive_lag_messages and the logs.

# Extract only the native command; it needs neither PostgreSQL nor Python.
FROM debian:13-slim AS archiver
ARG VERSION
ARG TARGETARCH
COPY dist/${VERSION}/zebridge_${VERSION}_${TARGETARCH}.deb /tmp/zebridge.deb
RUN dpkg-deb -x /tmp/zebridge.deb /extract \
 && test -x /extract/usr/bin/bridge_archiver

# DuckDB's official C library, pinned independently of the bridge release.
FROM debian:13-slim AS duckdb
ARG TARGETARCH
ARG DUCKDB_VERSION=1.5.5
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl unzip \
 && case "$TARGETARCH" in amd64|arm64) ;; *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; esac \
 && curl -fsSL -o /tmp/duckdb.zip "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-${TARGETARCH}.zip" \
 && unzip -j /tmp/duckdb.zip libduckdb.so -d /out

FROM debian:13-slim
COPY --from=archiver /extract/usr/bin/bridge_archiver /usr/local/bin/bridge_archiver
COPY --from=duckdb /out/libduckdb.so /usr/local/lib/libduckdb.so
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
 && ldconfig \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --no-create-home zbarchiver
COPY --chmod=755 deploy/archiver-entrypoint.sh /usr/local/bin/archiver-entrypoint
USER zbarchiver
WORKDIR /tmp
# NATS_URL empty, not absent: the archiver then stops and asks for it, rather than dial
# 127.0.0.1, which in a container is the container itself.
ENV NATS_URL="" NATS_CREDS=/creds ZB_DUCKDB_LIB=/usr/local/lib/libduckdb.so
ENTRYPOINT ["/usr/local/bin/archiver-entrypoint"]

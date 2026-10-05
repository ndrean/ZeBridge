# The airports responder of examples/10-airports as a container: libzb (native), DuckDB as
# its replica, zb-python and the service. For a site near its users — a leaf node's host, an
# edge container platform, any Kubernetes — where a process lives on: one NATS connection,
# a replica that follows CDC, native speed.
#
# Debian, not Alpine: libzbcore.so (deploy/build-linux.sh) and DuckDB's release are glibc
# builds; musl would need both rebuilt. Build from the repository's root, after
# deploy/build-linux.sh for the target CPU:
#
#   deploy/build-linux.sh [aarch64]
#   docker build -f deploy/airports-responder.Dockerfile --build-arg ARCH=x86_64 -t zb-airports .
#
# Run it with a responder's creds (bridge --mint-responder) mounted, and the NATS server to
# join; behind a leaf node, the hub's JetStream domain:
#
#   docker run -d --name airports --network host -v /etc/zebridge/creds/airports-leaf.creds:/creds:ro \
#     -e NATS_URL=tls://leaf.example.com:4222 -e NATS_JS_DOMAIN=hub -e ZB_SERVICE_NAME=airports-leaf zb-airports

ARG ARCH=x86_64

FROM debian:13-slim AS duckdb
ARG ARCH
ARG DUCKDB_VERSION=1.5.5
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl unzip \
 && case "$ARCH" in x86_64) d=amd64 ;; aarch64) d=arm64 ;; *) echo "ARCH: x86_64 or aarch64" >&2; exit 1 ;; esac \
 && curl -fsSL -o /tmp/duckdb.zip "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-${d}.zip" \
 && unzip -j /tmp/duckdb.zip libduckdb.so -d /out

FROM debian:13-slim
ARG ARCH
# python3: the service; ca-certificates: libzb on Linux reads the system's roots.
RUN apt-get update && apt-get install -y --no-install-recommends python3 ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --home-dir /var/lib/airports --create-home airports
COPY --from=duckdb /out/libduckdb.so /usr/local/lib/
COPY libzb/zig-out/linux-${ARCH}/lib/libzbcore.so /usr/local/lib/
RUN ldconfig
COPY zb-python/src/zebridge/*.py /opt/airports/zebridge/
COPY examples/10-airports/airport_service.py /opt/airports/

USER airports
WORKDIR /var/lib/airports
ENV ZB_LIB=/usr/local/lib/libzbcore.so \
    PYTHONPATH=/opt/airports \
    PYTHONUNBUFFERED=1
# The replica lives in /var/lib/airports: a volume there keeps it across restarts, so a
# restart resumes from its position instead of seeding again.
VOLUME /var/lib/airports
ENTRYPOINT ["python3", "/opt/airports/airport_service.py", "--creds", "/creds", "--db", "/var/lib/airports/airports.duckdb"]

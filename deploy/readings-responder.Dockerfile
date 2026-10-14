# The readings responder of examples/16-archive as a container: questions about one tenant's
# sensor readings (query.<tenant>.count, per_minute, latest), answered from the Parquet files
# its archiver wrote to S3 (Cloudflare R2, or any S3) plus the readings that came after.
# It answers through libzb (zb.serve), so the image carries libzb's .deb from the same
# release, zb-python, DuckDB and boto3.
#
# Build from the repository's root, with libzb's .deb of the version in dist/<version>/
# (in CI: the release run's own artifact):
#
#   gh release download v0.2.0 -p 'libzb_0.2.0_*.deb' --dir dist/0.2.0
#   docker build -f deploy/readings-responder.Dockerfile --build-arg VERSION=0.2.0 -t zb-readings .
#
# Run it with a responder's creds (bridge --mint-responder --tenant acme) mounted, the NATS
# server and the S3 settings (a read-only key is enough). With nats-server running natively
# on the host, join it through the host's network, by the name its certificate carries:
#
#   docker run -d --name readings-acme --restart on-failure --network host \
#     --env-file /etc/zebridge/readings.env \
#     -v /etc/zebridge/creds/readings-acme.creds:/creds:ro -v readings-acme:/var/cache/zb-readings \
#     -e NATS_URL=tls://nats.example.com:4222 \
#     ghcr.io/ndrean/zebridge-readings-responder --tenant acme
#
# It holds nothing that cannot be rebuilt: the volume only spares fetching the files again.

FROM python:3.13-slim
ARG VERSION
ARG TARGETARCH
ARG DEBIAN_FRONTEND=noninteractive
COPY dist/${VERSION}/libzb_${VERSION}_${TARGETARCH}.deb /tmp/
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates /tmp/libzb_${VERSION}_${TARGETARCH}.deb \
 && rm -rf /var/lib/apt/lists/* /tmp/*.deb \
 && pip install --no-cache-dir nats-py[nkeys]==2.15.0 nkeys==0.2.1 duckdb==1.5.5 boto3==1.43.112 \
 && useradd --system --uid 10001 --no-create-home zbreadings \
 && mkdir -p /var/cache/zb-readings && chown 10001:10001 /var/cache/zb-readings
COPY zb-python/src/zebridge/ /opt/zb-readings/zebridge/
COPY examples/16-archive/responder.py examples/16-archive/common.py /opt/zb-readings/
USER zbreadings
WORKDIR /var/cache/zb-readings
# NATS_URL empty, not absent: the responder stops and asks for it rather than dial 127.0.0.1.
ENV NATS_URL="" PYTHONPATH=/opt/zb-readings PYTHONUNBUFFERED=1
VOLUME /var/cache/zb-readings
ENTRYPOINT ["python", "/opt/zb-readings/responder.py", "--creds", "/creds", "--cache", "/var/cache/zb-readings"]

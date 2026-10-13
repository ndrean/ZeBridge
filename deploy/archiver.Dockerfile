# The readings archiver of examples/16-archive as a container: one tenant's readings, from
# its stream READINGS_<tenant> to Parquet files in S3 (Cloudflare R2, or any S3). One
# container per tenant, next to the NATS server that holds the stream (the hub).
#
# Build from the repository's root:
#
#   docker build -f deploy/archiver.Dockerfile -t zb-archiver .
#
# Run it with an archiver's creds (bridge --mint-archiver --tenant acme) mounted, and the
# S3 settings. It holds no state: its position is the manifest in NATS KV, so a restart, a
# move or a rebuild loses nothing.
#
#   docker run -d --name archiver-acme --restart on-failure \
#     -v /etc/zebridge/creds/archiver-acme.creds:/creds:ro \
#     -e NATS_URL=tls://nats.example.com:4222 \
#     -e S3_ENDPOINT=https://<account>.r2.cloudflarestorage.com -e S3_BUCKET=zb-events \
#     -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
#     ghcr.io/ndrean/zebridge-archiver --tenant acme --flush-s 60
#
# `--restart on-failure` restarts it after a crash; exits 2, 3 and 4 mean a person must look
# (the stream keeps readings too briefly, another archiver runs for this tenant, readings were
# lost) — Docker restarts those too, so watch bridge_archive_lag_messages and the logs.

FROM python:3.13-slim
RUN pip install --no-cache-dir nats-py[nkeys]==2.15.0 nkeys==0.2.1 duckdb==1.5.5 boto3==1.43.112 \
 && useradd --system --uid 10001 --no-create-home zbarchiver
COPY examples/16-archive/archiver.py examples/16-archive/common.py /opt/zb-archiver/
USER zbarchiver
WORKDIR /tmp
ENV NATS_CREDS=/creds PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/opt/zb-archiver/archiver.py"]

# The release image: the bridge and the sweeper, installed from the release's own .deb, so
# apt brings libpq5 and libzstd1 exactly as on a server. Built by .github/workflows/release.yml:
#
#   ghcr.io/ndrean/zebridge:<version>        linux/amd64 and linux/arm64
#
#   docker run --rm ghcr.io/ndrean/zebridge:<version> --version
#   docker run ... --entrypoint bridge_sweeper ghcr.io/ndrean/zebridge:<version>
#
# The build context is a folder holding zebridge_<version>_<amd64|arm64>.deb, and quickstart/
# with init.sh and demo.sql: docker-compose.quickstart.yml's one-time setup, so the
# quickstart needs no file of the repository. postgresql-client is psql, for
# `bridge --init-sql | psql`.
FROM debian:13-slim
ARG VERSION
ARG TARGETARCH
# The build has no terminal: say so, so apt's debconf does not try three prompts first.
# ARG, not ENV: it applies to this build only, not to the running container.
ARG DEBIAN_FRONTEND=noninteractive
COPY zebridge_${VERSION}_${TARGETARCH}.deb /tmp/
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates postgresql-client /tmp/zebridge_${VERSION}_${TARGETARCH}.deb \
 && rm -rf /var/lib/apt/lists/* /tmp/*.deb \
 && useradd --system --no-create-home --shell /usr/sbin/nologin zebridge
COPY quickstart/ /usr/share/zebridge/quickstart/
COPY compose/ /usr/share/zebridge/compose/
USER zebridge
ENTRYPOINT ["/usr/bin/bridge"]

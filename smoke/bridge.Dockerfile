# The bridge as a user installs it: from the APT repository, with its signing key, at the
# version under test (DISTRIBUTION.md, "Linux packages").
FROM debian:13-slim
ARG ZB_VERSION
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl postgresql-client \
 && curl -fsSL https://packages.zebridge.eu/zebridge.gpg -o /usr/share/keyrings/zebridge.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/zebridge.gpg] https://packages.zebridge.eu stable main" \
      > /etc/apt/sources.list.d/zebridge.list \
 && apt-get update && apt-get install -y --no-install-recommends "zebridge=${ZB_VERSION}" "libzb=${ZB_VERSION}" \
 && rm -rf /var/lib/apt/lists/*
COPY setup.sh smoke.sql /smoke/

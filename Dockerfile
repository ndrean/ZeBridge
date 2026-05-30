# Multi-stage build for production
# Using alpine:3.22 for minimal footprint with musl libc
ARG BASE_IMAGE=alpine:3.22

FROM ${BASE_IMAGE} AS builder

RUN apk add --no-cache \
    curl \
    xz \
    git \
    cmake \
    make \
    gcc \
    g++ \
    musl-dev \
    postgresql-dev \
    openssl-dev \
    openssl-libs-static \
    zstd-dev \
    zstd-static

# Download and install Zig 0.16.0 (required by minimum_zig_version in build.zig.zon)
# Detect architecture and download appropriate version
RUN ARCH=$(uname -m) && \
    cd /tmp && \
    if [ "$ARCH" = "aarch64" ]; then \
    curl -L https://ziglang.org/download/0.16.0/zig-aarch64-linux-0.16.0.tar.xz -o zig.tar.xz && \
    tar -xf zig.tar.xz && \
    mv zig-aarch64-linux-0.16.0 /usr/local/zig; \
    else \
    curl -L https://ziglang.org/download/0.16.0/zig-x86_64-linux-0.16.0.tar.xz -o zig.tar.xz && \
    tar -xf zig.tar.xz && \
    mv zig-x86_64-linux-0.16.0 /usr/local/zig; \
    fi && \
    ln -s /usr/local/zig/zig /usr/local/bin/zig && \
    rm zig.tar.xz

WORKDIR /build
COPY . .

# Clean any existing builds
RUN rm -rf libs/libpq-install zig-out zig-cache

RUN zig build -Doptimize=ReleaseFast

FROM ${BASE_IMAGE}

# Runtime dependencies (dynamically linked)
# libpq pulls in libssl3/libcrypto3 transitively on Alpine
RUN apk add --no-cache \
    libpq \
    zstd-libs \
    ca-certificates

COPY --from=builder /build/zig-out/bin/bridge /usr/local/bin/bridge

# Default command
ENTRYPOINT ["/usr/local/bin/bridge"]

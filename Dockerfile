# =============================================================================
# hc-zwave — HomeCore Z-Wave Plugin
# Alpine Linux — minimal, static-friendly runtime
# =============================================================================
#
# Build:
#   docker build -t hc-zwave:latest .
#
# Run:
#   docker run -d \
#     -v ./config/config.toml:/opt/hc-zwave/config/config.toml:ro \
#     -v hc-zwave-logs:/opt/hc-zwave/logs \
#     hc-zwave:latest
#
# Note: This plugin bridges to a zwave-js WebSocket server.
#       The Z-Wave USB dongle and zwave-js-ui must run on the host
#       or in a separate container with USB device access.
#
# Volumes:
#   /opt/hc-zwave/config   config.toml (zwave-js ws URL, credentials)
#   /opt/hc-zwave/logs     rolling log files
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1 — Build
# -----------------------------------------------------------------------------
FROM rust:1.95-alpine3.23@sha256:606fd313a0f49743ee2a7bd49a0914bab7deedb12791f3a846a34a4711db7ed2 AS builder

RUN apk upgrade --no-cache && apk add --no-cache musl-dev openssl-dev pkgconfig

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

RUN cargo build --release --bin hc-zwave

# -----------------------------------------------------------------------------
# Stage 2 — Runtime
# -----------------------------------------------------------------------------
FROM alpine:3.23@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11

# `apk upgrade` first pulls CVE patches for packages baked into the
# alpine:3 base since the upstream image was last rebuilt. Defense
# in depth — without this, `apk add --no-cache` only refreshes the
# named packages, leaving busybox/musl/etc. on the base's frozen
# versions.
RUN apk upgrade --no-cache && \
    apk add --no-cache \
        ca-certificates \
        libssl3 \
        tzdata

RUN adduser -D -h /opt/hc-zwave hczwave

COPY --from=builder /build/target/release/hc-zwave /usr/local/bin/hc-zwave
RUN chmod 755 /usr/local/bin/hc-zwave

RUN mkdir -p /opt/hc-zwave/config /opt/hc-zwave/logs

COPY config/config.toml.example /opt/hc-zwave/config/config.toml.example

RUN chown -R hczwave:hczwave /opt/hc-zwave

USER hczwave
WORKDIR /opt/hc-zwave

VOLUME ["/opt/hc-zwave/config", "/opt/hc-zwave/logs"]

ENV RUST_LOG=info

ENTRYPOINT ["hc-zwave"]

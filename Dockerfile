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
FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev openssl-dev pkgconfig

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

RUN cargo build --release --bin hc-zwave

# -----------------------------------------------------------------------------
# Stage 2 — Runtime
# -----------------------------------------------------------------------------
FROM alpine:3

RUN apk add --no-cache \
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

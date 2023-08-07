# syntax=docker/dockerfile:1

# ================
# CONFIGURATION
# ================
# Rust version
ARG RUST_VERSION="1.70.0"
# Distroless tag
ARG DISTROLESS_TAG="latest"
# Working directory
ARG WORKDIR="/usr/src/rumqttd"

# ================
# BUILDER
# ================
FROM rust:${RUST_VERSION}-alpine as builder
ARG WORKDIR

# Working directory
WORKDIR ${WORKDIR}

# Install packages
RUN apk add \
    build-base \
    openssl-dev

# Copy
COPY . .

# Build
RUN cargo build \
    --release \
    --package rumqttd

# ================
# PRODUCTION
# ================
FROM gcr.io/distroless/cc-debian11:${DISTROLESS_TAG}
ARG WORKDIR

# Environment
ENV RUST_LOG="info"

# Copy binary
COPY --from=builder ${WORKDIR}/target/release/rumqttd /rumqttd

# Entrypoint
ENTRYPOINT ["/rumqttd"]

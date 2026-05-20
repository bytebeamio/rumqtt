# Build context must be the repo root:
#   docker build -f rumqttd/Dockerfile -t rumqttd .
FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /usr/src/rumqtt

COPY Cargo.toml ./
COPY rumqttd/ rumqttd/

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/rumqtt/target \
    cargo build --release -p rumqttd && \
    cp target/release/rumqttd /usr/local/bin/rumqttd

FROM alpine:latest
RUN apk add --no-cache curl
COPY --from=builder /usr/local/bin/rumqttd /usr/local/bin/rumqttd
COPY rumqttd/rumqttd.toml /etc/rumqttd/rumqttd.toml

ENV RUST_LOG="info"
ARG PORT=1884
EXPOSE ${PORT}

ENTRYPOINT ["rumqttd"]
CMD ["-c", "/etc/rumqttd/rumqttd.toml"]
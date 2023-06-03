FROM rust:alpine as builder
RUN apk add build-base openssl-dev protoc
WORKDIR /usr/src/rumqtt
COPY . .
RUN cargo build --release -p rumqttd

FROM alpine:latest
COPY --from=builder /usr/src/rumqtt/target/release/rumqttd /usr/local/bin/rumqttd
ENV RUST_LOG="info"
ENTRYPOINT ["rumqttd"]

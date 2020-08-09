FROM ubuntu:20.04

COPY target/rumqttd rumqttd
COPY target/config config

ENV RUST_LOG="info"
CMD ["./rumqttd"]

FROM ubuntu:20.04

COPY stage/rumqttd rumqttd
COPY stage/config config

ENV RUST_LOG="info"
CMD ["./rumqttd"]

FROM ubuntu:latest
RUN apt update
RUN apt install build-essential checkinstall zlib1g-dev libssl-dev -y
COPY ./stage/rumqttd-ng .
COPY ./demo.toml .
ENV RUST_LOG="info"
ENTRYPOINT ["./rumqttd-ng",  "-c", "demo.toml"]

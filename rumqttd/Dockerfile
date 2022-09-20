FROM ubuntu:latest
RUN apt update
RUN apt install build-essential checkinstall zlib1g-dev libssl-dev -y
COPY ./stage/rumqttd /usr/bin/
COPY ./demo.toml /
ENV RUST_LOG="info"
ENTRYPOINT ["rumqttd"]

#! /bin/sh

set -ex

cargo build --release

docker stop rumqttd || true
docker rm rumqttd || true

mkdir -p stage
cp -r ../target/release/rumqttd stage/

docker build -t rumqttd .

version=`awk -F ' = ' '$1 ~ /version/ { gsub(/[\"]/, "", $2); printf("%s",$2) }' Cargo.toml`
docker tag rumqttd:latest bytebeamio/rumqttd:$version
docker tag rumqttd:latest bytebeamio/rumqttd:latest

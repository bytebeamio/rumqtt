#! /bin/sh

set -ex

cargo build --release

docker stop rumqttd-ng || true
docker rm rumqttd-ng || true

mkdir -p stage
cp -r ../target/release/rumqttd-ng stage/

docker build -t rumqttd-ng .

version=`awk -F ' = ' '$1 ~ /version/ { gsub(/[\"]/, "", $2); printf("%s",$2) }' ../rumqttd-ng/Cargo.toml`
docker tag rumqttd-ng:latest asia.gcr.io/bytebeam/rumqttd-ng:$version
docker tag rumqttd-ng:latest asia.gcr.io/bytebeam/rumqttd-ng:latest

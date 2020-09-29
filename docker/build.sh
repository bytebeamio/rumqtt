#! /bin/sh

set -ex

cargo build --release

docker stop rumqttd || true
docker rm rumqttd || true

cp -r ../target/release/rumqttd stage/
cp -r ../rumqttd/config stage/

docker build -t rumqttd .

version=`awk -F ' = ' '$1 ~ /version/ { gsub(/[\"]/, "", $2); printf("%s",$2) }' ../rumqttd/Cargo.toml`
docker tag rumqttd:latest asia.gcr.io/bytebeam/rumqttd:$version
docker tag rumqttd:latest asia.gcr.io/bytebeam/rumqttd:latest

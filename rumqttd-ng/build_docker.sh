#! /bin/sh

set -ex

cargo build --release

docker stop rumqttd || true
docker rm rumqttd || true

mkdir -p stage
cp -r ../target/release/rumqttd stage/

docker build -t rumqttd .

version=`awk -F ' = ' '$1 ~ /version/ { gsub(/[\"]/, "", $2); printf("%s",$2) }' ../rumqttd-ng/Cargo.toml`
docker tag rumqttd:latest rumqtt/rumqttd:$version
docker tag rumqttd:latest rumqtt/rumqttd:latest

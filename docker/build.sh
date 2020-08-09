#! /bin/sh

set -ex

docker stop rumqttd || true

cp -r ../target/release/rumqttd target/
cp -r ../rumqttd/config target/

docker build -t rumqttd .

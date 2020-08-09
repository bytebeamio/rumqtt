#! /bin/sh

set -ex

cp -r ../target/release/rumqttd target
cp -r ../rumqttd/config target

docker build -t rumqttd .

#!/bin/sh
set -e

cargo run -q --bin rumqttasync --release
cargo run -q --bin rumqttsync --release
cargo run -q --bin pahoasync --release
#cargo run -q --bin pahosync --release
go run paho.go

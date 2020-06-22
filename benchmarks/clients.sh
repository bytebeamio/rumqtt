#!/bin/sh
set -e

cargo run -q --bin rumqttasync --release
cargo run -q --bin rumqttsync --release
cargo run -q --bin paho --release
go run paho.go

#!/bin/sh
set -e

cargo run --bin rumqttasync --release | tee benchmarks.txt
cargo run --bin rumqttsync --release | tee -a benchmarks.txt
# cargo run --bin pahoasync --release | tee -a benchmarks.txt
cargo run -q --bin pahosync --release | tee -a benchmarks.txt
go run paho.go | tee -a benchmarks.txt


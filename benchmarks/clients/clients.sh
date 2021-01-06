#!/bin/sh
set -e

cargo run --bin rumqttasync --release | tee results/clients.txt
cargo run --bin rumqttsync --release | tee -a results/clients.txt
# cargo run --bin rumqttasyncqos0 --release | tee -a results/clients.txt
# cargo run --bin pahoasync --release | tee -a results/clients.txt
# cargo run -q --bin pahosync --release | tee -a results/clients.txt
go run clients/paho.go | tee -a results/clients.txt


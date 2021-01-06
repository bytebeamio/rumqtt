#!/bin/sh
set -e

cargo run --bin mqttbytesparser --release | tee results/parsers.txt
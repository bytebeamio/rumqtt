#!/bin/sh
set -e

cargo run --bin v4parser --release | tee results/parsers.txt
cargo run --bin v5parser --release | tee -a results/parsers.txt
cargo run --bin natsparser --release | tee -a results/parsers.txt

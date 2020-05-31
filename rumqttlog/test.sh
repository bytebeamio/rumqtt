#! /bin/bash

cargo test
cargo run --example routernxn --release |& > routernxn.txt

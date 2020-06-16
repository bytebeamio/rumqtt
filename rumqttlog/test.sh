#! /bin/bash

cargo test
cargo run --example routernxn --release |& tee routernxn.txt

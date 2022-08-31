# rumqttd

[![crates.io page](https://img.shields.io/crates/v/rumqttd.svg)](https://crates.io/crates/rumqttd)
[![docs.rs page](https://docs.rs/rumqttd/badge.svg)](https://docs.rs/rumqttd)

Rumqttd is a high performance MQTT broker written in Rust. It's light weight and embeddable, meaning
you can use it as a library in your code and extend functionality

## Currently supported features

- MQTT 3.1.1
- QoS 0 and 1
- Retained messages
- Connection via TLS
- Last will
- All MQTT 3.1.1 packets

## Upcoming features

- QoS 2
- Retransmission after reconnect
- MQTT 5


## Getting started

You can directly run the broker by running the binary with a config file with:

```
cargo run --release -- -c demo.toml

```

Example config file is provided on the root of the repo.


## Running with docker


## Run with TLS


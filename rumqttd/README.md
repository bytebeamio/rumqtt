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

In order to run rumqttd within a docker container, build the image by running `build_docker.sh`. The shell script will build the rumqttd binary file and copy it into the `stage/` directory before building the docker image. The docker image contains a `demo.toml` with default broker configurations, one can use this config by running the following command:

```bash
docker run -it -p 1883:1883 -p 8883:8883 rumqttd
```

**NOTE:** in order to use your own config, edit the `COPY` statement inside the `Dockerfile`;

# How to use with TLS

To connect an MQTT client to rumqttd over TLS, create relevant certificates for the broker and client using [provision](https://github.com/bytebeamio/provision) as follows:
```bash
provision ca // generates ca.cert.pem and ca.key.pem
provision server --ca ca.cert.pem --cakey ca.key.pem --domain localhost // generates localhost.cert.pem and localhost.key.pem
provision client --ca ca.cert.pem --cakey ca.key.pem --device 1 --tenant a // generates 1.cert.pem and 1.key.pem
```

Update config files for rumqttd and rumqttc with the generated certificates:
```toml
[v4.2.tls]
    certpath = "path/to/localhost.cert.pem"
    keypath = "path/to/localhost.key.pem"
    capath = "path/to/ca.cert.pem"
```

You may also use [certgen](https://github.com/minio/certgen), [tls-gen](https://github.com/rabbitmq/tls-gen) or [openssl](https://www.baeldung.com/openssl-self-signed-cert) to generate self-signed certificates, though we recommend using provision.

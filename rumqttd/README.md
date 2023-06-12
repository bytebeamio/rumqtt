# rumqttd

[![crates.io page](https://img.shields.io/crates/v/rumqttd.svg)](https://crates.io/crates/rumqttd)
[![docs.rs page](https://docs.rs/rumqttd/badge.svg)](https://docs.rs/rumqttd)

Rumqttd is a high performance MQTT broker written in Rust. It's light weight and embeddable, meaning
you can use it as a library in your code and extend functionality

## Getting started

You can directly run the broker by running the binary with a config file with:

```
cargo run --release -- -c rumqttd.toml

```

Example config file is provided on the root of the repo.


#### Building the docker image

In order to run rumqttd within a docker container, build the image by running `build_rumqttd_docker.sh` from the project's root directory. The shell script will use docker to build rumqttd and package it along in an [alpine](https://hub.docker.com/_/alpine) image. You can then run `rumqttd` using default config with:

```bash
./build_rumqttd_docker.sh
docker run -p 1883:1883 -p 1884:1884 -it rumqttd
```

Or you can run `rumqttd` with the custom config file by mounting the file and passing it as argument:

```bash
./build_rumqttd_docker.sh
docker run -p 1883:1883 -p 1884:1884 -v /absolute/path/to/rumqttd.toml:/rumqttd.toml -it rumqttd -c /rumqttd.toml
```

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

**NOTE:** Mount the folders containing the generated tls certificates and the proper config file(with absolute paths to the certificate) to enable tls connections with rumqttd running inside docker.

## Dynamically update log filter

Log levels and filters can by dynamically updated without restarting broker.
To update the filter, we can send a POST request to `/logs` endpoint, which is exposed by our console, with new filter as plaintext in body.
For example, to get logs of rumqttd ( running locally and expose console at port 3030 ) with log level "debug", we can do:
```sh
curl -H "Content-Type: text/plain" -d "rumqttd=debug" 0.0.0.0:3030/logs
```

The general syntax for filter is:
```
target[span{field=value}]=level
```
So filter for logs of client with id "pub-001" which has occurred any any span will be `[{client_id=pub-001}]`. Know more about it [here](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/struct.EnvFilter.html#directives)


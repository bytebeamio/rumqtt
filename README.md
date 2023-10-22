<div align="center">
    <img alt="rumqtt Logo" src="docs/rumqtt.png" width="60%" />
</div>
<div align="center">
  <a href="https://github.com/bytebeamio/rumqtt/actions/workflows/build.yml">
    <img alt="build status" src="https://github.com/bytebeamio/rumqtt/actions/workflows/build.yml/badge.svg">
  </a>
  <a href="https://discord.gg/mpkSqDg">
    <img alt="Discord chat" src="https://img.shields.io/discord/633193308033646605?style=flat">
  </a>
</div>
<br/>

## What is rumqtt?

rumqtt is an opensource set of libraries written in rust-lang to implement the MQTT standard while striving to be simple, robust and performant.

| Crate | Description | version |
| -- | -- | -- |
| [rumqttc](./rumqttc/)| A high level, easy to use mqtt client | [![crates.io page](https://img.shields.io/crates/v/rumqttc.svg)](https://crates.io/crates/rumqttc) |
| [rumqttd](./rumqttd/) | A high performance, embeddable MQTT broker |[![crates.io page](https://img.shields.io/crates/v/rumqttd.svg)](https://crates.io/crates/rumqttd) |


# Contents

* [Installation and Usage](#installation-and-usage)
    * [rumqttd](#rumqttd)
        * [Run using docker](#run-using-docker)
        * [Prebuild binaries](#prebuilt-binaries)
        * [Install using cargo](#install-using-cargo)
        * [Install using AUR](#install-using-aur)
        * [Compile from source](#compile-from-source)
    * [rumqttc](#rumqttc)
* [Features](#features)
    * [rumqttd](#rumqttd-1)
    * [rumqttc](#rumqttc-1)
* [Community](#community)
* [Contributing](#contributing)
* [License](#license)

# Installation and Usage

## rumqttd

### Run using docker

rumqttd can be used with docker by pulling the image from docker hub as follows:
```bash
docker pull bytebeamio/rumqttd
```

To run rumqttd docker image you can simply run:
```bash
docker run -p 1883:1883 -p 1884:1884 -it bytebeamio/rumqttd
```

Or you can run `rumqttd` with the custom config file by mounting the file and passing it as argument:
```bash
docker run -p 1883:1883 -p 1884:1884 -v /absolute/path/to/rumqttd.toml:/rumqttd.toml -it rumqttd -c /rumqttd.toml
```

<br/>

### Prebuilt binaries

For prebuilt binaries checkout our [releases](https://github.com/bytebeamio/rumqtt/releases), download suitable binary for your system and move it to any directory in your [PATH](https://en.wikipedia.org/wiki/PATH_(variable)).

<br/>

### Install using cargo

```
cargo install --git https://github.com/bytebeamio/rumqtt rumqttd
```

download the demo config file

```
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/bytebeamio/rumqtt/main/rumqttd/rumqttd.toml > rumqttd.toml
```

and run the broker using

```
rumqttd --config rumqttd.toml
```

Note: Make sure to you correct rumqttd.toml file for a specific version of rumqttd

<br/>

### Install using AUR

```
paru -S rumqttd-bin
```

replace `paru` with whatever AUR helper you are using.

Note: Configuration is found in `/etc/rumqtt/config.toml` and systemd service name `rumqtt.service`

<br/>

### Compile from source

Clone the repo using git clone.

```
git clone --depth=1 https://github.com/bytebeamio/rumqtt/
```

Change directory to that folder and run

```
cd rumqtt
cargo run --release --bin rumqttd -- -c rumqttd/rumqttd.toml -vvv
```

<br/>

for more information look at rumqttd's [README](https://github.com/bytebeamio/rumqtt/blob/main/rumqttd/README.md)

## rumqttc

Add rumqttc to your project using

```
cargo add rumqttc
```

<br/>

for more information look at rumqttc's [README](https://github.com/bytebeamio/rumqtt/blob/main/rumqttc/README.md)


# Features

## <a id="rumqttd-1"></a> rumqttd

- [x] MQTT 3.1.1
- [x] QoS 0 and 1
- [x] Connection via TLS
- [x] Retransmission after reconnect
- [x] Last will
- [x] Retained messages
- [x] QoS 2
- [ ] MQTT 5


# <a id="rumqttc-1"></a> rumqttc

- [x] MQTT 3.1.1
- [x] MQTT 5

# Community

- Follow us on [Twitter](https://twitter.com/bytebeamhq)
- Connect with us on [LinkedIn](https://www.linkedin.com/company/bytebeam/)
- Chat with us on [Discord](https://discord.gg/mpkSqDg)
- Read our official [Blog](https://bytebeam.io/blog/)

# Contributing
Please follow the [code of conduct](docs/CoC.md) while opening issues to report bugs or before you contribute fixes, also do read our [contributor guide](CONTRIBUTING.md) to get a better idea of what we'd appreciate and what we won't.

# License

This project is released under The Apache License, Version 2.0 ([LICENSE](./LICENSE) or http://www.apache.org/licenses/LICENSE-2.0)

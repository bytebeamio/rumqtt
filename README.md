<!-- markdownlint-disable MD033 MD041 -->
<div align="center">
    <img alt="rumqtt Logo" src="./docs/rumqtt.png" width="75%" />
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
<!-- markdownlint-enable MD033 -->

# What is rumqtt?

`rumqtt` is an Open Source set of libraries written in [Rust](https://www.rust-lang.org) to implement the MQTT standard while striving to be simple, robust and performant.

| Crate                | Description                                | version                                                                                            |
| -------------------- | ------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| [rumqttc](./rumqttc) | A high level, easy to use mqtt client      | [![crates.io page](https://img.shields.io/crates/v/rumqttc.svg)](https://crates.io/crates/rumqttc) |
| [rumqttd](./rumqttd) | A high performance, embeddable MQTT broker | [![crates.io page](https://img.shields.io/crates/v/rumqttd.svg)](https://crates.io/crates/rumqttd) |

# Contents

-   [Installation and Usage](#installation-and-usage)
    -   [rumqttd](#rumqttd)
        -   [Run using docker](#run-using-docker)
        -   [Pre-built binaries](#pre-built-binaries)
        -   [Install using cargo](#install-using-cargo)
        -   [Install using AUR](#install-using-aur)
        -   [Compile from source](#compile-from-source)
    -   [rumqttc](#rumqttc)
-   [Features](#features)
    -   [rumqttd](#rumqttd-feat)
    -   [rumqttc](#rumqttc-feat)
-   [Community](#community)
-   [Contributing](#contributing)
-   [License](#license)

# Installation and Usage

## rumqttd

### Run using docker

`rumqttd` can be used with Docker by pulling the image from docker hub as follows:

```sh
docker pull bytebeamio/rumqttd
```

To run `rumqttd` Docker image you can simply run:

```sh
docker run -p 1883:1883 -p 1884:1884 -it bytebeamio/rumqttd
```

Or you can run `rumqttd` with the custom config file by mounting the file and passing it as argument:

```sh
docker run -p 1883:1883 -p 1884:1884 -v /path/to/rumqttd.toml:/rumqttd.toml -it rumqttd -c /rumqttd.toml
```

### Pre-built binaries

For prebuilt binaries checkout our [releases](https://github.com/bytebeamio/rumqtt/releases), download suitable binary for your system and move it to any directory in your [PATH](<https://en.wikipedia.org/wiki/PATH_(variable)>).

### Install using cargo

```sh
cargo install --git https://github.com/bytebeamio/rumqtt rumqttd
```

Download the demo config file

> **Note**: Make sure `rumqttd.toml` file for a specific version of `rumqttd`

```sh
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/bytebeamio/rumqtt/main/rumqttd/rumqttd.toml > rumqttd.toml
```

and run the broker

```sh
rumqttd --config rumqttd.toml
```

### Install using AUR

> **Note**: Configuration is available in `/etc/rumqtt/config.toml`, and the `systemd` service name is `rumqtt.service`

> **Note**: Replace `paru` with your `AUR` helper

```sh
paru -S rumqttd-bin
```

### Compile from source

> **Note**: More information available at [`rumqttd/README.md`](./rumqttd/README.md)

Clone the repository

```sh
git clone --depth=1 https://github.com/bytebeamio/rumqtt.git
```

and change directory

```sh
cd rumqtt
```

and run

```sh
cargo run --release --bin rumqttd -- -c rumqttd/rumqttd.toml -vvv
```

## rumqttc

> **Note**: More information available at [`rumqttc/README.md`](./rumqttc/README.md)

Add `rumqttc` to your Rust project

```sh
cargo add rumqttc
```

# Features

<!-- markdownlint-disable MD033 -->
<h2 id="rumqttd-feat">rumqttd</h2>
<!-- markdownlint-enable MD033 -->

-   [x] MQTT 3.1.1
-   [x] QoS 0 and 1
-   [x] Connection via TLS
-   [x] Retransmission after reconnect
-   [x] Last will
-   [x] Retained messages
-   [x] QoS 2
-   [ ] MQTT 5

<!-- markdownlint-disable MD033 -->
<h2 id="rumqttc-feat">rumqttc</h2>
<!-- markdownlint-enable MD033 -->

-   [x] MQTT 3.1.1
-   [x] MQTT 5

# Community

-   Follow us on [Twitter](https://twitter.com/bytebeamhq)
-   Connect with us on [LinkedIn](https://www.linkedin.com/company/bytebeam)
-   Chat with us on [Discord](https://discord.gg/mpkSqDg)
-   Read our official [Blog](https://bytebeam.io/blog)

# Contributing

Please follow the [Code of Conduct](./CODE_OF_CONDUCT.md) while opening issues to report bugs or before you contribute fixes, also do read our [contributor guide](./CONTRIBUTING.md) to get a better idea of what we'd appreciate and what we won't.

# License

This project is released under The Apache License, Version 2.0 ([LICENSE](./LICENSE) or <http://www.apache.org/licenses/LICENSE-2.0>)

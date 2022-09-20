# rumqttd

[![crates.io page](https://img.shields.io/crates/v/rumqttd.svg)](https://crates.io/crates/rumqttd)
[![docs.rs page](https://docs.rs/rumqttd/badge.svg)](https://docs.rs/rumqttd)

## `native-tls` support

This crate, by default uses the `tokio-rustls` crate. There's also support for the `tokio-native-tls` crate.
Add it to your Cargo.toml like so:

```
rumqttd = { version = "0.5", default-features = false, features = ["use-native-tls"] }
```

Then in your config file make sure that you use the `pkcs12` entries under `certs` for your cert instead of `cert_path`, `key_path`, etc.

```toml
[rumqtt.servers.1]
port = 8883

[servers.1.cert]
pkcs12_path = "/root/identity.pfx"
pkcs12_pass = "<your password>"
```

Here's what a Rustls config looks like:

```toml
[servers.1]
port = 8883

[servers.1.cert]
cert_path = "tlsfiles/server.cert.pem"
key_path = "tlsfiles/server.key.pem"
ca_path = "tlsfiles/ca.cert.pem"
```


You can generate the `.p12`/`.pfx` file using `openssl`:

```
openssl pkcs12 -export -out identity.pfx -inkey ~/pki/private/test.key -in ~/pki/issued/test.crt -certfile ~/pki/ca.crt
```

Make sure if you use a password it matches the entry in `pkcs12_pass`. If no password, use an empty string `""`.


### Build and run as Docker image
------------------

```
./build.sh
docker run -it -p 1883:1883 --name rumqttd rumqttd                      # run foreground
docker run -d -p 1883:1883 --name rumqttd rumqttd                       # run background
docker run -d -p 1883:1883 -e "RUST_LOG=debug" --name rumqttd rumqttd   # override loglevel
```

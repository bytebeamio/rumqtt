# How to use rumqttd-ng with TLS

To connect an MQTT client to `rumqttd-ng` over TLS, create relevant certificates for the broker and client using [provision](https://github.com/bytebeamio/provision) as follows:
```bash
provision ca // generates ca.cert.pem and ca.key.pem
provision server --ca ca.cert.pem --cakey ca.key.pem --domain localhost // generates localhost.cert.pem and localhost.key.pem
provision client --ca ca.cert.pem --cakey ca.key.pem --device 1 --tenant a // generates 1.cert.pem and 1.key.pem
```

Update config files for `rumqttd-ng` and `rumqttc` with the generated certificates:
```toml
[v4.2.tls]
    certpath = "path/to/localhost.cert.pem"
    keypath = "path/to/localhost.key.pem"
    capath = "path/to/ca.cert.pem"
```

You may also use [certgen](https://github.com/minio/certgen), [tls-gen](https://github.com/rabbitmq/tls-gen) or [openssl](https://www.baeldung.com/openssl-self-signed-cert) to generate self-signed certificates, though we recommend using provision.

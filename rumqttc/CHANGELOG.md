# CHANGELOG

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

---

## [rumqttc 0.22.0] - 07-06-2023

### Added
- Added `outgoing_inflight_upper_limit` to MQTT5 `MqttOptions`. This sets the upper bound for the number of outgoing publish messages (#615)
- Added support for HTTP(s) proxy (#608)
    - Added `proxy` feature gate
    - Refactored `eventloop::network_connect` to allow setting proxy
    - Added proxy options to `MqttOptions`
- Update `rustls` to `0.21` and `tokio-rustls` to `0.24` (#606)
    - Adds support for TLS certificates containing IP addresses
    - Adds support for RFC8446 C.4 client tracking prevention

### Changed
- `MqttState::new` takes `max_outgoing_packet_size` which was set in `MqttOptions` but not used (#622)

### Deprecated

### Removed

### Fixed
- Enforce `max_outgoing_packet_size` on v4 client (#622)

### Security

## [rumqttc 0.21.0] - 01-05-2023

### Added
- Added support for MQTT5 features to v5 client
    - Refactored v5::mqttbytes to use associated functions & include properties
    - Added new API's on v5 client for properties, eg `publish_with_props` etc
    - Refactored `MqttOptions` to use `ConnectProperties` for some fields
    - Other minor changes for MQTT5

### Changed
- Remove `Box` on `Event::Incoming`

### Deprecated

### Removed
- Removed dependency on pollster

### Fixed
- Fixed v5::mqttbytes `Connect` packet returning wrong size on `write()`
    - Added tests for packet length for all v5 packets

### Security


## [rumqttc 0.20.0] - 17-01-2023

### Added
- `NetworkOptions` added to provide a way to configure low level network configurations (#545)

### Changed
- `options` in `Eventloop` now is called `mqtt_options` (#545)
- `ConnectionError` now has specific variant for type of `Timeout`, `FlushTimeout` and `NetworkTimeout` instead of generic `Timeout` for both (#545)
- `conn_timeout` is moved into `NetworkOptions` (#545)

---

Old changelog entries can be found at [CHANGELOG.md](../CHANGELOG.md)

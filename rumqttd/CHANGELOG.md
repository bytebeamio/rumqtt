# CHANGELOG

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Log warning if websocket config is getting ignored

### Changed
- Console endpoint /config prints Router Config instead of returning console settings
- v4 config is optional, user can specify v4 and/or v5 config
- websocket feature is enabled by default
- console configuration is optional

### Deprecated
- "websockets" feature is removed in favour of "websocket"

### Removed

### Fixed

### Security
- Update tungstenite and dependencies to fix [CVE](https://rustsec.org/advisories/RUSTSEC-2023-0065).

---

## [rumqttd 0.18.0] - 12-09-2023

### Added
- Will delay interval for MQTTv5 (#686)

### Changed
- Non-consuming builder pattern for constructing Connection

### Deprecated

### Removed
- Link and its implementation which were deprecated.

### Fixed
- Will Messages
- Retained Messages
- Publish properties in QoS2 publish

### Security
- Remove dependency on webpki. [CVE](https://rustsec.org/advisories/RUSTSEC-2023-0052)

---

## [rumqttd 0.17.0] - 15-08-2023

### Added
- Subscription IDs in v5 publish packets (#632)
- Shared Subscriptions with configurable strategies (#668)
- Bump dependencies to latest (#666)

### Changed

### Deprecated

### Removed

### Fixed

### Security

---

## [rumqttd 0.16.0] - 24-07-2023

### Added
- QoS2 support (#604)
- Support for Websocket connections (#633)
- LinkBuilder for constructing LinkRx/LinkTx (#659)
- Ability to configure segment size individually (#602)

### Changed

### Deprecated
- Link and its implementation, use LinkBuilder instead

### Removed

### Fixed
- Include reason code for UnsubAck in v5

### Security

---

## [rumqttd 0.15.0] - 30-05-2023

### Added
- Support for topic alias and message expiry in v5 (#616)

### Changed
- Certificate paths configured in config file are checked during startup and throws a panic if it is not valid. (#610)

### Deprecated

### Removed

### Fixed
- MQTTv5: Read the Unsubscribe package in match arms (#625)

### Security

---

## [rumqttd 0.14.0] - 31-03-2023

### Added
- `PrometheusSetting` now takes `listen` to specify listener address instead of default `127.0.0.1`. Do not use `listen` and `port` together.

### Deprecated
- `PrometheusSetting`'s `port` will be removed in favour of `listen`.

### Removed
- **Breaking:** Remove retained messages and lastwill features


## [rumqttd 0.13.0] - 08-03-2023

- No change, only version bump

## [rumqttd 0.12.7] - 04-03-2023

### Changed
- Re-design meters and alerts (#579)

## [rumqttd 0.12.6] - 14-02-2023

### Added
- Print version number with `--version` cli flag (#578)

## [rumqttd 0.12.5] - 07-02-2023

### Changed
- `structopt` is in maintainance mode, we now use `clap` instead (#571)

### Fixed
- Use `error` instead of `debug` to log message related to duplicate client_id (#572)

## [rumqttd 0.12.4] - 01-02-2023

### Fixed
- Client id with tenant prefix should be set globally (#564) 

## [rumqttd 0.12.3] - 23-01-2023

### Added
- Add one way bridging support via BridgeLink (#558)

### Changed
- Restructure AlertsLink and MetersLink to support writing to Clickhouse (#557)

## [rumqttd 0.12.2] - 16-01-2023

### Added
- Add AlertLink to get alerts about router events (#538)
- Add basic username and password authentication (#553)

### Changed
- Don't allow client's with empty client_id (#546)
- Disconnect existing client on a new connection with similar client_id (#546)
- Skip client certificate verification when using native-tls because it doesn't support it (#550)


---

Old changelog entries can be found at [CHANGELOG.md](../CHANGELOG.md)

### Unreleased
---
misc
---
- Remove rumqttd-old in favour of rumqttd (#530)

rumqttc
-------
- Add support for native-tls within rumqttc (#501)
- Fixed panicking in `recv_timeout` and `try_recv` by entering tokio runtime context (#492, #497)
- Removed unused dependencies and updated version of some of used libraries to fix dependabots warning (#475)

rumqttd
-------
- Make router::Meter public (#521)
- Add meters related to router, subscriptions, and connections (#508)
- Allow multi-tenancy validation for mtls clients with `Org` set in certificates (#505)
- Add `tracing` for structured, context-aware logging (#499, #503)
- Add the ablity to change log levels and filters dynamically at runtime (#499)
- Added properties field to `Unsubscribe`, `UnsubAck`, and `Disconnect` packets so its consistent with other packets. (#480)
- Changed default segment size in demo config to 100MB (#484)
- Allow subscription on topic's starting with `test` (#494)
-----------

### R17
---
rumqttc v0.17.0
-------
- Reimplement v5 with old `EvenLoop` design (#464)
- Implement `recv`, `try_recv`, `recv_timeout` for `Connection` (#458)
- Improve filter validation (#453) 
- Validate topic before sending a `Publish` (#449)
- Don't create a new runtime in `Iter::drop` (#405)
- Unpin exact version dependency on `tokio-rustls` (#448)

rumqttd v0.12.0-beta.1
-------
- MQTT5 support, StructOpt/Clap based CLI, change in config format (next generation broker) (#442)
- Make dependency on `rustls-pemfile` optional (#439)
- Build rumqttd docker image with alpine (#461)
-----------

### R16
---

rumqttc v0.16.0
---
- Remove `Eventloop::handle`, and stop re-exporting `flume` constructs (#441)
- Unchain features `url` and `url-rustls` (#440)
- Make dependency on `rustls-native-certs` optional (#438)
-----------

### R15
---
rumqttc v0.15.0
---
- Ensure re-export of `ClientConfig` is clearly from the `tokio_rustls` crate (#428)
- Derive standard like `Eq` and `Clone` traits on some types where required (#429)
- Create non-blocking interface for `Notifier` (#431)
- Use native certificates for encrypted transports in `MqttOptions::parse_url` (#436)

### R14
----------------
rumqttc v0.14.0
-----------
- Timeouts moved up to `connect()` call (#408)
- Tokio dependency `version = "1"` (#412)
- Fix empty property serialization (#413)
- Change variants in `ClientError` to not expose dependence on flume/`SendError` (#420)
- Revert erroring out when Subscription filter list is empty (#422)
- Remove the ability to cancel `EventLoop.poll()` mid execution (#421)
- Improve documentation and fix examples (#380, #401, #416, #417)

-----------

### R13
----------------
rumqttc v0.13.0
-----------
- Add code in `rumqttc::v5`, moving towards support for operating the client with MQTT 5 (#351, #393, #398)
- Add missing `self.inflight += 1` in `MqttState.save_pubrel()` (#389)
- Error out when Subscription filter list is empty (#392)
- Make public `rumqttc::client::Iter` (#402)

-----------

### R12
----------------
rumqttc v0.12.0
-----------
- Enable compilation without `rustls` as a dependency using `--no-default-features` (#365)
- Rework variants of `ConnectionError` (#370)
- New constructor `MqttOptions::parse()` using [`url`](https://docs.rs/url) (#379)
- Use `get_mut()` instead of index based access to ensure no panic (#384)
- Better error messages (#385) 

rumqttd v0.11.0
-----------
- Enable compilation without rustls as a dependency using `--no-default-features` (#365)
- Better error messages (#385)
-----------

### R11
----------------
rumqttc v0.11.0
-----------
- `tls::Error` is now public
- `rustls` upgraded to 0.20
- Manual acknowledgment of Publishes received by setting `MqttOptions.manual_acks = true` and passing received publish packet to `Client.ack()`

rumqttd v0.10.0
-----------
- `tokio_rustls` upgraded to 0.23.2

NOTE: mqttbytes moved into separate repo, dependency on the same from rumqtt client and broker crates are hence forth removed.

### R10
----------------

mqttbytes v0.6.0
-----------
* More public fields in v5

rumqttc v0.10.0
-----------
- Feature flag unix sockets
- From<Url> for MqttOptions
- Change keepalive api from u16 to Duration

rumqttd v0.9.0
-----------
- Use updated mqttbytes




### R9
----------------

mqttbytes v0.5.0
-----------
* Make username and password public in connect packet
* Make protocol field in v5 public

rumqttc v0.9.0
-----------
- Add unix sockets support

rumqttd v0.8.0
-----------
- Add server side password check


### R8
----------------

mqttbytes v0.4.0
-----------
* Make publish `len` public

rumqttc v0.6.0
-----------
- **changed** Update to mqttbytes 0.4
- **fix** Fix packet collisions and always enable collision protection [**breaking**] 
- **fix** Fix wrong error name

rumqttd v0.7.0
-----------
- **changed** Update to mqttbytes 0.4
- **changed** Native tls support

### R7
----------------

mqttbytes v0.3.0
-----------
- **fix** Connack and suback packet mixup during mqtt 5 implementation [**breaking**] 

rumqttc v0.5.0
-----------
- **changed** Update to mqttbytes 0.3 [**breaking**]

rumqttd v0.4.0
-----------
- **changed** Update to mqttbytes 0.2 [**breaking**]
-**changed** Make profiler a feature, not target.cfg (#243)
- **changed** Handling error cases if the key is parsed but is not valid. (#241)
-**changed** Replace hard coded 0.0.0.0 bind with configuration option (#262)

misc
----------
- update CI


### R6
----------------
mqttbytes v0.2.1
-----------
- **changed** Update README

rumqttd v0.5.0
-----------
- **changed** Improve error reporting
- **changed** Update to warp 0.3 and remove tokio compat
- **changed** Disable async link for this release due to compilaiton error [**breaking**]
- **fix** Fix windows compilation due to pprof
- **fix** Fix collision retransmission logic

benchmarks
-----------
- **feature** Adds NATS to compare parser throughput benchmarks


### R5
----------------
mqttbytes v0.2.0
-----------
- **feature** Complete mqtt 5 implementation
- **fix** Split mqtt 4 and 5 into modules [**breaking**] 

rumqttc v0.5.0
-----------
- **changed** Update to mqttbytes 0.2 [**breaking**]

rumqttd v0.4.0
-----------
- **changed** Update to mqttbytes 0.2 [**breaking**]

misc
----------
- Add mqtt 4 and 5 parsers to benchmarking suite for comparsion



### R4
----------------
mqttbytes v0.1.0
-----------
- **changed** Deprecate mqtt4/5bytes to combine them in mqttbytes

rumqttc v0.4.0
-----------
- **changed** Update to tokio 1.0 [**breaking**]

rumqttd v0.3.0
-----------
- **changed** Update to tokio 1.0 and `bytes` 1.0 [**breaking**]

misc
----------
- Improve benchmarking suite with plots

Thanks to all the contributors who made this release possible

Andrew Walbran
Jonas Platte
Daniel Egger
Mihail Malo
Alex Mikhalev


### R3
----------------
mqtt4bytes v0.4.0
-----------
- **changed** Update to `bytes` 0.6 [**breaking**]

rumqttc v0.3.0
-----------
- **feature** Refactor transport with websockets support
- **changed** Update to tokio 0.3 [**breaking**]

rumqttd v0.2.0
-----------
- **feature** Metrics server
- **feature** Improve configuration schema
- **feature** Support multiple servers on different ports
- **changed** Update to tokio 0.3 and `bytes` 0.6 [**breaking**]
- **fixed** Fix collisions due to publish batches

misc
----------
- Add license file
- Improve benchmarking suite with plots

### R2 
----------------
mqtt4bytes v0.3.0
-----------
- **changed** LastWill API now looks similar to publish API [**breaking**]

rumqttc v0.2.0
-----------
- **changed** Don't cancel the eventloop when `Client` is dropped. 
- **changed** Remove network from public interface [**breaking**]
- **internal** Fuse io into state
- **internal** Improve packet framing
- **fixed** Simplify collisions and fix collision event notification

rumqttd v0.1.0
-----------
- **fixed** 0 keepalive handling
- **fixed** Disconnect and qos 2 handling
- **feature** Offline storage, retained messages, will, unsubscribe
- **misc** Paho interoperability test suite conformance
- **internal** Performance improvements


### R1
----------------

rumqttc v0.1.2
-----------
- Move integration tests outside eventloop
- Improve integration tests
- Fix panics by properly handling connection failure

rumqttlog v0.2.0
-----------
- Redesign by embedding tracker inside router's connection
- Redesign with a hybrid of push and pull model

rumqttd v0.0.6
-----------
- Manually setup 1 router thread and 1 io thread
- Add names to threads
- Compatibility with rumqttlog



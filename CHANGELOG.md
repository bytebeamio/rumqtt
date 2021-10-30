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



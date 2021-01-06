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



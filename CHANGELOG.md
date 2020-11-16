
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



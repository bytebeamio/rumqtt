
### R2 Draft
----------------
#### mqtt4bytes
- **changed** LastWill API now looks similar to publish API [**breaking**]

#### rumqttc
- **changed** Don't cancel the eventloop when `Client` is dropped. 
- **changed** Remove network from public interface [**breaking**]
- **internal** Fuse io into state
- **internal** Improve packet framing
- **fixed** Simplify collisions and fix collision event notification

#### rumqttd
- **fixed** 0 keepalive handling
- **fixed** Disconnect and qos 2 handling
- **feature** Offline storage, retained messages, will, unsubscribe
- **misc** Paho interoperability test suite conformance
- **internal** Performance improvements

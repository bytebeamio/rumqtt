
### R2 Draft
----------------

#### rumqttc
- **changed** Don't cancel the eventloop when `Client` is dropped. 
- **changed** Remove network from public interface [**breaking**]
- **internal** Fuse io into state
- **internal** Improve packet framing
- **fixed** Simplify collisions and fix collision event notification

#### rumqttd
- **fixed** 0 keepalive handling
- **fixed** Disconnect and qos 2 handling
- Internal performance improvements



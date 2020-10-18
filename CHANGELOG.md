
### R2 Draft
----------------

#### rumqttc
- **changed** Don't cancel the eventloop when `Client` is dropped. 
- **changed** Remove network from public interface [**breaking**]
- **internal** Fuse io into state
- **internal** Improve packet framing

#### rumqttd
- **fixed** 0 keepalive handling
- Internal performance improvements



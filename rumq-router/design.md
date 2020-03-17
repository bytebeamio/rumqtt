Router is the stateful component of the broker. It keeps track of
subscribers and other routers for a distributed broker setup.

Assume a case where we want to handle a million connections with 1KB/s
average data rate per connection. 

Initial design with following constraints

- Million connections
- 1KB/s max per connection.
- A total of 1GB/s max required throughput


Possible bottlenecks

- Network throughput
- Cpu bottleneck due to parsing of packets
- Cpu bottleneck to orchestrate 'n' tokio connection tasks
- Memory To save the state

The split here will help us track these better

This also gives us effortless clarity on the throughput of connections vs router on a given machine

Design Goals
------------

Router should be pluggable (in process or across network)
-----------------------

Mqtt connection should have a choice communicating with the router using
channels, tcp (using mqtt) or not use router at all 




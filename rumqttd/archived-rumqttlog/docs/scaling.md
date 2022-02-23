High fanout
--------------

For cases like OTA and action from the backend, we need the broker to be scalable and efficient with high fanout

```$xslt
In these cases, payload remains same across multiple connections that broker needs to send to. 
Using crates like `Bytes` for zero copy publish makes a lot of sense here. This is an advantage
over kinesis where there is a fanout connecion limit per shard
```

```$xslt
A broker isn't going to handle all the connections. Router B is going to subscribe router A on
behalf of its connections. Router A can treat router B as another connection subscription
```

Push vs pull
---------------

1. Mqtt is a push based system. Both client and broker are going to push data with flow control implemented
using acks. When the inflight queue hits its size limit, the connection handle (of client/server) won't 
publish more messages. [point 4 is alternate to this]

2. The advantage with pull is that broker doesn't have to do any flow control. Connection asks for more data
when ever it is ready. 

3. But problem is with timers, 10000 connections polling with 10ms delay when idle is going to be costly.
This is not a problem w.r.t to broker clusters as number of servers is going to be in order of 10s.

4. With push, router is going to push data to connection task when ever it has data ready for that connection. We
can use channel capacity for flow control. Another advantage with push is fanout efficiency. Router can push same
`Bytes` message which are catching up to current position. EDIT: Same caching can also be implemented for pull.

5. With push, as there are no zero capacity channels, flow control will hold n connection * batch size messages in memory.
10K connections * 10MB * 2 (in transit data in connection task) = 20GB is memory intensive

6. With push, router has to also do a `try_send` on 10K connections as router shouldn't block is not cheap [experiment]

Keeping these in mind, as we anyway need grpc, kafka support, we start will pull method for both connection tasks and
routers. We will fix the cost of 10K 1ms polls by stopping the task poll when connection task is in sync with the commitlog


Replication
---------------

We can use Raft for metadata coordination and general mqtt subscription between routers for data. This allows below advantages
in the doc

Read [Replication protocol section](https://bravenewgeek.com/building-a-distributed-log-from-scratch-part-5-sketching-a-new-system/)

Leader election
----------------

- If we force the topic to have device id in the publish topic, A topic always belongs to a device. All the commitlogs are inherently mapped to a device

- If the conneciton/client/server fails, the server that device connects to next can become the leader. (If client reconnects to on of the ISRs)

- If there are 100 brokers, say only 3 brokers are ISRs for a device. Data from the device is only replicated in these 3 brokers. 

- Device connecting and publishing to broker which isn't part of ISR needs to proxy data to the leader. This is overhead. The load balancer should be smart here?

- Find ways to make the device reconnect to one of the ISRs and make the connected ISR leader

- For the backend, we can build something on top of mqtt/grpc to ask for leader before making an mqtt connection to the correct broker (leader).

- Find ways to achieve device broker affinity in general

- Only publishes can be enforced to have device-id as part of topic? For OTA, all the devices will subscribe to same topic. For
high scalability, all the connections are spread over all the 100 brokers. For this reason brokers will have 3 ISRs and subscription can happen from any broker.
Connection/Subscription having information about last offset will help when device connects to random brokers which are not part of ISRs

- So having device hold offset of subscription (instead of broker holding offset of a subscription) will allow device connect to 
any broker of 100 brokers and not loose data as router asks one of the ISR with correct offset.

- But publishes after reconnection should go to leader. Via router or connecting directly to the broker in ISR

Connection mesh
----------------

We start with a predefined set of brokers. Each router will have a link with every other router. For n routers, this is
n(n+1)/2 connections.

The connection logic goes like this for 5 routers
- All routers will listen on the port specified in config
- router 1 initiates connections with routers 2-5
- router 2 initiates connections with routers 3-5

By the end of this loop, all the routers are linked to each other

NOTE: Need to check if there is a better way to link routers 

Problems to solve

```asm
Connections should be robust. When there is a disconnection, client should reconnect. A ink in a router can be server
link or a client link. Client link should reinitiate the connection.
 
All the links are preregistered with the router as we have full knowledge of all the routers during initalization and this
task lives forever irrespective of the connection status. This simplifies registration with router as we'll do it once
and never take down the handles with router. We just update connection

Router being synchronous, should ideally never face connection channel full. When a router link is being reconnected, it
stops pulling data from the router channel. If the router has push semantics, this will fill the channel and router has to
handle failed messages.

Example: For subscriptions, router can push notifcation of new topic to the router link. But as discussed above, they can
fail. And this failure increases router state. More state is more processing single threaded router

Alternate is to make router write new publishes to metadat commitlog and make router links pull from it update the topics
that it wants to pull

Any disconnection in the router tcp link, should inform the supervison and wait for new connection.
```



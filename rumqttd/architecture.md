## Introduction
----------------

- Router: Data storage + routing logic. Connections (links) forward data to router


## Initialization

The `Broker` when created spawns a `Router` which listens on the receiver for any events. When `Broker::start()` is called, a TCP listener is spawned which listens for MQTT connections.


## `RemoteLink` and `Network`

`LinkRx` is responsible for sending `Notification` from `Router` to `RemoteLink` and `LinkTx` is responsible for sending `Event`s from `RemoteLink` to `Router`, though `RemoteLink` is not the only thing sending `Event`s to `Router`.

Whenever a new connection is received, a `Network` is spawned to handle the connection stream, and a `LinTx`-`LinkRx` pair wrapped in a `RemoteLink` is spawned which is responsible to:
1. initiate he MQTT connection using the corresponding `Network` and registering the connection with the `Router`.
2. asynchronously await on
	- `Network` for reading packets
	- `LinkRx` for notifications from router which need to be forwarded to `Network`.

There are separate shared buffers (`Arc<Mutex<..>>`s) for `LinkTx` and `LinkRx` which is used to actually send notifications and events over, and the mpsc channel is just used to notify that there is something present in the shared buffer.


## `Connection`

`Connection` is a struct used by `Router` to store all the metadata about a connection, as well as manage a QoS1 buffer for the same. `Connection` also holds onto the mpsc receiver which is used to notify the `RemoteLink` that are there some new notifications to send over to `Network`.


## `Router::readyqueue`

`Router::readyqueue` stores all the connections to which `Router` has some notifications to forward to, whether these are acks or publish messages.


## `DataRequest`

`DataRequest` represent a connection's request to fetch the data off from a topic it has subscribed to. The connection hasn't actually sent any request except the subscribe packet at start, but these are instead used to continuously read data off the logs to ensure that connection is up-to-date.


## `Tracker` (`Router::trackers`)

`Tracker` stores to current status of a given connection's `DataRequest`s. It has various unschedule flags, one if which is set whenever the connection is removed from the `Router::reaydqueue`, the reason for which in majority of cases is because the connection has caught up the with topic (`Tracker::caughtup_unschedule` flag is set in this case). Tracker also keeps all the `DataRequest`s for the given connection, as well as whether there are any pending acks for the connection in the `Router::ackslog`.


## `Waiters` (`Router::datalog::native[<filter>].waiters`)

`Router::datalog`, for each valid topic, stores all the pending `DataRequest`s. Note that these data requests pending because the corresponding connection has caught up with the logs. These `DataRequest`s being here means that they are not stored in `Router::trackers` when `Tracker::caughtup_unschedule` is set.


## `Router`

`Router::run()` loops over `Router::run_inner()`, which:
- if `Router::reaydqueue` is empty, blockingly wait over events receiver to prevent spinning in loop. There is nothing to send over to `RemoteLink`, and thus blocking here is fine.
- if `Router::readyqueue` is not empty, parses 0-500 events from receiver. This means that if there are no events to be parsed, router moves on to sending notifications over to `RemoteLink`.

`Router::consume(id: ConnectionId)` is the function where we for the given `id`:
1. send all the pending acks
2. send all the pending data, that is, all the messages from topics the connection has subscribed to, if any.


### New Connection Event

Whenever a new connection is formed (which `Router` gets informed of via `Event::Connect`), the router:
1. checks if adding new connection exceeds the total number of allowed connections or not.
2. if clean session is not requested, retrieve the any pending data and acks that were not sent
	- **NOTE:** The metrics of the previous session are still retrieved, regardless of whether a new session was requested or not.
3. necessary initializations are made
4. finally a connack is sent back

When a new connection is added, we don't add the corresponding `ConnectionId` to the `readyqueue` because we directly append the connack to the shared buffer and notify `RemoteLink` here itself instead of waiting for `Router::consume` to do so.


### Device Data Event

Whenever the `RemoteLink` reads off some bytes from `Network`, it appends it to the shared buffer and notifies the `Router` via `Event::DeviceData`. The router parses these bytes as some MQTT packets and performs the required actions.

#### Publish Packets

The publish packets are appended to `CommitLog`, a puback is added to ackslog for the connection, and the connection is added to `Router::readyqueue` so that the ack gets flushed.

For all the the waiters on the topic, they are added to `DataLog::notifications`, which at the end of parsing loop, get added to respective trackers and corresponding connections are added to readyqueue.

Publish packet are only rejected if topic is invalid, in which case, the connection is disconnected.

#### Subscribe Packets

For each filter subsribed to, we validate the subscription and call `Router::prepare_consumption()`, which adds connection to readyqueue. If subscription is invalid, connection is disconnected.

#### PubAck Packets

These packets are registed with the QoS1 buffer in `Connection`, and if the ack is out of order or unsolicited then we disconnect.

#### Other Packets

`PingReq` and disconnect packets are also parsed, rest are ignored.


### Disconnect Event

`Router::handle_disconnection()` is called here, as well as within data events when disconnection is required. The connection is removed, and so is corresponding acks in `Router::ackslog`. All the data requests in waiters is pushed back to tracker, and tracker itself is saved in graveyard if clean session is not asked.

### Ready Event

When a connection's buffer is full, the router pushes an unschedule notification at the last of the buffer. So whenever the `RemoteLink` encounters unschedule notification, it sends a ready event to router to let it know that buffer has now free space for more notifications.


## State machine transitions
----------------------------

NOTE: Transition to ConnectionReady will schedule the connection for consumption
 - Send acks (connack, suback, puback, pubrec ...) and data forwards
 - Possible output states
    - ConnectionCaughtup
    - ConnectionBusy
    - InflightFull

- New connection (event)
  - New tracker initialized with ConnectionBusy
  - ConnectionBusy -> ConnectionReady

- New subscription
  - Initialize tracker with requests
  - if ConnectionCaughtUp
    - ConnectionCaughtUp -> ConnectionReady

- New publish
  - Write to a filter/s
  - Reinitialize trackers with parked requests
  - If ConnectionCaughtUp
    - ConnectionCaughtUp -> ConnectionReady

- Acks
  - Handle outgoing state
  - If InflightFull
    - ConnectionCaughtUp -> ConnectionReady

- Connection ready
  - Connection should already be in ConnectionBusy
  - If ConnectionBusy
    - ConnectionBusy -> ConnectionReady




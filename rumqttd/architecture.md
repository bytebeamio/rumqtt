## Introduction

---

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

---

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

## Basic Concepts and Procedures

### Ontology

Here are a few terms to understand before we move on. These terms are used internally, within the codebase, to organize the files and modules.

#### Core Concepts and Entities

0. Input and Output Queues (`ibufs` and `obufs`)

   The datapath of `rumqttd` are two instances of `VecDequeue` from `std::collections`. They are [`Slab`](https://docs.rs/slab/latest/slab/) datatypes, which pre-allocates storage. Everything else just manages these and their interfaces to various other parts of the system. The queues are as follows -

   - `ibufs`: All incoming data to `rumqttd` (represented by a `Packet`) is stored immediately in `ibufs` (which holds a `VecDequeue`)
   - `obufs`: All outgoing data `rumqttd` (represented by `Notification`) is stored immediately in the `obufs` (which holds a `VecDequeue`).

   `obufs` also has an _in-flight queue_ to track packets that have been sent via the network but have not been acknowledged.

1. Broker

   Top-level entity that handles _everything_, including, but not limited to

   - Configuring and creating the Router
   - Starting the Router
   - Maintaining a channel that communicates with the router

   When you create a new Broker, a new Router is also automatically created.

   When you start the Broker, the following happens ->

   1. The Router is started, and we recieve a channel (`router_tx`), which all other Links and Servers pass `Event`s into
   2. (on a new thread) Creates the metrics server
   3. (on a new thread) Starts all `mqttv4` servers
   4. (on a new thread) Starts all `mqttv5` servers
   5. (on a new thread, if enabled) Starts all `websocket` servers
   6. (on a new thread) Starts the prometheus listener
   7. (on the same thread) Starts the ConsoleLink to allow HTTP queries to monitor and manage the broker itself.

2. Router

   Entity created by the broker that controls the flow of data in `rumqttd`. The router is responsible for managing, authorizing, and scheduling the flow of data between components within and connected to `rumqttd`.

   <!-- At any given time, the router's event loop must choose between handling incoming messages or sending outgoing messages. -->

   The router is built on the reactor pattern, where it works primarily by _reacting_ to events that happen. As and when an event is added to `ibufs`, the router reacts by checking its current state (for a certain connection or message) and dispatching the appropriate action. This can involve changing other in-memory data structures, or writing the message to `obufs` to make sure it's handled by the appropriate link.

   An action of any sort on the router's end which involves communication usually involves the router adding a structure or packet to one of these buffers.

   The router also shares Channels, which do not contain the events themselves, but rather contain notifications to events that might be added into the shared event buffers. This also has the added benefit of preventing lock contention between different futures/tasks, which would've happened if they had to keep checking the buffers

   When you create the router, the following happens ->

   1. Transmission Channels get created to and from the router
   2. A `Graveyard` to store information to persist connections
   3. A tracker for All the alerts and metrics that are necessary to be handled
   4. A Map between subscriptions and devices gets created
   5. The two communication buffers `ibufs` and `obufs` get initialized
   6. Logs for message acknowledgements, subscriptions, and alerts get created
   7. A Packet Cache is created
   8. The Scheduler struct is created

   Once all necessary things have been initialized, the `spawn` method intializes the router's event loop on a separate thread.
   This method returns a cloneable transmission channel that the Broker passes to all links so they can communicate with the router.

3. Link

   An Asynchronous and usually network-facing entity that Handles transmission of information to and from the Router. Information here can mean things including but not limited to

   1. Messages incoming from devices (publish)
   2. Messages outgoing to devices (subscribe)
   3. Information about metrics
   4. Other events such as pings

   There are various types of Links. Here they are, explored in minimal detail -

   1. Bridge -> Can be used to link two `rumqttd` instances for high availability.
   2. Alerts -> Since `rumqttd` is embeddable, this allows developers to create custom hooks to handle alerts that `rumqttd` gives out.
   3. Console -> Creates a lightweight HTTP Server to obtain information and metrics from the `rumqttd` instance.
   4. Local -> Contains functions to perform Publish and Subscribe actions to the router. This is used inside RemoteLink.
   5. Meters -> Creates a link specifically to consume metrics, is useful for external observability or hooks
   6. Remote -> `RemoteLink` is an asynchronous entity that represents a network connection. It provides its own event loop, which does the following -
      - Reads from the network socket, writes to `ibufs` and notifies the router
      - Writes to the network socket when required
   7. Timer -> Handles timing actions for metrics, and sends `Event`s related to alerts and metrics to their respective links.

#### Folder Structure

- `link` -> Contains all the different `link`s and their individual logic
- `protocol` -> Handles serializing/deserializing MQTT v4 and v5 Packets while providing a unified abstraction to process the same. Pinned version of [mqttbytes](https://github.com/bytebeamio/mqttbytes)
- `replicator` (not in use) -> Contains primitives for a clustered setup
- `router` -> Contains the logic for the router, central I/O Buffers, Connection and Client Management, Scheduler, Timer
- `segments` -> Contains the logic for disk persistence and Memory Management
- `server` -> Contains the Broker, TLS Connection Logic, and some Commmon Types

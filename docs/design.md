## Library Design

There are two main libraries which are part of rumqtt, the client(`rumqttc`) and the broker(`rumqttd`). In this document we shall discuss the high-level design for the client library, `rumqttc` as described in the diagram below. 

```text
      ┌──────┐
      │Broker│
      └──┬▲──┘
         ││
      sub││pub (network)
         ││
     ┌───▼┴────┐                ┌───────────┐
     │EventLoop◄────────────────┤AsyncClient│
     └───┬─────┘                └─────▲─────┘
         └──────────────┐┌────────────┘
             .poll()    ││  - .ack()
                        ││  - .publish()
                        ││  - .subscribe()
                        ││  - .unsubscribe()
                ┌───────▼┴───────┐
                │User Application│
                └────────────────┘
```

The main component that interacts with the network is the `EventLoop`, to proceed or move a step forwards, we poll(`.await`) the `.poll()` method on the `EventLoop`. This includes making important decisions regarding acknowledgement of QoS 1/2 requests, receiving publishes on subscribed topics, etc. `EventLoop` supports communication with the broker over TCP or WebSockets, with or without TLS. If `.poll()` is blocked or not polled for quite some time, all requests from user and broker will fail to be processed in time and this can lead to serious problems which may even need a hard-reset.

The user application can send requests to the `EventLoop` through an `AsyncClient` to `.subscribe()` or `.unsubscribe()` from certain topics, to `.publish()` to a topic or if necessary(when `manual_ack` is enabled) to `.ack()` received QoS 1/2 publishes. This is made possible with the presence of a channel that forwards user requests to the `EventLoop` as and when they are recieved by the `AsyncClient`.
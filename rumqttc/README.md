# rumqttc

A pure rust MQTT client which strives to be robust, efficient and easy to use.
This library is backed by an async (tokio) eventloop which handles all the robustness and
and efficiency parts of MQTT but naturally fits into both sync and async worlds as we'll see

Let's jump into examples right away

A simple synchronous publish and subscribe
----------------------------

```rust
use rumqttc::{MqttOptions, Client, QoS};
use std::time::Duration;
use std::thread;

fn main() {
    let mut mqttoptions = MqttOptions::new("rumqtt-sync-client", "test.mosquitto.org", 1883);
    mqttoptions.set_keep_alive(5).set_throttle(Duration::from_secs(1));

    let (mut client, mut connection) = Client::new(mqttoptions, 10);
    client.subscribe("hello/rumqtt", QoS::AtMostOnce).unwrap();
    thread::spawn(move || for i in 0..10 {
       client.publish("hello/rumqtt", QoS::AtLeastOnce, false, vec![i; i as usize]).unwrap();
       thread::sleep(Duration::from_millis(100));
    });

    // Iterate to poll the eventloop for connection progress
    for (i, notification) in connection.iter().enumerate() {
        println!("Notification = {:?}", notification);
    }
}
```

What's happening behind the scenes
- Eventloop orchestrates user requests and incoming packets concurrently and hadles the state
- Ping the broker when necessary and detects client side half open connections as well
- Throttling of outgoing packets
- Queue size based flow control on outgoing packets
- Automatic reconnections
- Natural backpressure to the client during slow network

In short, everything necessary to maintain a robust connection

**NOTE**: Looping on `connection.iter()` is necessary to run the eventloop. It yields both
incoming and outgoing activity notifications which allows customization as user sees fit.
Blocking here will block connection progress

A simple asynchronous publish and subscribe
------------------------------
```rust
use rumqttc::{MqttOptions, Request, EventLoop};
use std::time::Duration;
use std::error::Error;

#[tokio::main(core_threads = 1)]
async fn main() {
    let mut mqttoptions = MqttOptions::new("rumqtt-async", "test.mosquitto.org", 1883);
    let requests_rx = tokio::stream::iter(Vec::new());;
    let mut eventloop = EventLoop::new(mqttoptions, requests_rx).await;

    loop {
        let notification = eventloop.poll().await.unwrap();
        println!("Received = {:?}", notification);
        tokio::time::delay_for(Duration::from_secs(1)).await;
    }
}
```
- Reconnects if polled again after an error
- Takes any `Stream` for requests and hence offers a lot of customization

**Few of our real world use cases**
- Bounded or unbounded requests
- A stream which orchestrates data between disk and memory by detecting backpressure and never (practically) loose data
- A stream which juggles data between several channels based on priority of the data

Since eventloop is externally polled (with `iter()/poll()`) out side the library, users can
- Distribute incoming messages based on topics
- Stop it when required
- Access internal state for use cases like graceful shutdown

License: MIT

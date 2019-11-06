
TODO
------

- [ ] Connection
- [ ] Publish and ack
- [ ] Keep alive
- [ ] Reconnection
- [ ] Throttling
- [ ] Tls
- [ ] First alpha release

Connection and Automatic reconnections
-------

Open question: When should event loop start taking control of reconnections? After initial success or should
we expose options similar to current implementation? what should be the default behavior

```
Reconnect::AfterFirstSuccess
Reconnect::Always
Reconnect::Never
```

Let's do this for starters

```$xslt
    // create an eventloop after the initial mqtt connection is successful
    // user will decide if this should be retried or not
    let eventloop = connect(mqttoptions) -> Result<EventLoop, Error>

    // during intermittent reconnetions due to bad network, eventloop will
    // behave as per configured reconnection options to the eventloop
    let stream = eventloop.assemble(reconnection_options, inputs);
```

Possible reconnection options

```$xslt
Reconnect::Never
Reconnect::Automatic
```

I feel this a good middle ground between manual user control and rumq being very opinionated about
reconnection behaviour. 

But why not leave the intermittent reconnection behaviour as well to the user you ask? 

Because maintaining
state is a much more complicated business than just retrying the connection. We don't want the user to think
about mqttstate by default. If the user intends for a much more custom behaviour, he/she can use
`Reconnect::Never` and pass the returned `MqttState` to the next connection.

Takes any stream type as input
-------

Allows eventloop to be channel implementation agnostic. This opens up interesting usage patterns. Instead of providing
methods to communicate with the eventloop over a channel, we can directly communicate with th eventloop over the specified stream. Users can customize the stream in any way that fits their usecase. For example we plugin streams which orchestrate data between 
disk and memory.

This strategy prevents resource overhead in cases where user uses a different kind of channel than what rumqtt uses. E.g if
rumqtt uses futures channnel and user wants to plugin data coming from different kind of stream, user has to make extra copies
passing data fromn the channel of his choice to futures channel (requires user to run an extra thread which does the handover)

```
// thread 1
user_channel_tx.send(data)

// thread 2
data = user_channel_rx.recv();
rumqtt_channel_tx.send(data);

// thread 3
rumqtt_eventloop.start(rumqtt_channel_rx);
```

vs

```
// thread 1
user_channel_tx.send(data)

// thread 2
rumqtt_eventloop.start(user_channel_rx_stream);
```


Don't spawn any thread from the library
-------

Provide all the eventloop handling necessary for a robust mqtt connection but don't spawn any inner threads. This choice is
left to the user. Usecases might vary from running streams with fixed input to channels that keep producing data forever

```
let stream = vec![publish1, publish2, publish3];
let eventloop = MqttEventLoop::new(options);
eventloop.run(stream);
```

or

```
let (tx, rx) = channel::bound(10);

thread::spawn(move || loop {
    tx.send(publish);
    sleep(1)
});

let eventloop = MqttEventLoop::new(options);
eventloop.run(rx);
```


Support both synchronous and asynchronous use cases
-------

Leverage on the above pattern to support both synchronous and asynchronus publishes with timeouts

```
let publishes = [p1, p2, p3];
eventloop.run_timeout(publishes, 10 * Duration::SECS);
```

Eventloop will wait for all the acks within timeout (where ever necessary) and exits


Command channels to configure eventloop dynamically
-------

reconnections, disconnection, throttle speed, pause/resume (without disconnections)


#### shutdown, disconnection, reconnection, pause/resume semantics

there might be no need to implement separate shutdown to stop the eventloop and return existing state

```
let eventloop = MqttEventloop::new();

thread::spawn(|| {
    command.shutdown()
})

enum EventloopStatus  {
    Shutdown(MqttState)
    // sends disconnect to the server and waits for server server disconnection
    // should't process any new data from the user channel
    Disconnect 
}

eventloop.run() //return -> Result<MqttSt>
```


Keep additional functionality like gcloud jwt auth and http connect proxy out of rumqtt
-------

Prevents (some) conflicts w.r.t different versions of ring. conflicts because of rustls are still possible but atleast
prevents ones w.r.t jsonwebtoken.

Keeps the codebase small which eases some maintainence burden
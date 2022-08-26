use crate::framed::Network;
#[cfg(feature = "use-rustls")]
use crate::tls;
use crate::{Incoming, MqttOptions, MqttState, Outgoing, Packet, Request, StateError, Transport};

use crate::mqttbytes::v4::*;
#[cfg(feature = "websocket")]
use async_tungstenite::tokio::connect_async;
#[cfg(all(feature = "use-rustls", feature = "websocket"))]
use async_tungstenite::tokio::connect_async_with_tls_connector;
use flume::{bounded, Receiver, Sender};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::select;
use tokio::time::{self, error::Elapsed, Instant, Sleep};
#[cfg(feature = "websocket")]
use ws_stream_tungstenite::WsStream;

use std::io;
#[cfg(unix)]
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;
use std::vec::IntoIter;

/// Critical errors during eventloop polling
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Mqtt state: {0}")]
    MqttState(#[from] StateError),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[cfg(feature = "websocket")]
    #[error("Websocket: {0}")]
    Websocket(#[from] async_tungstenite::tungstenite::error::Error),
    #[cfg(feature = "websocket")]
    #[error("Websocket Connect: {0}")]
    WsConnect(#[from] http::Error),
    #[cfg(feature = "use-rustls")]
    #[error("TLS: {0}")]
    Tls(#[from] tls::Error),
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("Connection refused, return code: {0:?}")]
    ConnectionRefused(ConnectReturnCode),
    #[error("Expected ConnAck packet, received: {0:?}")]
    NotConnAck(Packet),
    #[error("Requests done")]
    RequestsDone,
}

/// Eventloop with all the state of a connection
pub struct EventLoop {
    /// Options of the current mqtt connection
    pub options: MqttOptions,
    /// Current state of the connection
    pub state: MqttState,
    /// Request stream
    pub requests_rx: Receiver<Request>,
    /// Requests handle to send requests
    pub requests_tx: Sender<Request>,
    /// Pending packets from last session
    pub pending: IntoIter<Request>,
    /// Network connection to the broker
    pub(crate) network: Option<Network>,
    /// Keep alive time
    pub(crate) keepalive_timeout: Option<Pin<Box<Sleep>>>,
}

/// Events which can be yielded by the event loop
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Incoming(Incoming),
    Outgoing(Outgoing),
}

impl EventLoop {
    /// New MQTT `EventLoop`
    ///
    /// When connection encounters critical errors (like auth failure), user has a choice to
    /// access and update `options`, `state` and `requests`.
    pub fn new(options: MqttOptions, cap: usize) -> EventLoop {
        let (requests_tx, requests_rx) = bounded(cap);
        let pending = Vec::new();
        let pending = pending.into_iter();
        let max_inflight = options.inflight;
        let manual_acks = options.manual_acks;

        EventLoop {
            options,
            state: MqttState::new(max_inflight, manual_acks),
            requests_tx,
            requests_rx,
            pending,
            network: None,
            keepalive_timeout: None,
        }
    }

    /// Returns a handle to communicate with this eventloop
    pub(crate) fn handle(&self) -> Sender<Request> {
        self.requests_tx.clone()
    }

    fn clean(&mut self) {
        self.network = None;
        self.keepalive_timeout = None;
        let pending = self.state.clean();
        self.pending = pending.into_iter();
    }

    /// Yields Next notification or outgoing request and periodically pings
    /// the broker. Continuing to poll will reconnect to the broker if there is
    /// a disconnection.
    /// **NOTE** Don't block this while iterating
    pub async fn poll(&mut self) -> Result<Event, ConnectionError> {
        if self.network.is_none() {
            let (network, connack) = time::timeout(
                Duration::from_secs(self.options.connection_timeout()),
                connect(&self.options),
            )
            .await??;
            self.network = Some(network);

            if self.keepalive_timeout.is_none() {
                self.keepalive_timeout = Some(Box::pin(time::sleep(self.options.keep_alive)));
            }

            return Ok(Event::Incoming(connack));
        }

        match self.select().await {
            Ok(v) => Ok(v),
            Err(e) => {
                self.clean();
                Err(e)
            }
        }
    }

    /// Select on network and requests and generate keepalive pings when necessary
    async fn select(&mut self) -> Result<Event, ConnectionError> {
        let network = self.network.as_mut().unwrap();
        // let await_acks = self.state.await_acks;
        let inflight_full = self.state.inflight >= self.options.inflight;
        let throttle = self.options.pending_throttle;
        let pending = self.pending.len() > 0;
        let collision = self.state.collision.is_some();

        // Read buffered events from previous polls before calling a new poll
        if let Some(event) = self.state.events.pop_front() {
            return Ok(event);
        }

        // this loop is necessary since self.incoming.pop_front() might return None. In that case,
        // instead of returning a None event, we try again.
        select! {
            // Pull a bunch of packets from network, reply in bunch and yield the first item
            o = network.readb(&mut self.state) => {
                o?;
                // flush all the acks and return first incoming packet
                network.flush(&mut self.state.write).await?;
                Ok(self.state.events.pop_front().unwrap())
            },
            // Pull next request from user requests channel.
            // If conditions in the below branch are for flow control. We read next user
            // user request only when inflight messages are < configured inflight and there
            // are no collisions while handling previous outgoing requests.
            //
            // Flow control is based on ack count. If inflight packet count in the buffer is
            // less than max_inflight setting, next outgoing request will progress. For this
            // to work correctly, broker should ack in sequence (a lot of brokers won't)
            //
            // E.g If max inflight = 5, user requests will be blocked when inflight queue
            // looks like this                 -> [1, 2, 3, 4, 5].
            // If broker acking 2 instead of 1 -> [1, x, 3, 4, 5].
            // This pulls next user request. But because max packet id = max_inflight, next
            // user request's packet id will roll to 1. This replaces existing packet id 1.
            // Resulting in a collision
            //
            // Eventloop can stop receiving outgoing user requests when previous outgoing
            // request collided. I.e collision state. Collision state will be cleared only
            // when correct ack is received
            // Full inflight queue will look like -> [1a, 2, 3, 4, 5].
            // If 3 is acked instead of 1 first   -> [1a, 2, x, 4, 5].
            // After collision with pkid 1        -> [1b ,2, x, 4, 5].
            // 1a is saved to state and event loop is set to collision mode stopping new
            // outgoing requests (along with 1b).
            o = self.requests_rx.recv_async(), if !inflight_full && !pending && !collision => match o {
                Ok(request) => {
                    self.state.handle_outgoing_packet(request)?;
                    network.flush(&mut self.state.write).await?;
                    Ok(self.state.events.pop_front().unwrap())
                }
                Err(_) => Err(ConnectionError::RequestsDone),
            },
            // Handle the next pending packet from previous session. Disable
            // this branch when done with all the pending packets
            Some(request) = next_pending(throttle, &mut self.pending), if pending => {
                self.state.handle_outgoing_packet(request)?;
                network.flush(&mut self.state.write).await?;
                Ok(self.state.events.pop_front().unwrap())
            },
            // We generate pings irrespective of network activity. This keeps the ping logic
            // simple. We can change this behavior in future if necessary (to prevent extra pings)
            _ = self.keepalive_timeout.as_mut().unwrap() => {
                let timeout = self.keepalive_timeout.as_mut().unwrap();
                timeout.as_mut().reset(Instant::now() + self.options.keep_alive);

                self.state.handle_outgoing_packet(Request::PingReq)?;
                network.flush(&mut self.state.write).await?;
                Ok(self.state.events.pop_front().unwrap())
            }
        }
    }
}

/// This stream internally processes requests from the request stream provided to the eventloop
/// while also consuming byte stream from the network and yielding mqtt packets as the output of
/// the stream.
/// This function (for convenience) includes internal delays for users to perform internal sleeps
/// between re-connections so that cancel semantics can be used during this sleep
async fn connect(options: &MqttOptions) -> Result<(Network, Incoming), ConnectionError> {
    // connect to the broker
    let mut network = network_connect(options).await?;

    // make MQTT connection request (which internally awaits for ack)
    let packet = mqtt_connect(options, &mut network).await?;

    // Last session might contain packets which aren't acked. MQTT says these packets should be
    // republished in the next session
    // move pending messages from state to eventloop
    // let pending = self.state.clean();
    // self.pending = pending.into_iter();
    Ok((network, packet))
}

async fn network_connect(options: &MqttOptions) -> Result<Network, ConnectionError> {
    let network = match options.transport() {
        Transport::Tcp => {
            let addr = options.broker_addr.as_str();
            let port = options.port;
            let socket = TcpStream::connect((addr, port)).await?;
            Network::new(socket, options.max_incoming_packet_size)
        }
        #[cfg(feature = "use-rustls")]
        Transport::Tls(tls_config) => {
            let socket = tls::tls_connect(options, &tls_config).await?;
            Network::new(socket, options.max_incoming_packet_size)
        }
        #[cfg(unix)]
        Transport::Unix => {
            let file = options.broker_addr.as_str();
            let socket = UnixStream::connect(Path::new(file)).await?;
            Network::new(socket, options.max_incoming_packet_size)
        }
        #[cfg(feature = "websocket")]
        Transport::Ws => {
            let request = http::Request::builder()
                .method(http::Method::GET)
                .uri(options.broker_addr.as_str())
                .header("Sec-WebSocket-Protocol", "mqttv3.1")
                .body(())?;

            let (socket, _) = connect_async(request).await?;

            Network::new(WsStream::new(socket), options.max_incoming_packet_size)
        }
        #[cfg(all(feature = "use-rustls", feature = "websocket"))]
        Transport::Wss(tls_config) => {
            let request = http::Request::builder()
                .method(http::Method::GET)
                .uri(options.broker_addr.as_str())
                .header("Sec-WebSocket-Protocol", "mqttv3.1")
                .body(())?;

            let connector = tls::tls_connector(&tls_config).await?;

            let (socket, _) = connect_async_with_tls_connector(request, Some(connector)).await?;

            Network::new(WsStream::new(socket), options.max_incoming_packet_size)
        }
    };

    Ok(network)
}

async fn mqtt_connect(
    options: &MqttOptions,
    network: &mut Network,
) -> Result<Incoming, ConnectionError> {
    let keep_alive = options.keep_alive().as_secs() as u16;
    let clean_session = options.clean_session();
    let last_will = options.last_will();

    let mut connect = Connect::new(options.client_id());
    connect.keep_alive = keep_alive;
    connect.clean_session = clean_session;
    connect.last_will = last_will;

    if let Some((username, password)) = options.credentials() {
        let login = Login::new(username, password);
        connect.login = Some(login);
    }

    // send mqtt connect packet
    network.connect(connect).await?;

    // validate connack
    match network.read().await? {
        Incoming::ConnAck(connack) if connack.code == ConnectReturnCode::Success => {
            Ok(Packet::ConnAck(connack))
        }
        Incoming::ConnAck(connack) => Err(ConnectionError::ConnectionRefused(connack.code)),
        packet => Err(ConnectionError::NotConnAck(packet)),
    }
}

/// Returns the next pending packet asynchronously to be used in select!
/// This is a synchronous function but made async to make it fit in select!
pub(crate) async fn next_pending(
    delay: Duration,
    pending: &mut IntoIter<Request>,
) -> Option<Request> {
    // return next packet with a delay
    time::sleep(delay).await;
    pending.next()
}

#[cfg(test)]
mod tests {
    use crate::broker::Broker;
    use crate::*;
    use flume::Sender;
    use std::time::{Duration, Instant};
    use tokio::{task, time};

    #[tokio::test]
    async fn some_outgoing_and_no_incoming_should_trigger_pings_on_time() {
        let keep_alive = 5;
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1886);

        options.set_keep_alive(Duration::from_secs(keep_alive));

        // start sending qos0 publishes. this makes sure that there is
        // outgoing activity but no incoming activity
        let mut eventloop = EventLoop::new(options, 5);
        let requests_tx = eventloop.handle();

        // Start sending publishes
        task::spawn(async move {
            start_requests(10, QoS::AtMostOnce, 1, requests_tx).await;
        });

        // start the eventloop
        task::spawn(async move {
            run(&mut eventloop, false).await.unwrap();
        });

        let mut broker = Broker::new(1886, 0).await;
        let mut count = 0;
        let mut start = Instant::now();

        loop {
            let event = broker.tick().await;

            if event == Event::Incoming(Incoming::PingReq) {
                // wait for 3 pings
                count += 1;
                if count == 3 {
                    break;
                }

                assert_eq!(start.elapsed().as_secs(), keep_alive as u64);
                broker.pingresp().await;
                start = Instant::now();
            }
        }

        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn requests_are_blocked_after_max_inflight_queue_size() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1887);
        options.set_inflight(5);
        let inflight = options.inflight();

        // start sending qos0 publishes. this makes sure that there is
        // outgoing activity but no incoming activity
        let mut eventloop = EventLoop::new(options, 5);
        let requests_tx = eventloop.handle();
        task::spawn(async move {
            start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
        });

        // start the eventloop
        task::spawn(async move {
            run(&mut eventloop, false).await.unwrap();
        });

        let mut broker = Broker::new(1887, 0).await;
        for i in 1..=10 {
            let packet = broker.read_publish().await;

            if i > inflight {
                assert!(packet.is_none());
            }
        }
    }

    #[tokio::test]
    async fn requests_are_recovered_after_inflight_queue_size_falls_below_max() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1888);
        options.set_inflight(3);

        let mut eventloop = EventLoop::new(options, 5);
        let requests_tx = eventloop.handle();

        task::spawn(async move {
            start_requests(5, QoS::AtLeastOnce, 1, requests_tx).await;
            time::sleep(Duration::from_secs(60)).await;
        });

        // start the eventloop
        task::spawn(async move {
            run(&mut eventloop, true).await.unwrap();
        });

        let mut broker = Broker::new(1888, 0).await;

        // packet 1, 2, and 3
        assert!(broker.read_publish().await.is_some());
        assert!(broker.read_publish().await.is_some());
        assert!(broker.read_publish().await.is_some());

        // no packet 4. client inflight full as there aren't acks yet
        assert!(broker.read_publish().await.is_none());

        // ack packet 1 and client would produce packet 4
        broker.ack(1).await;
        assert!(broker.read_publish().await.is_some());
        assert!(broker.read_publish().await.is_none());

        // ack packet 2 and client would produce packet 5
        broker.ack(2).await;
        assert!(broker.read_publish().await.is_some());
        assert!(broker.read_publish().await.is_none());
    }

    #[tokio::test]
    async fn packet_id_collisions_are_detected_and_flow_control_is_applied() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1891);
        options.set_inflight(10);

        let mut eventloop = EventLoop::new(options, 5);
        let requests_tx = eventloop.handle();

        task::spawn(async move {
            start_requests(15, QoS::AtLeastOnce, 0, requests_tx).await;
            time::sleep(Duration::from_secs(60)).await;
        });

        task::spawn(async move {
            let mut broker = Broker::new(1891, 0).await;

            // read all incoming packets first
            for i in 1..=4 {
                let packet = broker.read_publish().await;
                assert_eq!(packet.unwrap().payload[0], i);
            }

            // out of order ack
            broker.ack(3).await;
            broker.ack(4).await;
            time::sleep(Duration::from_secs(5)).await;
            broker.ack(1).await;
            broker.ack(2).await;

            // read and ack remaining packets in order
            for i in 5..=15 {
                let packet = broker.read_publish().await;
                let packet = packet.unwrap();
                assert_eq!(packet.payload[0], i);
                broker.ack(packet.pkid).await;
            }

            time::sleep(Duration::from_secs(10)).await;
        });

        time::sleep(Duration::from_secs(1)).await;

        // sends 4 requests. 5th request will trigger collision
        // Poll until there is collision.
        loop {
            match eventloop.poll().await.unwrap() {
                Event::Outgoing(Outgoing::AwaitAck(1)) => break,
                v => {
                    println!("Poll = {:?}", v);
                    continue;
                }
            }
        }

        loop {
            let start = Instant::now();
            let event = eventloop.poll().await.unwrap();
            println!("Poll = {:?}", event);

            match event {
                Event::Outgoing(Outgoing::Publish(ack)) => {
                    if ack == 1 {
                        let elapsed = start.elapsed().as_millis() as i64;
                        let deviation_millis: i64 = (5000 - elapsed).abs();
                        assert!(deviation_millis < 100);
                        break;
                    }
                }
                _ => continue,
            }
        }
    }

    // #[tokio::test]
    // async fn packet_id_collisions_are_timedout_on_second_ping() {
    //     let mut options = MqttOptions::new("dummy", "127.0.0.1", 1892);
    //     options.set_inflight(4).set_keep_alive(5);
    //
    //     let mut eventloop = EventLoop::new(options, 5);
    //     let requests_tx = eventloop.handle();
    //
    //     task::spawn(async move {
    //         start_requests(10, QoS::AtLeastOnce, 0, requests_tx).await;
    //         time::sleep(Duration::from_secs(60)).await;
    //     });
    //
    //     task::spawn(async move {
    //         let mut broker = Broker::new(1892, 0).await;
    //         // read all incoming packets first
    //         for i in 1..=4 {
    //             let packet = broker.read_publish().await;
    //             assert_eq!(packet.unwrap().payload[0], i);
    //         }
    //
    //         // out of order ack
    //         broker.ack(3).await;
    //         broker.ack(4).await;
    //         time::sleep(Duration::from_secs(15)).await;
    //     });
    //
    //     time::sleep(Duration::from_secs(1)).await;
    //
    //     // Collision error but no network disconneciton
    //     match run(&mut eventloop, false).await.unwrap() {
    //         Event::Outgoing(Outgoing::AwaitAck(1)) => (),
    //         o => panic!("Expecting collision error. Found = {:?}", o),
    //     }
    //
    //     match run(&mut eventloop, false).await {
    //         Err(ConnectionError::MqttState(StateError::CollisionTimeout)) => (),
    //         o => panic!("Expecting collision error. Found = {:?}", o),
    //     }
    // }

    #[tokio::test]
    async fn reconnection_resumes_from_the_previous_state() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 3001);
        options.set_keep_alive(Duration::from_secs(5));

        // start sending qos0 publishes. Makes sure that there is out activity but no in activity
        let mut eventloop = EventLoop::new(options, 5);
        let requests_tx = eventloop.handle();
        task::spawn(async move {
            start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
            time::sleep(Duration::from_secs(10)).await;
        });

        // start the eventloop
        task::spawn(async move {
            run(&mut eventloop, true).await.unwrap();
        });

        // broker connection 1
        let mut broker = Broker::new(3001, 0).await;
        for i in 1..=2 {
            let packet = broker.read_publish().await.unwrap();
            assert_eq!(i, packet.payload[0]);
            broker.ack(packet.pkid).await;
        }

        // NOTE: An interesting thing to notice here is that reassigning a new broker
        // is behaving like a half-open connection instead of cleanly closing the socket
        // and returning error immediately
        // Manually dropping (`drop(broker.framed)`) the connection or adding
        // a block around broker with {} is closing the connection as expected

        // broker connection 2
        let mut broker = Broker::new(3001, 0).await;
        for i in 3..=4 {
            let packet = broker.read_publish().await.unwrap();
            assert_eq!(i, packet.payload[0]);
            broker.ack(packet.pkid).await;
        }
    }

    #[tokio::test]
    async fn reconnection_resends_unacked_packets_from_the_previous_connection_first() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 3002);
        options.set_keep_alive(Duration::from_secs(5));

        // start sending qos0 publishes. this makes sure that there is
        // outgoing activity but no incoming activity
        let mut eventloop = EventLoop::new(options, 5);
        let requests_tx = eventloop.handle();
        task::spawn(async move {
            start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
            time::sleep(Duration::from_secs(10)).await;
        });

        // start the client eventloop
        task::spawn(async move {
            run(&mut eventloop, true).await.unwrap();
        });

        // broker connection 1. receive but don't ack
        let mut broker = Broker::new(3002, 0).await;
        for i in 1..=2 {
            let packet = broker.read_publish().await.unwrap();
            assert_eq!(i, packet.payload[0]);
        }

        // broker connection 2 receives from scratch
        let mut broker = Broker::new(3002, 0).await;
        for i in 1..=6 {
            let packet = broker.read_publish().await.unwrap();
            assert_eq!(i, packet.payload[0]);
        }
    }

    async fn start_requests(count: u8, qos: QoS, delay: u64, requests_tx: Sender<Request>) {
        for i in 1..=count {
            let topic = "hello/world".to_owned();
            let payload = vec![i, 1, 2, 3];

            let publish = Publish::new(topic, qos, payload);
            let request = Request::Publish(publish);
            drop(requests_tx.send_async(request).await);
            time::sleep(Duration::from_secs(delay)).await;
        }
    }

    async fn run(eventloop: &mut EventLoop, reconnect: bool) -> Result<(), ConnectionError> {
        'reconnect: loop {
            loop {
                let o = eventloop.poll().await;
                println!("Polled = {:?}", o);
                match o {
                    Ok(_) => continue,
                    Err(_) if reconnect => continue 'reconnect,
                    Err(e) => return Err(e),
                }
            }
        }
    }
}

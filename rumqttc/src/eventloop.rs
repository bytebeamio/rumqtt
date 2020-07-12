use crate::state::{MqttState, StateError};
use crate::{tls, Incoming, Request};
use crate::{MqttOptions, Outgoing};
use crate::framed::Network;

use async_channel::{bounded, Receiver, Sender};
use tokio::select;
use tokio::time::{self, Delay, Elapsed, Instant};
use tokio::net::TcpStream;
use tokio::stream::{Stream, StreamExt};
use mqtt4bytes::*;

use std::io;
use std::time::Duration;
use std::vec::IntoIter;

/// Critical errors during eventloop polling
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Mqtt state")]
    MqttState(#[from] StateError),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Packet parsing error")]
    Mqtt4Bytes(mqtt4bytes::Error),
    #[error("Network")]
    Network(#[from] tls::Error),
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Stream done")]
    StreamDone,
    #[error("Requests done")]
    RequestsDone,
    #[error("Cancel request by the user")]
    Cancel,
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
    /// Buffered packets
    pub buffered: IntoIter<Incoming>,
    /// Network connection to the broker
    pub(crate) network: Option<Network>,
    /// Keep alive time
    pub(crate) keepalive_timeout: Delay,
    /// Handle to read cancellation requests
    pub(crate) cancel_rx: Receiver<()>,
    /// Handle to send cancellation requests (and drops)
    pub(crate) cancel_tx: Option<Sender<()>>,
    /// Delay between reconnection (after a failure)
    pub(crate) reconnection_delay: Duration,
}

impl EventLoop {
    /// New MQTT `EventLoop`
    ///
    /// When connection encounters critical errors (like auth failure), user has a choice to
    /// access and update `options`, `state` and `requests`.
    pub async fn new(options: MqttOptions, cap: usize) -> EventLoop {
        let keepalive = options.keep_alive;
        let (cancel_tx, cancel_rx) = bounded(5);
        let (requests_tx, requests_rx) = bounded(cap);
        let buffered = Vec::new();
        let buffered = buffered.into_iter();
        let pending = Vec::new();
        let pending = pending.into_iter();
        let max_inflight = options.inflight;

        EventLoop {
            options,
            state: MqttState::new(max_inflight),
            requests_tx,
            requests_rx,
            pending,
            buffered,
            network: None,
            keepalive_timeout: time::delay_for(keepalive),
            cancel_rx,
            cancel_tx: Some(cancel_tx),
            reconnection_delay: Duration::from_secs(0),
        }
    }

    /// Returns a handle to communicate with this eventloop
    pub fn handle(&self) -> Sender<Request> {
        self.requests_tx.clone()
    }

    /// Set delay between (automatic) reconnections
    pub fn set_reconnection_delay(&mut self, delay: Duration) {
        self.reconnection_delay = delay;
    }

    /// Handle for cancelling the eventloop.
    ///
    /// Can be useful in cases when connection should be halted immediately
    /// between half-open connection detections or (re)connection timeouts
    pub fn take_cancel_handle(&mut self) -> Option<Sender<()>> {
        self.cancel_tx.take()
    }

    /// Next notification or outgoing request
    pub async fn poll(&mut self) -> Result<(Option<Incoming>, Option<Outgoing>), ConnectionError> {
        // This method used to return only incoming network notification while silently looping through
        // outgoing requests. Internal loops inside async functions are risky. Imagine this function
        // with 100 requests and 1 incoming packet. If this `Stream` (which internally loops) is
        // selected with other streams, can potentially do more internal polling (if the socket is ready)
        if self.network.is_none(){
            let connack = self.connect_or_cancel().await?;
            return Ok((Some(connack), None))
        }

        let (incoming, outgoing) = match self.select().await {
            Ok((i, o)) => (i, o),
            Err(e) => {
                self.network = None;
                return Err(e)
            }
        };

        Ok((incoming, outgoing))
    }


    /// Select on network and requests and generate keepalive pings when necessary
    async fn select(&mut self) -> Result<(Option<Incoming>, Option<Outgoing>), ConnectionError> {
        let network = self.network.as_mut().unwrap();
        // let await_acks = self.state.await_acks;
        let inflight_full = self.state.inflight >= self.options.inflight;
        let throttle = self.options.pending_throttle;
        let pending = self.pending.len() > 0;

        select! {
            // Pull a bunch of packets from network, reply in bunch and yield the first item
            o = network.readb(), if self.buffered.len() == 0 => match o {
                Ok(packets) => {
                    let (incoming, outgoing) = self.state.handle_incoming_packets(packets)?;
                    self.buffered = incoming.into_iter();
                    network.writeb(outgoing).await?;
                    return Ok((self.buffered.next(), None))
                }
                Err(e) => return Err(ConnectionError::Io(e))
            },
            // yield the next incoming packet of already (handled) buffered incoming packets
            o = next_buffered(&mut self.buffered), if self.buffered.len() > 0 => {
                    return Ok((o, None))
            }
            // Pull next request from user requests channel.
            // If condition in the below branch if for flow control. We read next user request
            // only when max inflight settings are honoured.
            // Flow control is based on ack count. As long as number of inflight packets in buffer
            // are less than max_inflight setting, next request will progress. For this
            // to work correctly, broker should ack in sequence and any sane broker will do so
            // If it doesn't, this will result in connection drop. Use should decide
            // E.g If max inflight = 5, requests will be blocked when inflight queue looks like this
            // [1, 2, 3, 4, 5]. Assume broker acking 2 instead of 1 -> [1, x, 3, 4, 5]. While
            // rolling pkid, next packet will be written in 1 (which is unacked) and results in
            // an error
            // This can be fixed by using packet id boundary instead of count for flow control.
            // This trick will make the client robust against brokers which doesn't ack sequentially
            // at the cost of throughput. Consider an example with max inflight of 5.
            // Full inflight queue will look like -> [1, 2, 3, 4, 5].
            // If 3 is acked instead of 1 first -> [1, 2, 4, 5].
            // If we flow control at boundary (5), Every ack should be received before rolling pkid
            // and hence overwrites aren't a problem anymore
            // Also note that, next_packet_id resets to 1 every time inflight packets are acked
            o = self.requests_rx.next(), if !inflight_full && !pending => match o {
                Some(request) => {
                    let request = self.state.handle_outgoing_packet(request)?;
                    let outgoing = network.fill(request)?;
                    // bulk up publish requests
                    if self.options.max_request_batch > 0 && self.requests_rx.len() > 0 {
                        bulk_fill(&self.options, &mut self.state, &self.requests_rx, network)?;
                        network.flush().await?;
                        // Ids are not available when individual request batching is enabled
                        return Ok((None, Some(Outgoing::Batch)))
                    }

                    network.flush().await?;
                    return Ok((None, Some(outgoing)))
                }
                None => return Err(ConnectionError::RequestsDone),
            },
            // Handle the next pending packet from previous session. Disable
            // this branch when done with all the pending packets
            Some(request) = next_pending(throttle, &mut self.pending), if pending => {
                let request = self.state.handle_outgoing_packet(request)?;
                let outgoing = network.write(request).await?;
                Ok((None, Some(outgoing)))
            },
            // We generate pings irrespective of network activity. This keeps the ping logic
            // simple. We can change this behavior in future if necessary (to prevent extra pings)
            _ = &mut self.keepalive_timeout => {
                self.keepalive_timeout.reset(Instant::now() + self.options.keep_alive);
                let request = self.state.handle_outgoing_packet(Request::PingReq)?;
                let outgoing = network.write(request).await?;
                Ok((None, Some(outgoing)))
            }
            // cancellation requests to stop the polling
            _ = self.cancel_rx.next() => {
                return Err(ConnectionError::Cancel)
            }
        }
    }


}

impl EventLoop {
    pub(crate) async fn connect_or_cancel(&mut self) -> Result<Incoming, ConnectionError> {
        let cancel_rx = self.cancel_rx.clone();
        // select here prevents cancel request from being blocked until connection request is
        // resolved. Returns with an error if connections fail continuously
        select! {
            o = self.connect() => o,
            _ = cancel_rx.recv() => {
                Err(ConnectionError::Cancel)
            }
        }
    }

    /// This stream internally processes requests from the request stream provided to the eventloop
    /// while also consuming byte stream from the network and yielding mqtt packets as the output of
    /// the stream.
    /// This function (for convenience) includes internal delays for users to perform internal sleeps
    /// between re-connections so that cancel semantics can be used during this sleep
    async fn connect(&mut self) -> Result<Incoming, ConnectionError> {
        self.state.await_pingresp = false;

        // connect to the broker
        match self.network_connect().await {
            Ok(network) => network,
            Err(e) => {
                time::delay_for(self.reconnection_delay).await;
                return Err(e);
            }
        };

        // make MQTT connection request (which internally awaits for ack)
        let packet = match self.mqtt_connect().await {
            Ok(p) => p,
            Err(e) => {
                time::delay_for(self.reconnection_delay).await;
                return Err(e);
            }
        };

        // let packet = Packet::ConnAck(ConnAck::new(ConnectReturnCode::Accepted, false));
        // Last session might contain packets which aren't acked. MQTT says these packets should be
        // republished in the next session
        // move pending messages from state to eventloop
        let pending = self.state.clean();
        self.pending = pending.into_iter();
        Ok(packet)
    }

    async fn network_connect(&mut self) -> Result<(), ConnectionError> {
        let network = time::timeout(Duration::from_secs(5), async {
            let network = if self.options.ca.is_some() {
                let socket = tls::tls_connect(&self.options).await?;
                Network::new(socket)
            } else {
                let addr = self.options.broker_addr.as_str();
                let port = self.options.port;
                let socket = TcpStream::connect((addr, port)).await?;
                Network::new(socket)
            };

            Ok::<_, ConnectionError>(network)
        })
        .await??;

        self.network = Some(network);
        Ok(())
    }

    async fn mqtt_connect(&mut self) -> Result<Incoming, ConnectionError> {
        let network = self.network.as_mut().unwrap();
        let id = self.options.client_id();
        let keep_alive = self.options.keep_alive().as_secs() as u16;
        let clean_session = self.options.clean_session();
        let last_will = self.options.last_will();

        let mut connect = Connect::new(id);
        connect.keep_alive = keep_alive;
        connect.clean_session = clean_session;
        connect.last_will = last_will;

        if let Some((username, password)) = self.options.credentials() {
            connect.set_username(username).set_password(password);
        }

        // mqtt connection with timeout
        time::timeout(Duration::from_secs(5), async {
            network.connect(connect).await?;
            Ok::<_, ConnectionError>(())
        })
        .await??;

        // wait for 'timeout' time to validate connack
        let packet = time::timeout(Duration::from_secs(5), async {
            let packet = network.read_connack().await?;
            Ok::<_, ConnectionError>(packet)
        })
        .await??;

        Ok(packet)
    }
}

/// Returns the next pending packet asynchronously to be used in select!
/// This is a synchronous function but made async to make it fit in select!
pub(crate) async fn next_pending(delay: Duration, pending: &mut IntoIter<Request>) -> Option<Request> {
    // return next packet with a delay
    time::delay_for(delay).await;
    pending.next()
}

/// Returns the next buffered packet from last buffered read
pub(crate) async fn next_buffered(incoming: &mut IntoIter<Incoming>) -> Option<Incoming> {
    incoming.next()
}

fn bulk_fill(
    options: &MqttOptions,
    state: &mut MqttState,
    requests: &Receiver<Request>,
    network: &mut Network,
) -> Result<(), ConnectionError> {
    // Be eager in reading more data till inflight buffers are full.
    // Make sure size of total inflight messages isn't greater than
    // OS tcp write buffer size to prevent bounded buffer deadlocks
    for _i in 0..options.max_request_batch {
        let request = match requests.try_recv() {
            Ok(r) => r,
            Err(_) => break
        };
        let request = state.handle_outgoing_packet(request)?;
        let _outgoing = network.fill(request);
        if state.inflight >= options.inflight {
            break
        }
    }

    Ok(())
}

pub trait Requests: Stream<Item = Request> + Unpin + Send {}
impl<T> Requests for T where T: Stream<Item = Request> + Unpin + Send {}

#[cfg(test)]
mod test {
    use super::broker::*;
    use super::*;
    use crate::state::StateError;
    use crate::{ConnectionError, MqttOptions, Request};
    use async_channel::Sender;
    use std::time::{Duration, Instant};
    use tokio::{task, time};

    async fn start_requests(count: u8, qos: QoS, delay: u64, requests_tx: Sender<Request>) {
        for i in 1..=count {
            let topic = "hello/world".to_owned();
            let payload = vec![i, 1, 2, 3];

            let publish = Publish::new(topic, qos, payload);
            let request = Request::Publish(publish);
            let _ = requests_tx.send(request).await;
            time::delay_for(Duration::from_secs(delay)).await;
        }
    }

    async fn run(mut eventloop: EventLoop, reconnect: bool) -> Result<(), ConnectionError> {
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

    #[tokio::test]
    async fn connection_should_timeout_on_time() {
        task::spawn(async move {
            let _broker = Broker::new(1880, false).await;
            time::delay_for(Duration::from_secs(10)).await;
        });

        time::delay_for(Duration::from_secs(1)).await;
        let options = MqttOptions::new("dummy", "127.0.0.1", 1880);
        let mut eventloop = EventLoop::new(options, 5).await;

        let start = Instant::now();
        let o = eventloop.poll().await;
        let elapsed = start.elapsed();

        match o {
            Ok(_) => assert!(false),
            Err(ConnectionError::Timeout(_)) => assert!(true),
            Err(_) => assert!(false),
        }

        assert_eq!(elapsed.as_secs(), 5);
    }

    #[tokio::test]
    async fn idle_connection_triggers_pings_on_time() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1885);
        options.set_keep_alive(5);
        let keep_alive = options.keep_alive();

        // start sending requests
        let eventloop = EventLoop::new(options, 5).await;
        // start the eventloop
        task::spawn(async move {
            run(eventloop, false).await.unwrap();
        });

        let mut broker = Broker::new(1885, true).await;

        // check incoming rate at th broker
        let start = Instant::now();
        let mut ping_received = false;

        for _ in 0..10 {
            let packet = broker.read_packet().await;
            let elapsed = start.elapsed();
            match packet {
                Packet::PingReq => {
                    ping_received = true;
                    assert_eq!(elapsed.as_secs(), keep_alive.as_secs());
                    break;
                }
                _ => ()
            }
        }

        assert!(ping_received);
    }

    #[tokio::test]
    async fn some_outgoing_and_no_incoming_packets_should_trigger_pings_on_time() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1886);
        options.set_keep_alive(5);
        let keep_alive = options.keep_alive();

        // start sending qos0 publishes. this makes sure that there is
        // outgoing activity but no incomin activity
        let eventloop = EventLoop::new(options, 5).await;
        let requests_tx = eventloop.handle();
        task::spawn(async move {
            start_requests(10, QoS::AtMostOnce, 1, requests_tx).await;
        });

        // start the eventloop
        task::spawn(async move {
            run(eventloop, false).await.unwrap();
        });

        let mut broker = Broker::new(1886, true).await;

        let start = Instant::now();
        let mut ping_received = false;

        for _ in 0..10 {
            let packet = broker.read_packet_and_respond().await;
            let elapsed = start.elapsed();
            match packet {
                Packet::PingReq => {
                    ping_received = true;
                    assert_eq!(elapsed.as_secs(), keep_alive.as_secs());
                    break;
                }
                _ => ()
            }
        }

        assert!(ping_received);
    }

    #[tokio::test]
    async fn some_incoming_and_no_outgoing_packets_should_trigger_pings_on_time() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 2000);
        options.set_keep_alive(5);
        let keep_alive = options.keep_alive();

        let eventloop = EventLoop::new(options, 5).await;
        task::spawn(async move {
            run(eventloop, false).await.unwrap();
        });

        let mut broker = Broker::new(2000, true).await;
        let start = Instant::now();
        broker
            .start_publishes(5, QoS::AtMostOnce, Duration::from_secs(1))
            .await;
        let packet = broker.read_packet().await;
        match packet {
            Packet::PingReq => (),
            packet => panic!("Expecting pingreq. Found = {:?}", packet)
        };
        assert_eq!(start.elapsed().as_secs(), keep_alive.as_secs());
    }

    #[tokio::test]
    async fn detects_halfopen_connections_in_the_second_ping_request() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 2001);
        options.set_keep_alive(5);

        // A broker which consumes packets but doesn't reply
        task::spawn(async move {
            let mut broker = Broker::new(2001, true).await;
            broker.blackhole().await;
        });

        time::delay_for(Duration::from_secs(1)).await;
        let start = Instant::now();
        let mut eventloop = EventLoop::new(options, 5).await;
        loop {
            if let Err(e) = eventloop.poll().await {
                match e {
                    ConnectionError::MqttState(StateError::AwaitPingResp) => break,
                    v => panic!("Expecting pingresp error. Found = {:?}", v),
                }
            }
        }

        assert_eq!(start.elapsed().as_secs(), 10);
    }

    #[tokio::test]
    async fn requests_are_blocked_after_max_inflight_queue_size() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1887);
        options.set_inflight(5);
        let inflight = options.inflight();

        // start sending qos0 publishes. this makes sure that there is
        // outgoing activity but no incoming activity
        let eventloop = EventLoop::new(options, 5).await;
        let requests_tx = eventloop.handle();
        task::spawn(async move {
            start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
        });

        // start the eventloop
        task::spawn(async move {
            run(eventloop, false).await.unwrap();
        });

        let mut broker = Broker::new(1887, true).await;
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

        let eventloop = EventLoop::new(options, 5).await;
        let requests_tx = eventloop.handle();

        task::spawn(async move {
            start_requests(5, QoS::AtLeastOnce, 1, requests_tx).await;
            time::delay_for(Duration::from_secs(60)).await;
        });

        // start the eventloop
        task::spawn(async move {
            run(eventloop, true).await.unwrap();
        });

        let mut broker = Broker::new(1888, true).await;

        // packet 1
        let packet = broker.read_publish().await;
        assert!(packet.is_some());
        // packet 2
        let packet = broker.read_publish().await;
        assert!(packet.is_some());
        // packet 3
        let packet = broker.read_publish().await;
        assert!(packet.is_some());

        // no packet 4. client inflight full as there aren't acks yet
        let packet = broker.read_publish().await;
        assert!(packet.is_none());

        // ack packet 1 and client would produce packet 4
        broker.ack(1).await;
        let packet = broker.read_publish().await;
        assert!(packet.is_some());
        let packet = broker.read_publish().await;
        assert!(packet.is_none());

        // ack packet 2 and client would produce packet 5
        broker.ack(2).await;
        let packet = broker.read_publish().await;
        assert!(packet.is_some());
        let packet = broker.read_publish().await;
        assert!(packet.is_none());
    }

    #[tokio::test]
    async fn packet_id_collisions_are_detected() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1891);
        options.set_inflight(4);

        let eventloop = EventLoop::new(options, 5).await;
        let requests_tx = eventloop.handle();

        task::spawn(async move {
            start_requests(5, QoS::AtLeastOnce, 1, requests_tx).await;
            time::delay_for(Duration::from_secs(60)).await;
        });

        task::spawn(async move {
            let mut broker = Broker::new(1891, true).await;
            for _ in 1..=4 {
                let packet = broker.read_publish().await;
                assert!(packet.is_some());
            }

            // out of order ack
            broker.ack(2).await;
            time::delay_for(Duration::from_secs(5)).await;
        });


        // panics because of
        time::delay_for(Duration::from_secs(1)).await;
        match run(eventloop, false).await {
            Err(ConnectionError::MqttState(StateError::Collision)) => (),
            o => panic!("Expecting collision error. Found = {:?}", o),
        }
    }

    #[tokio::test]
    async fn reconnection_resumes_from_the_previous_state() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1889);
        options.set_keep_alive(5);

        // start sending qos0 publishes. Makes sure that there is out activity but no in activity
        let eventloop = EventLoop::new(options, 5).await;
        let requests_tx = eventloop.handle();
        task::spawn(async move {
            start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
            time::delay_for(Duration::from_secs(10)).await;
        });

        // start the eventloop
        task::spawn(async move {
            run(eventloop, true).await.unwrap();
        });

        // broker connection 1
        let mut broker = Broker::new(1889, true).await;
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
        let mut broker = Broker::new(1889, true).await;
        for i in 3..=4 {
            let packet = broker.read_publish().await.unwrap();
            assert_eq!(i, packet.payload[0]);
            broker.ack(packet.pkid).await;
        }
    }

    #[tokio::test]
    async fn reconnection_resends_unacked_packets_from_the_previous_connection_first() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1890);
        options.set_keep_alive(5);

        // start sending qos0 publishes. this makes sure that there is
        // outgoing activity but no incoming activity
        let eventloop = EventLoop::new(options, 5).await;
        let requests_tx = eventloop.handle();
        task::spawn(async move {
            start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
            time::delay_for(Duration::from_secs(10)).await;
        });

        // start the client eventloop
        task::spawn(async move {
            run(eventloop, true).await.unwrap();
        });

        // broker connection 1. receive but don't ack
        let mut broker = Broker::new(1890, true).await;
        for i in 1..=2 {
            let packet = broker.read_publish().await.unwrap();
            assert_eq!(i, packet.payload[0]);
        }

        // broker connection 2 receives from scratch
        let mut broker = Broker::new(1890, true).await;
        for i in 1..=6 {
            let packet = broker.read_publish().await.unwrap();
            assert_eq!(i, packet.payload[0]);
        }
    }
}

#[cfg(test)]
mod broker {
    use mqtt4bytes::*;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::select;
    use tokio::stream::StreamExt;
    use tokio::time;
    use crate::framed::Network;
    use crate::Request;

    pub struct Broker {
        pub(crate) framed: Network,
    }

    impl Broker {
        /// Create a new broker which accepts 1 mqtt connection
        pub async fn new(port: u16, send_connack: bool) -> Broker {
            let addr = format!("127.0.0.1:{}", port);
            let mut listener = TcpListener::bind(&addr).await.unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = Network::new(stream);

            let packet = framed.read().await.unwrap();
            if let Packet::Connect(_) = packet {
                if send_connack {
                    let connack = ConnAck::new(ConnectReturnCode::Accepted, false);
                    framed.connack(connack).await.unwrap();
                }
            } else {
                panic!("Expecting connect packet");
            }

            Broker { framed }
        }

        // Reads a publish packet from the stream with 2 second timeout
        pub async fn read_publish(&mut self) -> Option<Publish> {
            let packet = time::timeout(Duration::from_secs(2), async {
                self.framed.read().await
            });

            match packet.await {
                Ok(Ok(Packet::Publish(publish))) => Some(publish),
                Ok(Ok(packet)) => panic!("Expecting a publish. Received = {:?}", packet),
                Ok(Err(e)) => panic!("Error = {:?}", e),
                // timedout
                Err(_) => None,
            }
        }

        /// Reads next packet from the stream
        pub async fn read_packet(&mut self) -> Packet {
            let packet = time::timeout(Duration::from_secs(30), async {
                let p = self.framed.read().await;
                // println!("Broker read = {:?}", p);
                p.unwrap()
            });

            let packet = packet.await.unwrap();
            packet
        }

        pub async fn read_packet_and_respond(&mut self) -> Packet {
            let packet = time::timeout(Duration::from_secs(30), async {
                self.framed.read().await
            });
            let packet = packet.await.unwrap().unwrap();

            match packet.clone() {
                Packet::Publish(publish) => {
                    if publish.pkid > 0 {
                        let packet = PubAck::new(publish.pkid);
                        self.framed.write(Request::PubAck(packet)).await.unwrap();
                    }
                }
                _ => (),
            }

            packet
        }

        /// Reads next packet from the stream
        pub async fn blackhole(&mut self) -> Packet {
            loop {
                let _packet = self.framed.read().await.unwrap();
            }
        }

        /// Sends an acknowledgement
        pub async fn ack(&mut self, pkid: u16) {
            let packet = Request::PubAck(PubAck::new(pkid));
            self.framed.write(packet).await.unwrap();
        }

        /// Send a bunch of publishes and ping response
        pub async fn start_publishes(&mut self, count: u8, qos: QoS, delay: Duration) {
            let mut interval = time::interval(delay);
            for i in 0..count {
                select! {
                    _ = interval.next() => {
                        let topic = "hello/world".to_owned();
                        let payload = vec![1, 2, 3, i];
                        let publish = Publish::new(topic, qos, payload);
                        let packet = Request::Publish(publish);
                        self.framed.write(packet).await.unwrap();
                    }
                    packet = self.framed.read() => match packet.unwrap() {
                        Packet::PingReq => {
                            self.framed.write(Request::PingResp).await.unwrap();
                        }
                        _ => ()
                    }
                }
            }
        }
    }
}

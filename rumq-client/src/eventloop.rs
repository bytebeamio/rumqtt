use crate::state::{MqttState, StateError};
use crate::{MqttOptions, Outgoing};
use crate::{network, Incoming, Request};

use futures_util::sink::{Sink, SinkExt};
use futures_util::stream::{Stream, StreamExt};
use rumq_core::mqtt4::codec::MqttCodec;
use rumq_core::mqtt4::{Connect, Packet, PacketIdentifier, Publish};
use tokio::select;
use tokio::time::{self, Delay, Elapsed, Instant, Throttle};
use tokio_util::codec::Framed;
use async_channel::{bounded, Sender, Receiver};

use std::collections::VecDeque;
use std::io;
use std::mem;
use std::time::Duration;

/// Critical errors during eventloop polling
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Mqtt state")]
    MqttState(#[from] StateError),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Rumq")]
    Rumq(#[from] rumq_core::Error),
    #[error("Network")]
    Network(#[from] network::Error),
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Stream done")]
    StreamDone,
    #[error("Requests done")]
    RequestsDone,
    #[error("No network link. Call eventloop.connect()")]
    NoLink,
    #[error("Cancel request by the user")]
    Cancel,
}

/// Complete state of the eventloop
pub struct EventLoop<R: Requests> {
    /// Options of the current mqtt connection
    pub options: MqttOptions,
    /// Current state of the connection
    pub state: MqttState,
    /// Request stream
    pub requests: Throttle<R>,
    /// Pending publishes from last session
    pending_pub: VecDeque<Publish>,
    /// Pending releases from last session
    pending_rel: VecDeque<PacketIdentifier>,
    /// Flag to disable pending branch while polling
    has_pending: bool,
    /// Network connection to the broker
    network: Option<Framed<Box<dyn N>, MqttCodec>>,
    /// Keep alive time
    keepalive_timeout: Delay,
    /// Handle to read cancellation requests
    cancel_rx: Receiver<()>,
    /// Handle to send cancellation requests (and drops)
    cancel_tx: Option<Sender<()>>,
    /// Delay between reconnection (after a failure)
    reconnection_delay: Duration
}

impl<R: Requests> EventLoop<R> {
    /// Returns an object which encompasses state of the connection.
    /// Use this to create a `Stream` with `connect().await?` method and poll it with tokio.
    ///
    /// The choice of separating `MqttEventLoop` and `stream` methods is to get access to the
    /// internal state and mqtt options after the work with the `Stream` is done or stopped.
    /// This is useful in scenarios like shutdown where the current state should be persisted or
    /// during reconnection when the state from last disconnection should be resumed.
    /// For a similar reason, requests are also initialized as part of this method to reuse same
    /// request stream while retrying after the previous `Stream` has stopped
    /// ```ignore
    /// let mut eventloop = eventloop(options, requests);
    /// loop {
    ///     let mut stream = eventloop.connect(reconnection_options).await.unwrap();
    ///     while let Some(notification) = stream.next().await() {}
    /// }
    /// ```
    /// When mqtt `stream` ends due to critical errors (like auth failure), user has a choice to
    /// access and update `options`, `state` and `requests`.
    /// For example, state and requests can be used to save state to disk before shutdown.
    /// Options can be used to update gcp iotcore password
    pub async fn new(options: MqttOptions, requests: R) -> EventLoop<R> {
        let keepalive = options.keep_alive;
        let (cancel_tx, cancel_rx) = bounded(5);
        let requests = time::throttle(options.throttle, requests);

        EventLoop {
            options,
            state: MqttState::new(),
            requests,
            pending_pub: VecDeque::new(),
            pending_rel: VecDeque::new(),
            has_pending: false,
            network: None,
            keepalive_timeout: time::delay_for(keepalive),
            cancel_rx,
            cancel_tx: Some(cancel_tx),
            reconnection_delay: Duration::from_secs(0)
        }
    }

    pub fn set_reconnection_delay(&mut self, delay: Duration) {
        self.reconnection_delay = delay;
    }

    pub fn take_cancel_handle(&mut self) -> Option<Sender<()>> {
        self.cancel_tx.take()
    }

    /// Next notification or outgoing request
    /// This method used to return only incoming network notification while silently looping through
    /// outgoing requests. Internal loops inside async functions are risky. Imagine this function
    /// with 100 requests and 1 incoming packet. If this `Stream` (which internally loops) is
    /// selected with other streams, can potentially do more internal polling (if the socket is ready)
    pub async fn poll(&mut self) -> Result<(Option<Incoming>, Option<Outgoing>), ConnectionError> {
        let (notification, outpacket) = self.select().await?;
        let mut out = None;

        // write the reply back to the network. flush??
        if let Some(packet) = outpacket {
            out = Some(outgoing(&packet));
            self.network.as_mut().unwrap().send(packet).await?;
        }

        Ok((notification, out))
    }

    /// Last session might contain packets which aren't acked. MQTT says these packets should be
    /// republished in the next session. We save all such pending packets here.
    fn populate_pending(&mut self) {
        let mut pending_pub = mem::replace(&mut self.state.outgoing_pub, VecDeque::new());
        self.pending_pub.append(&mut pending_pub);

        let mut pending_rel = mem::replace(&mut self.state.outgoing_rel, VecDeque::new());
        self.pending_rel.append(&mut pending_rel);

        if !self.pending_pub.is_empty() || !self.pending_rel.is_empty() {
            self.has_pending = true;
        }
    }

    /// Select on network and requests and generate keepalive pings when necessary
    async fn select(&mut self) -> Result<(Option<Incoming>, Option<Packet>), ConnectionError> {
        let network = match &mut self.network {
            Some(network) => network,
            None => return Err(ConnectionError::NoLink)
        };

        let inflight_full = self.state.outgoing_pub.len() >= self.options.inflight;
        let o  = select! {
            // Pull next packet from network
            o = network.next() => match o {
                Some(packet) => self.state.handle_incoming_packet(packet?)?,
                None => return Err(ConnectionError::StreamDone)
            },
            // Pull next request from user requests channel.
            // we read user requests only when we are done sending pending
            // packets and inflight queue has space (for flow control)
            o = self.requests.next(), if !inflight_full && !self.has_pending => match o {
                Some(request) => self.state.handle_outgoing_packet(request.into())?,
                None => return Err(ConnectionError::RequestsDone),
            },
            // Handle the next pending packet from previous session. Disable
            // this branch when done with all the pending packets
            o = next_pending(self.options.throttle, &mut self.pending_pub, &mut self.pending_rel), if self.has_pending => match o {
                Some(packet) => self.state.handle_outgoing_packet(packet)?,
                None => {
                    self.has_pending = false;
                    // this is the only place where poll returns a spurious (None, None)
                    (None, None)
                }
            },
            // We generate pings irrespective of network activity. This keeps the ping
            // logic simple. We can change this behavior in future if necessary
            _ = &mut self.keepalive_timeout => {
                self.keepalive_timeout.reset(Instant::now() + self.options.keep_alive);
                let notification = None;
                let packet = Packet::Pingreq;
                self.state.handle_outgoing_packet(packet)?;
                let packet = Some(Packet::Pingreq);
                (notification, packet)
            }
            // cancellation requests to stop the polling
            _ = self.cancel_rx.next() => {
                return Err(ConnectionError::Cancel)
            }
        };

        Ok(o)
    }
}



impl<R: Requests> EventLoop<R> {
    pub async fn connect_or_cancel(&mut self) -> Result<(), ConnectionError> {
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
    pub async fn connect(&mut self) -> Result<(), ConnectionError> {
        self.state.await_pingresp = false;

        // connect to the broker
        let mut network = match self.network_connect().await {
            Ok(network) => network,
            Err(e) => {
                time::delay_for(self.reconnection_delay).await;
                return Err(e)
            }
        };

        // make MQTT connection request (which internally awaits for ack)
        if let Err(e) = self.mqtt_connect(&mut network).await {
            time::delay_for(self.reconnection_delay).await;
            return Err(e)
        }


        // move pending messages from state to eventloop and create a pending stream of requests
        self.network = Some(network);
        self.populate_pending();
        Ok(())
    }

    async fn network_connect(&self) -> Result<Framed<Box<dyn N>, MqttCodec>, ConnectionError> {
        let network = time::timeout(Duration::from_secs(5), async {
            let network = if self.options.ca.is_some() {
                let o = network::tls_connect(&self.options).await?;
                let o = Box::new(o) as Box<dyn N>;
                Framed::new(o, MqttCodec::new(self.options.max_packet_size))
            } else {
                let o = network::tcp_connect(&self.options).await?;
                let o = Box::new(o) as Box<dyn N>;
                Framed::new(o, MqttCodec::new(self.options.max_packet_size))
            };

            Ok::<Framed<Box<dyn N>, MqttCodec>, ConnectionError>(network)
        })
        .await??;

        Ok(network)
    }

    async fn mqtt_connect(&mut self, mut network: impl Network) -> Result<(), ConnectionError> {
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
            network.send(Packet::Connect(connect)).await?;
            self.state.handle_outgoing_connect()?;
            Ok::<_, ConnectionError>(())
        })
        .await??;

        // wait for 'timeout' time to validate connack
        time::timeout(Duration::from_secs(5), async {
            let packet = match network.next().await {
                Some(o) => o?,
                None => return Err(ConnectionError::StreamDone),
            };
            self.state.handle_incoming_connack(packet)?;
            Ok::<_, ConnectionError>(())
        })
        .await??;

        Ok(())
    }
}

/// Returns the next pending packet asyncronously to be used in select!
/// This is a synchronous function but made async to make it fit in select!
async fn next_pending(
    delay: Duration,
    pending_pub: &mut VecDeque<Publish>,
    pending_rel: &mut VecDeque<PacketIdentifier>
) -> Option<Packet> {
    time::delay_for(delay).await;

    // publishes are prioritized over releases
    // goto releases only after done with publishes
    if let Some(p) = pending_pub.pop_front() {
        return Some(Packet::Publish(p))
    }

    if let Some(p) = pending_rel.pop_front() {
        return Some(Packet::Pubrel(p))
    }

    None
}

fn outgoing(packet: &Packet) -> Outgoing {
    match packet {
        Packet::Publish(publish) => Outgoing::Publish(publish.pkid.clone()),
        Packet::Puback(pkid) => Outgoing::Puback(*pkid),
        Packet::Pubrec(pkid) => Outgoing::Pubrec(*pkid),
        Packet::Pubcomp(pkid) => Outgoing::Pubcomp(*pkid),
        Packet::Subscribe(subscribe) => Outgoing::Subscribe(subscribe.pkid),
        Packet::Unsubscribe(unsubscribe) => Outgoing::Unsubscribe(unsubscribe.pkid),
        Packet::Pingreq => Outgoing::Pingreq,
        Packet::Disconnect => Outgoing::Disconnect,
        packet => panic!("Invalid outgoing packet = {:?}", packet)
    }
}

impl From<Request> for Packet {
    fn from(item: Request) -> Self {
        match item {
            Request::Publish(publish) => Packet::Publish(publish),
            Request::Disconnect => Packet::Disconnect,
            Request::Subscribe(subscribe) => Packet::Subscribe(subscribe),
            Request::Unsubscribe(unsubscribe) => Packet::Unsubscribe(unsubscribe),
            _ => unimplemented!(),
        }
    }
}

use tokio::io::{AsyncRead, AsyncWrite};

pub trait N: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T> N for T where T: AsyncRead + AsyncWrite + Unpin + Send + Sync {}

pub trait Network: Stream<Item = Result<Packet, rumq_core::Error>> + Sink<Packet, Error = io::Error> + Unpin + Send {}
impl<T> Network for T where T: Stream<Item = Result<Packet, rumq_core::Error>> + Sink<Packet, Error = io::Error> + Unpin + Send {}

pub trait Requests: Stream<Item = Request> + Unpin + Send {}
impl<T> Requests for T where T: Stream<Item = Request> + Unpin + Send {}

#[cfg(test)]
mod test {
    use super::broker::*;
    use crate::state::StateError;
    use crate::{ConnectionError, MqttOptions, Incoming, Outgoing, Request};
    use futures_util::stream::StreamExt;
    use rumq_core::mqtt4::*;
    use std::time::{Duration, Instant};
    use tokio::{task, time};
    use async_channel::{bounded, Sender, Receiver};

    async fn start_requests(count: u8, qos: QoS, delay: u64, mut requests_tx: Sender<Request>) {
        for i in 0..count {
            let topic = "hello/world".to_owned();
            let payload = vec![i, 1, 2, 3];

            let publish = Publish::new(topic, qos, payload);
            let request = Request::Publish(publish);
            let _ = requests_tx.send(request).await;
            time::delay_for(Duration::from_secs(delay)).await;
        }
    }

    async fn eventloop(options: MqttOptions, requests: Receiver<Request>, reconnect: bool) {
        time::delay_for(Duration::from_secs(1)).await;
        let mut eventloop = super::EventLoop::new(options, requests).await;
        'reconnect: loop {
            eventloop.connect().await;
            loop {
                let o = eventloop.poll().await;
                // println!("{:?}", o);
                match o {
                    Ok(v) => continue,
                    Err(e) if reconnect => continue 'reconnect,
                    Err(e) => break 'reconnect,
                }
            }
        }

    }

    #[tokio::test]
    async fn connection_should_timeout_on_time() {
        let (_requests_tx, requests_rx) = bounded(5);

        task::spawn(async move {
            let _broker = Broker::new(1880, false).await;
            time::delay_for(Duration::from_secs(10)).await;
        });

        time::delay_for(Duration::from_secs(1)).await;
        let options = MqttOptions::new("dummy", "127.0.0.1", 1880);
        let mut eventloop = super::EventLoop::new(options, requests_rx).await;

        let start = Instant::now();
        let o = eventloop.connect().await;
        let elapsed = start.elapsed();

        match o {
            Ok(_) => assert!(false),
            Err(ConnectionError::Timeout(_)) => assert!(true),
            Err(_) => assert!(false),
        }

        assert_eq!(elapsed.as_secs(), 5);
    }

    // TODO: This tests fails on ci with elapsed time of 955 milliseconds. This drift
    // (less than set delay) isn't observed in other tests
    #[tokio::test]
    async fn throttled_requests_works_with_correct_delays_between_requests() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1882);
        options.set_throttle(Duration::from_secs(1));
        let options2 = options.clone();

        // start sending requests
        let (requests_tx, requests_rx) = bounded(5);
        task::spawn(async move {
            start_requests(10, QoS::AtLeastOnce, 0, requests_tx).await;
        });

        // start the eventloop
        task::spawn(async move {
            eventloop(options, requests_rx, false).await;
        });

        let mut broker = Broker::new(1882, true).await;
        // check incoming rate at th broker
        for i in 0..10 {
            let start = Instant::now();
            let _ = broker.read_packet().await;
            let elapsed = start.elapsed();

            if i > 0 {
                dbg!(elapsed.as_millis());
                assert_eq!(elapsed.as_secs(), options2.throttle.as_secs())
            }
        }
    }

        #[tokio::test]
        async fn idle_connection_triggers_pings_on_time() {
            let mut options = MqttOptions::new("dummy", "127.0.0.1", 1885);
            options.set_keep_alive(5);
            let keep_alive = options.keep_alive();

            // start sending requests
            let (_requests_tx, requests_rx) = bounded(5);
            // start the eventloop
            task::spawn(async move {
                eventloop(options, requests_rx, false).await;
            });

            let mut broker = Broker::new(1885, true).await;

            // check incoming rate at th broker
            let start = Instant::now();
            let mut ping_received = false;

            for _ in 0..10 {
                let packet = broker.read_packet().await;
                let elapsed = start.elapsed();
                if packet == Packet::Pingreq {
                    ping_received = true;
                    assert_eq!(elapsed.as_secs(), keep_alive.as_secs());
                    break;
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
                let (requests_tx, requests_rx) = bounded(5);
                task::spawn(async move {
                    start_requests(10, QoS::AtMostOnce, 1, requests_tx).await;
                });

                // start the eventloop
                task::spawn(async move {
                    eventloop(options, requests_rx, false).await;
                });

                let mut broker = Broker::new(1886, true).await;

                let start = Instant::now();
                let mut ping_received = false;

                for _ in 0..10 {
                    let packet = broker.read_packet_and_respond().await;
                    let elapsed = start.elapsed();
                    if packet == Packet::Pingreq {
                        ping_received = true;
                        assert_eq!(elapsed.as_secs(), keep_alive.as_secs());
                        break;
                    }
                }

                assert!(ping_received);
            }

            #[tokio::test]
            async fn some_incoming_and_no_outgoing_packets_should_trigger_pings_on_time() {
                let mut options = MqttOptions::new("dummy", "127.0.0.1", 2000);
                options.set_keep_alive(5);
                let keep_alive = options.keep_alive();

                let (_requests_tx, requests_rx) = bounded(5);
                task::spawn(async move {
                    eventloop(options, requests_rx, false).await;
                });

                let mut broker = Broker::new(2000, true).await;
                let start = Instant::now();
                broker.start_publishes(5, QoS::AtMostOnce, Duration::from_secs(1)).await;
                let packet = broker.read_packet().await;
                assert_eq!(packet, Packet::Pingreq);
                assert_eq!(start.elapsed().as_secs(), keep_alive.as_secs());
            }

    /*
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
            let (_requests_tx, requests_rx) = bounded(5);
            let mut eventloop = super::EventLoop::new(options, requests_rx).await;
            eventloop.connect().await.unwrap();

            let start = Instant::now();

            loop {
                match eventloop.poll().await {
                    Err(ConnectionError::MqttState(StateError::AwaitPingResp)) => assert_eq!(start.elapsed().as_secs(), 10),
                    Ok((_, outgoing)) => assert_eq!(Some(Outgoing::Pingreq), outgoing),
                    v => panic!("Expecting pingreq or pingresp error. Found = {:?}", v),
                }
            }
        }
     */

        #[tokio::test]
        async fn requests_are_blocked_after_max_inflight_queue_size() {
            let mut options = MqttOptions::new("dummy", "127.0.0.1", 1887);
            options.set_inflight(5);
            let inflight = options.inflight();

            // start sending qos0 publishes. this makes sure that there is
            // outgoing activity but no incoming activity
            let (requests_tx, requests_rx) = bounded(5);
            task::spawn(async move {
                start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
            });

            // start the eventloop
            task::spawn(async move {
                eventloop(options, requests_rx, false).await;
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

            let (requests_tx, requests_rx) = bounded(5);
            task::spawn(async move {
                start_requests(5, QoS::AtLeastOnce, 1, requests_tx).await;
                time::delay_for(Duration::from_secs(60)).await;
            });

            // start the eventloop
            task::spawn(async move {
                eventloop(options, requests_rx, true).await;
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
            // packet 4
            let packet = broker.read_publish().await;
            assert!(packet.is_none());
            // ack packet 1 and we should receiver packet 4
            broker.ack(PacketIdentifier(1)).await;
            let packet = broker.read_publish().await;
            assert!(packet.is_some());
            // packet 5
            let packet = broker.read_publish().await;
            assert!(packet.is_none());
            // ack packet 2 and we should receiver packet 5
            broker.ack(PacketIdentifier(2)).await;
            let packet = broker.read_publish().await;
            assert!(packet.is_some());
        }

        #[tokio::test]
        async fn reconnection_resumes_from_the_previous_state() {
            let options = MqttOptions::new("dummy", "127.0.0.1", 1889);

            // start sending qos0 publishes. this makes sure that there is
            // outgoing activity but no incomin activity
            let (requests_tx, requests_rx) = bounded(5);
            task::spawn(async move {
                start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
                time::delay_for(Duration::from_secs(10)).await;
            });

            // start the eventloop
            task::spawn(async move {
                eventloop(options, requests_rx, true).await;
            });

            // broker connection 1
            {
                let mut broker = Broker::new(1889, true).await;
                for i in 1..=2 {
                    let packet = broker.read_publish().await;
                    assert_eq!(PacketIdentifier(i), packet.unwrap());
                    broker.ack(packet.unwrap()).await;
                }
            }

            // broker connection 2
            {
                let mut broker = Broker::new(1889, true).await;
                for i in 3..=4 {
                    let packet = broker.read_publish().await;
                    assert_eq!(PacketIdentifier(i), packet.unwrap());
                    broker.ack(packet.unwrap()).await;
                }
            }
        }


        #[tokio::test]
        async fn reconnection_resends_unacked_packets_from_the_previous_connection_before_sending_current_connection_requests() {
            let options = MqttOptions::new("dummy", "127.0.0.1", 1890);

            // start sending qos0 publishes. this makes sure that there is
            // outgoing activity but no incoming activity
            let (requests_tx, requests_rx) = bounded(5);
            task::spawn(async move {
                start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
                time::delay_for(Duration::from_secs(10)).await;
            });

            // start the client eventloop
            task::spawn(async move {
                eventloop(options, requests_rx, true).await;
            });

            // broker connection 1. receive but don't ack
            {
                let mut broker = Broker::new(1890, true).await;
                for i in 1..=2 {
                    let packet = broker.read_publish().await;
                    assert_eq!(PacketIdentifier(i), packet.unwrap());
                }
            }

            // broker connection 2 receives from scratch
            {
                let mut broker = Broker::new(1890, true).await;
                for i in 1..=6 {
                    let packet = broker.read_publish().await;
                    assert_eq!(PacketIdentifier(i), packet.unwrap());
                }
            }
        }
    }

#[cfg(test)]
mod broker {
    use futures_util::sink::SinkExt;
    use rumq_core::mqtt4::*;
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::select;
    use tokio::stream::StreamExt;
    use tokio::time;
    use tokio_util::codec::Framed;

    pub struct Broker {
        framed: Framed<TcpStream, codec::MqttCodec>,
    }

    impl Broker {
        /// Create a new broker which accepts 1 mqtt connection
        pub async fn new(port: u16, send_connack: bool) -> Broker {
            let addr = format!("127.0.0.1:{}", port);
            let mut listener = TcpListener::bind(&addr).await.unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(stream, codec::MqttCodec::new(1024 * 1024));

            let packet = framed.next().await.unwrap().unwrap();
            if let Packet::Connect(_) = packet {
                if send_connack {
                    let connack = Connack::new(ConnectReturnCode::Accepted, false);
                    let packet = Packet::Connack(connack);
                    framed.send(packet).await.unwrap();
                }
            } else {
                panic!("Expecting connect packet");
            }

            Broker { framed }
        }

        // Reads a publish packet from the stream with 2 second timeout
        pub async fn read_publish(&mut self) -> Option<PacketIdentifier> {
            let packet = time::timeout(Duration::from_secs(2), async { self.framed.next().await.unwrap() });
            match packet.await {
                Ok(Ok(Packet::Publish(publish))) => publish.pkid,
                Ok(Ok(packet)) => panic!("Expecting a publish. Received = {:?}", packet),
                Ok(Err(e)) => panic!("Error = {:?}", e),
                // timedout
                Err(_) => None,
            }
        }

        /// Reads next packet from the stream
        pub async fn read_packet(&mut self) -> Packet {
            let packet = time::timeout(Duration::from_secs(30), async { self.framed.next().await.unwrap() });
            packet.await.unwrap().unwrap()
        }

        pub async fn read_packet_and_respond(&mut self) -> Packet {
            let packet = time::timeout(Duration::from_secs(30), async { self.framed.next().await.unwrap() });
            let packet = packet.await.unwrap().unwrap();

            match packet.clone() {
                Packet::Publish(publish) => {
                    if let Some(pkid) = publish.pkid {
                        self.framed.send(Packet::Puback(pkid)).await.unwrap();
                    }
                }
                _ => (),
            }

            packet
        }

        /// Reads next packet from the stream
        pub async fn blackhole(&mut self) -> Packet {
            loop {
                let _packet = self.framed.next().await.unwrap().unwrap();
            }
        }

        /// Sends an acknowledgement
        pub async fn ack(&mut self, pkid: PacketIdentifier) {
            let packet = Packet::Puback(pkid);
            self.framed.send(packet).await.unwrap();
            self.framed.flush().await.unwrap();
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
                        let packet = Packet::Publish(publish);
                        self.framed.send(packet).await.unwrap();
                    }
                    packet = self.framed.next() => match packet.unwrap().unwrap() {
                        Packet::Pingreq => self.framed.send(Packet::Pingresp).await.unwrap(),
                        _ => ()
                    }
                }
            }
        }
    }
}

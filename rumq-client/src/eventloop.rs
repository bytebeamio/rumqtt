use crate::state::{MqttState, StateError};
use crate::MqttOptions;
use crate::{network, Notification, Request};

use async_stream::stream;
use futures_util::sink::{Sink, SinkExt};
use futures_util::stream::{Stream, StreamExt};
use rumq_core::mqtt4::codec::MqttCodec;
use rumq_core::mqtt4::{Connect, Packet, PacketIdentifier, Publish};
use tokio::select;
use tokio::stream::iter;
use tokio::time::{self, Delay, Elapsed, Instant};
use tokio_util::codec::Framed;

use std::collections::VecDeque;
use std::io;
use std::mem;
use std::time::Duration;

/// Complete state of the eventloop
pub struct MqttEventLoop {
    // intermediate state of the eventloop. this is set
    // by the state machine when the streaming ends
    /// Options of the current mqtt connection
    pub options: MqttOptions,
    /// Current state of the connection
    pub state: MqttState,
    /// Request stream
    pub requests: Box<dyn Requests>,
    pending_pub: VecDeque<Publish>,
    pending_rel: VecDeque<PacketIdentifier>,
}

/// Critical errors during eventloop polling
#[derive(Debug, thiserror::Error)]
pub enum EventLoopError {
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
}

/// Returns an object which encompasses state of the connection.
/// Use this to create a `Stream` with `stream()` method and poll it with tokio.
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
pub fn eventloop(options: MqttOptions, requests: impl Requests + 'static) -> MqttEventLoop {
    MqttEventLoop {
        options: options,
        state: MqttState::new(),
        requests: Box::new(requests),
        pending_pub: VecDeque::new(),
        pending_rel: VecDeque::new(),
    }
}

impl MqttEventLoop {
    /// Connects to the broker and returns a stream that does everything MQTT.
    /// This stream internally processes requests from the request stream provided to the eventloop
    /// while also consuming byte stream from the network and yielding mqtt packets as the output of
    /// the stream
    pub async fn connect<'eventloop>(&'eventloop mut self) -> Result<impl Stream<Item = Notification> + 'eventloop, EventLoopError> {
        self.state.await_pingresp = false;

        // connect to the broker
        let mut network = self.network_connect().await?;
        self.mqtt_connect(&mut network).await?;

        // move pending messages from state to eventloop and create a pending stream of requests
        self.populate_pending();

        // create mqtt stream
        let stream = stream! {
            let pending_rel = iter(self.pending_rel.drain(..)).map(Packet::Pubrec);
            let mut pending = iter(self.pending_pub.drain(..)).map(Packet::Publish).chain(pending_rel);
            let mut pending = time::throttle(self.options.throttle, pending);
            let mut requests = time::throttle(self.options.throttle, &mut self.requests);

            let mut timeout = time::delay_for(self.options.keep_alive);
            let mut inout_marker = 0;
            let mut pending_done = false;

            loop {
                let inflight_full = self.state.outgoing_pub.len() >= self.options.inflight;
                let o = select(
                    &mut network,
                    &mut pending,
                    &mut requests,
                    &mut self.state,
                    self.options.keep_alive,
                    &mut inout_marker,
                    inflight_full,
                    &mut pending_done,
                    &mut timeout
                ).await;

                let (notification, outpacket) = match o {
                    Ok((n, p)) => (n, p),
                    Err(e) => {
                        yield Notification::Abort(e.into());
                        break
                    }
                };

                // write the reply back to the network
                // FIXME flush??
                if let Some(p) = outpacket {
                    if let Err(e) = network.send(p).await {
                        yield Notification::Abort(e.into());
                        break
                    }
                }

                // yield the notification to the user
                if let Some(n) = notification { yield n }
            }
        };

        Ok(Box::pin(stream))
    }

    fn populate_pending(&mut self) {
        let mut pending_pub = mem::replace(&mut self.state.outgoing_pub, VecDeque::new());
        self.pending_pub.append(&mut pending_pub);

        let mut pending_rel = mem::replace(&mut self.state.outgoing_rel, VecDeque::new());
        self.pending_rel.append(&mut pending_rel);
    }
}

async fn select<R: Requests, P: Packets>(
    network: &mut Framed<Box<dyn N>, MqttCodec>,
    mut pending: P,
    mut requests: R,
    state: &mut MqttState,
    keepalive: Duration,
    inout_marker: &mut u8,
    inflight_full: bool,
    pending_done: &mut bool,
    mut timeout: &mut Delay,
) -> Result<(Option<Notification>, Option<Packet>), EventLoopError> {
    // Select on network and requests and orchestrate keep alive delay timer based on it
    let ticker = &mut timeout;
    let o = select! {
        o = network.next() => match o {
            Some(packet) => state.handle_incoming_packet(packet?)?,
            None => return Err(EventLoopError::StreamDone)
        },
        o = requests.next(), if !inflight_full && *pending_done => match o {
            Some(request) => state.handle_outgoing_packet(request.into())?,
            None => return Err(EventLoopError::RequestsDone),
        },
        o = pending.next(), if !*pending_done => match o {
            Some(packet) => state.handle_outgoing_packet(packet)?,
            None => {
                *pending_done = true;
                (None, None)
            }
        },
        _ = ticker => {
            timeout.reset(Instant::now() + keepalive);
            *inout_marker = 0;
            let notification = None;
            let packet = Packet::Pingreq;
            state.handle_outgoing_packet(packet)?;
            let packet = Some(Packet::Pingreq);
            return Ok((notification, packet))
        }
    };

    let (notification, packet) = (o.0.is_some(), o.1.is_some());
    match (notification, packet) {
        (true, true) => *inout_marker |= 3,
        (true, false) => *inout_marker |= 1,
        (false, true) => *inout_marker |= 2,
        (false, false) => (),
    }

    // Extend the keep alive ping window
    if *inout_marker == 3 {
        timeout.reset(Instant::now() + keepalive);
        *inout_marker = 0;
    }

    Ok(o)
}

impl MqttEventLoop {
    async fn network_connect(&self) -> Result<Framed<Box<dyn N>, MqttCodec>, EventLoopError> {
        let network = time::timeout(Duration::from_secs(5), async {
            let network = if self.options.ca.is_some() {
                let o = network::tls_connect(&self.options).await?;
                let o = Box::new(o) as Box<dyn N>;
                Framed::new(o, MqttCodec::new(10 * 1024))
            } else {
                let o = network::tcp_connect(&self.options).await?;
                let o = Box::new(o) as Box<dyn N>;
                Framed::new(o, MqttCodec::new(10 * 1024))
            };

            Ok::<Framed<Box<dyn N>, MqttCodec>, EventLoopError>(network)
        })
        .await??;

        Ok(network)
    }

    async fn mqtt_connect(&mut self, mut network: impl Network) -> Result<(), EventLoopError> {
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
            Ok::<_, EventLoopError>(())
        })
        .await??;

        // wait for 'timeout' time to validate connack
        time::timeout(Duration::from_secs(5), async {
            let packet = match network.next().await {
                Some(o) => o?,
                None => return Err(EventLoopError::StreamDone),
            };
            self.state.handle_incoming_connack(packet)?;
            Ok::<_, EventLoopError>(())
        })
        .await??;

        Ok(())
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

pub trait N: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> N for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub trait Network: Stream<Item = Result<Packet, rumq_core::Error>> + Sink<Packet, Error = io::Error> + Unpin + Send {}
impl<T> Network for T where T: Stream<Item = Result<Packet, rumq_core::Error>> + Sink<Packet, Error = io::Error> + Unpin + Send {}

pub trait Requests: Stream<Item = Request> + Unpin + Send + Sync {}
impl<T> Requests for T where T: Stream<Item = Request> + Unpin + Send + Sync {}

pub trait Packets: Stream<Item = Packet> + Unpin + Send + Sync {}
impl<T> Packets for T where T: Stream<Item = Packet> + Unpin + Send + Sync {}

#[cfg(test)]
mod test {
    use super::broker::*;
    use crate::state::StateError;
    use crate::{EventLoopError, MqttOptions, Notification, Request};
    use futures_util::stream::StreamExt;
    use rumq_core::mqtt4::*;
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc::{channel, Sender};
    use tokio::{task, time};

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

    #[tokio::test]
    async fn connection_should_timeout_on_time() {
        let (_requests_tx, requests_rx) = channel(5);

        task::spawn(async move {
            let _broker = Broker::new(1880, false).await;
            time::delay_for(Duration::from_secs(10)).await;
        });

        time::delay_for(Duration::from_secs(1)).await;
        let options = MqttOptions::new("dummy", "127.0.0.1", 1880);
        let mut eventloop = super::eventloop(options, requests_rx);

        let start = Instant::now();
        let o = eventloop.connect().await;
        let elapsed = start.elapsed();

        match o {
            Ok(_) => assert!(false),
            Err(super::EventLoopError::Timeout(_)) => assert!(true),
            Err(_) => assert!(false),
        }

        assert_eq!(elapsed.as_secs(), 5);
    }

    // TODO: This tests fails on ci with elapsed time of 955 milliseconds. This drift
    // (less than set delay) isn't observed in other tests
    #[tokio::test]
    async fn throttled_requests_works_with_correct_delays_between_requests() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1881);
        options.set_throttle(Duration::from_secs(1));
        let options2 = options.clone();

        // start sending requests
        let (requests_tx, requests_rx) = channel(5);
        task::spawn(async move {
            start_requests(10, QoS::AtLeastOnce, 0, requests_tx).await;
        });

        // start the eventloop
        task::spawn(async move {
            time::delay_for(Duration::from_secs(1)).await;
            let mut eventloop = super::eventloop(options, requests_rx);
            let mut stream = eventloop.connect().await.unwrap();

            while let Some(_) = stream.next().await {}
        });

        let mut broker = Broker::new(1881, true).await;

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
        let (_requests_tx, requests_rx) = channel(5);
        // start the eventloop
        task::spawn(async move {
            time::delay_for(Duration::from_secs(1)).await;
            let mut eventloop = super::eventloop(options, requests_rx);
            let mut stream = eventloop.connect().await.unwrap();

            while let Some(_) = stream.next().await {}
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
        let (requests_tx, requests_rx) = channel(5);
        task::spawn(async move {
            start_requests(10, QoS::AtMostOnce,1,  requests_tx).await;
        });

        // start the eventloop
        task::spawn(async move {
            time::delay_for(Duration::from_secs(1)).await;
            let mut eventloop = super::eventloop(options, requests_rx);
            let mut stream = eventloop.connect().await.unwrap();

            while let Some(_) = stream.next().await {}
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

        task::spawn(async move {
            time::delay_for(Duration::from_secs(1)).await;
            let (_requests_tx, requests_rx) = channel(5);
            let mut eventloop = super::eventloop(options, requests_rx);
            let mut stream = eventloop.connect().await.unwrap();
            while let Some(_) = stream.next().await {}
        });

        let mut broker = Broker::new(2000, true).await;
        broker.start_publishes(5, QoS::AtMostOnce, Duration::from_secs(1)).await;
        let packet = broker.read_packet().await;
        assert_eq!(packet, Packet::Pingreq);
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
        let (_requests_tx, requests_rx) = channel(5);
        let mut eventloop = super::eventloop(options, requests_rx);
        let mut stream = eventloop.connect().await.unwrap();

        let start = Instant::now();
        match stream.next().await.unwrap() {
            Notification::Abort(EventLoopError::MqttState(StateError::AwaitPingResp)) => assert_eq!(start.elapsed().as_secs(), 10),
            _ => panic!("Expecting await pingresp error"),
        }
    }

    #[tokio::test]
    async fn requests_are_blocked_after_max_inflight_queue_size() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1887);
        options.set_inflight(5);
        let inflight = options.inflight();

        // start sending qos0 publishes. this makes sure that there is
        // outgoing activity but no incomin activity
        let (requests_tx, requests_rx) = channel(5);
        task::spawn(async move {
            start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
        });

        // start the eventloop
        task::spawn(async move {
            time::delay_for(Duration::from_secs(1)).await;
            let mut eventloop = super::eventloop(options, requests_rx);
            let mut stream = eventloop.connect().await.unwrap();

            while let Some(_) = stream.next().await {}
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

        let (requests_tx, requests_rx) = channel(5);
        task::spawn(async move {
            start_requests(5, QoS::AtLeastOnce, 1, requests_tx).await;
            time::delay_for(Duration::from_secs(60)).await;
        });

        // start the eventloop
        task::spawn(async move {
            time::delay_for(Duration::from_secs(1)).await;
            let mut eventloop = super::eventloop(options, requests_rx);
            let mut stream = eventloop.connect().await.unwrap();
            while let Some(_p) = stream.next().await {}
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
        let (requests_tx, requests_rx) = channel(5);
        task::spawn(async move {
            start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
            time::delay_for(Duration::from_secs(10)).await;
        });

        // start the eventloop
        task::spawn(async move {
            time::delay_for(Duration::from_secs(1)).await;
            let mut eventloop = super::eventloop(options, requests_rx);

            loop {
                let mut stream = eventloop.connect().await.unwrap();
                while let Some(_) = stream.next().await {}
            }
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
        // outgoing activity but no incomin activity
        let (requests_tx, requests_rx) = channel(5);
        task::spawn(async move {
            start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
            time::delay_for(Duration::from_secs(10)).await;
        });

        // start the client eventloop
        task::spawn(async move {
            time::delay_for(Duration::from_secs(1)).await;
            let mut eventloop = super::eventloop(options, requests_rx);

            loop {
                let mut stream = eventloop.connect().await.unwrap();
                while let Some(_) = stream.next().await {}
            }
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
                Err(_)  => None,
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
                Packet::Publish(publish) => if let Some(pkid) = publish.pkid {
                    self.framed.send(Packet::Puback(pkid)).await.unwrap();
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

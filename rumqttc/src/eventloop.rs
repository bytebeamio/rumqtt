#[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
use crate::tls;
use crate::{framed::Network, Transport};
use crate::{Incoming, MqttState, Packet, Request, StateError};
use crate::{MqttOptions, Outgoing};

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
    #[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
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
    requests_rx: Receiver<Request>,
    /// Requests handle to send requests
    pub(crate) requests_tx: Sender<Request>,
    /// Pending packets from last session
    pub pending: IntoIter<Request>,
    /// Network connection to the broker
    network: Option<Network>,
    /// Keep alive time
    keepalive_timeout: Option<Pin<Box<Sleep>>>,
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
        #[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
        Transport::Tls(tls_config) => {
            let socket = tls::tls_connect(&options.broker_addr, options.port, &tls_config).await?;
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

            let connector = tls::rustls_connector(&tls_config).await?;

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

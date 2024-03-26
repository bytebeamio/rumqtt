use super::framed::Network;
use super::mqttbytes::v5::*;
use super::{Incoming, MqttOptions, MqttState, Outgoing, Request, StateError, Transport};
use crate::eventloop::socket_connect;
use crate::framed::AsyncReadWrite;

use flume::Receiver;
use futures_util::{Stream, StreamExt};
use tokio::select;
use tokio::time::Interval;
use tokio::time::{self, error::Elapsed};

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use super::mqttbytes::v5::ConnectReturnCode;

#[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
use crate::tls;

#[cfg(unix)]
use {std::path::Path, tokio::net::UnixStream};

#[cfg(feature = "websocket")]
use {
    crate::websockets::{split_url, validate_response_headers, UrlError},
    async_tungstenite::tungstenite::client::IntoClientRequest,
    ws_stream_tungstenite::WsStream,
};

#[cfg(feature = "proxy")]
use crate::proxy::ProxyError;

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
    #[error("Connection refused, return code: `{0:?}`")]
    ConnectionRefused(ConnectReturnCode),
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Expected ConnAck packet, received: {0:?}")]
    NotConnAck(Box<Packet>),
    #[error("Requests done")]
    RequestsDone,
    #[cfg(feature = "websocket")]
    #[error("Invalid Url: {0}")]
    InvalidUrl(#[from] UrlError),
    #[cfg(feature = "proxy")]
    #[error("Proxy Connect: {0}")]
    Proxy(#[from] ProxyError),
    #[cfg(feature = "websocket")]
    #[error("Websocket response validation error: ")]
    ResponseValidation(#[from] crate::websockets::ValidationError),
}

/// Eventloop with all the state of a connection
pub struct EventLoop {
    /// Options of the current mqtt connection
    pub options: MqttOptions,
    /// Current state of the connection
    pub state: MqttState,
    /// Batch size
    batch_size: usize,
    /// Request stream
    requests: Receiver<Request>,
    /// Pending requests from the last session
    pending: IntervalQueue<Request>,
    /// Network connection to the broker
    network: Option<Network>,
    /// Keep alive time
    keepalive_interval: Interval,
}

/// Events which can be yielded by the event loop
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Incoming(Incoming),
    Outgoing(Outgoing),
}

impl EventLoop {
    /// New MQTT `EventLoop`
    pub(crate) fn new(options: MqttOptions, requests: Receiver<Request>) -> EventLoop {
        let inflight_limit = options.outgoing_inflight_upper_limit.unwrap_or(u16::MAX);
        let manual_acks = options.manual_acks;
        let pending = IntervalQueue::new(options.pending_throttle);
        let batch_size = options.max_batch_size;
        let state = MqttState::new(inflight_limit, manual_acks);
        assert!(!options.keep_alive.is_zero());
        let keepalive_interval = time::interval(options.keep_alive());

        EventLoop {
            options,
            state,
            batch_size,
            requests,
            pending,
            network: None,
            keepalive_interval,
        }
    }

    /// Last session might contain packets which aren't acked. MQTT says these packets should be
    /// republished in the next session. Move pending messages from state to eventloop, drops the
    /// underlying network connection and clears the keepalive timeout if any.
    ///
    /// > NOTE: Use only when EventLoop is blocked on network and unable to immediately handle disconnect.
    /// > Also, while this helps prevent data loss, the pending list length should be managed properly.
    /// > For this reason we recommend setting [`AsycClient`](super::AsyncClient)'s channel capacity to `0`.
    pub fn clean(&mut self) {
        self.network = None;
        self.pending.extend(self.state.clean());

        // drain requests from channel which weren't yet received
        self.pending.extend(self.requests.drain());
    }

    /// Yields Next notification or outgoing request and periodically pings
    /// the broker. Continuing to poll will reconnect to the broker if there is
    /// a disconnection.
    /// **NOTE** Don't block this while iterating
    pub async fn poll(&mut self) -> Result<Event, ConnectionError> {
        if self.network.is_none() {
            let (network, connack) = time::timeout(
                Duration::from_secs(self.options.connection_timeout()),
                connect(&mut self.options),
            )
            .await??;
            self.network = Some(network);

            // A connack never produces a response packet. Safe to ignore the return value
            // of `handle_incoming_packet`
            self.state.handle_incoming_packet(connack)?;

            self.pending.reset_immediately();
            self.keepalive_interval.reset();
        }

        // Read buffered events from previous polls before calling a new poll
        if let Some(event) = self.state.events.pop_front() {
            Ok(event)
        } else {
            match self.poll_process().await {
                Ok(v) => Ok(v),
                Err(e) => {
                    self.clean();
                    Err(e)
                }
            }
        }
    }

    /// Select on network and requests and generate keepalive pings when necessary
    async fn poll_process(&mut self) -> Result<Event, ConnectionError> {
        let network = self.network.as_mut().unwrap();

        for _ in 0..self.batch_size {
            let inflight_full = self.state.is_inflight_full();
            let collision = self.state.has_collision();

            select! {
                // Handles pending and new requests.
                // If available, prioritises pending requests from previous session.
                // Else, pulls next request from user requests channel.
                // If conditions in the below branch are for flow control.
                // The branch is disabled if there's no pending messages and new user requests
                // cannot be serviced due flow control.
                // We read next user user request only when inflight messages are < configured inflight
                // and there are no collisions while handling previous outgoing requests.
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
                Some(request) = self.pending.next(), if !inflight_full && !collision => {
                    if let Some(packet) = self.state.handle_outgoing_packet(request)? {
                        network.write(packet).await?;
                    }
                },
                request = self.requests.recv_async(), if self.pending.is_empty() && !inflight_full && !collision => {
                    let request = request.map_err(|_| ConnectionError::RequestsDone)?;
                    if let Some(packet) = self.state.handle_outgoing_packet(request)? {
                        network.write(packet).await?;
                    }
                },
                // Process next packet from io
                packet = network.read() => {
                    // Reset keepalive interval due to packet reception
                    self.keepalive_interval.reset();
                    match packet? {
                        Some(packet) => if let Some(packet) = self.state.handle_incoming_packet(packet)? {
                            let flush = matches!(packet, Packet::PingResp(_));
                            network.write(packet).await?;
                            if flush {
                                break;
                            }
                        }
                        None => return Err(ConnectionError::ConnectionClosed),
                    }
                },
                // Send a ping request on each interval tick
                _ = self.keepalive_interval.tick() => {
                    if let Some(packet) = self.state.handle_outgoing_packet(Request::PingReq)? {
                        network.write(packet).await?;
                    }
                }
                else => unreachable!("Eventloop select is exhaustive"),
            }

            // Break early if there is no request pending and no more incoming bytes polled into the read buffer
            // This implementation is suboptimal: The loop is *not* broken if a incomplete packets resides in the
            // rx buffer of `Network`. Until that frame is complete the outgoing queue is *not* flushed.
            // Since the incomplete packet is already started to appear in the buffer it should be fine to await
            // more data on the stream before flushing.
            if self.pending.is_empty()
                && self.requests.is_empty()
                && network.read_buffer_remaining() == 0
            {
                break;
            }
        }

        network.flush().await?;

        self.state
            .events
            .pop_front()
            .ok_or_else(|| unreachable!("empty event queue"))
    }
}

/// Pending items yielded with a configured rate. If the queue is empty the stream will yield pending.
struct IntervalQueue<T> {
    /// Interval
    interval: Option<time::Interval>,
    /// Pending requests
    queue: VecDeque<T>,
}

impl<T> IntervalQueue<T> {
    /// Construct a new Pending instance
    pub fn new(interval: Duration) -> Self {
        let interval = (!interval.is_zero()).then(|| time::interval(interval));
        IntervalQueue {
            interval,
            queue: VecDeque::new(),
        }
    }

    /// Returns true this queue is not empty
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Extend the request queue
    pub fn extend(&mut self, requests: impl IntoIterator<Item = T>) {
        self.queue.extend(requests);
    }

    /// Reset the pending interval tick. Next tick yields immediately
    pub fn reset_immediately(&mut self) {
        if let Some(interval) = self.interval.as_mut() {
            interval.reset_immediately();
        }
    }
}

impl<T: Unpin> Stream for IntervalQueue<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if self.is_empty() {
            Poll::Pending
        } else {
            match self.interval.as_mut() {
                Some(interval) => match interval.poll_tick(cx) {
                    Poll::Ready(_) => Poll::Ready(self.queue.pop_front()),
                    Poll::Pending => Poll::Pending,
                },
                None => Poll::Ready(self.queue.pop_front()),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.queue.len(), Some(self.queue.len()))
    }
}

/// This stream internally processes requests from the request stream provided to the eventloop
/// while also consuming byte stream from the network and yielding mqtt packets as the output of
/// the stream.
/// This function (for convenience) includes internal delays for users to perform internal sleeps
/// between re-connections so that cancel semantics can be used during this sleep
async fn connect(options: &mut MqttOptions) -> Result<(Network, Incoming), ConnectionError> {
    // connect to the broker
    let mut network = network_connect(options).await?;

    // make MQTT connection request (which internally awaits for ack)
    let packet = mqtt_connect(options, &mut network).await?;
    Ok((network, packet))
}

async fn network_connect(options: &MqttOptions) -> Result<Network, ConnectionError> {
    let mut max_incoming_pkt_size = Some(options.default_max_incoming_size);

    // Override default value if max_packet_size is set on `connect_properties`
    if let Some(connect_props) = &options.connect_properties {
        if let Some(max_size) = connect_props.max_packet_size {
            max_incoming_pkt_size = Some(max_size);
        }
    }

    // Process Unix files early, as proxy is not supported for them.
    #[cfg(unix)]
    if matches!(options.transport(), Transport::Unix) {
        let file = options.broker_addr.as_str();
        let socket = UnixStream::connect(Path::new(file)).await?;
        let network = Network::new(socket, max_incoming_pkt_size);
        return Ok(network);
    }

    // For websockets domain and port are taken directly from `broker_addr` (which is a url).
    let (domain, port) = match options.transport() {
        #[cfg(feature = "websocket")]
        Transport::Ws => split_url(&options.broker_addr)?,
        #[cfg(all(feature = "use-rustls", feature = "websocket"))]
        Transport::Wss(_) => split_url(&options.broker_addr)?,
        _ => options.broker_address(),
    };

    let tcp_stream: Box<dyn AsyncReadWrite> = {
        #[cfg(feature = "proxy")]
        match options.proxy() {
            Some(proxy) => {
                proxy
                    .connect(&domain, port, options.network_options())
                    .await?
            }
            None => {
                let addr = format!("{domain}:{port}");
                let tcp = socket_connect(addr, options.network_options()).await?;
                Box::new(tcp)
            }
        }
        #[cfg(not(feature = "proxy"))]
        {
            let addr = format!("{domain}:{port}");
            let tcp = socket_connect(addr, options.network_options()).await?;
            Box::new(tcp)
        }
    };

    let network = match options.transport() {
        Transport::Tcp => Network::new(tcp_stream, max_incoming_pkt_size),
        #[cfg(any(feature = "use-native-tls", feature = "use-rustls"))]
        Transport::Tls(tls_config) => {
            let socket =
                tls::tls_connect(&options.broker_addr, options.port, &tls_config, tcp_stream)
                    .await?;
            Network::new(socket, max_incoming_pkt_size)
        }
        #[cfg(unix)]
        Transport::Unix => unreachable!(),
        #[cfg(feature = "websocket")]
        Transport::Ws => {
            let mut request = options.broker_addr.as_str().into_client_request()?;
            request
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", "mqtt".parse().unwrap());

            if let Some(request_modifier) = options.request_modifier() {
                request = request_modifier(request).await;
            }

            let (socket, response) =
                async_tungstenite::tokio::client_async(request, tcp_stream).await?;
            validate_response_headers(response)?;

            Network::new(WsStream::new(socket), max_incoming_pkt_size)
        }
        #[cfg(all(feature = "use-rustls", feature = "websocket"))]
        Transport::Wss(tls_config) => {
            let mut request = options.broker_addr.as_str().into_client_request()?;
            request
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", "mqtt".parse().unwrap());

            if let Some(request_modifier) = options.request_modifier() {
                request = request_modifier(request).await;
            }

            let connector = tls::rustls_connector(&tls_config).await?;

            let (socket, response) = async_tungstenite::tokio::client_async_tls_with_connector(
                request,
                tcp_stream,
                Some(connector),
            )
            .await?;
            validate_response_headers(response)?;

            Network::new(WsStream::new(socket), max_incoming_pkt_size)
        }
    };

    Ok(network)
}

async fn mqtt_connect(
    options: &mut MqttOptions,
    network: &mut Network,
) -> Result<Incoming, ConnectionError> {
    let keep_alive = options.keep_alive().as_secs() as u16;
    let clean_start = options.clean_start();
    let client_id = options.client_id();
    let properties = options.connect_properties();

    let connect = Connect {
        keep_alive,
        client_id,
        clean_start,
        properties,
    };

    // send mqtt connect packet
    network.connect(connect, options).await?;

    // validate connack
    match network.read().await? {
        Some(Incoming::ConnAck(connack)) if connack.code == ConnectReturnCode::Success => {
            if let Some(props) = &connack.properties {
                // Override local keep_alive value if set by server.
                if let Some(keep_alive) = props.server_keep_alive {
                    options.keep_alive = Duration::from_secs(keep_alive as u64);
                }

                // Override max packet size
                network.set_max_outgoing_size(props.max_packet_size);
            }
            Ok(Packet::ConnAck(connack))
        }
        Some(Incoming::ConnAck(connack)) => Err(ConnectionError::ConnectionRefused(connack.code)),
        Some(packet) => Err(ConnectionError::NotConnAck(Box::new(packet))),
        None => Err(ConnectionError::ConnectionClosed),
    }
}

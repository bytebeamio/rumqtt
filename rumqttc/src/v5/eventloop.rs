use super::framed::Network;
use super::mqttbytes::v5::*;
use super::{Incoming, MqttOptions, MqttState, Outgoing, Request, StateError, Transport};
use crate::eventloop::socket_connect;
use crate::framed::AsyncReadWrite;

use flume::Receiver;
use futures_util::{Stream, StreamExt};
use tokio::select;
use tokio::time::{self, error::Elapsed, Instant, Sleep};

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

/// Number of packets or requests processed before flusing the network
const BATCH_SIZE: usize = 10;

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
    /// Request stream
    request_rx: Receiver<Request>,
    /// Pending requests from the last session
    pending: PendingRequests,
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
    pub fn new(options: MqttOptions, request_rx: Receiver<Request>) -> EventLoop {
        let inflight_limit = options.outgoing_inflight_upper_limit.unwrap_or(u16::MAX);
        let manual_acks = options.manual_acks;
        let pending = PendingRequests::new(options.pending_throttle);

        EventLoop {
            options,
            state: MqttState::new(inflight_limit, manual_acks),
            request_rx,
            pending,
            network: None,
            keepalive_timeout: None,
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
        self.keepalive_timeout = None;
        self.pending.extend(self.state.clean());

        // drain requests from channel which weren't yet received
        let requests_in_channel = self.request_rx.drain();
        self.pending.extend(requests_in_channel);
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

            if self.keepalive_timeout.is_none() {
                self.keepalive_timeout = Some(Box::pin(time::sleep(self.options.keep_alive)));
            }

            self.state.handle_incoming_packet(connack)?;
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
        let inflight_full = self.state.is_inflight_full();
        let collision = self.state.has_collision();

        // Read buffered events from previous polls before calling a new poll
        if let Some(event) = self.state.events.pop_front() {
            return Ok(event);
        }

        // this loop is necessary since self.incoming.pop_front() might return None. In that case,
        // instead of returning a None event, we try again.
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
                self.state.handle_outgoing_packet(request)?;
                network.flush().await?;
                Ok(self.state.events.pop_front().unwrap())
            },
            request = self.request_rx.recv_async(), if self.pending.is_empty() && !inflight_full && !collision => {
                // Process first request
                let request = request.map_err(|_| ConnectionError::RequestsDone)?;
                self.state.handle_outgoing_packet(request)?;

                // Take up to BATCH_SIZE - 1 requests from the channel until
                // - the channel is empty
                // - the inflight queue is full
                // - there is a collision
                // If the channel is closed this is reported in the next iteration from the async recv above.
                for _ in 0..(BATCH_SIZE - 1) {
                    if self.request_rx.is_empty() || self.state.is_inflight_full() || self.state.has_collision()
                    {
                        break;
                    }

                    // Safe to call the blocking `recv` in here since we know the channel is not empty.
                    // Ensure a flush in case of any error.
                    if let Err(e) = self
                        .request_rx
                        .recv()
                        .map_err(|_| ConnectionError::RequestsDone)
                        .and_then(|request| {
                            self.state
                                .handle_outgoing_packet(request)
                                .map_err(Into::into)
                        })
                    {
                        network.flush().await?;
                        return Err(e);
                    }
                }

                network.flush().await?;
                Ok(self.state.events.pop_front().unwrap())
            },
            // Pull a bunch of packets from network, reply in bunch and yield the first item
            o = network.readb(&mut self.state, BATCH_SIZE) => {
                o?;
                // flush all the acks and return first incoming packet
                network.flush().await?;
                Ok(self.state.events.pop_front().unwrap())
            },
            // We generate pings irrespective of network activity. This keeps the ping logic
            // simple. We can change this behavior in future if necessary (to prevent extra pings)
            _ = self.keepalive_timeout.as_mut().unwrap() => {
                let timeout = self.keepalive_timeout.as_mut().unwrap();
                timeout.as_mut().reset(Instant::now() + self.options.keep_alive);

                self.state.handle_outgoing_packet(Request::PingReq)?;
                network.flush().await?;
                Ok(self.state.events.pop_front().unwrap())
            }
            else => unreachable!("Eventloop select is exhaustive"),
        }
    }
}

/// Pending requets yielded with a configured rate. If the queue is empty the stream will yield pending.
struct PendingRequests {
    /// Interval
    interval: Option<time::Interval>,
    /// Pending requests
    requests: VecDeque<Request>,
}

impl PendingRequests {
    pub fn new(interval: Duration) -> Self {
        let interval = (!interval.is_zero()).then(|| time::interval(interval));
        PendingRequests {
            interval,
            requests: VecDeque::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub fn extend(&mut self, requests: impl IntoIterator<Item = Request>) {
        self.requests.extend(requests);
    }
}

impl Stream for PendingRequests {
    type Item = Request;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Request>> {
        if self.is_empty() {
            Poll::Pending
        } else {
            match self.interval.as_mut() {
                Some(interval) => match interval.poll_tick(cx) {
                    Poll::Ready(_) => Poll::Ready(self.requests.pop_front()),
                    Poll::Pending => Poll::Pending,
                },
                None => Poll::Ready(self.requests.pop_front()),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.requests.len(), Some(self.requests.len()))
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

    // Last session might contain packets which aren't acked. MQTT says these packets should be
    // republished in the next session
    // move pending messages from state to eventloop
    // let pending = self.state.clean();
    // self.pending = pending.into_iter();
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
        Incoming::ConnAck(connack) if connack.code == ConnectReturnCode::Success => {
            // Override local keep_alive value if set by server.
            if let Some(props) = &connack.properties {
                if let Some(keep_alive) = props.server_keep_alive {
                    options.keep_alive = Duration::from_secs(keep_alive as u64);
                }
                network.set_max_outgoing_size(props.max_packet_size);
            }
            Ok(Packet::ConnAck(connack))
        }
        Incoming::ConnAck(connack) => Err(ConnectionError::ConnectionRefused(connack.code)),
        packet => Err(ConnectionError::NotConnAck(Box::new(packet))),
    }
}

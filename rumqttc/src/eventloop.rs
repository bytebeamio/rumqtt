use crate::framed::Network;
use crate::{tls, Incoming, MqttState, Request, StateError};
use crate::{MqttOptions, Outgoing};

use async_channel::{bounded, Receiver, Sender};
use mqtt4bytes::*;
use tokio::net::TcpStream;
use tokio::select;
use tokio::stream::{Stream, StreamExt};
use tokio::time::{self, Delay, Elapsed, Instant};

use std::collections::VecDeque;
use std::io;
use std::time::Duration;
use std::vec::IntoIter;

/// Critical errors during eventloop polling
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Mqtt state: {0}")]
    MqttState(#[from] StateError),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Packet parsing error: {0}")]
    Mqtt4Bytes(mqtt4bytes::Error),
    #[error("Network: {0}")]
    Network(#[from] tls::Error),
    #[error("I/O: {0}")]
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
    /// Buffered incoming packets
    pub incoming: VecDeque<Incoming>,
    /// Buffered outgoing packets
    pub outgoing: VecDeque<Outgoing>,
    /// Pending packets from last session
    pub pending: IntoIter<Request>,
    /// Network connection to the broker
    pub(crate) network: Option<Network>,
    /// Keep alive time
    pub(crate) keepalive_timeout: Option<Delay>,
    /// Handle to read cancellation requests
    pub(crate) cancel_rx: Receiver<()>,
    /// Handle to send cancellation requests (and drops)
    pub(crate) cancel_tx: Option<Sender<()>>,
}

/// Events which can be yielded by the event loop
#[derive(Debug, PartialEq)]
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
        let (cancel_tx, cancel_rx) = bounded(5);
        let (requests_tx, requests_rx) = bounded(cap);
        let pending = Vec::new();
        let pending = pending.into_iter();
        let max_inflight = options.inflight;

        EventLoop {
            options,
            state: MqttState::new(max_inflight),
            requests_tx,
            requests_rx,
            incoming: VecDeque::with_capacity(100),
            outgoing: VecDeque::with_capacity(100),
            pending,
            network: None,
            keepalive_timeout: None,
            cancel_rx,
            cancel_tx: Some(cancel_tx),
        }
    }

    /// Returns a handle to communicate with this eventloop
    pub fn handle(&self) -> Sender<Request> {
        self.requests_tx.clone()
    }

    /// Handle for cancelling the eventloop.
    ///
    /// Can be useful in cases when connection should be halted immediately
    /// between half-open connection detections or (re)connection timeouts
    pub(crate) fn take_cancel_handle(&mut self) -> Option<Sender<()>> {
        self.cancel_tx.take()
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
    #[must_use = "Eventloop should be iterated over a loop to make progress"]
    pub async fn poll(&mut self) -> Result<Event, ConnectionError> {
        if self.network.is_none() {
            let (network, connack) = connect_or_cancel(&self.options, &self.cancel_rx).await?;
            self.network = Some(network);

            if self.keepalive_timeout.is_none() {
                self.keepalive_timeout = Some(time::delay_for(self.options.keep_alive));
            }

            return Ok(Event::Incoming(connack));
        }

        match self.select().await {
            Ok(v) => Ok(v),
            Err(e) => {
                // don't disconnect the network in case collision safety is enabled
                if let ConnectionError::MqttState(StateError::Collision(pkid)) = e {
                    if self.options.collision_safety() {
                        return Err(ConnectionError::MqttState(StateError::Collision(pkid)));
                    }
                }
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

        // Return buffered outgoing notification first so that, if incoming
        // packets 1, 2, 3 are acked in bulk before reading new incoming packet,
        // we return buffered outgoing notifications of sent packets first before
        // new incoming notification. This ensures that notification order is in
        // sync with the order in which select is doing things
        if let Some(outgoing) = self.outgoing.pop_front() {
            return Ok(Event::Outgoing(outgoing));
        }

        // Return buffered incoming packets before hitting network again
        if let Some(incoming) = self.incoming.pop_front() {
            return Ok(Event::Incoming(incoming));
        }

        // this loop is necessary since self.incoming.pop_front() might return None. In that case,
        // instead of returning a None event, we try again.
        select! {
            // Pull a bunch of packets from network, reply in bunch and yield the first item
            o = network.readb(&mut self.incoming) => {
                let incoming = o?;

                // handle 1st incoming packet
                if let Some(request) = self.state.handle_incoming_packet(&incoming)? {
                    let outgoing = network.fill(request)?;
                    self.outgoing.push_back(outgoing)
                }

                // be eager to handle more incoming packets
                for incoming in self.incoming.iter() {
                    if let Some(request) = self.state.handle_incoming_packet(incoming)? {
                        let outgoing = network.fill(request)?;
                        self.outgoing.push_back(outgoing)
                    }
                }

                // flush all the acks and return first incoming packet
                network.flush().await?;
                Ok(Event::Incoming(incoming))
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
            // This can be fixed in 2 ways
            //
            // 1. using packet id boundary instead of count for flow control
            // ---------------------
            // Full inflight queue will look like -> [1, 2, 3, 4, 5].
            // If 3 is acked instead of 1 first   -> [1, 2, x, 4, 5].
            // If we flow control at boundary (5) to make sure that all the packet ids before
            // 5 are acked, there would be no collisions.
            // But this method comes at the cost of throughput. Synchronizing all the acks
            // everytime at boundary will affect throughput, even if the broker is acking in
            // sequence
            //
            // 2. using collisions for flow control
            // ---------------------
            // Eventloop can stop receiving outgoing user requests when previous outgoing
            // request collided. I.e collision state. Collision state will be cleared only
            // when correct ack is received
            // Full inflight queue will look like -> [1a, 2, 3, 4, 5].
            // If 3 is acked instead of 1 first   -> [1a, 2, x, 4, 5].
            // After collision with pkid 1        -> [1b ,2, x, 4, 5].
            // 1a is saved to state and event loop is set to collision mode stopping new
            // outgoing requests (along with 1b).
            o = self.requests_rx.next(), if !inflight_full && !pending && !collision => match o {
                Some(request) => {
                    let request = self.state.handle_outgoing_packet(request)?;
                    let outgoing = network.fill(request)?;
                    // During high load, pull more data from channel to batch more requests.
                    // Note: Make sure size of total inflight messages isn't greater than
                    // OS tcp write buffer size to prevent bounded buffer deadlocks
                    for _ in 0..self.options.max_request_batch {
                        // Honor inflight limitations during batching
                        if self.state.inflight >= self.options.inflight { break }
                        match self.requests_rx.try_recv() {
                            Ok(r) => {
                                // handle, send and buffer outgoing packet ids
                                let request = self.state.handle_outgoing_packet(r)?;
                                let outgoing = network.fill(request)?;
                                self.outgoing.push_back(outgoing);
                            }
                            Err(_) => break,
                        };

                    }

                    network.flush().await?;
                    Ok(Event::Outgoing(outgoing))
                }
                None => Err(ConnectionError::RequestsDone),
            },
            // Handle the next pending packet from previous session. Disable
            // this branch when done with all the pending packets
            Some(request) = next_pending(throttle, &mut self.pending), if pending => {
                let request = self.state.handle_outgoing_packet(request)?;
                let outgoing = network.write(request).await?;
                Ok(Event::Outgoing(outgoing))
            },
            // We generate pings irrespective of network activity. This keeps the ping logic
            // simple. We can change this behavior in future if necessary (to prevent extra pings)
            _ = self.keepalive_timeout.as_mut().unwrap() => {
                let timeout = self.keepalive_timeout.as_mut().unwrap();
                timeout.reset(Instant::now() + self.options.keep_alive);
                let request = self.state.handle_outgoing_packet(Request::PingReq)?;
                let outgoing = network.write(request).await?;
                Ok(Event::Outgoing(outgoing))
            }
            // cancellation requests to stop the polling
            _ = self.cancel_rx.next() => {
                Err(ConnectionError::Cancel)
            }
        }
    }
}

async fn connect_or_cancel(
    options: &MqttOptions,
    cancel_rx: &Receiver<()>,
) -> Result<(Network, Incoming), ConnectionError> {
    // select here prevents cancel request from being blocked until connection request is
    // resolved. Returns with an error if connections fail continuously
    select! {
        o = connect(options) => o,
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
async fn connect(options: &MqttOptions) -> Result<(Network, Incoming), ConnectionError> {
    // connect to the broker
    let mut network = match network_connect(options).await {
        Ok(network) => network,
        Err(e) => {
            return Err(e);
        }
    };

    // make MQTT connection request (which internally awaits for ack)
    let packet = match mqtt_connect(options, &mut network).await {
        Ok(p) => p,
        Err(e) => return Err(e),
    };

    // Last session might contain packets which aren't acked. MQTT says these packets should be
    // republished in the next session
    // move pending messages from state to eventloop
    // let pending = self.state.clean();
    // self.pending = pending.into_iter();
    Ok((network, packet))
}

async fn network_connect(options: &MqttOptions) -> Result<Network, ConnectionError> {
    let network = if options.ca.is_some() || options.tls_client_config.is_some() {
        let socket = tls::tls_connect(options).await?;
        Network::new(socket, options.max_incoming_packet_size)
    } else {
        let addr = options.broker_addr.as_str();
        let port = options.port;
        let socket = TcpStream::connect((addr, port)).await?;
        Network::new(socket, options.max_incoming_packet_size)
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

    // mqtt connection with timeout
    time::timeout(Duration::from_secs(options.connection_timeout()), async {
        network.connect(connect).await?;
        Ok::<_, ConnectionError>(())
    })
    .await??;

    // wait for 'timeout' time to validate connack
    let packet = time::timeout(Duration::from_secs(options.connection_timeout()), async {
        let packet = match network.read().await? {
            Incoming::ConnAck(connack) if connack.code == ConnectReturnCode::Accepted => {
                Packet::ConnAck(connack)
            }
            Incoming::ConnAck(connack) => {
                let error = format!("Broker rejected. Reason = {:?}", connack.code);
                return Err(io::Error::new(io::ErrorKind::InvalidData, error));
            }
            packet => {
                let error = format!("Expecting connack. Received = {:?}", packet);
                return Err(io::Error::new(io::ErrorKind::InvalidData, error));
            }
        };

        Ok::<_, io::Error>(packet)
    })
    .await??;

    Ok(packet)
}

/// Returns the next pending packet asynchronously to be used in select!
/// This is a synchronous function but made async to make it fit in select!
pub(crate) async fn next_pending(
    delay: Duration,
    pending: &mut IntoIter<Request>,
) -> Option<Request> {
    // return next packet with a delay
    time::delay_for(delay).await;
    pending.next()
}

pub trait Requests: Stream<Item = Request> + Unpin + Send {}
impl<T> Requests for T where T: Stream<Item = Request> + Unpin + Send {}

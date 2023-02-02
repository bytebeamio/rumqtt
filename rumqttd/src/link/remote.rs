use crate::link::local::{Link, LinkError, LinkRx, LinkTx};
use crate::link::network;
use crate::link::network::Network;
use crate::protocol::{Connect, Packet, Protocol};
use crate::router::{Event, Notification};
use crate::{ConnectionId, ConnectionSettings};

use flume::{RecvError, SendError, Sender, TrySendError};
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::error::Elapsed;
use tokio::{select, time};
use tracing::{trace, Span};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Zero keep alive")]
    ZeroKeepAlive,
    #[error("Not connect packet")]
    NotConnectPacket(Packet),
    #[error("Network {0}")]
    Network(#[from] network::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Persistent session requires valid client id")]
    InvalidClientId,
    #[error("Unexpected router message")]
    NotConnectionAck,
    #[error("ConnAck error {0}")]
    ConnectionAck(String),
    #[error("Authentication error")]
    InvalidAuth,
    #[error("Channel try send error")]
    TrySend(#[from] TrySendError<(ConnectionId, Event)>),
    #[error("Link error = {0}")]
    Link(#[from] LinkError),
}

/// Orchestrates between Router and Network.
pub struct RemoteLink<P> {
    connect: Connect,
    pub(crate) client_id: String,
    pub(crate) connection_id: ConnectionId,
    network: Network<P>,
    link_tx: LinkTx,
    link_rx: LinkRx,
    notifications: VecDeque<Notification>,
}

impl<P: Protocol> RemoteLink<P> {
    pub async fn new(
        config: Arc<ConnectionSettings>,
        router_tx: Sender<(ConnectionId, Event)>,
        tenant_id: Option<String>,
        mut network: Network<P>,
    ) -> Result<RemoteLink<P>, Error> {
        // Wait for MQTT connect packet and error out if it's not received in time to prevent
        // DOS attacks by filling total connections that the server can handle with idle open
        // connections which results in server rejecting new connections
        let connection_timeout_ms = config.connection_timeout_ms.into();
        let dynamic_filters = config.dynamic_filters;
        let packet = time::timeout(Duration::from_millis(connection_timeout_ms), async {
            let packet = network.read().await?;
            Ok::<_, network::Error>(packet)
        })
        .await??;

        let (connect, lastwill, login) = match packet {
            Packet::Connect(connect, _, lastwill, _, login) => (connect, lastwill, login),
            packet => return Err(Error::NotConnectPacket(packet)),
        };
        Span::current().record("client_id", &connect.client_id);

        // If authentication is configured in config file check for username and password
        if let Some(auths) = &config.auth {
            // if authentication is configured and connect packet doesn't have login details return
            // an error
            if let Some(login) = login {
                let is_authenticated = auths
                    .iter()
                    .any(|(user, pass)| (user, pass) == (&login.username, &login.password));

                if !is_authenticated {
                    return Err(Error::InvalidAuth);
                }
            } else {
                return Err(Error::InvalidAuth);
            }
        }

        // When keep_alive feature is disabled client can live forever, which is not good in
        // distributed broker context so currenlty we don't allow it.
        if connect.keep_alive == 0 {
            return Err(Error::ZeroKeepAlive);
        }

        // Register this connection with the router. Router replys with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let client_id = connect.client_id.clone();
        let clean_session = connect.clean_session;

        // Don't allow empty client_id
        if client_id.is_empty() {
            return Err(Error::InvalidClientId);
        }

        let (link_tx, link_rx, notification) = Link::new(
            tenant_id,
            &client_id,
            router_tx,
            clean_session,
            lastwill,
            dynamic_filters,
        )?;
        let id = link_rx.id();
        Span::current().record("connection_id", id);

        if let Some(packet) = notification.into() {
            network.write(packet).await?;
        }

        Ok(RemoteLink {
            connect,
            client_id,
            connection_id: id,
            network,
            link_tx,
            link_rx,
            notifications: VecDeque::with_capacity(100),
        })
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        self.network.set_keepalive(self.connect.keep_alive);

        // Note:
        // Shouldn't result in bounded queue deadlocks because of blocking n/w send
        loop {
            select! {
                o = self.network.read() => {
                    let packet = o?;
                    let len = {
                        let mut buffer = self.link_tx.buffer();
                        buffer.push_back(packet);
                        self.network.readv(&mut buffer)?;
                        buffer.len()
                    };

                    trace!("Packets read from network, count = {}", len);
                    self.link_tx.notify().await?;
                }
                // Receive from router when previous when state isn't in collision
                // due to previously received data request
                o = self.link_rx.exchange(&mut self.notifications) => {
                    o?;
                    let mut packets = VecDeque::new();
                    let mut unscheduled = false;

                    for notif in self.notifications.drain(..) {
                        if let Some(packet) = notif.into() {
                            packets.push_back(packet);
                        } else {
                            unscheduled = true;
                        }

                    }
                    self.network.writev(packets).await?;
                    if unscheduled {
                        self.link_rx.wake().await?;
                    }
                }
            }
        }
    }
}

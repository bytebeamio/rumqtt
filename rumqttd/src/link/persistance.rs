use crate::link::local::{LinkError, LinkRx, LinkTx};
use crate::link::network;
use crate::link::network::Network;
use crate::local::Link;
use crate::protocol::{self, AsyncProtocol, Connect, LastWill, Packet};
use crate::router::{Event, FilterIdx, Notification};
use crate::{ConnectionId, ConnectionSettings, Offset};

use disk::Storage;
use flume::{Receiver, RecvError, SendError, Sender, TrySendError};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, io};
use tokio::time::error::Elapsed;
use tokio::{select, time};
use tracing::{error, info, trace};

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
    #[error("Channel try send error")]
    TrySend(#[from] TrySendError<(ConnectionId, Event)>),
    #[error("Link error = {0}")]
    Link(#[from] LinkError),
}

/// Orchestrates between Router and Network.
pub struct PersistanceLink<P: AsyncProtocol> {
    _connect: Connect,
    pub(crate) client_id: String,
    pub(crate) connection_id: ConnectionId,
    pub(crate) network: Network<P>,
    link_tx: LinkTx,
    link_rx: LinkRx,
    notifications: VecDeque<Notification>,
    disk_handler: DiskHandler<P>,
    network_update_rx: Receiver<Network<P>>,
    connack: Notification,
    inflight_publishes: VecDeque<Notification>,
}

pub(super) struct DiskHandler<P: AsyncProtocol> {
    storage: Storage,
    protocol: P,
}

impl<P: AsyncProtocol> DiskHandler<P> {
    fn new(client_id: &str, protocol: P) -> Result<Self, Error> {
        // TODO: take params from config
        let path = format!("/tmp/rumqttd/{}", &client_id);
        fs::create_dir_all(&path)?;

        Ok(DiskHandler {
            storage: Storage::new(path, 1024, 30)?,
            protocol,
        })
    }

    pub fn write(
        &mut self,
        notifications: &mut VecDeque<Notification>,
    ) -> HashMap<FilterIdx, Offset> {
        // let ack_list = VecDeque::new();

        let mut stored_filter_offset_map: HashMap<FilterIdx, Offset> = HashMap::new();
        for notif in notifications.drain(..) {
            let packet_or_unscheduled = notif.clone().into();
            if let Some(packet) = packet_or_unscheduled {
                if let Err(e) = self.protocol.write(packet, self.storage.writer()) {
                    error!("Failed to write to storage: {e}");
                    continue;
                }

                if let Err(e) = self.storage.flush_on_overflow() {
                    error!("Failed to flush storage: {e}");
                    continue;
                }

                match &notif {
                    Notification::Forward(forward) => {
                        stored_filter_offset_map
                            .entry(forward.filter_idx)
                            .and_modify(|cursor| {
                                if forward.cursor > *cursor {
                                    *cursor = forward.cursor
                                }
                            })
                            .or_insert(forward.cursor);
                    }
                    _ => continue,
                }
            }
        }

        stored_filter_offset_map
    }

    pub fn read(&mut self, buffer: &mut VecDeque<Packet>) {
        if let Err(e) = self.storage.reload_on_eof() {
            error!("Failed to reload storage: {e}");
        }

        // TODO: fix max incoming size of packet
        loop {
            match self
                .protocol
                .read_mut(self.storage.reader(), 10240) //TODO: Don't hardcode max_incoming_size
            {
                Ok(packet) => {
                    buffer.push_back(packet);
                    let connection_buffer_length = buffer.len();
                    //TODO: Don't hardcode max_connection_buffer_len
                    if connection_buffer_length >= 100 {
                        return
                    }
                }
                Err(protocol::Error::InsufficientBytes(_)) => {
                    if let Err(e) = self.storage.reload() {
                        error!("Failed to reload storage: {e}");
                        return
                    }
                    // Reader empty after reloading means
                    // all disk backups have been read
                    if self.storage.reader().is_empty() {
                        return
                    }
                },
                Err(e) => {
                    error!("Failed to read from storage: {e}");
                    return
                }
            }
        }
    }
}

impl<P: AsyncProtocol> PersistanceLink<P> {
    pub async fn new(
        config: Arc<ConnectionSettings>,
        router_tx: Sender<(ConnectionId, Event)>,
        tenant_id: Option<String>,
        connect: Connect,
        lastwill: Option<LastWill>,
        mut network: Network<P>,
    ) -> Result<(Sender<Network<P>>, PersistanceLink<P>), Error> {
        let dynamic_filters = config.dynamic_filters;

        // When keep_alive feature is disabled client can live forever, which is not good in
        // distributed broker context so currenlty we don't allow it.
        if connect.keep_alive == 0 {
            return Err(Error::ZeroKeepAlive);
        }

        // Register this connection with the router. Router replys with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let client_id = connect.client_id.clone();
        let clean_session = connect.clean_session;
        if !clean_session && client_id.is_empty() {
            return Err(Error::InvalidClientId);
        }

        let (link_tx, link_rx, notification) = Link::new(
            tenant_id,
            &client_id,
            router_tx,
            clean_session,
            lastwill,
            dynamic_filters,
            true,
        )?;
        let id = link_rx.id();

        network.write(notification.clone()).await?;
        let protocol = network.protocol.clone();
        let (network_update_tx, network_update_rx) = flume::bounded(1);

        Ok((
            network_update_tx,
            PersistanceLink {
                _connect: connect,
                client_id: client_id.to_string(),
                connection_id: id,
                network,
                link_tx,
                link_rx,
                notifications: VecDeque::with_capacity(100),
                disk_handler: DiskHandler::new(&client_id, protocol)?,
                network_update_rx,
                connack: notification,
                inflight_publishes: VecDeque::with_capacity(100),
            },
        ))
    }

    /// Read the first `Connect` packet from `Network`
    pub async fn peek_first_connect(
        config: Arc<ConnectionSettings>,
        network: &mut Network<P>,
    ) -> Result<(Connect, Option<LastWill>), Error> {
        let connection_timeout_ms = config.connection_timeout_ms.into();

        let packet = time::timeout(Duration::from_millis(connection_timeout_ms), async {
            let packet = network.read().await?;
            Ok::<_, io::Error>(packet)
        })
        .await??;

        let (connect, lastwill) = match packet {
            Packet::Connect(connect, _, lastwill, ..) => (connect, lastwill),
            packet => return Err(Error::NotConnectPacket(packet)),
        };

        Ok((connect, lastwill))
    }

    async fn disconnected(&mut self) -> Result<State, Error> {
        info!(state = ?State::Disconnected, "Disconnected from persistent connection");

        loop {
            select! {
                // wait for reconnection
                network = self.network_update_rx.recv_async() => {
                    self.network = network?;
                    // TODO: if network write throws an error then this means we again got network I/O error
                    self.network.write(self.connack.clone()).await?;
                    return Ok(State::Normal)
                },
                // write to disk
                o = self.link_rx.exchange(&mut self.notifications) => {
                    o?;
                    // TODO: write only publishes
                    self.write_to_disconnected_client().await?;
                }
            }
        }
    }

    async fn write_to_disconnected_client(&mut self) -> Result<(), Error> {
        for notif in self.notifications.drain(..) {
            match notif {
                Notification::Forward(_) | Notification::ForwardWithProperties(_, _) => {
                    self.inflight_publishes.push_back(notif)
                }
                _ => continue,
            }
        }

        // write publishes to disk
        if !self.inflight_publishes.is_empty() {
            let acked_offsets = self.disk_handler.write(&mut self.inflight_publishes);
            if let Err(e) = self.link_tx.persist(acked_offsets).await {
                error!("Failed to inform router of read progress: {e}")
            };
        }
        Ok(())
    }

    async fn write_to_active_client(&mut self) -> Result<(), Error> {
        // separate notifications out
        let mut non_publish = VecDeque::new();
        // let mut acked_publishes = VecDeque::new();
        for notif in self.notifications.drain(..) {
            match notif {
                Notification::Forward(_) | Notification::ForwardWithProperties(_, _) => {
                    self.inflight_publishes.push_back(notif)
                }
                // Notification::AckDone => {
                //     // release all publishes in storage
                //     self.disk_handler.read(&mut acked_publishes);
                // }
                _ => non_publish.push_back(notif),
            }
        }

        // write non-publishes to network
        let mut unscheduled = self.network.writev(&mut non_publish).await?;
        // write acked-publishes
        // unscheduled = unscheduled || self.network.writev(&mut acked_publishes).await?;
        if unscheduled {
            self.link_rx.wake().await?;
        };

        // write publishes to disk
        if !self.inflight_publishes.is_empty() {
            let acked_offsets = self.disk_handler.write(&mut self.inflight_publishes);
            if let Err(e) = self.link_tx.persist(acked_offsets).await {
                error!("Failed to inform router of read progress: {e}")
            };
        }
        // read publishes from disk
        let mut buffer = VecDeque::new();
        self.disk_handler.read(&mut buffer);

        // TODO: if network write throws an error then this means we again got network I/O error
        // write publishes to network

        let unscheduled = self.network.writev(&mut buffer).await?;
        if unscheduled {
            self.link_rx.wake().await?;
        };

        Ok(())
    }

    async fn read_from_client(&mut self, packet: Packet) -> Result<(), Error> {
        let len = {
            let mut buffer = self.link_tx.buffer();
            buffer.push_back(packet);
            self.network.readv(&mut buffer)?;
            buffer.len()
        };
        trace!("Packets read from network, count = {}", len);
        self.link_tx.notify().await?;
        Ok(())
    }

    async fn run(&mut self) -> Result<State, Error> {
        info!(state = ?State::Normal, "Persistent connection is running in normal mode");
        loop {
            select! {
                // read from remote client
                o = self.network.read() => {
                    match o {
                        // this method reads from in memory buffer of MQTT packets,
                        // hence any error returned from this method should drop the
                        // persistence link
                        Ok(packet) => self.read_from_client(packet).await?,
                        // change state to disconnected on I/O connection errors and
                        // wait for a reconnection
                        Err(e) => match e.kind() {
                            io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionReset => return Ok(State::Disconnected),
                            _ => return Err(e.into())
                        }
                    };
                }
                // write to remote client
                o = self.link_rx.exchange(&mut self.notifications) => {
                    // exchange will through error if all senders to router are dropped
                    // which is not possible since Persistent Link always lives
                    o?;
                    self.write_to_active_client().await?;
                }
            }
        }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let mut state = State::Normal;

        loop {
            let next = match state {
                State::Normal => self.run().await?,
                State::Disconnected => self.disconnected().await?,
            };

            state = next;
        }
    }
}

#[derive(Debug)]
pub enum State {
    Normal,
    Disconnected,
}

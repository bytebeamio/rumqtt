use crate::link::local::{LinkError, LinkRx, LinkTx};
use crate::link::network;
use crate::link::network::Network;
use crate::protocol::{AsyncProtocol, Connect, LastWill, Packet, self};
use crate::router::{Event, Notification};
use crate::{ConnectionId, ConnectionSettings, Link};

use disk::Storage;
use flume::{Receiver, RecvError, SendError, Sender, TrySendError};
use std::collections::{VecDeque};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::error::Elapsed;
use tokio::{select, time};

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
}

pub(super) struct DiskHandler<P: AsyncProtocol> {
    storage: Storage,
    protocol: P,
}

impl<P: AsyncProtocol> DiskHandler<P> {
    fn new(protocol: P) -> Result<Self, Error> {
        // TODO: take storage params from config
        // 100 mb x 3 backup files
        Ok(DiskHandler {
            storage: Storage::new("/tmp/rumqttd", 104857600, 3)?,
            protocol,
        })
    }
    pub fn write(&mut self, notifications: &mut VecDeque<Notification>) {
        // TODO: empty out notifs at the end of write
        for notif in notifications.clone() {
            // TODO: write only certain types of notifications  
            let packet_or_unscheduled = notif.into();
            if let Some(packet) = packet_or_unscheduled {
                if let Err(e) = self.protocol.write(packet, self.storage.writer()) {
                    error!("{:15.15} failed to write to storage: {e}", "");
                }
            }
        }
        if let Err(e) = self.storage.flush_on_overflow() {
            error!("{:15.15} failed to flush storage: {e}", "");
        }

        notifications.clear();
    }

    pub fn read(&mut self, buffer: &mut VecDeque<Packet>) {
        match self.storage.reload_on_eof() {
            Ok(true) => info!("read buffer still has data"),
            Ok(false) => info!("read buffer is loaded"),
            Err(_) => error!("well this is embarrassing"),
        }

        // TODO: fix max incoming size of packet
        // this is network::readv() code straight up
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
                Err(protocol::Error::InsufficientBytes(_)) => return,
                Err(e) => {
                    error!("{:15.15} failed to read from storage: {e}", "");
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
        // tenant_id: Option<String>,
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
            // tenant_id,
            &client_id,
            router_tx,
            clean_session,
            lastwill,
            dynamic_filters,
        )?;
        let id = link_rx.id();

        network.write(notification.clone()).await?;
        let protocol = network.protocol.clone();
        let (network_update_tx, network_update_rx) = flume::bounded(1);

        Ok((
            network_update_tx,
            PersistanceLink {
                _connect: connect,
                client_id,
                connection_id: id,
                network,
                link_tx,
                link_rx,
                notifications: VecDeque::with_capacity(100),
                disk_handler: DiskHandler::new(protocol)?,
                network_update_rx,
                connack: notification,
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
        info!("{:15.15} persistent mode: disconnected", self.client_id);

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
                    self.disk_handler.write(&mut self.notifications);
                }
            }
        }
    }

    async fn write_to_client(&mut self) -> Result<(), Error> {
        // take 1 packet, try to send it to client, if the future resolves too slowly then take more notifications and write them to the writer

        // write to disk
        self.disk_handler.write(&mut self.notifications);

        // read from disk_handler
        let mut buffer = VecDeque::new();
        self.disk_handler.read(&mut buffer);

        // write to network
        // TODO: if network write throws an error then this means we again got network I/O error
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
        debug!(
            "{:15.15}[I] {:20} buffercount = {}",
            self.client_id, "packets", len
        );
        self.link_tx.notify().await?;
        Ok(())
    }

    async fn run(&mut self) -> Result<State, Error> {
        info!("{:15.15}[I] persistent mode: normal", self.client_id);
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
                    self.write_to_client().await?;
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

pub enum State {
    Normal,
    Disconnected,
}

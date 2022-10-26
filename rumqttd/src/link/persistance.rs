use crate::link::local::{LinkError, LinkRx, LinkTx};
use crate::link::network;
use crate::link::network::Network;
use crate::protocol::{self, Connect, LastWill, Packet, Protocol};
use crate::router::{Event, Notification};
use crate::{ConnectionId, ConnectionSettings, Filter, Link};

use bytes::BytesMut;
use flume::{Receiver, RecvError, SendError, Sender, TrySendError};
use futures_util::{FutureExt, StreamExt};
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::PathBuf;
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
pub struct PersistanceLink<P: Protocol + 'static> {
    connect: Connect,
    pub(crate) client_id: String,
    pub(crate) connection_id: ConnectionId,
    pub(crate) network: Network<P>,
    link_tx: LinkTx,
    link_rx: LinkRx,
    notifications: VecDeque<Notification>,
    disk_handler: DiskHandler,
    network_rx: Receiver<Network<P>>,
}

pub(super) struct DiskHandler {
    /// Directory in which to store files in.
    pub(crate) dir: PathBuf,
    map: HashMap<Filter, File>,
}

impl DiskHandler {
    fn new() -> Self {
        DiskHandler {
            dir: PathBuf::new(),
            map: HashMap::new(),
        }
    }
    pub fn write(&mut self, notification: Notification) {
        match notification {
            Notification::Forward(forward) => {
                let publish = forward.publish;
                let topic =
                    String::from_utf8(publish.topic.to_vec()).expect("should be a valid topic");

                if !self.map.contains_key(&topic) {
                    let mut path = self.dir.clone();
                    path.push(topic.clone());
                    let file = fs::File::create(path).expect("cannot create file");
                    self.map.insert(topic.clone(), file);
                }

                let mut buffer = BytesMut::new();
                let len = protocol::v4::publish::write(&publish, &mut buffer)
                    .unwrap()
                    .to_string();
                let mut file = self.map.get(&topic).unwrap();
                // This writes can be batch'ed by keeping a HashMap<Filter, Bytes> and writing to
                // file after all the notifications are being itererated
                file.write_all(len.as_bytes());
                file.write_all(&buffer);
            }
            _ => unreachable!(),
        }
    }
}

impl<P: Protocol + 'static> PersistanceLink<P> {
    pub async fn new(
        config: Arc<ConnectionSettings>,
        router_tx: Sender<(ConnectionId, Event)>,
        // tenant_id: Option<String>,
        connect: Connect,
        lastwill: Option<LastWill>,
        mut network: Network<P>,
    ) -> Result<(Sender<Network<P>>, PersistanceLink<P>), Error> {
        // Wait for MQTT connect packet and error out if it's not received in time to prevent
        // DOS attacks by filling total connections that the server can handle with idle open
        // connections which results in server rejecting new connections
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

        network.write(notification).await?;

        let (tx, rx) = flume::bounded(10);

        Ok((
            tx,
            PersistanceLink {
                connect,
                client_id,
                connection_id: id,
                network,
                link_tx,
                link_rx,
                notifications: VecDeque::with_capacity(100),
                disk_handler: DiskHandler::new(),
                network_rx: rx,
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

    pub async fn start(&mut self) -> Result<(), Error> {
        self.network.set_keepalive(self.connect.keep_alive);

        // Note:
        // Shouldn't result in bounded queue deadlocks because of blocking n/w send

        // loop {
        //     select! {
        //         Ok(packet) = self.network.read() => {},
        //         _ = self.link_rx.exchange(&mut self.notifications) => {},
        //         _ = self.network_rx.stream().next() => {},
        //         //
        //     }
        // }

        loop {
            select! {
                o = self.network.read() => {
                    let packet = o?;
                    let len = {
                        let mut buffer = self.link_tx.buffer();
                        // TODO: Detect subscribe packets and create a entry in disk_handler for
                        // that file
                        buffer.push_back(packet);
                        self.network.readv(&mut buffer)?;
                        buffer.len()
                    };

                    debug!("{:15.15}[I] {:20} buffercount = {}", self.client_id, "packets", len);
                    self.link_tx.notify().await?;
                }
                // Receive from router and write to disk
                o = self.link_rx.exchange(&mut self.notifications) => {
                    for notif in self.notifications.drain(..) {
                        self.disk_handler.write(notif);
                    }
                }
                // Read from disk and write to subscribers
            }
        }
    }
}

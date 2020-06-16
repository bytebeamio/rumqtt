use crate::router::RouterInMessage;
use crate::{Config, MeshConfig, IO};

mod link;
mod codec;

use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::task;
use tokio::time;
use tokio_util::codec::Framed;
use tokio::stream::StreamExt;
use async_channel::{bounded, Sender, Receiver};

use link::Link;
use std::collections::HashMap;
use tokio::time::Duration;
use std::io;
use tokio::sync::mpsc::error::SendError;
use crate::mesh::codec::{MeshCodec, Packet};
use futures_util::SinkExt;

#[derive(thiserror::Error, Debug)]
#[error("...")]
pub enum Error {
    Io(#[from] io::Error),
    Send(#[from] SendError<(String, RouterInMessage)>),
    StreamDone,
    ConnectionHandover,
    WrongPacket(Packet)
}

type ConnectionId = usize;

/// Each router maintains a task to communicate with every other broker. Which router initiates the
/// connection is implemented using handehake combination by sequentially following routers in config
pub struct Mesh {
    /// Config which holds details of all the routers for distributed mesh
    config: Config,
    /// Router handle to pass to links
    router_tx: Sender<(ConnectionId, RouterInMessage)>,
    /// Handles to all the links
    links: HashMap<u8, LinkHandle<TcpStream>>,
    /// Supervisor receiver
    supervisor_rx: Receiver<u8>,
    /// Supervisor sender to handle to links
    supervisor_tx: Sender<u8>,
}

impl Mesh {
    pub(crate) fn new(config: Config, router_tx: Sender<(ConnectionId, RouterInMessage)>) -> Mesh {
        let (supervisor_tx, supervisor_rx) = bounded(100);
        Mesh {
            config,
            router_tx,
            links: HashMap::new(),
            supervisor_rx,
            supervisor_tx,
        }
    }

    /// This starts the replication thread. Each new connection (incoming or outgoing) is informed
    /// to the router along with the connection handle. Each router connection task can now pull
    /// data it required from the router. Router will put data retrieved from the commitlog into
    /// correct task's handle
    #[tokio::main(core_threads = 1)]
    pub(crate) async fn start(&mut self) {
        let (head, this, tail) = self.extract_servers();
        let this_id = this.id;

        // start outgoing (client) links and then incoming (server) links
        self.start_router_links(tail, true).await;
        self.start_router_links(head, false).await;

        let addr = format!("{}:{}", this.host, this.port);
        let mut listener = TcpListener::bind(&addr).await.unwrap();
        info!("Listening on address: {}", addr);

        // start the supervision
        loop {
            select! {
                o = listener.accept() => {
                    let (stream, addr) = o.unwrap();
                    debug!("Received a tcp connection from {}", addr);
                    let mut framed = Framed::new(stream, MeshCodec::new());
                    let id = await_connect(&mut framed).await.unwrap();
                    let handle = self.links.get_mut(&id).unwrap();
                    if let Err(_e) = handle.connections_tx.send(framed).await {
                        error!("Failed to send the connection to link");
                    }
                },
                o = self.supervisor_rx.recv() => {
                    let remote_id = o.unwrap();
                    // TODO bring LinkHandle to this file
                    let handle: LinkHandle<_> = self.links.get(&remote_id).unwrap().clone();
                    debug!("New connection request remote = {}", remote_id);
                    task::spawn(async move {
                        let mut handle = handle;
                        loop {
                            let stream = match TcpStream::connect(&handle.addr).await {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("Failed to connect to router. Error = {:?}. Reconnecting", e);
                                    time::delay_for(Duration::from_secs(1)).await;
                                    continue
                                }
                            };

                            let framed = Framed::new(stream, MeshCodec::new());
                            if let Err(e) = handle.connect(this_id, framed).await {
                                error!("Failed to connect to router. Error = {:?}. Reconnecting", e);
                                time::delay_for(Duration::from_secs(1)).await;
                                continue
                            }

                            break
                        }
                    });
                }
            }
        }
    }

    async fn start_router_links(&mut self, config: Vec<MeshConfig>, is_client: bool) {
        // launch the client links. We'll connect later
        for server in config.iter() {
            self.start_link(is_client, server.id, &server.host, server.port).await;
        }
    }

    async fn start_link(&mut self, is_client: bool, id: u8, host: &str, port: u16) {
        let (connections_tx, connections_rx) = bounded(100);
        let router_tx = self.router_tx.clone();
        let supervisor_tx = self.supervisor_tx.clone();
        let addr = format!("{}:{}", host, port);
        let link_handle = LinkHandle::new(id, addr, connections_tx);
        self.links.insert(id, link_handle);

        task::spawn(async move {
            let mut link = Link::new(id, router_tx, supervisor_tx, is_client).await;
            match link.start(connections_rx).await {
                Ok(_) => error!("Link with {} down", id),
                Err(e) => error!("Link with {} down with error = {:?}", id, e),
            }
        });
    }

    /// Extract routers from the config. Returns
    /// - Incoming connections that this router is expecting
    /// - Config of this router
    /// - Outgoing connections that this router should make
    fn extract_servers(&self) -> (Vec<MeshConfig>, MeshConfig, Vec<MeshConfig>) {
        let id = self.config.id.clone();
        let mut routers = self.config.routers.clone().unwrap();
        let position = routers.iter().position(|v| v.id == id);
        let position = position.unwrap();

        let tail = routers.split_off(position + 1);
        let (this, head) = routers.split_last().unwrap().clone();

        (head.into(), this.clone(), tail)
    }
}

/// Await mqtt connect packet for incoming connections from a router
async fn await_connect<S: IO>(framed: &mut Framed<S, MeshCodec>) -> Result<u8, Error> {
    // wait for mesh connect packet with id
    let packet = match framed.next().await {
        Some(packet) => packet,
        None => return Err(Error::StreamDone),
    };

    let id = match packet? {
        Packet::Connect(id) => id,
        packet => return Err(Error::WrongPacket(packet)),
    };

    framed.send(Packet::ConnAck).await?;
    Ok(id)
}

pub struct LinkHandle<S> {
    pub id: u8,
    pub addr: String,
    pub connections_tx: Sender<Framed<S, MeshCodec>>,
}

impl<S: IO> LinkHandle<S> {
    pub fn new(id: u8, addr: String, connections_tx: Sender<Framed<S, MeshCodec>>) -> LinkHandle<S> {
        LinkHandle {
            id,
            addr,
            connections_tx,
        }
    }


    pub async fn connect(&mut self, this_id: u8, mut framed: Framed<S, MeshCodec>) -> Result<(), Error> {
        framed.send(Packet::Connect(this_id)).await?;
        let packet = match framed.next().await {
            Some(packet) => packet,
            None => return Err(Error::StreamDone),
        };

        match packet? {
            Packet::ConnAck => (),
            packet => return Err(Error::WrongPacket(packet)),
        };

        if let Err(_) = self.connections_tx.send(framed).await {
            return Err(Error::ConnectionHandover);
        }

        Ok(())
    }
}

impl<H> Clone for LinkHandle<H> {
    fn clone(&self) -> Self {
        LinkHandle {
            id: self.id,
            addr: self.addr.to_string(),
            connections_tx: self.connections_tx.clone()
        }
    }
}

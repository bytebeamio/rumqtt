use crate::router::RouterInMessage;
use crate::{Config, RouterConfig};

mod link;
mod tracker;

use mqtt4bytes::*;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task;
use tokio::time;
use tokio_util::codec::Framed;

use link::{LinkConfig, LinkHandle, Link};
use std::collections::HashMap;
use tokio::time::Duration;
use std::io;
use tokio::sync::mpsc::error::SendError;

#[derive(thiserror::Error, Debug)]
#[error("...")]
pub enum Error {
    Io(#[from] io::Error),
    Mqtt4(#[from] mqtt4bytes::Error),
    Send(#[from] SendError<(String, RouterInMessage)>),
    StreamDone,
}

/// Each router maintains a task to communicate with every other broker. Which router initiates the
/// connection is implemented using handehake combination by sequentially following routers in config
pub struct Mesh {
    /// Config which holds details of all the routers for distributed mesh
    config: Config,
    /// Router handle to pass to links
    router_tx: Sender<(String, RouterInMessage)>,
    /// Handles to all the links
    links: HashMap<String, LinkHandle<TcpStream>>,
    /// Supervisor receiver
    supervisor_rx: Receiver<String>,
    /// Supervisor sender to handle to links
    supervisor_tx: Sender<String>,
}

impl Mesh {
    pub(crate) fn new(config: Config, router_tx: Sender<(String, RouterInMessage)>) -> Mesh {
        let (supervisor_tx, supervisor_rx) = channel(100);
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
        let this_id = format!("router-{}", this.id);

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
                    let mut framed = Framed::new(stream, MqttCodec::new(1024 * 1024 * 10));
                    // TODO Receive connect and send this 'framed' to correct link
                },
                o = self.supervisor_rx.recv() => {
                    let remote_id = o.unwrap();
                    let this_id = this_id.clone();
                    let handle: LinkHandle<_> = self.links.get(&remote_id).unwrap().clone();
                    debug!("New connection request remote = {}", remote_id);
                    task::spawn(async move {
                        let mut handle = handle;
                        loop {
                            let this_id = this_id.clone();
                            let stream = match TcpStream::connect(&handle.addr).await {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("Failed to connect to router. Error = {:?}. Reconnecting", e);
                                    time::delay_for(Duration::from_secs(1)).await;
                                    continue
                                }
                            };

                            let mut framed = Framed::new(stream, MqttCodec::new(1024 * 1024 * 10));
                            // TODO connect and hand this to link
                        }
                    });
                }
            }
        }
    }

    async fn start_router_links(&mut self, config: Vec<RouterConfig>, is_client: bool) {
        // launch the client links. We'll connect later
        for server in config.iter() {
            let id = format!("router-{}", server.id);
            self.start_link(is_client, id, &server.host, server.port).await;
        }
    }

    async fn start_link(&mut self, is_client: bool, id: String, host: &str, port: u16) {
        let (connections_tx, connections_rx) = channel(100);
        let router_tx = self.router_tx.clone();
        let supervisor_tx = self.supervisor_tx.clone();
        let addr = format!("{}:{}", host, port);
        let link_handle = LinkHandle::new(id.clone(), addr, connections_tx);
        self.links.insert(id.clone(), link_handle);

        task::spawn(async move {
            let handles = LinkConfig { id: id.clone(), router_tx, supervisor_tx, is_client };
            let mut link = Link::new(handles).await;
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
    fn extract_servers(&self) -> (Vec<RouterConfig>, RouterConfig, Vec<RouterConfig>) {
        let id = self.config.id.clone();
        let mut routers = self.config.routers.clone().unwrap();
        let position = routers.iter().position(|v| v.id == id);
        let position = position.unwrap();

        let tail = routers.split_off(position + 1);
        let (this, head) = routers.split_last().unwrap().clone();

        (head.into(), this.clone(), tail)
    }
}


use crate::{Config, Id, MeshSettings};

use crate::network::Network;
use crate::replicationlink::link::ReplicationLink;
use mqtt4bytes::{ConnAck, ConnectReturnCode};
use rumqttlog::{bounded, Event, Sender};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task;
use tokio::time::{self, error::Elapsed};

mod link;

#[derive(thiserror::Error, Debug)]
#[error("...")]
pub enum Error {
    Io(#[from] io::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
}

/// Each router maintains a task to communicate with every other broker. Which router initiates the
/// connection is implemented using handehake combination by sequentially following routers in config
pub struct Mesh {
    /// Config which holds details of all the routers for distributed mesh
    config: Arc<Config>,
    /// Router handle to pass to links
    router_tx: Sender<(Id, Event)>,
    /// Handles to all the links
    links: HashMap<String, LinkHandle>,
}

impl Mesh {
    pub(crate) fn new(config: Arc<Config>, router_tx: Sender<(Id, Event)>) -> Mesh {
        Mesh {
            config,
            router_tx,
            links: HashMap::new(),
        }
    }

    /// This starts the replication thread. Each new connection (incoming or outgoing) is informed
    /// to the router along with the connection handle. Each router connection task can now pull
    /// data it required from the router. Router will put data retrieved from the commitlog into
    /// correct task's handle
    #[tokio::main(worker_threads = 1)]
    pub(crate) async fn start(&mut self) {
        if self.config.replicator.is_none() || self.config.cluster.is_none() {
            return;
        }

        let local_id = self.config.id;
        let (incoming, this, outgoing) = self.extract_servers();

        // start outgoing (client) links and then incoming (server) links
        self.start_replicators(local_id, outgoing, true).await;
        self.start_replicators(local_id, incoming, false).await;

        let addr = format!("{}:{}", this.host, this.port);
        let listener = TcpListener::bind(&addr).await.unwrap();
        info!("Listening on address: {}", addr);

        // start the supervision
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            debug!("Received a tcp connection from {}", addr);
            let mut network = Network::new(stream, 1024 * 1024);
            let id = match await_connect(&mut network).await {
                Ok(id) => id,
                Err(e) => {
                    error!("Failed to await connect. Error = {:?}", e);
                    continue;
                }
            };

            let handle = self.links.get_mut(&id).unwrap();
            if let Err(_e) = handle.connections_tx.send(network) {
                error!("Failed to send the connection to link");
            }
        }
    }

    /// launch client replicators. We'll connect later
    async fn start_replicators(
        &mut self,
        local_id: usize,
        remote: HashMap<String, MeshSettings>,
        is_client: bool,
    ) {
        for (server_id, server) in remote.into_iter() {
            let (connections_tx, connections_rx) = bounded(1);
            let router_tx = self.router_tx.clone();
            let addr = format!("{}:{}", server.host, server.port);

            let link_handle = LinkHandle::new(server_id.clone(), addr.clone(), connections_tx);
            self.links.insert(server_id.clone(), link_handle);

            let remote = if is_client { addr } else { "".to_owned() };
            let config = Arc::new(self.config.replicator.clone().unwrap());

            task::spawn(async move {
                let mut replicator = match ReplicationLink::new(
                    config,
                    local_id,
                    server_id,
                    router_tx,
                    connections_rx,
                    remote,
                )
                .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Failed to create replication link. Error: {:?}", e);
                        return;
                    }
                };

                if let Err(e) = replicator.start().await {
                    error!("Replicator error: {:?}", e);
                }
            });
        }
    }

    /// Extract routers from the config. Returns
    /// - Incoming connections that this router is expecting
    /// - Config of this router
    /// - Outgoing connections that this router should make
    fn extract_servers(
        &self,
    ) -> (
        HashMap<String, MeshSettings>,
        MeshSettings,
        HashMap<String, MeshSettings>,
    ) {
        let id = self.config.id.to_string();

        let mut cluster = self.config.cluster.clone().unwrap();
        let mut incoming = HashMap::new();
        let mut outgoing = HashMap::new();
        let this = cluster.remove(&id).unwrap();

        // Link establishment algorithm: Manners towards new guests
        // - Old guests initiates handshake with new guests
        // - New guests wait for handshake from old guests
        //
        // 0 creates outgoing connections for 1 and 2
        // 1 waits for incoming connection from 0 and creates outgoing connection for 2
        // 2 waits for incoming connection from 0 and 1
        for (node_id, node) in cluster.into_iter() {
            // Current node is older in comparison. Initiate handshake to `node_id`
            if id < node_id {
                outgoing.insert(node_id, node);
            }
            // Current node is newer in comparison. Expect handshake from `node_id`
            else if id > node_id {
                incoming.insert(node_id, node);
            }
        }

        (incoming, this, outgoing)
    }
}

/// Await mqtt connect packet for incoming connections from a router
async fn await_connect(network: &mut Network) -> Result<String, Error> {
    let id = time::timeout(Duration::from_secs(5), async {
        let connect = network.read_connect().await?;
        let connack = ConnAck::new(ConnectReturnCode::Accepted, false);
        network.connack(connack).await?;
        Ok::<_, Error>(connect.client_id)
    })
    .await??;

    Ok(id)
}

pub struct LinkHandle {
    pub id: String,
    pub addr: String,
    pub connections_tx: Sender<Network>,
}

impl LinkHandle {
    pub fn new(id: String, addr: String, connections_tx: Sender<Network>) -> LinkHandle {
        LinkHandle {
            id,
            addr,
            connections_tx,
        }
    }
}

impl Clone for LinkHandle {
    fn clone(&self) -> Self {
        LinkHandle {
            id: self.id.clone(),
            addr: self.addr.to_string(),
            connections_tx: self.connections_tx.clone(),
        }
    }
}

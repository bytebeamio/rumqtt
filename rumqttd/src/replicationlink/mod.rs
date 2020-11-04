use crate::{Config, Id, MeshConfig};

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
use tokio::time::{self, Elapsed};

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
    links: HashMap<usize, LinkHandle>,
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
    #[tokio::main(core_threads = 1)]
    pub(crate) async fn start(&mut self) {
        let (head, this, tail) = self.extract_servers();

        // start outgoing (client) links and then incoming (server) links
        self.start_replicators(this.id, tail, true).await;
        self.start_replicators(this.id, head, false).await;

        let addr = format!("{}:{}", this.host, this.port);
        let mut listener = TcpListener::bind(&addr).await.unwrap();
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
        remote: Vec<MeshConfig>,
        is_client: bool,
    ) {
        for server in remote.iter() {
            let (connections_tx, connections_rx) = bounded(1);
            let router_tx = self.router_tx.clone();
            let addr = format!("{}:{}", server.host, server.port);
            let server_id = server.id;
            let link_handle = LinkHandle::new(server_id, addr.clone(), connections_tx);
            self.links.insert(server_id, link_handle);

            let remote = if is_client { addr } else { "".to_owned() };
            let config = Arc::new(self.config.replicator.clone());

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
    fn extract_servers(&self) -> (Vec<MeshConfig>, MeshConfig, Vec<MeshConfig>) {
        let id = self.config.router.id.clone();
        let mut routers = self.config.router.mesh.clone().unwrap();
        let position = routers.iter().position(|v| v.id == id);
        let position = position.unwrap();

        let tail = routers.split_off(position + 1);
        let (this, head) = routers.split_last().unwrap().clone();

        (head.into(), this.clone(), tail)
    }
}

/// Await mqtt connect packet for incoming connections from a router
async fn await_connect(network: &mut Network) -> Result<usize, Error> {
    let id = time::timeout(Duration::from_secs(5), async {
        let connect = network.read_connect().await?;

        let id: usize = connect.client_id.parse().unwrap();
        let connack = ConnAck::new(ConnectReturnCode::Accepted, false);
        network.connack(connack).await?;
        Ok::<_, Error>(id)
    })
    .await??;

    Ok(id)
}

pub struct LinkHandle {
    pub id: usize,
    pub addr: String,
    pub connections_tx: Sender<Network>,
}

impl LinkHandle {
    pub fn new(id: usize, addr: String, connections_tx: Sender<Network>) -> LinkHandle {
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
            id: self.id,
            addr: self.addr.to_string(),
            connections_tx: self.connections_tx.clone(),
        }
    }
}

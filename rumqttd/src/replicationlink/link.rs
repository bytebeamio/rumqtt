use std::io;

use crate::network::Network;
use crate::remotelink::RemoteLink;
use crate::{remotelink, ConnectionSettings, Id};
use mqtt4bytes::Connect;
use rumqttlog::{Event, Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::time;

#[derive(Error, Debug)]
#[error("Replication link error")]
pub enum LinkError {
    Io(#[from] io::Error),
    Remote(#[from] remotelink::Error),
}

/// A link is a connection to another router
pub struct ReplicationLink {
    config: Arc<ConnectionSettings>,
    /// Id of the link. Id of the replica node that this connection is linked with
    remote_id: String,
    /// Id of this router node. MQTT connection is made with this ID
    local_id: usize,
    /// Handle to communicate with router
    router_tx: Sender<(Id, Event)>,
    /// Connections receiver in server mode
    connections_rx: Receiver<Network>,
    /// Remote address to connect to. Used in client mode
    remote: String,
    /// Remote link
    link: RemoteLink,
}

impl ReplicationLink {
    /// New mesh link. This task is always alive unlike a connection task event though the connection
    /// might have been down. When the connection is broken, this task informs supervisor about it
    /// which establishes a new connection on behalf of the link and forwards the connection to this
    /// task. If this link is a server, it waits for the other end to initiate the connection
    pub async fn new(
        config: Arc<ConnectionSettings>,
        local_id: usize,
        remote_id: String,
        router_tx: Sender<(Id, Event)>,
        connections_rx: Receiver<Network>,
        remote: String,
    ) -> Result<ReplicationLink, LinkError> {
        // Register this link with router even though there is no network connection
        // with other router yet. Actual connection will be requested in `start`
        info!("Connecting {} <-> {} <{:?}>", local_id, remote_id, remote);
        let network = connect(&remote, local_id, &connections_rx).await;
        let (_, _, link) = RemoteLink::new(config.clone(), router_tx.clone(), network).await?;

        Ok(ReplicationLink {
            config,
            remote_id,
            local_id,
            router_tx,
            connections_rx,
            remote,
            link,
        })
    }

    pub async fn start(&mut self) -> Result<(), LinkError> {
        loop {
            if let Err(e) = self.link.start().await {
                error!("Link failed. Error = {:?}", e);
            }

            info!(
                "Reconnecting {} <-> {} <{:?}>",
                self.local_id, self.remote_id, self.remote
            );

            let config = self.config.clone();
            let router_tx = self.router_tx.clone();
            let network = connect(&self.remote, self.local_id, &self.connections_rx).await;
            let (_, _, link) = RemoteLink::new(config, router_tx, network).await?;
            self.link = link;
        }
    }
}

/// Inform the supervisor for new connection if this is a client link. Wait for
/// a new connection handle if this is a server link
async fn connect(addr: &str, local_id: usize, connections_rx: &Receiver<Network>) -> Network {
    if !addr.is_empty() {
        loop {
            let stream = match TcpStream::connect(addr).await {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to connect to node {}. Error = {:?}", addr, e);
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            let mut network = Network::new(stream, 1024 * 1024);
            let connect = Connect::new(local_id.to_string());
            if let Err(e) = network.connect(connect).await {
                error!("Failed to mqtt connect to node {}. Error = {:?}", addr, e);
                time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            if let Err(e) = network.read_connack().await {
                error!(
                    "Failed to read connack from node = {}. Error = {:?}",
                    addr, e
                );
                time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            return network;
        }
    }

    let network = connections_rx.async_recv().await.unwrap();
    network
}

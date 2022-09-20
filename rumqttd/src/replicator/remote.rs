use crate::link::local::{LinkError, LinkRx, LinkTx};
use crate::link::network;
use crate::link::network::Network;
use crate::NodeId;
use std::collections::HashMap;
use std::time::Duration;
use std::{io, thread};
use tokio::net::TcpListener;
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::error::Elapsed;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Network {0}")]
    Network(#[from] network::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Link error = {0}")]
    Link(#[from] LinkError),
}

pub struct Cluster {
    node_id: NodeId,
    /// Address list of replicas that this node should connect to
    listeners: Vec<(NodeId, String)>,
    /// Address at which this server is listening on
    listen: String,
    /// Replica handles returned by the router
    replicas: HashMap<NodeId, (LinkTx, LinkRx)>,
}

impl Cluster {
    pub fn new<A: Into<String>>(id: NodeId, listen: &str, listeners: Vec<(NodeId, A)>) -> Cluster {
        let listeners = listeners.into_iter().map(|v| (v.0, v.1.into())).collect();
        Cluster {
            node_id: id,
            listeners,
            listen: listen.to_owned(),
            replicas: HashMap::new(),
        }
    }

    pub fn add_replica_router_handle(&mut self, id: NodeId, replica: (LinkTx, LinkRx)) {
        self.replicas.insert(id, replica);
    }

    /// Connection initiating replica
    async fn network_connect(_remote: &str, _id: &str) -> Result<Network, Error> {
        unimplemented!();
        // let socket = TcpStream::connect(remote).await?;
        // let mut network = Network::new(Box::new(socket), 1024 * 1024);
        // network.send_connect(Connect::new(id)).await?;
        // Ok(network)
    }

    async fn client(
        remote_addr: &str,
        remote_id: NodeId,
        connector_id: NodeId,
        mut link_tx: LinkTx,
        mut link_rx: LinkRx,
    ) {
        // Reconnect and run indefinitely
        loop {
            // Connect to remote replica on 'remote_addr'
            let network =
                match Cluster::network_connect(remote_addr, &connector_id.to_string()).await {
                    Ok(n) => n,
                    Err(e) => {
                        error!("Failed to connect to replica. Error = {:?}", e);
                        time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

            // Create a replica representing 'remote_id'
            let mut replica = Replica::new(remote_id, network);
            if let Err(e) = replica.run(&mut link_tx, &mut link_rx).await {
                error!("Replica stopped! Error = {:?}", e);
                time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        }
    }

    async fn accept(&mut self) {
        let listener = TcpListener::bind(&self.listen).await.unwrap();
        let delay = Duration::from_millis(100);
        let mut tasks: HashMap<NodeId, JoinHandle<(LinkTx, LinkRx)>> = HashMap::new();

        info!("Waiting for connections on {}. Server = {}", self.listen, self.node_id);
        loop {
            let (stream, _addr) = listener.accept().await.unwrap();
            let mut network = Network::new(Box::new(stream), 1024 * 1024, 100);
            let replica_id: NodeId = match network.read_connect(100).await {
                Ok(connect) => connect.client_id.parse().unwrap(),
                Err(e) => {
                    error!("Error awaiting connect from replica = {:?}", e);
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            let (link_tx, link_rx) = match self.replicas.remove(&replica_id) {
                Some(v) => v,
                None => match tasks.get_mut(&replica_id) {
                    Some(t) => t.await.unwrap(),
                    None => {
                        error!("Router handles not found for replica = {}", replica_id);
                        time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                },
            };

            // let replica_id_clone = replica_id.clone();
            let t = task::spawn(async move {
                let mut link_tx = link_tx;
                let mut link_rx = link_rx;

                // Create a replica representing 'remote_id'
                let mut replica = Replica::new(replica_id, network);
                if let Err(e) = replica.run(&mut link_tx, &mut link_rx).await {
                    error!("Replica stopped! Error = {:?}", e);
                }

                (link_tx, link_rx)
            });

            tasks.insert(replica_id, t);
            time::sleep(delay).await;
        }
    }

    /// Spawns client connections
    async fn spawn_connectors(&mut self) {
        for (remote_id, remote_addr) in self.listeners.clone() {
            let (link_tx, link_rx) = match self.replicas.remove(&remote_id) {
                Some(l) => l,
                None => panic!("Failed to fetch router links for replica = {}", remote_id),
            };

            let connector_id = self.node_id.clone();
            task::spawn(async move {
                Cluster::client(&remote_addr, remote_id, connector_id, link_tx, link_rx).await;
            });
        }
    }

    /// Start the cluster
    #[tokio::main(flavor = "current_thread")]
    pub async fn start_inner(&mut self) {
        self.spawn_connectors().await;
        self.accept().await;
    }

    pub fn spawn(mut self) {
        let t2 = thread::Builder::new().name(format!("cluster-{}", self.node_id));
        t2.spawn(move || error!("Cluster done! Reason = {:?}", self.start_inner()))
            .unwrap();
    }
}

pub struct Replica {
    /// Replica id of node this connection is representing
    id: NodeId,
    /// Link to read from and write to network
    network: Network,
}

impl Replica {
    fn new(id: NodeId, network: Network) -> Replica {
        Replica { id, network }
    }

    pub async fn run(&mut self, _link_tx: &mut LinkTx, link_rx: &mut LinkRx) -> Result<(), Error> {
        unimplemented!()
        // link_tx.replica_subscribe("#").unwrap();
        // self.network.set_keepalive(10);

        // Note:
        // Shouldn't result in bounded queue deadlocks because of blocking n/w send
        // loop {
        //     select! {
        //         o = self.network.readb() => {
        //             let packets = o?.freeze();
        //             debug!("::{} {:10} {:20} size = {} bytes", self.id, "read", "remote", packets.len());
        //             // link_tx.async_send_replica_data(packets).await?;
        //         }
        //         // Receive from router when previous when state isn't in collision
        //         // due to previously received data request
        //         notification = link_rx.next() => {
        //             let message = match notification? {
        //                 Some(v) => v,
        //                 None => continue
        //             };

        //             let write = match message {
        //                 Notification::ReplicaData{payload, ..} => payload,
        //                 Notification::ReplicaAcks{payload, ..} => payload,
        //                 v => unreachable!("Expecting only data or device acks. Received = {:?}", v)
        //             };

        //             debug!("::{} {:10} {:20} size = {} bytes", self.id, "write", "remote", write.len());
        //             self.network.write_all(&write).await?;
        //         }
        //     }
        // }
    }
}

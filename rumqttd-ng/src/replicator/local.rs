use crate::link::local::{LinkError, LinkRx, LinkTx};
use crate::router::Notification;
use crate::NodeId;
use bytes::Bytes;
use flume::{bounded, Receiver, Sender};
use std::collections::HashMap;
use std::thread;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Link error = {0}")]
    Link(#[from] LinkError),
}

pub struct Cluster {
    /// Id on this cluster's native replica (used to connect to other replicas)
    node_id: NodeId,
    /// Channel on which this server is listening on
    listen_rx: Receiver<ReplicationData>,
    /// Channel list of replicas that this node should connect to
    seniors: Vec<(NodeId, Sender<ReplicationData>)>,
    /// Replica handles returned by the router
    replicas: HashMap<NodeId, (LinkTx, LinkRx)>,
}

impl Cluster {
    pub fn new(
        node_id: NodeId,
        seniors: Vec<(NodeId, Sender<ReplicationData>)>,
    ) -> (Cluster, Sender<ReplicationData>) {
        let (listen_tx, listen_rx) = bounded(10);
        let cluster = Cluster {
            node_id,
            seniors,
            listen_rx,
            replicas: HashMap::new(),
        };
        (cluster, listen_tx)
    }

    /// Add router handle of replicas representing other nodes of the cluster
    pub fn add_replica_router_handle(&mut self, id: NodeId, replica: (LinkTx, LinkRx)) {
        self.replicas.insert(id, replica);
    }

    // Router 2 connects to router 0 and 1
    // Router 1 connects to router 0
    // Router 0 just listens for connections
    fn spawn_connectors(&mut self) {
        for (replica_id, listener) in self.seniors.iter() {
            let (link_tx, link_rx) = self.replicas.remove(replica_id).unwrap();
            let (native_tx, native_rx) = bounded(10);
            let (remote_tx, remote_rx) = bounded(10);
            let connect = ReplicationData::Connect {
                id: self.node_id,
                tx: native_tx,
                rx: remote_rx,
            };
            listener.send(connect).unwrap();
            let mut replica = Replica::new(replica_id.clone(), remote_tx, native_rx).unwrap();
            replica.start(link_tx, link_rx);
        }
    }

    // Accepts connections from other routers
    fn accept(&mut self) {
        loop {
            match self.listen_rx.recv().unwrap() {
                ReplicationData::Connect {
                    id: replica_id,
                    tx,
                    rx,
                } => {
                    info!("{}:{:?} {:11} {:20}", self.node_id, replica_id, "connect", "replica");
                    let (link_tx, link_rx) = self.replicas.remove(&replica_id).unwrap();
                    thread::spawn(move || {
                        let remote_tx = tx;
                        let local_rx = rx;
                        let mut replica = Replica::new(replica_id, remote_tx, local_rx).unwrap();
                        replica.start(link_tx, link_rx);
                    });
                }
                data => {
                    error!("Invalid listen data. {:?} is not a connect packet", data);
                    continue;
                }
            }
        }
    }

    pub fn spawn(mut self) {
        self.spawn_connectors();
        let t2 = thread::Builder::new().name(format!("cluster-{}", self.node_id));
        t2.spawn(move || error!("Cluster done! Reason = {:?}", self.accept()))
            .unwrap();
    }
}

pub struct Replica {
    id: NodeId,
    replica_tx: Option<Sender<ReplicationData>>,
    replica_rx: Option<Receiver<ReplicationData>>,
}

impl Replica {
    pub fn new(
        id: NodeId,
        replica_tx: Sender<ReplicationData>,
        replica_rx: Receiver<ReplicationData>,
    ) -> Result<Replica, Error> {
        Ok(Replica {
            id,
            replica_tx: Some(replica_tx),
            replica_rx: Some(replica_rx),
        })
    }

    pub fn start(&mut self, _link_tx: LinkTx, mut link_rx: LinkRx) {
        let replica_rx = self.replica_rx.take().unwrap();
        let id = self.id.clone();

        thread::spawn(move || {
            // link_tx.replica_subscribe("#").unwrap();

            loop {
                let data = replica_rx.recv().unwrap();
                match data {
                    ReplicationData::Data { offset, payload } => {
                        trace!(
                            "::{} {:11} {:20} Count = {} Offset = {:?}",
                            id,
                            "repl-data",
                            "incoming",
                            payload.len(),
                            offset
                        );

                        // link_tx.send_replica_data(payload).unwrap();
                    }
                    ReplicationData::Ack { offset, .. } => {
                        trace!(
                            "::{} {:11} {:20}  Offset = {:?}",
                            id,
                            "repl-ack",
                            "incoming",
                            offset
                        );

                        // link_tx.send_replica_data(payload).unwrap();
                    }
                    data => {
                        error!("Invalid replicated data. {:?}", data);
                        continue;
                    }
                };
            }
        });

        let replica_tx = self.replica_tx.take().unwrap();
        let id = self.id.clone();
        loop {
            let notification = match link_rx.recv().unwrap() {
                Some(v) => v,
                None => continue,
            };

            match notification {
                Notification::ReplicaData {
                    cursor: offset,
                    size,
                    payload,
                } => {
                    let count = payload.len();
                    trace!(
                        "::{} {:11} {:20} Count = {} Size = {}",
                        id,
                        "rout-data",
                        "outgoing",
                        count,
                        size
                    );
                    replica_tx
                        .send(ReplicationData::Data { offset, payload })
                        .unwrap();
                }
                Notification::ReplicaAcks { offset, payload } => {
                    replica_tx
                        .send(ReplicationData::Ack { offset, payload })
                        .unwrap();
                }
                notification => {
                    unreachable!("Unexpected notification on replicator = {:?}", notification)
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum ReplicationData {
    Connect {
        id: NodeId,
        tx: Sender<ReplicationData>,
        rx: Receiver<ReplicationData>,
    },
    Data {
        offset: (u64, u64),
        payload: Bytes,
    },
    Ack {
        offset: (u64, u64),
        payload: Bytes,
    },
}

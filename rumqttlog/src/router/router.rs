use std::collections::HashMap;
use std::{io, mem, thread};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use super::commitlog::CommitLog;
use super::{Connection, RouterInMessage, RouterOutMessage};
use crate::router::commitlog::TopicLog;
use crate::router::{DataReply, DataRequest, TopicsReply, TopicsRequest, Data, ConnectionType, ConnectionAck, DataAck};
use crate::Config;
use thiserror::Error;
use tokio::sync::mpsc::error::TrySendError;
use super::bytes::Bytes;
use crate::mesh::Mesh;

#[derive(Error, Debug)]
#[error("...")]
pub enum Error {
    Mpsc(#[from] TrySendError<RouterOutMessage>),
    Io(#[from] io::Error),
}

type ConnectionId = usize;
type Topic = String;
type Offset = u64;

pub struct Router {
    config: Config,
    /// Commit log by topic. Commit log stores all the of given topic. The
    /// details are very similar to what kafka does. Who know, we might
    /// even make the broker kafka compatible and directly feed it to databases
    commitlog: CommitLog,
    /// Messages directly received from connections should be separated from messages
    /// received by the router replication to make sure that linker of the current
    /// instance doesn't pull these again
    replicatedlog: CommitLog,
    /// Captures new topic just like commitlog
    topiclog: TopicLog,
    /// Array of all the connections. Preinitialized to a fixed set of connections.
    /// `Connection` struct is initialized when there is an actual network connection
    /// Index of this array represents `ConnectionId`
    /// Replace this with [Option<Connection>, 10000] when `Connection` support `Copy`?
    connections: Vec<Option<Connection>>,
    /// Waiter on a topic. These are used to wake connections/replicators
    /// which are caught up all the data on a topic. Map[topic]List[Connections Ids]
    data_waiters: HashMap<Topic, Vec<(ConnectionId, DataRequest)>>,
    /// Waiters on new topics
    topics_waiters: Vec<(ConnectionId, TopicsRequest)>,
    /// Watermarks of all the replicas. Map[topic]List[u64]. Each index
    /// represents a router in the mesh
    watermarks: HashMap<Topic, Vec<Offset>>,
    /// Channel receiver to receive data from all the active connections and
    /// replicators. Each connection will have a tx handle which they use
    /// to send data and requests to router
    router_rx: Receiver<(ConnectionId, RouterInMessage)>,
    /// A sender to the router. This is handed to `Mesh`
    router_tx: Sender<(ConnectionId, RouterInMessage)>,
}

/// Router is the central node where most of the state is held. Connections and
/// replicators ask router for data and router responds by putting data into
/// relevant connection handle
impl Router {
    pub fn new(config: Config) -> (Self, Sender<(ConnectionId, RouterInMessage)>) {
        let (router_tx, router_rx) = channel(1000);
        let commitlog = CommitLog::new(config.clone());
        let replicatedlog = CommitLog::new(config.clone());
        let topiclog = TopicLog::new();

        let router = Router {
            config: config.clone(),
            commitlog,
            replicatedlog,
            topiclog,
            connections: vec![None; 1000],
            data_waiters: HashMap::new(),
            topics_waiters: Vec::new(),
            watermarks: HashMap::new(),
            router_rx,
            router_tx: router_tx.clone(),
        };

        (router, router_tx)
    }

    fn enable_replication(&mut self) {
        debug!("Enabling replication");
        let mut replicator = Mesh::new(self.config.clone(), self.router_tx.clone());
        thread::spawn(move || {
            replicator.start();
        });
    }

    pub async fn start(&mut self) {
        if self.config.routers.is_some() {
            self.enable_replication();
        }

        // All these methods will handle state and errors
        while let Some((id, data)) = self.router_rx.recv().await {
            match data {
                RouterInMessage::Connect(connection) => self.handle_new_connection(connection),
                RouterInMessage::Data(data) => self.handle_incoming_data(id, data),
                RouterInMessage::DataRequest(request) => {
                    let reply = self.extract_data(&request);
                    let reply = match reply {
                        Some(r) => r,
                        None => continue
                    };

                    // This connection/linker is completely caught up with this topic.
                    // Add this connection/linker into waiters list of this topic.
                    // Note that we also send empty reply to the link with uses this to mark
                    // this topic as a caught up to not make a request again while iterating
                    // through its list of topics. Not sending an empty response will block
                    // the 'link' from polling next topic
                    if reply.payload.is_empty() {
                        self.register_data_waiter(id, request, &reply);
                    }
                    self.reply_data(id, reply);
                }
                RouterInMessage::TopicsRequest(request) => {
                    let reply = self.extract_topics(&request);
                    // register this id to wake up when there are new topics.
                    // don't send a reply of empty topics
                    if reply.topics.is_empty() {
                        self.register_topics_waiter(id, request);
                        continue;
                    }
                    self.reply_topics(id, reply);
                },
            }
        }

        error!("Router stopped!!");
    }

    fn handle_new_connection(&mut self, mut connection: Connection) {
        let id = match &connection.conn {
            ConnectionType::Replicator(id)  => *id,
            ConnectionType::Device(did) => {
                let mut id = 0;
                for (i, connection) in self.connections.iter_mut().enumerate() {
                    // positions 0..9 are reserved for replicators
                    if connection.is_none() && i >= 10 {
                        id = i;
                        break
                    }
                }

                if id == 0 {
                    error!("No empty slots found for incoming connection = {:?}", did);
                    // TODO ack connection with failure as there are no empty slots
                    return
                }

                id
            }
        };

        let message = RouterOutMessage::ConnectionAck(ConnectionAck::Success(id));
        if let Err(e) = connection.handle.try_send(message) {
            error!("Failed to send connection ack. Error = {:?}", e.to_string());
        }

        info!("New Connection. Incoming ID = {:?}, Router assigned ID = {:?}", connection.conn, id);
        if let Some(_) = mem::replace(&mut self.connections[id], Some(connection)) {
            warn!("Replacing an existing connection with same ID");
        }
    }

    /// Handles
    fn handle_incoming_data(&mut self, id: ConnectionId, data: Data) {
        let Data {
            pkid,
            topic,
            payload,
        } = data;

        if payload.len() == 0 {
            error!("Empty publish. Ignoring");
            return
        }

        // Acknowledge the connection after writing data to commitlog
        if let Some(offset) = self.append_to_commitlog(id, &topic, payload) {
            self.ack_data(id, pkid, offset)
        }

        // If there is a new unique append, send it to connection/linker waiting
        // on it. This is equivalent to hybrid of block and poll and we don't need
        // timers. Connections/Replicator will make a request and request fails as
        // there is no new data. Router caches the failed request on the topic.
        // If there is new data on this topic, router fulfills the last failed request.
        // This completely eliminates the need of polling
        if self.topiclog.unique_append(&topic) {
            self.fresh_topics_notification(id);
        }

        self.fresh_data_notification(id, &topic);
    }

    /// Send notifications to links which registered them
    fn fresh_topics_notification(&mut self, id: ConnectionId) {
        // TODO too many indirection to get a link to the handle, probably directly
        // TODO clone a handle to the link and save it instead of saving ids?
        let waiters = mem::replace(&mut self.topics_waiters, Vec::new());
        for (link_id, request) in waiters {
            // don't send replicated topic notifications to replication link
            // id 0-10 are reserved for replications which are linked to other routers in the mesh
            let replication_data = id < 10;
            if replication_data {
                continue;
            }

            // Send reply to the link which registered this notification
            let reply = self.extract_topics(&request);
            self.reply_topics(link_id, reply);

            // NOTE:
            // ----------------------
            // When the reply is empty don't register notification on behalf of the link.
            // This will cause the channel to be full. Register the notification only when
            // link has made the request and reply doesn't contain any data
        }
    }

    /// Send data to links which registered them
    fn fresh_data_notification(&mut self, id: ConnectionId, topic: &str) {
        let waiters = match self.data_waiters.remove(topic) {
            Some(w) => w,
            None => return
        };

        for (link_id, request) in waiters {
            let replication_data = id < 10;
            // don't send replicated data notifications to replication link
            if replication_data {
                continue;
            }

            // Send reply to the link which registered this notification
            if let Some(reply) = self.extract_data(&request) {
                self.reply_data(link_id, reply);
            }

            // NOTE:
            // ----------------------
            // When the reply is empty don't register notification on behalf of the link.
            // This will cause the channel to be full. Register the notification only when
            // link has made the request and reply doesn't contain any data
        }
    }

    /// Separate logs due to replication and logs from connections. Connections pull
    /// logs from both replication and connections where as linker only pull logs
    /// from connections
    fn append_to_commitlog(&mut self, id: ConnectionId, topic: &str, bytes: Bytes) -> Option<u64> {
        // id 0-10 are reserved for replications which are linked to other routers in the mesh
        let replication_data =  id < 10;
        if replication_data {
            debug!("Receiving replication data from {}, topic = {}", id, topic);
            if let Err(e) = self.replicatedlog.append(&topic, bytes) {
                error!("Commitlog append failed. Error = {:?}", e);
            }

            // No need to ack the replicator
            None
        } else {
            match self.commitlog.append(&topic, bytes) {
                Ok(offset) => Some(offset),
                Err(e) => {
                    error!("Commitlog append failed. Error = {:?}", e);
                    None
                }
            }
        }
    }

    fn extract_topics(&mut self,  request: &TopicsRequest) -> TopicsReply {
        let o = self.topiclog.read(request.offset, request.count);
        let reply = TopicsReply {
            done: o.0,
            offset: o.1,
            topics: o.2,
        };

        reply
    }

    /// Sends topics to link
    fn reply_topics(&mut self, id: ConnectionId, reply: TopicsReply) {
        let connection = match self.connections.get_mut(id).unwrap() {
            Some(c) => c,
            None => {
                error!("2. Invalid id = {:?}", id);
                return
            }
        };

        let reply = RouterOutMessage::TopicsReply(reply);
        if let Err(e) = connection.handle.try_send(reply) {
            error!("Failed to topics refresh reply. Error = {:?}", e);
        }
    }

    fn register_topics_waiter(&mut self, id: ConnectionId, request: TopicsRequest) {
        let request = (id.to_owned(), request);
        self.topics_waiters.push(request);
    }

    /// extracts data from correct commitlog's segment and sends it
    fn extract_data(&mut self, request: &DataRequest) -> Option<DataReply> {
        debug!("Data request. Topic = {}, Segment = {}, offset = {}", request.topic, request.segment, request.offset);
        let o = match self.commitlog.readv(&request.topic, request.segment, request.offset, request.size) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to extract data from commitlog. Error = {:?}", e);
                return None;
            }
        };

        let o = match o {
            Some(o) => o,
            None => {
                return None
            },
        };

        // TODO handle acks
        let reply = DataReply {
            done: o.0,
            topic: request.topic.clone(),
            payload: o.5,
            segment: o.1,
            offset: o.2,
            pkids: o.4,
        };

        Some(reply)
    }

    /// Sends data that connection/replicator asked for
    fn reply_data(&mut self, id: ConnectionId, reply: DataReply) {
        debug!(
            "Data reply.   Topic = {}, Segment = {}, offset = {}, size = {}",
            reply.topic,
            reply.segment,
            reply.offset,
            reply.payload.len(),
        );

        let connection = match self.connections.get_mut(id).unwrap() {
            Some(c) => c,
            None => {
                error!("1. Invalid id = {:?}", id);
                return
            }
        };

        let reply = RouterOutMessage::DataReply(reply);
        if let Err(e) = connection.handle.try_send(reply) {
            error!("Failed to data reply. Error = {:?}", e.to_string());
        }
    }

    /// Register data waiter
    fn register_data_waiter(&mut self, id: ConnectionId, mut request: DataRequest, reply: &DataReply) {
        request.segment = reply.segment;
        request.offset = reply.offset + 1;
        let request = (id, request);
        if let Some(waiters) = self.data_waiters.get_mut(&reply.topic) {
            waiters.push(request);
        } else {
            let mut waiters = Vec::new();
            waiters.push(request);
            self.data_waiters.insert(reply.topic.to_owned(), waiters);
        }
    }

    /// Acknowledge connection after the data is written to commitlog
    fn ack_data(&mut self, id: ConnectionId, pkid: u64, offset: u64) {
        let connection = match self.connections.get_mut(id).unwrap() {
            Some(c) => c,
            None => {
                error!("2. Invalid id = {:?}", id);
                return
            }
        };

        let ack = RouterOutMessage::DataAck(DataAck { pkid, offset });
        if let Err(e) = connection.handle.try_send(ack) {
            error!("Failed to topics refresh reply. Error = {:?}", e);
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Router, ConnectionId};
    use crate::{Config, RouterInMessage, Connection, RouterOutMessage};
    use tokio::sync::mpsc::{Sender, channel, Receiver};
    use bytes::Bytes;
    use crate::router::{ConnectionType, ConnectionAck, TopicsRequest, Data};
    use std::thread;
    use std::time::Duration;

    #[tokio::test(core_threads = 1)]
    async fn new_data_on_existing_topic_should_to_reply_to_waiting_links() {
        // start router in background
        let mut router_tx = new_router().await;
        let conn = ConnectionType::Device("test".to_string());
        let (connection_id, connection_rx) = new_link(conn, &mut router_tx).await;
        let conn = ConnectionType::Replicator(0);
        let (replicator_id, mut replicator_rx) = new_link(conn, &mut router_tx).await;

        dbg!(connection_id, replicator_id);
        let message = (replicator_id, RouterInMessage::TopicsRequest(TopicsRequest { offset: 0, count: 10 }));
        router_tx.try_send(message).unwrap();

        let data = Data {
            pkid: 0,
            topic: "hello/world".to_string(),
            payload: Bytes::from(vec![1, 2, 3])
        };

        let message = (connection_id, RouterInMessage::Data(data));
        router_tx.try_send(message).unwrap();

        let o = replicator_rx.recv().await.unwrap();
        dbg!(o);
    }

    #[test]
    fn new_topic_should_to_reply_to_waiting_links() {}


    #[test]
    fn new_replicated_data_and_topics_should_not_notifiy_replicator() {}

    async fn new_router() -> Sender<(ConnectionId, RouterInMessage)> {
        let (router, router_tx) = Router::new(Config::default());
        tokio::task::spawn(async move {
            let mut router = router;
            router.start().await;
        });

        router_tx
    }

    async fn new_link(conn: ConnectionType, router_tx: &mut Sender<(ConnectionId, RouterInMessage)>) -> (ConnectionId,  Receiver<RouterOutMessage>) {
        let (link_tx, mut link_rx) = channel(4);
        let connection = Connection {
            conn,
            handle: link_tx,
        };

        let message = RouterInMessage::Connect(connection);
        router_tx.send((0, message)).await.unwrap();
        let id = match link_rx.recv().await.unwrap() {
            RouterOutMessage::ConnectionAck(ConnectionAck::Success(id)) => id,
            o => panic!("Unexpected connection ack = {:?}", o)
        };


        (id, link_rx)
    }
}

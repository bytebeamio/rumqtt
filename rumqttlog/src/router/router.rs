use std::collections::HashMap;
use std::{io, mem, thread};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use super::bytes::Bytes;
use super::commitlog::CommitLog;
use super::{Connection, RouterInMessage, RouterOutMessage};
use crate::mesh::Mesh;
use crate::router::commitlog::TopicLog;
use crate::router::{
    ConnectionAck, ConnectionType, Data, DataAck, DataReply, DataRequest, TopicsReply,
    TopicsRequest,
};
use crate::Config;
use thiserror::Error;
use tokio::sync::mpsc::error::TrySendError;

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
                        None => continue,
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
                }
            }
        }

        error!("Router stopped!!");
    }

    fn handle_new_connection(&mut self, mut connection: Connection) {
        let id = match &connection.conn {
            ConnectionType::Replicator(id) => *id,
            ConnectionType::Device(did) => {
                let mut id = 0;
                for (i, connection) in self.connections.iter_mut().enumerate() {
                    // positions 0..9 are reserved for replicators
                    if connection.is_none() && i >= 10 {
                        id = i;
                        break;
                    }
                }

                if id == 0 {
                    error!("No empty slots found for incoming connection = {:?}", did);
                    // TODO ack connection with failure as there are no empty slots
                    return;
                }

                id
            }
        };

        let message = RouterOutMessage::ConnectionAck(ConnectionAck::Success(id));
        if let Err(e) = connection.handle.try_send(message) {
            error!("Failed to send connection ack. Error = {:?}", e.to_string());
        }

        info!(
            "New Connection. Incoming ID = {:?}, Router assigned ID = {:?}",
            connection.conn, id
        );
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
            // TODO More debug info. Id etc
            error!("Empty publish. Ignoring");
            return;
        }

        // Acknowledge the connection after writing data to commitlog
        if let Some(offset) = self.append_to_commitlog(id, pkid, &topic, payload) {
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
        let replication_data = id < 10;
        for (link_id, request) in waiters {
            // don't send replicated topic notifications to replication link
            // id 0-10 are reserved for replicatiors which are linked to other routers in the mesh
            if replication_data && link_id < 10 {
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
            None => return,
        };

        let replication_data = id < 10;
        for (link_id, request) in waiters {
            // don't send replicated data notifications to replication link
            if replication_data && link_id < 10 {
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
    fn append_to_commitlog(&mut self, id: ConnectionId, pkid: u64, topic: &str, bytes: Bytes) -> Option<u64> {
        // id 0-10 are reserved for replications which are linked to other routers in the mesh
        let replication_data = id < 10;
        if replication_data {
            debug!("Receiving replication data from {}, topic = {}", id, topic);
            match self.replicatedlog.append(&topic, bytes) {
                Ok(_) => Some(pkid),
                Err(e) => {
                    error!("Commitlog append failed. Error = {:?}", e);
                    None
                }
            }
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

    fn extract_topics(&mut self, request: &TopicsRequest) -> TopicsReply {
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
                return;
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
        debug!(
            "Data request. Topic = {}, Segment = {}, offset = {}",
            request.topic, request.segment, request.offset
        );
        let o = match self.commitlog.readv(
            &request.topic,
            request.segment,
            request.offset,
            request.size,
        ) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to extract data from commitlog. Error = {:?}", e);
                return None;
            }
        };

        let o = match o {
            Some(o) => o,
            None => return None,
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
                return;
            }
        };

        let reply = RouterOutMessage::DataReply(reply);
        if let Err(e) = connection.handle.try_send(reply) {
            error!("Failed to data reply. Error = {:?}", e.to_string());
        }
    }

    /// Register data waiter
    fn register_data_waiter(
        &mut self,
        id: ConnectionId,
        mut request: DataRequest,
        reply: &DataReply,
    ) {
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
                return;
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
    use super::{ConnectionId, Router};
    use crate::router::{ConnectionAck, ConnectionType, Data, TopicsRequest, TopicsReply, DataAck};
    use crate::{Config, Connection, RouterInMessage, RouterOutMessage, DataRequest, DataReply};
    use bytes::Bytes;
    use std::time::Duration;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::{channel, Receiver, Sender};

    #[tokio::test(core_threads = 1)]
    async fn new_topic_from_connection_should_notify_replicator_and_connection() {
        let (mut router_tx, replicator_id, mut replicator_rx, connection_id, mut connection_rx) = setup().await;

        // Send request for new topics. Router should reply with new topics when there are any
        new_topics_request(replicator_id, &mut router_tx);
        new_topics_request(connection_id, &mut router_tx);

        // see if routers replys with topics
        assert!(wait_for_new_topics(&mut replicator_rx).await.is_none());

        // Send new data to router to be written to commitlog
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        assert!(wait_for_ack(&mut connection_rx).await.is_some());

        // see if routers replys with topics
        assert_eq!(wait_for_new_topics(&mut replicator_rx).await.unwrap().topics[0], "hello/world");
        assert_eq!(wait_for_new_topics(&mut connection_rx).await.unwrap().topics[0], "hello/world");
    }


    #[tokio::test(core_threads = 1)]
    async fn new_topic_from_replicator_should_notify_only_connection() {
        let (mut router_tx, replicator_id, mut replicator_rx, connection_id, mut connection_rx) = setup().await;
        // Send request for new topics. Router should reply with new topics when there are any
        new_topics_request(replicator_id, &mut router_tx);
        new_topics_request(connection_id, &mut router_tx);

        // see if routers replys with topics
        assert!(wait_for_new_topics(&mut replicator_rx).await.is_none());

        // Send new data to router to be written to commitlog
        write_to_commitlog(replicator_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        assert!(wait_for_ack(&mut replicator_rx).await.is_some());

        // see if routers replys with topics
        assert_eq!(wait_for_new_topics(&mut connection_rx).await.unwrap().topics[0], "hello/world");
        assert!(wait_for_new_topics(&mut replicator_rx).await.is_none());
    }


    #[tokio::test(core_threads = 1)]
    async fn new_data_from_connection_should_notify_replicator_and_connection() {
        pretty_env_logger::init();
        let (mut router_tx, replicator_id, mut replicator_rx, connection_id, mut connection_rx) = setup().await;

        // Send new data from connection to router to be written to commitlog
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        assert!(wait_for_ack(&mut connection_rx).await.is_some());

        // Send request for new topics. Router should reply with new topics when there are any
        new_data_request(connection_id, &mut router_tx, "hello/world", 0);
        new_data_request(replicator_id, &mut router_tx, "hello/world", 0);

        // first wait on connection_rx should return some data and next wait none
        assert_eq!(wait_for_new_data(&mut connection_rx).await.unwrap().payload[0].as_ref(), &[1, 2, 3]);
        assert!(wait_for_new_data(&mut connection_rx).await.is_none());

        // first wait on replicator_rx should return some data and next wait none
        assert_eq!(wait_for_new_data(&mut replicator_rx).await.unwrap().payload[0].as_ref(), &[1, 2, 3]);
        assert!(wait_for_new_data(&mut replicator_rx).await.is_none());

        // Send request for new topics. Router should reply with new topics when there are any
        new_data_request(connection_id, &mut router_tx, "hello/world", 1);
        new_data_request(replicator_id, &mut router_tx, "hello/world", 1);
        assert_eq!(wait_for_new_data(&mut connection_rx).await.unwrap().payload.len(), 0);
        assert_eq!(wait_for_new_data(&mut replicator_rx).await.unwrap().payload.len(), 0);

        // write new data at offset 1
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![4, 5, 6]);
        assert!(wait_for_ack(&mut connection_rx).await.is_some());

        new_data_request(connection_id, &mut router_tx, "hello/world", 1);
        new_data_request(replicator_id, &mut router_tx, "hello/world", 1);
        assert_eq!(wait_for_new_data(&mut connection_rx).await.unwrap().payload[0].as_ref(), &[4, 5, 6]);
        assert_eq!(wait_for_new_data(&mut replicator_rx).await.unwrap().payload[0].as_ref(), &[4, 5, 6]);
    }


    #[tokio::test(core_threads = 1)]
    async fn new_data_from_replicator_should_notify_only_connection() {

    }


    #[test]
    fn new_replicated_data_and_topics_should_not_notifiy_replicator() {}

    // -------------------- All helper methods to make tests clean and readable --------------------------------------

    /// Creates a router, a connection link, a replicator link and returns them along with IDs
    async fn setup() -> (
        Sender<(ConnectionId, RouterInMessage)>,
        ConnectionId,
        Receiver<RouterOutMessage>,
        ConnectionId,
        Receiver<RouterOutMessage>,
    ) {
        let (router, mut router_tx) = Router::new(Config::default());
        tokio::task::spawn(async move {
            let mut router = router;
            router.start().await;
        });

        let (link_tx, mut replicator_rx) = channel(4);
        let connection = Connection {
            conn: ConnectionType::Replicator(0),
            handle: link_tx,
        };

        let message = RouterInMessage::Connect(connection);
        router_tx.send((0, message)).await.unwrap();

        let replicator_id = match replicator_rx.recv().await.unwrap() {
            RouterOutMessage::ConnectionAck(ConnectionAck::Success(id)) => id,
            o => panic!("Unexpected connection ack = {:?}", o),
        };

        let (link_tx, mut connection_rx) = channel(4);
        let connection = Connection {
            conn: ConnectionType::Device("blah".to_string()),
            handle: link_tx,
        };

        let message = RouterInMessage::Connect(connection);
        router_tx.send((0, message)).await.unwrap();

        let connection_id = match connection_rx.recv().await.unwrap() {
            RouterOutMessage::ConnectionAck(ConnectionAck::Success(id)) => id,
            o => panic!("Unexpected connection ack = {:?}", o),
        };

        (
            router_tx,
            replicator_id,
            replicator_rx,
            connection_id,
            connection_rx,
        )
    }

    fn write_to_commitlog(
        id: usize,
        router_tx: &mut Sender<(ConnectionId, RouterInMessage)>,
        topic: &str,
        payload: Vec<u8>,
    ) {
        let message = (
            id,
            RouterInMessage::Data(Data {
                pkid: 0,
                topic: topic.to_string(),
                payload: Bytes::from(payload),
            }),
        );

        router_tx.try_send(message).unwrap();
    }

    fn new_topics_request(id: usize, router_tx: &mut Sender<(ConnectionId, RouterInMessage)>) {
        let message = (
            id,
            RouterInMessage::TopicsRequest(TopicsRequest {
                offset: 0,
                count: 10,
            }),
        );
        router_tx.try_send(message).unwrap();
    }

    fn new_data_request(
        id: usize,
        router_tx: &mut Sender<(ConnectionId, RouterInMessage)>,
        topic: &str,
        offset: u64
    ) {
        let message = (
            id,
            RouterInMessage::DataRequest(DataRequest {
                topic: topic.to_string(),
                segment: 0,
                offset,
                size: 100 * 1024
            }),
        );
        router_tx.try_send(message).unwrap();
    }

    async fn wait_for_new_topics(rx: &mut Receiver<RouterOutMessage>) -> Option<TopicsReply> {
        tokio::time::delay_for(Duration::from_secs(1)).await;
        match rx.try_recv() {
            Ok(RouterOutMessage::TopicsReply(reply)) => Some(reply),
            v => {
                error!("{:?}", v);
                None
            }
        }
    }

    async fn wait_for_ack(rx: &mut Receiver<RouterOutMessage>) -> Option<DataAck> {
        tokio::time::delay_for(Duration::from_secs(1)).await;
        match rx.try_recv() {
            Ok(RouterOutMessage::DataAck(ack)) => Some(ack),
            v => {
                error!("{:?}", v);
                None
            }
        }
    }


    async fn wait_for_new_data(rx: &mut Receiver<RouterOutMessage>) -> Option<DataReply> {
        tokio::time::delay_for(Duration::from_secs(1)).await;
        match rx.try_recv() {
            Ok(RouterOutMessage::DataReply(reply)) => Some(reply),
            v => {
                error!("{:?}", v);
                None
            }
        }
    }
}

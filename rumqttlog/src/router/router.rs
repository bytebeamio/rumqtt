use async_channel::{bounded, Receiver, Sender};
use std::collections::HashMap;
use std::{io, mem, thread};

use super::bytes::Bytes;
use super::commitlog::CommitLog;
use super::{Connection, RouterInMessage, RouterOutMessage};
use crate::mesh::Mesh;
use crate::router::commitlog::TopicLog;
use crate::router::{ConnectionAck, ConnectionType, DataReply, DataRequest, TopicsReply, TopicsRequest, AcksReply, AcksRequest, Disconnection, ReplicationAck};

use crate::{Config, ReplicationData};
use thiserror::Error;
use tokio::stream::StreamExt;
use rumqttc::Publish;
use crate::router::watermarks::Watermarks;

#[derive(Error, Debug)]
#[error("...")]
pub enum Error {
    Io(#[from] io::Error),
}

type ConnectionId = usize;
type Topic = String;

pub struct Router {
    /// Router configuration
    config: Config,
    /// Id of this router. Used to index native commitlog to store data from
    /// local connections
    id: ConnectionId,
    /// Commit log by topic. Commit log stores all the of given topic. The
    /// details are very similar to what kafka does. Who knows, we might
    /// even make the broker kafka compatible and directly feed it to databases
    commitlog: [CommitLog; 3],
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
    /// Waiters on watermark updates
    /// Whenever a topic is replicated, it's watermark is updated till the offset
    /// that replication has happened
    ack_waiters: HashMap<Topic, Vec<(ConnectionId, AcksRequest)>>,
    /// Watermarks of all the replicas. Map[topic]List[u64]. Each index
    /// represents a router in the mesh
    /// Watermark 'n' implies data till n-1 is synced with the other node
    watermarks: HashMap<Topic, Watermarks>,
    /// Channel receiver to receive data from all the active connections and
    /// replicators. Each connection will have a tx handle which they use
    /// to send data and requests to router
    router_rx: Receiver<(ConnectionId, RouterInMessage)>,
    /// A sender to the router. This is handed to `Mesh`
    router_tx: Sender<(ConnectionId, RouterInMessage)>,
}

/// Router is the central node where most of the state is held. Connections and
/// replicators ask router for data and router responds by putting data int
/// relevant connection handle
impl Router {
    pub fn new(config: Config) -> (Self, Sender<(ConnectionId, RouterInMessage)>) {
        let (router_tx, router_rx) = bounded(1000);
        let topiclog = TopicLog::new();
        let commitlog = [CommitLog::new(config.clone()), CommitLog::new(config.clone()), CommitLog::new(config.clone())];

        let router = Router {
            config: config.clone(),
            id: config.id,
            commitlog,
            topiclog,
            connections: vec![None; 1000],
            data_waiters: HashMap::new(),
            topics_waiters: Vec::new(),
            ack_waiters: HashMap::new(),
            watermarks: HashMap::new(),
            router_rx,
            router_tx: router_tx.clone(),
        };

        (router, router_tx)
    }

    fn enable_replication(&mut self) {
        let mut replicator = Mesh::new(self.config.clone(), self.router_tx.clone());
        thread::spawn(move || {
            replicator.start();
        });
    }

    pub async fn start(&mut self) {
        if self.config.mesh.is_some() {
            self.enable_replication();
        }

        // All these methods will handle state and errors
        while let Some((id, data)) = self.router_rx.next().await {
            match data {
                RouterInMessage::Connect(connection) => self.handle_new_connection(connection),
                RouterInMessage::ConnectionData(data) => self.handle_connection_data(id, data),
                RouterInMessage::ReplicationData(data) => self.handle_replication_data(id, data),
                RouterInMessage::ReplicationAcks(ack) => self.handle_replication_acks(id, ack),
                RouterInMessage::TopicsRequest(request) => self.handle_topics_request(id, request),
                RouterInMessage::DataRequest(request) => self.handle_data_request(id, request),
                RouterInMessage::AcksRequest(request) => self.handle_acks_request(id, request),
                RouterInMessage::Disconnect(request) => self.handle_disconnection(id, request),
                RouterInMessage::AllTopicsRequest => self.handle_all_topics_request(id),
            }
        }

        error!("Router stopped!!");
    }

    fn handle_new_connection(&mut self, connection: Connection) {
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

                // TODO ack connection with failure as there are no empty slots
                if id == 0 {
                    error!("No empty slots found for incoming connection = {:?}", did);
                    return;
                }

                id
            }
        };

        let message = RouterOutMessage::ConnectionAck(ConnectionAck::Success(id));
        if let Err(e) = connection.handle.try_send(message) {
            error!("Failed to send connection ack. Error = {:?}", e.to_string());
            return
        }

        info!("New Connection. In ID = {:?}, Router assigned ID = {:?}", connection.conn, id);
        if let Some(_) = mem::replace(&mut self.connections[id], Some(connection)) {
            warn!("Replacing an existing connection with same ID");
        }
    }

    fn handle_disconnection(&mut self, id: ConnectionId, disconnect: Disconnection) {
        info!("Cleaning ID [{}] = {:?} from router", disconnect.id, id);
        if mem::replace(&mut self.connections[id], None).is_none() {
            warn!("Weird, removing a non existent connection")
        }

        // FIXME We are iterating through all the topics to remove this connection
        for (_, watermarks) in self.watermarks.iter_mut() {
            mem::replace(&mut watermarks.pkid_offset_map[id], None) ;
        }

        // FIXME iterates through all the topics and all the (pending) requests remove the request
        for waiters in self.data_waiters.values_mut() {
            if let Some(index) =  waiters.iter().position(|x| x.0 == id) {
                waiters.swap_remove(index);
            }
        }

        for waiters in self.ack_waiters.values_mut() {
            if let Some(index) = waiters.iter().position(|x| x.0 == id) {
                waiters.swap_remove(index);
            }
        }

        if let Some(index) = self.topics_waiters.iter().position(|x| x.0 == id) {
            self.topics_waiters.swap_remove(index);
        }
    }

    /// Handles new incoming data on a topic
    fn handle_connection_data(&mut self, id: ConnectionId, data: Vec<Publish>) {
        trace!("{:08} {:14} Id = {}, Count = {}", "data", "nativecommit", id, data.len());
        for publish in data {
            if publish.payload.len() == 0 {
                error!("Empty publish. ID = {:?}, topic = {:?}", id, publish.topic);
                return;
            }

            let Publish { pkid, topic, payload, qos, .. } = publish;

            let is_new_topic = match self.append_to_commitlog(id, &topic, payload) {
                Some((is_new_topic, offset)) => {
                    // Store packet ids with offset mapping for QoS > 0
                    if qos as u8 > 0 {
                        let watermarks = match self.watermarks.get_mut(&topic) {
                            Some(w) => w,
                            None => {
                                let replication_count = if self.config.mesh.is_some() { 1 } else { 0 };
                                let watermarks = Watermarks::new(&topic, replication_count);
                                self.watermarks.insert(topic.clone(), watermarks);
                                self.watermarks.get_mut(&topic).unwrap()
                            },
                        };

                        watermarks.update_pkid_offset_map(id, pkid, offset);
                    }

                    is_new_topic
                }
                None => continue
            };


            // If there is a new unique append, send it to connection waiting on it
            // This is equivalent to hybrid of block and poll and we don't need timers.
            // Connections/Replicator will make a request and request might fail as
            // there are no new topic publishes. Router caches the failed request on the topic.
            // If there is new topic, router fulfills the last failed request.
            // Note that the above commitlog append distinguishes `is_new_topic` by connection's
            // commitlog (i.e native or replica commitlog). So there is a chance that topic log
            // has duplicate topics. Tracker filters these duplicates though
            if is_new_topic {
                self.topiclog.append(&topic);
                self.fresh_topics_notification(id);
            }

            // Notify waiters on this topic of new data
            self.fresh_data_notification(id, &topic);

            // Data from topics with replication factor = 0 should be acked immediately if there are
            // waiters registered. We shouldn't rely on replication acks for data acks in this case
            self.fresh_acks_notification(&topic);
        }
    }

    fn handle_replication_data(&mut self, id: ConnectionId, data: Vec<ReplicationData>) {
        trace!("{:08} {:14} Id = {}, Count = {}", "data", "replicacommit", id, data.len());
        for data in data {
            let ReplicationData { pkid, topic, payload, .. } = data;
            let mut is_new_topic = false;
            for payload in payload {
                if payload.len() == 0 {
                    error!("Empty publish. ID = {:?}, topic = {:?}", id, topic);
                    return;
                }

                if let Some((new_topic, _)) = self.append_to_commitlog(id, &topic, payload) {
                    is_new_topic = new_topic;
                }
            }

            // TODO: Is this necessary for replicated data
            let watermarks = match self.watermarks.get_mut(&topic) {
                Some(w) => w,
                None => {
                    let watermarks = Watermarks::new(&topic, 0);
                    self.watermarks.insert(topic.clone(), watermarks);
                    self.watermarks.get_mut(&topic).unwrap()
                },
            };

            // we can ignore offset mapping for replicated data
            watermarks.update_pkid_offset_map(id, pkid, 0);

            if is_new_topic {
                self.topiclog.append(&topic);
                self.fresh_topics_notification(id);
            }

            // we can probably handle multiple requests better
            self.fresh_data_notification(id, &topic);


            // Replicated data should be acked immediately when there are pending requests
            // in waiters
            self.fresh_acks_notification(&topic);
        }
    }

    fn handle_replication_acks(&mut self, id: ConnectionId, acks: Vec<ReplicationAck>) {
        for ack in acks {
            let watermarks = match self.watermarks.get_mut(&ack.topic) {
                Some(w) => w,
                None => continue
            };

            watermarks.update_cluster_offsets(id, ack.offset);
            self.fresh_acks_notification(&ack.topic);
        }
    }

    fn handle_all_topics_request(&mut self, id: ConnectionId) {
        let request = TopicsRequest { offset: 0, count: 0 };

        trace!("{:08} {:14} Id = {}, Offset = {}", "topics", "request", id, request.offset);

        let reply = match self.topiclog.topics() {
            Some((offset, topics)) => {
                TopicsReply { offset: offset + 1, topics }
            },
            None => {
                TopicsReply { offset: 0, topics: Vec::new() }
            },
        };

        trace!("{:08} {:14} Id = {}, Offset = {}", "topics", "response", id, reply.offset);
        self.reply(id, RouterOutMessage::AllTopicsReply(reply));
    }

    fn handle_topics_request(&mut self, id: ConnectionId, request: TopicsRequest) {
        trace!("{:08} {:14} Id = {}, Offset = {}", "topics", "request", id, request.offset);

        let reply = self.extract_topics(&request);
        let reply = match reply {
            Some(r) => r,
            None => {
                // register the connection 'id' to notify when there are new topics
                self.register_topics_waiter(id, request);
                return
            },
        };

        trace!("{:08} {:14} Id = {}, Offset = {}", "topics", "response", id, reply.offset);
        self.reply(id, RouterOutMessage::TopicsReply(reply));
    }

    fn handle_data_request(&mut self, id: ConnectionId, mut request: DataRequest) {
        trace!("{:08} {:14} Topic = {}, Offsets = {:?}", "data", "request", request.topic, request.cursors);
        // Replicator asking data implies that previous data has been replicated
        // We update replication watermarks at this point
        // Also, extract only connection data if this request is from a replicator
        let reply = if id < 10 {
            self.extract_connection_data(&mut request)
        } else {
            self.extract_all_data(&mut request)
        };

        // If extraction fails due to some error/topic doesn't exist yet, reply with empty response.
        // This ensures that links continue their polling of next topic during new subscriptions
        // which doesn't have publishes yet
        // The same logic doesn't exists while notifying new data because topic should always
        // exists while sending notification due to new data
        let reply = match reply {
            Some(r) => r,
            None => {
                self.register_data_waiter(id, request);
                return
            },
        };

        trace!("{:08} {:14} Topic = {}, Offsets = {:?}", "data", "response", reply.topic, reply.cursors);
        let reply = RouterOutMessage::DataReply(reply);
        self.reply(id, reply);
    }

    pub fn handle_acks_request(&mut self, id: ConnectionId, request: AcksRequest) {
        trace!("{:08} {:14} Id = {}, Topic = {}, Offset = {:?}", "acks", "request", id, request.topic, request.offset);
        // I don't think we'll need offset as next set of pkids will always be at head
        let reply = match self.watermarks.get_mut(&request.topic) {
            Some(watermarks) => {
                let pkids = watermarks.acks(id);

                // Caught up. Register for notifications when we can ack more
                if pkids.is_empty() {
                    self.register_acks_waiter(id, request);
                    return
                } else {
                    AcksReply {
                        topic: request.topic.clone(),
                        pkids,
                        offset: 100,
                    }
                }
            }
            None => {
                self.register_acks_waiter(id, request);
                return
            },
        };

        trace!("{:08} {:14} Id = {}, Topic = {}, Offset = {:?}", "acks", "response", id, reply.topic, reply.offset);
        let reply = RouterOutMessage::AcksReply(reply);
        self.reply(id, reply);
    }

    fn register_topics_waiter(&mut self, id: ConnectionId, request: TopicsRequest) {
        trace!("{:08} {:14} Id = {}", "topics", "register", id);
        let request = (id.to_owned(), request);
        self.topics_waiters.push(request);
    }

    /// Register data waiter
    fn register_data_waiter(&mut self, id: ConnectionId, request: DataRequest) {
        trace!("{:08} {:14} Id = {}, Topic = {}", "data", "register", id, request.topic);
        let topic = request.topic.clone();
        let request = (id, request);

        match self.data_waiters.get_mut(&topic) {
            Some(waiters) => waiters.push(request),
            None => {
                let waiters = vec![request];
                self.data_waiters.insert(topic, waiters);
            }
        }
    }

    fn register_acks_waiter(&mut self, id: ConnectionId, request: AcksRequest) {
        trace!("{:08} {:14} Id = {}, Topic = {}", "acks", "register", id, request.topic);
        let topic = request.topic.clone();
        match self.ack_waiters.get_mut(&request.topic) {
            Some(waiters) => waiters.push((id, request)),
            None => {
                let waiters = vec![(id, request)];
                self.ack_waiters.insert(topic, waiters);
            }
        }
    }

    /// Send notifications to links which registered them
    fn fresh_topics_notification(&mut self, id: ConnectionId) {
        let waiters = mem::replace(&mut self.topics_waiters, Vec::new());
        let replication_data = id < 10;
        for (link_id, request) in waiters {
            // Don't send replicated topic notifications to replication link
            // id 0-10 are reserved for replicators which are linked to other routers in the mesh
            if replication_data && link_id < 10 {
                continue;
            }

            // Send reply to the link which registered this notification
            let reply = self.extract_topics(&request).unwrap();

            trace!("{:08} {:14} Id = {}, Topics = {:?}", "topics", "notification", link_id, reply.topics);
            let reply = RouterOutMessage::TopicsReply(reply);
            self.reply(link_id, reply);

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
        for (link_id, mut request) in waiters {
            let reply = match link_id {
                // don't extract new replicated data for replication links
                0..=9 if replication_data => continue,
                // extract new native data to be sent to replication link
                0..=9 => match self.extract_connection_data(&mut request) {
                    Some(reply) => reply,
                    None => continue,
                },
                // extract all data to be sent to connection link
                _ => match self.extract_all_data(&mut request) {
                    Some(reply) => reply,
                    None => continue,
                },
            };

            trace!("{:08} {:14} Id = {}, Topic = {}", "data", "notification", link_id, reply.topic);
            let reply = RouterOutMessage::DataReply(reply);
            self.reply(link_id, reply);

            // NOTE:
            // ----------------------
            // When the reply is empty don't register notification on behalf of the link.
            // This will cause the channel to be full. Register the notification only when
            // link has made the request and reply doesn't contain any data
        }
    }

    fn fresh_acks_notification(&mut self, topic: &str) {
        let waiters = match self.ack_waiters.remove(topic) {
            Some(w) => w,
            None => return,
        };

        // Multiple connections can send data on a same topic. WaterMarks splits acks to offset
        // mapping by connection id. We iterate through all the connections that are waiting for
        // acks for a give topic
        for (link_id, request) in waiters {
            if let Some(watermarks) = self.watermarks.get_mut(&request.topic) {
                let pkids = watermarks.acks(link_id);

                // Even though watermarks are updated, replication count might not be enough for
                // `watermarks.acks()` to return acks. Send empty pkids in that case. Tracker will
                // just add a new AcksRequest
                let reply = AcksReply {
                    topic: request.topic,
                    pkids,
                    // I don't think we'll need offset as next set of pkids will always be at head
                    offset: 100,
                };

                trace!("{:08} {:14} Id = {}, Topic = {}", "acks", "notification", link_id, reply.topic);
                let reply = RouterOutMessage::AcksReply(reply);
                self.reply(link_id, reply);
            }
        }
    }

    /// Connections pull logs from both replication and connections where as replicator
    /// only pull logs from connections.
    /// Data from replicator and data from connection are separated for this reason
    fn append_to_commitlog(&mut self, id: ConnectionId, topic: &str, bytes: Bytes) -> Option<(bool, u64)> {
        // id 0-10 are reserved for replications which are linked to other routers in the mesh
        let replication_data = id < 10;
        if replication_data {
            match self.commitlog[id].append(&topic, bytes) {
                Ok(v) => Some(v),
                Err(e) => {
                    error!("Commitlog append failed. Error = {:?}", e);
                    None
                }
            }
        } else {
            match self.commitlog[self.id].append(&topic, bytes) {
                Ok(v) => Some(v),
                Err(e) => {
                    error!("Commitlog append failed. Error = {:?}", e);
                    None
                }
            }

        }
    }

    /// Extracts topics from topics commitlog.Returns None if the log is caught up
    /// TODO: Remove this for consistency with all_topics_request
    fn extract_topics(&mut self, request: &TopicsRequest) -> Option<TopicsReply> {
        match self.topiclog.readv(request.offset, request.count) {
            Some((_, topics)) if topics.is_empty() => None,
            Some((offset, topics)) => Some(TopicsReply {offset: offset + 1, topics}),
            None => None,
        }
    }

    /// Extracts data from native log. Returns None in case the
    /// log is caught up or encountered an error while reading data
    fn extract_connection_data(&mut self, request: &mut DataRequest) -> Option<DataReply> {
        let native_id = self.id;
        let topic = &request.topic;
        let commitlog = &mut self.commitlog[native_id];

        let cursors = match request.cursors {
            Some(cursors) => cursors,
            None => {
                let (base_offset, record_offset)  = match commitlog.last_offset(topic) {
                    Some(v) => v,
                    None => return None
                };

                let mut cursors = [(0, 0); 3];
                cursors[native_id] = (base_offset, record_offset + 1);
                request.cursors = Some(cursors);
                return None
            }
        };

        let (segment, offset) = cursors[native_id];
        debug!("Pull native data. Topic = {}, seg = {}, offset = {}", topic, segment, offset);
        let mut reply = DataReply {
            topic: request.topic.clone(),
            cursors,
            size: 0,
            payload: Vec::new()
        };

        match commitlog.readv(topic, segment, offset) {
            Ok(Some((done, base_offset, record_offset, payload))) => {
                // Update reply's cursors only when read has returned some data
                // Move the reply to next segment if we are done with the current one
                if payload.is_empty() { return None }
                match done {
                    true => reply.cursors[native_id] = (base_offset + 1, base_offset + 1),
                    false => reply.cursors[native_id] = (base_offset, record_offset + 1),
                }

                reply.payload = payload;
                Some(reply)
            }
            Ok(None) => None,
            Err(e) => {
                error!("Failed to extract data from commitlog. Error = {:?}", e);
                None
            }
        }
    }

    /// Extracts data from native and replicated logs. Returns None in case the
    /// log is caught up or encountered an error while reading data
    fn extract_all_data(&mut self, request: &mut DataRequest) -> Option<DataReply> {
        let topic = &request.topic;
        debug!("Pull data. Topic = {}, cursors = {:?}", topic, request.cursors);

        let mut cursors = [(0, 0); 3];
        let mut payload = Vec::new();

        // Iterate through native and replica commitlogs to collect data (of a topic)
        for (i, commitlog) in self.commitlog.iter_mut().enumerate() {
            let (segment, offset) = match request.cursors {
                Some(cursors) => cursors[i],
                None => {
                    let (base_offset, record_offset)  = match commitlog.last_offset(topic) {
                        Some(v) => v,
                        None => continue
                    };

                    // Uninitialized requests are always registered with next offset. Collect next offsets
                    // in all the commitlogs. End logic will update request with these offsets
                    cursors[i] = (base_offset, record_offset + 1);
                    continue
                }
            };


            match commitlog.readv(topic, segment, offset) {
                Ok(Some((done, base_offset, record_offset, mut data))) => {
                    if data.is_empty() { continue }
                    match done {
                        true => cursors[i] = (base_offset + 1, base_offset + 1),
                        false => cursors[i] = (base_offset, record_offset + 1),
                    }

                    payload.append(&mut data);
                }
                Ok(None) => continue,
                Err(e) => {
                    error!("Failed to extract data from commitlog. Error = {:?}", e);
                }
            }
        }

        // When payload is empty due to uninitialized request,
        // update request with latest offsets and return None so
        // that caller registers the request with updated offsets
        if request.cursors.is_none() {
            request.cursors = Some(cursors);
            return None
        }

        match payload.is_empty() {
            true => None,
            false => {
                Some(DataReply {
                    topic: request.topic.clone(),
                    cursors,
                    size: 0,
                    payload
                })
            }
        }
    }

    /// Send message to link
    fn reply(&mut self, id: ConnectionId, reply: RouterOutMessage) {
        let connection = match self.connections.get_mut(id).unwrap() {
            Some(c) => c,
            None => {
                error!("Invalid id while replying = {:?}", id);
                return;
            }
        };

        if let Err(e) = connection.handle.try_send(reply) {
            let error = format!("{:?}", e);
            error!("Failed to reply. Error = {:?}, Message = {:?}", error, e.into_inner());
        }
    }
}


#[cfg(test)]
mod test {
    /*
    use super::{ConnectionId, Router};
    use crate::router::{
        ConnectionAck, ConnectionType, Data, DataAck, TopicsReply, TopicsRequest, AcksReply,
        AcksRequest,
    };
    use crate::{Config, Connection, DataReply, DataRequest, RouterInMessage, RouterOutMessage};
    use async_channel::{bounded, Receiver, Sender};
    use bytes::Bytes;
    use std::time::Duration;

    #[tokio::test(core_threads = 1)]
    async fn router_doesnt_give_data_when_not_asked() {
        let (mut router_tx, _, _, connection_id, mut connection_rx) = setup().await;
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![4, 5, 6]);
        assert!(wait_for_new_data(&mut connection_rx).await.is_none());
    }

    #[tokio::test(core_threads = 1)]
    async fn router_registers_and_doesnt_repond_when_a_topic_is_caughtup() {
        let (mut router_tx, _, _, connection_id, mut connection_rx) = setup().await;
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![4, 5, 6]);

        new_data_request(connection_id, &mut router_tx, "hello/world", 0, 0);
        let reply = wait_for_new_data(&mut connection_rx).await.unwrap();
        assert_eq!(reply.native_offset, 1);
        assert_eq!(reply.payload[0].as_ref(), &[1, 2, 3]);
        assert_eq!(reply.payload[1].as_ref(), &[4, 5, 6]);

        new_data_request(connection_id, &mut router_tx, "hello/world", 2, 0);
        let reply = wait_for_new_data(&mut connection_rx).await;
        assert!(reply.is_none());
    }

    #[tokio::test(core_threads = 1)]
    async fn connection_reads_existing_native_and_replicated_data() {
        let (mut router_tx, replicator_id, mut replicator_rx, connection_id, mut connection_rx) =
            setup().await;

        // write data from a native connection and read from connection
        write_to_commitlog(replicator_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![4, 5, 6]);

        // new data request from the replicator
        new_data_request(connection_id, &mut router_tx, "hello/world", 0, 0);
        let reply = wait_for_new_data(&mut connection_rx).await.unwrap();
        assert_eq!(reply.native_count, 1);
        assert_eq!(reply.replica_count, 1);
        assert_eq!(reply.payload[0].as_ref(), &[4, 5, 6]);
        assert_eq!(reply.payload[1].as_ref(), &[1, 2, 3]);
    }

    #[tokio::test(core_threads = 1)]
    async fn replicator_reads_existing_native_data_but_not_replicated_data() {
        let (mut router_tx, replicator_id, mut replicator_rx, connection_id, mut connection_rx) =
            setup().await;

        // write data from a native connection and read from connection
        write_to_commitlog(replicator_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![4, 5, 6]);

        // new data request from the replicator
        new_data_request(replicator_id, &mut router_tx, "hello/world", 0, 0);
        let reply = wait_for_new_data(&mut replicator_rx).await.unwrap();
        assert_eq!(reply.native_count, 1);
        assert_eq!(reply.replica_count, 0);
        assert_eq!(reply.payload[0].as_ref(), &[4, 5, 6]);
    }

    #[tokio::test(core_threads = 1)]
    async fn new_topic_from_connection_should_notify_replicator_and_connection() {
        let (mut router_tx, replicator_id, mut replicator_rx, connection_id, mut connection_rx) =
            setup().await;

        // Send request for new topics. Router should reply with new topics when there are any
        new_topics_request(replicator_id, &mut router_tx);
        new_topics_request(connection_id, &mut router_tx);
        // TODO: Add test to check no response at every place like this

        // Send new data to router to be written to commitlog
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![1, 2, 3]);

        // see if routers replys with topics
        assert_eq!(
            wait_for_new_topics(&mut replicator_rx)
                .await
                .unwrap()
                .topics[0],
            "hello/world"
        );
        assert_eq!(
            wait_for_new_topics(&mut connection_rx)
                .await
                .unwrap()
                .topics[0],
            "hello/world"
        );
    }

    #[tokio::test(core_threads = 1)]
    async fn new_topic_from_replicator_should_notify_only_connection() {
        let (mut router_tx, replicator_id, mut replicator_rx, connection_id, mut connection_rx) =
            setup().await;
        // Send request for new topics. Router should reply with new topics when there are any
        new_topics_request(replicator_id, &mut router_tx);
        new_topics_request(connection_id, &mut router_tx);

        // Send new data to router to be written to commitlog
        write_to_commitlog(replicator_id, &mut router_tx, "hello/world", vec![1, 2, 3]);

        // see if routers replys with topics
        assert_eq!(
            wait_for_new_topics(&mut connection_rx)
                .await
                .unwrap()
                .topics[0],
            "hello/world"
        );
        assert!(wait_for_new_topics(&mut replicator_rx).await.is_none());
    }

    #[tokio::test(core_threads = 1)]
    async fn new_data_from_connection_should_notify_replicator_and_connection() {
        let (mut router_tx, replicator_id, mut replicator_rx, connection_id, mut connection_rx) =
            setup().await;

        // Request data on non existent topic. Router will reply when there is data on this topic (new/old)
        new_data_request(connection_id, &mut router_tx, "hello/world", 0, 0);
        new_data_request(replicator_id, &mut router_tx, "hello/world", 0, 0);

        // Write data and old requests should be catered
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![4, 5, 6]);
        assert_eq!(
            wait_for_new_data(&mut connection_rx).await.unwrap().payload[0].as_ref(),
            &[4, 5, 6]
        );
        assert_eq!(
            wait_for_new_data(&mut replicator_rx).await.unwrap().payload[0].as_ref(),
            &[4, 5, 6]
        );
    }

    #[tokio::test(core_threads = 1)]
    async fn new_data_from_replicator_should_notify_only_connection() {
        let (mut router_tx, replicator_id, mut replicator_rx, connection_id, mut connection_rx) =
            setup().await;

        // Send request for new topics. Router should reply with new topics when there are any
        new_data_request(connection_id, &mut router_tx, "hello/world", 0, 0);
        new_data_request(replicator_id, &mut router_tx, "hello/world", 0, 0);

        write_to_commitlog(replicator_id, &mut router_tx, "hello/world", vec![4, 5, 6]);
        assert_eq!(
            wait_for_new_data(&mut connection_rx).await.unwrap().payload[0].as_ref(),
            &[4, 5, 6]
        );
    }

    #[tokio::test(core_threads = 1)]
    async fn replicated_data_updates_watermark_as_expected() {
        let (mut router_tx, replicator_id, mut replicator_rx, connection_id, mut connection_rx) =
            setup().await;

        // this registers watermark notification in router as the topic isn't existent yet
        new_watermarks_request(connection_id, &mut router_tx, "hello/world");

        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![4, 5, 6]);
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![7, 8, 9]);

        // new data request from the router means previous request is replicated. First request
        // will only add this topic to watermark list
        new_data_request(replicator_id, &mut router_tx, "hello/world", 0, 0);
        assert_eq!(
            wait_for_new_data(&mut replicator_rx)
                .await
                .unwrap()
                .payload
                .len(),
            3
        );

        // Second data request from replicator implies that previous request has been replicated
        new_data_request(replicator_id, &mut router_tx, "hello/world", 3, 0);
        let reply = wait_for_new_watermarks(&mut connection_rx).await.unwrap();
        assert_eq!(reply.offset, 3);
    }

    // ---------------- All helper methods to make tests clean and readable ------------------

    /// Creates a router, a connection link, a replicator link and returns them along with IDs
    async fn setup() -> (
        Sender<(ConnectionId, RouterInMessage)>,
        ConnectionId,
        Receiver<RouterOutMessage>,
        ConnectionId,
        Receiver<RouterOutMessage>,
    ) {
        let (router, router_tx) = Router::new(Config::default());
        tokio::task::spawn(async move {
            let mut router = router;
            router.start().await;
        });

        let (link_tx, replicator_rx) = bounded(4);
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

        let (link_tx, connection_rx) = bounded(4);
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
        native_offset: u64,
        replica_offset: u64,
    ) {
        let message = (
            id,
            RouterInMessage::DataRequest(DataRequest {
                topic: topic.to_string(),
                native_segment: 0,
                replica_segment: 0,
                native_offset,
                replica_offset,
                size: 100 * 1024,
            }),
        );
        router_tx.try_send(message).unwrap();
    }

    fn new_watermarks_request(
        id: usize,
        router_tx: &mut Sender<(ConnectionId, RouterInMessage)>,
        topic: &str,
    ) {
        let message = (
            id,
            RouterInMessage::WatermarksRequest(AcksRequest {
                topic: topic.to_owned(),
                offset: 0,
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

    async fn wait_for_new_watermarks(
        rx: &mut Receiver<RouterOutMessage>,
    ) -> Option<AcksReply> {
        tokio::time::delay_for(Duration::from_secs(1)).await;
        match rx.try_recv() {
            Ok(RouterOutMessage::WatermarksReply(reply)) => Some(reply),
            v => {
                error!("{:?}", v);
                None
            }
        }
    }

     */
}

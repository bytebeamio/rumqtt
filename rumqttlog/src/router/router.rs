use async_channel::{bounded, Receiver, Sender};
use std::collections::{HashMap, VecDeque};
use std::{io, mem, thread};

use super::bytes::Bytes;
use super::commitlog::CommitLog;
use super::{Connection, RouterInMessage, RouterOutMessage};
use crate::mesh::Mesh;
use crate::router::commitlog::TopicLog;
use crate::router::{ConnectionAck, ConnectionType, DataReply, DataRequest, TopicsReply, TopicsRequest, AcksReply, AcksRequest, Disconnection};

use crate::{Config, ReplicationData};
use thiserror::Error;
use tokio::stream::StreamExt;
use rumqttc::{Publish, QoS};

#[derive(Error, Debug)]
#[error("...")]
pub enum Error {
    Io(#[from] io::Error),
}

type ConnectionId = usize;
type Topic = String;
type Offset = u64;
type Pkid = u16;

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
    watermark_waiters: HashMap<Topic, Vec<(ConnectionId, AcksRequest)>>,
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
            watermark_waiters: HashMap::new(),
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
                RouterInMessage::TopicsRequest(request) => self.handle_topics_request(id, request),
                RouterInMessage::DataRequest(request) => self.handle_data_request(id, request),
                RouterInMessage::AcksRequest(request) => {
                    self.handle_acks_request(id, request)
                }
                RouterInMessage::Disconnect(request) => {
                    self.handle_disconnection(id, request)
                }
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

        // TODO Remove this connection from all types of waiters
    }

    /// Handles new incoming data on a topic
    fn handle_connection_data(&mut self, id: ConnectionId, data: Vec<Publish>) {
        for publish in data {
            if publish.payload.len() == 0 {
                error!("Empty publish. ID = {:?}, topic = {:?}", id, publish.topic);
                return;
            }

            let Publish { pkid, topic, payload, qos, .. } = publish;
            if let Some(offset) = self.append_to_commitlog(id, &topic, payload) {
                // Store packet ids with offset mapping for QoS > 0
                if qos == QoS::AtLeastOnce || qos == QoS::ExactlyOnce {
                    let watermarks = match self.watermarks.get_mut(&topic) {
                        Some(w) => w,
                        None => {
                            self.watermarks.insert(topic.clone(), Watermarks::new(0));
                            self.watermarks.get_mut(&topic).unwrap()
                        },
                    };

                    if !self.config.instant_ack {
                        watermarks.update_pkid_offset_map(id, pkid, offset);
                    }
                }
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

            // TODO We are notifying in the loop per id. When router does bulk `try_recv`
            // we can probably handle multiple requests better
            self.fresh_data_notification(id, &topic);

            // After a bulk of publishes have been written to commitlog, if the replication factor
            // of this topic is zero, it can be acked immediately if there is an AcksRequest already
            // waiting. If we don't ack replication factor 0 acks here, these acks will be stuck
            // TODO: This can probably be moved to connection to ack immediately?
            // But connection has to maintain topic map to identify replication factor which router
            // already does
            if !self.config.instant_ack {
                self.fresh_watermarks_notification(&topic);
            }
        }
    }

    fn handle_replication_data(&mut self, id: ConnectionId, data: Vec<ReplicationData>) {
        for data in data {
            let ReplicationData { pkid, topic, payload, .. } = data;
            for payload in payload {
                if payload.len() == 0 {
                    error!("Empty publish. ID = {:?}, topic = {:?}", id, topic);
                    return;
                }

                self.append_to_commitlog(id, &topic, payload);
            }

            let watermarks = match self.watermarks.get_mut(&topic) {
                Some(w) => w,
                None => {
                    self.watermarks.insert(topic.clone(), Watermarks::new(0));
                    self.watermarks.get_mut(&topic).unwrap()
                },
            };

            // we can ignore offset mapping for replicated data
            watermarks.update_pkid_offset_map(id, pkid, 0);

            // If there is a new unique append, send it to connection/linker waiting
            // on it. This is equivalent to hybrid of block and poll and we don't need
            // timers. Connections/Replicator will make a request and request fails as
            // there is no new data. Router caches the failed request on the topic.
            // If there is new data on this topic, router fulfills the last failed request.
            // This completely eliminates the need of polling
            if self.topiclog.unique_append(&topic) {
                dbg!();
                self.fresh_topics_notification(id);
            }

            // we can probably handle multiple requests better
            self.fresh_data_notification(id, &topic);
        }
    }

    fn handle_topics_request(&mut self, id: ConnectionId, request: TopicsRequest) {
        let reply = self.extract_topics(&request);
        let reply = match reply {
            Some(r) => r,
            None => {
                self.register_topics_waiter(id, request);
                return
            },
        };

        // register the connection 'id' to notify when there are new topics
        if reply.topics.is_empty() {
            self.register_topics_waiter(id, request);
            return
        }

        self.reply(id, RouterOutMessage::TopicsReply(reply));
    }

    fn handle_data_request(&mut self, id: ConnectionId, request: DataRequest) {
        // Replicator asking data implies that previous data has been replicated
        // We update replication watermarks at this point
        // Also, extract only connection data if this request is from a replicator
        let reply = if id < 10 {
            self.update_cluster_offsets(id, &request);
            self.extract_connection_data(&request)
        } else {
            self.extract_all_data(&request)
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

        // This connectionr/replicator is completely caught up with this topic.
        // Add this connection/linker into waiters list of this topic.
        // We don't send any response
        // TODO: This check can probably be removed after verifying the flow
        if reply.payload.is_empty() {
            self.register_data_waiter(id, request);
            return
        }

        let reply = RouterOutMessage::DataReply(reply);
        self.reply(id, reply);
    }

    pub fn handle_acks_request(&mut self, id: ConnectionId, request: AcksRequest) {
        // I don't think we'll need offset as next set of pkids will always be at head
        let reply = match self.watermarks.get_mut(&request.topic) {
            Some(watermarks) => {
                // let caught_up = watermarks.cluster_offsets == request.offset;
                let pkids = watermarks.acks(id);
                let caught_up = pkids.is_empty();
                if caught_up {
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

        let reply = RouterOutMessage::AcksReply(reply);
        self.reply(id, reply);
    }

    /// Updates replication watermarks
    fn update_cluster_offsets(&mut self, id: ConnectionId, request: &DataRequest) {
        if let Some(watermarks) = self.watermarks.get_mut(&request.topic) {
            watermarks.cluster_offsets.insert(id as usize, request.cursors[self.id].1);
            self.fresh_watermarks_notification(&request.topic);
        } else {
            // Fresh topic only initialize with 0 offset. Updates during the next request
            // Watermark 'n' implies data till n-1 is synced with the other node
            self.watermarks.insert(request.topic.clone(), Watermarks::new(0));
        }
    }

    /// Send notifications to links which registered them
    fn fresh_topics_notification(&mut self, id: ConnectionId) {
        // TODO too many indirection to get a link to the handle, probably directly
        // TODO clone a handle to the link and save it instead of saving ids?
        let waiters = mem::replace(&mut self.topics_waiters, Vec::new());
        let replication_data = id < 10;
        for (link_id, request) in waiters {
            // don't send replicated topic notifications to replication link
            // id 0-10 are reserved for replicators which are linked to other routers in the mesh
            if replication_data && link_id < 10 {
                continue;
            }

            // Send reply to the link which registered this notification
            let reply = self.extract_topics(&request).unwrap();
            debug!("Sending new topics {:?} notification to id = {}", reply.topics, link_id);

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
        for (link_id, request) in waiters {
            let reply = match link_id {
                // don't extract new replicated data for replication links
                0..=9 if replication_data => continue,
                // extract new native data to be sent to replication link
                0..=9 => match self.extract_connection_data(&request) {
                    Some(reply) => reply,
                    None => continue,
                },
                // extract all data to be sent to connection link
                _ => match self.extract_all_data(&request) {
                    Some(reply) => reply,
                    None => continue,
                },
            };

            let reply = RouterOutMessage::DataReply(reply);
            self.reply(link_id, reply);

            // NOTE:
            // ----------------------
            // When the reply is empty don't register notification on behalf of the link.
            // This will cause the channel to be full. Register the notification only when
            // link has made the request and reply doesn't contain any data
        }
    }

    fn fresh_watermarks_notification(&mut self, topic: &str) {
        let waiters = match self.watermark_waiters.remove(topic) {
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

                let reply = RouterOutMessage::AcksReply(reply);
                self.reply(link_id, reply);
            }
        }
    }

    /// Connections pull logs from both replication and connections where as replicator
    /// only pull logs from connections.
    /// Data from replicator and data from connection are separated for this reason
    fn append_to_commitlog(&mut self, id: ConnectionId, topic: &str, bytes: Bytes) -> Option<u64> {
        // id 0-10 are reserved for replications which are linked to other routers in the mesh
        let replication_data = id < 10;
        if replication_data {
            debug!("Receiving replication data from {}, topic = {}", id, topic);
            match self.commitlog[id].append(&topic, bytes) {
                Ok(offset) => Some(offset),
                Err(e) => {
                    error!("Commitlog append failed. Error = {:?}", e);
                    None
                }
            }
        } else {
            debug!("Receiving data from connection {}, topic = {}", id, topic);
            match self.commitlog[self.id].append(&topic, bytes) {
                Ok(offset) => Some(offset),
                Err(e) => {
                    error!("Commitlog append failed. Error = {:?}", e);
                    None
                }
            }

        }
    }

    fn extract_topics(&mut self, request: &TopicsRequest) -> Option<TopicsReply> {
        match self.topiclog.readv(request.offset, request.count) {
            Some(o) => {
                let reply = TopicsReply {
                    offset: o.0,
                    topics: o.1,
                };

                Some(reply)
            }
            None => None,
        }
    }

    fn extract_connection_data(&mut self, request: &DataRequest) -> Option<DataReply> {
        let topic = &request.topic;
        let (segment, offset) = request.cursors[self.id];
        let size = request.max_size;

        debug!("Pull native data. Topic = {}, seg = {}, offset = {}", topic, segment, offset);
        match self.commitlog[self.id].readv(topic, segment, offset, size) {
            Ok(Some(v)) => {
                // copy and update native commitlog's offsets
                let mut cursors = request.cursors;
                cursors[self.id] = (v.1, v.2 + 1);

                let reply = DataReply {
                    topic: request.topic.clone(),
                    cursors,
                    size: v.3,
                    payload: v.5,
                };

                Some(reply)
            }
            Ok(None) => None,
            Err(e) => {
                error!("Failed to extract data from commitlog. Error = {:?}", e);
                None
            }
        }
    }

    fn extract_all_data(&mut self, request: &DataRequest) -> Option<DataReply> {
        let topic = &request.topic;

        debug!("Pull data. Topic = {}, cursors = {:?}", topic, request.cursors);
        let mut reply = DataReply {
            topic: request.topic.clone(),
            cursors: request.cursors,
            size: 0,
            payload: Vec::new()
        };

        for (i, commitlog) in self.commitlog.iter_mut().enumerate() {
            let (segment, offset) = request.cursors[i];
            match commitlog.readv(topic, segment, offset, request.max_size) {
                Ok(Some(mut v)) => {
                    reply.cursors[i] = (v.1, v.2 + 1);
                    reply.size += v.3;
                    reply.payload.append(&mut v.5);

                }
                Ok(None) => {
                    debug!("Nothing more to extract from commitlog {}!!", i);
                },
                Err(e) => {
                    error!("Failed to extract data from commitlog. Error = {:?}", e);
                }
            }
        }

        match reply.payload.is_empty() {
            true => None,
            false => Some(reply)
        }
    }

    fn register_topics_waiter(&mut self, id: ConnectionId, request: TopicsRequest) {
        debug!("Registering connection {} for topic notifications", id);
        let request = (id.to_owned(), request);
        self.topics_waiters.push(request);
    }

    /// Register data waiter
    fn register_data_waiter(&mut self, id: ConnectionId, request: DataRequest) {
        debug!("Registering id = {} for {} data notifications", id, request.topic);
        let topic = request.topic.clone();
        let request = (id, request);

        if let Some(waiters) = self.data_waiters.get_mut(&topic) {
            waiters.push(request);
        } else {
            let waiters = vec![request];
            self.data_waiters.insert(topic, waiters);
        }
    }

    fn register_acks_waiter(&mut self, id: ConnectionId, request: AcksRequest) {
        debug!("Registering id = {} for {} ack notifications", id, request.topic);
        let topic = request.topic.clone();
        if let Some(waiters) = self.watermark_waiters.get_mut(&request.topic) {
            waiters.push((id, request));
        } else {
            let waiters = vec![(id, request)];
            self.watermark_waiters.insert(topic, waiters);
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
            error!("Failed to reply. Message = {:?}", e.into_inner());
        }
    }
}

/// Watermarks for a given topic
pub struct Watermarks {
    replication: usize,
    /// A map of packet ids and commitlog offsets for a given connection (identified by index)
    pkid_offset_map: Vec<Option<(VecDeque<Pkid>, VecDeque<Offset>)>>,
    /// Offset till which replication has happened (per mesh node)
    cluster_offsets: Vec<Offset>
}

impl Watermarks {
    pub fn new(replication: usize) -> Watermarks {
        Watermarks {
            replication,
            pkid_offset_map: vec![None; 1000],
            cluster_offsets: vec![0, 0, 0]
        }
    }

    /*
    pub fn update_cluster_offsets(&mut self, id: usize, offset: u64) {
        if let Some(position) = self.cluster_offsets.get_mut(id) {
            *position = offset
        } else {
            panic!("We only support a maximum of 3 nodes at the moment. Received id = {}", id);
        }
    }
     */

    pub fn acks(&mut self, id: usize) -> VecDeque<Pkid> {
        let is_replica = id < 10;
        if self.replication == 0 || is_replica {
            if let Some(connection) = self.pkid_offset_map.get_mut(id).unwrap() {
                let new_ids = (VecDeque::new(), VecDeque::new());
                let (pkids, _offsets) = mem::replace(connection, new_ids);
                return pkids
            }
        }

        return VecDeque::new()
    }

    pub fn update_pkid_offset_map(&mut self, id: usize, pkid: u16, offset: u64) {
        // connection ids which are greater than supported count should be rejected during
        // connection itself. Crashing here is a bug
        let connection = self.pkid_offset_map.get_mut(id).unwrap();
        let map = connection.get_or_insert((VecDeque::new(), VecDeque::new()));
        map.0.push_back(pkid);
        // save offsets only if replication > 0
        if self.replication > 0 {
            map.1.push_back(offset);
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

use async_channel::{bounded, Receiver, Sender};
use std::collections::HashMap;
use std::{io, mem, thread};

use super::bytes::Bytes;
use super::commitlog::CommitLog;
use super::{Connection, RouterInMessage, RouterOutMessage};
use crate::mesh::Mesh;
use crate::router::commitlog::TopicLog;
use crate::router::{
    ConnectionAck, ConnectionType, Data, DataAck, DataReply, DataRequest, TopicsReply,
    TopicsRequest, WatermarksReply, WatermarksRequest,
};
use crate::Config;
use futures_util::StreamExt;
use thiserror::Error;

#[derive(Error, Debug)]
#[error("...")]
pub enum Error {
    Io(#[from] io::Error),
}

type ConnectionId = usize;
type Topic = String;
type Offset = u64;

pub struct Router {
    config: Config,
    /// Commit log by topic. Commit log stores all the of given topic. The
    /// details are very similar to what kafka does. Who knows, we might
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
    /// Waiters on watermark updates
    /// Whenever a topic is replicated, it's watermark is updated till the offset
    /// that replication has happened
    watermark_waiters: HashMap<Topic, Vec<(ConnectionId, WatermarksRequest)>>,
    /// Watermarks of all the replicas. Map[topic]List[u64]. Each index
    /// represents a router in the mesh
    /// Watermark 'n' implies data till n-1 is synced with the other node
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
        let (router_tx, router_rx) = bounded(1000);
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
            watermark_waiters: HashMap::new(),
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
        while let Some((id, data)) = self.router_rx.next().await {
            match data {
                RouterInMessage::Connect(connection) => self.handle_new_connection(connection),
                RouterInMessage::Data(data) => self.handle_incoming_data(id, data),
                RouterInMessage::TopicsRequest(request) => self.handle_topics_request(id, request),
                RouterInMessage::DataRequest(request) => self.handle_data_request(id, request),
                RouterInMessage::WatermarksRequest(request) => {
                    self.handle_watermarks_request(id, request)
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

    /// Handles new incoming data on a topic
    fn handle_incoming_data(&mut self, id: ConnectionId, data: Data) {
        if data.payload.len() == 0 {
            error!(
                "Empty publish. Ignoring. ID = {:?}, topic = {:?}",
                id, data.topic
            );
            return;
        }

        let Data {
            pkid,
            topic,
            payload,
        } = data;

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

    fn handle_topics_request(&mut self, id: ConnectionId, request: TopicsRequest) {
        let reply = self.extract_topics(&request);
        let reply = match reply {
            Some(r) => r,
            None => TopicsReply {
                offset: request.offset,
                topics: Vec::new(),
            },
        };

        // register this id to wake up when there are new topics and send an empty reply
        // for link's tracker to proceed with next poll
        if reply.topics.is_empty() {
            self.register_topics_waiter(id, request);
        }
        self.reply(id, RouterOutMessage::TopicsReply(reply));
    }

    fn handle_data_request(&mut self, id: ConnectionId, request: DataRequest) {
        // Replicator asking data implies that previous data has been replicated
        // We update replication watermarks at this point
        // Also, extract only connection data if this request is from a replicator
        let reply = if id < 10 {
            self.update_watermarks(id, &request);
            self.extract_connection_data(&request)
        } else {
            self.extract_data(&request)
        };

        // If extraction fails due to some error/topic doesn't exist yet, reply with empty response.
        // This ensures that links continue their polling of next topic during new subscriptions
        // which doesn't have publishes yet
        // The same logic doesn't exists while notifying new data because topic should always
        // exists while sending notification due to new data
        let reply = match reply {
            Some(r) => r,
            None => DataReply {
                done: true,
                topic: request.topic.clone(),
                native_segment: request.native_segment,
                native_offset: request.native_offset,
                native_count: 0,
                replica_segment: request.replica_segment,
                replica_offset: request.replica_offset,
                replica_count: 0,
                pkids: vec![],
                payload: vec![],
                tracker_topic_offset: 0,
            },
        };

        // This connection/linker is completely caught up with this topic.
        // Add this connection/linker into waiters list of this topic.
        // Note that we also send empty reply to the link with uses this to mark
        // this topic as a caught up to not make a request again while iterating
        // through its list of topics. Not sending an empty response will block
        // the 'link' from polling next topic
        if reply.payload.is_empty() {
            self.register_data_waiter(id, request);
        }

        let reply = RouterOutMessage::DataReply(reply);
        self.reply(id, reply);
    }

    pub fn handle_watermarks_request(&mut self, id: ConnectionId, request: WatermarksRequest) {
        let reply = match self.watermarks.get(&request.topic) {
            Some(watermarks) => {
                let caught_up = watermarks == &request.watermarks;
                if caught_up {
                    WatermarksReply {
                        topic: request.topic.clone(),
                        watermarks: Vec::new(),
                        tracker_topic_offset: request.tracker_topic_offset,
                    }
                } else {
                    WatermarksReply {
                        topic: request.topic.clone(),
                        watermarks: watermarks.clone(),
                        tracker_topic_offset: request.tracker_topic_offset,
                    }
                }
            }
            None => WatermarksReply {
                topic: request.topic.clone(),
                watermarks: Vec::new(),
                tracker_topic_offset: request.tracker_topic_offset,
            },
        };

        if reply.watermarks.is_empty() {
            self.register_watermarks_waiter(id, request);
        }

        let reply = RouterOutMessage::WatermarksReply(reply);
        self.reply(id, reply);
    }

    /// Updates replication watermarks
    fn update_watermarks(&mut self, id: ConnectionId, request: &DataRequest) {
        if let Some(watermarks) = self.watermarks.get_mut(&request.topic) {
            watermarks.insert(id as usize, request.native_offset);
            self.fresh_watermarks_notification(&request.topic);
        } else {
            // Fresh topic only initialize with 0 offset. Updates during the next request
            // Watermark 'n' implies data till n-1 is synced with the other node
            self.watermarks
                .insert(request.topic.clone(), vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
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
            // id 0-10 are reserved for replicatiors which are linked to other routers in the mesh
            if replication_data && link_id < 10 {
                continue;
            }

            // Send reply to the link which registered this notification
            let reply = self.extract_topics(&request).unwrap();
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
                // extract new native connection data to be sent to replication link
                0..=9 => match self.extract_connection_data(&request) {
                    Some(reply) => reply,
                    None => continue,
                },
                // extract new replication data to be sent to connection link
                _ if replication_data => match self.extract_replicated_data(&request) {
                    Some(reply) => reply,
                    None => continue,
                },
                // extract new native connection data to be sent to connection link
                _ => match self.extract_connection_data(&request) {
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

        for (link_id, request) in waiters {
            let reply = WatermarksReply {
                topic: topic.to_owned(),
                watermarks: self.watermarks.get(topic).unwrap().clone(),
                tracker_topic_offset: request.tracker_topic_offset,
            };

            let reply = RouterOutMessage::WatermarksReply(reply);
            self.reply(link_id, reply);
        }
    }

    /// Separate logs due to replication and logs from connections. Connections pull
    /// logs from both replication and connections where as linker only pull logs
    /// from connections
    fn append_to_commitlog(
        &mut self,
        id: ConnectionId,
        pkid: u64,
        topic: &str,
        bytes: Bytes,
    ) -> Option<u64> {
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
            debug!("Receiving connection data from {}, topic = {}", id, topic);
            match self.commitlog.append(&topic, bytes) {
                Ok(offset) => Some(offset),
                Err(e) => {
                    error!("Commitlog append failed. Error = {:?}", e);
                    None
                }
            }
        }
    }

    /// Acknowledge connection after the data is written to commitlog
    fn ack_data(&mut self, id: ConnectionId, pkid: u64, offset: u64) {
        let connection = match self.connections.get_mut(id).unwrap() {
            Some(c) => c,
            None => {
                error!("3. Invalid id = {:?}", id);
                return;
            }
        };

        let ack = RouterOutMessage::DataAck(DataAck { pkid, offset });
        if let Err(e) = connection.handle.try_send(ack) {
            error!("Failed to topics refresh reply. Error = {:?}", e);
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
        let segment = request.native_segment;
        let offset = request.native_offset;
        let size = request.size;

        debug!(
            "Pull native data. Topic = {}, seg = {}, offset = {}",
            topic, segment, offset
        );
        match self.commitlog.readv(topic, segment, offset, size) {
            Ok(Some(v)) => {
                let native_count = v.5.len();
                let reply = DataReply {
                    done: v.0,
                    topic: request.topic.clone(),
                    payload: v.5,
                    native_segment: v.1,
                    native_offset: v.2,
                    native_count,
                    replica_segment: request.replica_segment,
                    replica_offset: request.replica_offset,
                    replica_count: 0,
                    pkids: v.4,
                    tracker_topic_offset: request.tracker_topic_offset,
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

    fn extract_replicated_data(&mut self, request: &DataRequest) -> Option<DataReply> {
        let topic = &request.topic;
        let segment = request.replica_segment;
        let offset = request.replica_offset;
        let size = request.size;

        debug!(
            "Pull replicated data. Topic = {}, seg = {}, offset = {}",
            topic, segment, offset
        );
        match self.replicatedlog.readv(topic, segment, offset, size) {
            Ok(Some(v)) => {
                let replica_count = v.5.len();
                let reply = DataReply {
                    done: v.0,
                    topic: request.topic.clone(),
                    payload: v.5,
                    native_segment: request.native_segment,
                    native_offset: request.native_offset,
                    native_count: 0,
                    replica_segment: v.1,
                    replica_offset: v.2,
                    replica_count,
                    pkids: v.4,
                    tracker_topic_offset: request.tracker_topic_offset,
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

    /// extracts data from correct commitlog's segment and sends it
    fn extract_data(&mut self, request: &DataRequest) -> Option<DataReply> {
        if let Some(mut reply) = self.extract_connection_data(&request) {
            if let Some(mut replicated_data) = self.extract_replicated_data(&request) {
                reply.replica_segment = replicated_data.replica_segment;
                reply.replica_offset = replicated_data.replica_offset;
                reply.replica_count = replicated_data.replica_count;
                // TODO This copies data from one queue to another. Find if there is a way
                // TODO to reduce cost here. The best option here is probably send Vec<Vec<Bytes>>
                reply.payload.append(&mut replicated_data.payload);
            }

            Some(reply)
        } else if let Some(reply) = self.extract_replicated_data(&request) {
            Some(reply)
        } else {
            error!("Empty connection and replication data!!");
            None
        }
    }

    fn register_topics_waiter(&mut self, id: ConnectionId, request: TopicsRequest) {
        let request = (id.to_owned(), request);
        self.topics_waiters.push(request);
    }

    /// Register data waiter
    fn register_data_waiter(&mut self, id: ConnectionId, request: DataRequest) {
        let topic = request.topic.clone();
        let request = (id, request);

        if let Some(waiters) = self.data_waiters.get_mut(&topic) {
            waiters.push(request);
        } else {
            let waiters = vec![request];
            self.data_waiters.insert(topic, waiters);
        }
    }

    fn register_watermarks_waiter(&mut self, id: ConnectionId, request: WatermarksRequest) {
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
            error!("Failed to topics refresh reply. Error = {:?}", e);
        }
    }
}

#[cfg(test)]
mod test {
    use super::{ConnectionId, Router};
    use crate::router::{
        ConnectionAck, ConnectionType, Data, DataAck, TopicsReply, TopicsRequest, WatermarksReply,
        WatermarksRequest,
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
        assert!(wait_for_ack(&mut connection_rx).await.is_some());
        assert!(wait_for_ack(&mut connection_rx).await.is_some());

        assert!(wait_for_new_data(&mut connection_rx).await.is_none());
    }

    #[tokio::test(core_threads = 1)]
    async fn router_responds_with_empty_message_when_a_topic_is_caught_up() {
        let (mut router_tx, _, _, connection_id, mut connection_rx) = setup().await;
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![4, 5, 6]);
        assert!(wait_for_ack(&mut connection_rx).await.is_some());
        assert!(wait_for_ack(&mut connection_rx).await.is_some());

        new_data_request(connection_id, &mut router_tx, "hello/world", 0, 0);
        let reply = wait_for_new_data(&mut connection_rx).await.unwrap();
        assert_eq!(reply.native_offset, 1);
        assert_eq!(reply.payload[0].as_ref(), &[1, 2, 3]);
        assert_eq!(reply.payload[1].as_ref(), &[4, 5, 6]);

        new_data_request(connection_id, &mut router_tx, "hello/world", 2, 0);
        let reply = wait_for_new_data(&mut connection_rx).await.unwrap();
        assert_eq!(reply.native_count, 0);
        assert_eq!(reply.payload.len(), 0);
    }

    #[tokio::test(core_threads = 1)]
    async fn connection_reads_existing_native_and_replicated_data() {
        let (mut router_tx, replicator_id, mut replicator_rx, connection_id, mut connection_rx) =
            setup().await;

        // write data from a native connection and read from connection
        write_to_commitlog(replicator_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![4, 5, 6]);
        assert!(wait_for_ack(&mut replicator_rx).await.is_some());
        assert!(wait_for_ack(&mut connection_rx).await.is_some());

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
        assert!(wait_for_ack(&mut replicator_rx).await.is_some());
        assert!(wait_for_ack(&mut connection_rx).await.is_some());

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

        // see if routers replys with topics
        assert_eq!(
            wait_for_new_topics(&mut replicator_rx)
                .await
                .unwrap()
                .topics
                .len(),
            0
        );
        assert_eq!(
            wait_for_new_topics(&mut connection_rx)
                .await
                .unwrap()
                .topics
                .len(),
            0
        );

        // Send new data to router to be written to commitlog
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        assert!(wait_for_ack(&mut connection_rx).await.is_some());

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

        // see if routers replys with topics
        assert_eq!(
            wait_for_new_topics(&mut replicator_rx)
                .await
                .unwrap()
                .topics
                .len(),
            0
        );
        assert_eq!(
            wait_for_new_topics(&mut connection_rx)
                .await
                .unwrap()
                .topics
                .len(),
            0
        );

        // Send new data to router to be written to commitlog
        write_to_commitlog(replicator_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        assert!(wait_for_ack(&mut replicator_rx).await.is_some());

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
        assert_eq!(
            wait_for_new_data(&mut connection_rx)
                .await
                .unwrap()
                .payload
                .len(),
            0
        );
        assert_eq!(
            wait_for_new_data(&mut replicator_rx)
                .await
                .unwrap()
                .payload
                .len(),
            0
        );

        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![4, 5, 6]);
        assert!(wait_for_ack(&mut connection_rx).await.is_some());

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

        // first wait on connection_rx should return some data and next wait none
        // for replicated receiver, router only checks native data. replicator receives nothing back
        assert_eq!(
            wait_for_new_data(&mut connection_rx)
                .await
                .unwrap()
                .payload
                .len(),
            0
        );
        assert_eq!(
            wait_for_new_data(&mut replicator_rx)
                .await
                .unwrap()
                .payload
                .len(),
            0
        );

        write_to_commitlog(replicator_id, &mut router_tx, "hello/world", vec![4, 5, 6]);
        assert!(wait_for_ack(&mut replicator_rx).await.is_some());
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
        assert!(wait_for_new_watermarks(&mut connection_rx).await.is_some());

        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![1, 2, 3]);
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![4, 5, 6]);
        write_to_commitlog(connection_id, &mut router_tx, "hello/world", vec![7, 8, 9]);
        assert!(wait_for_ack(&mut connection_rx).await.is_some());
        assert!(wait_for_ack(&mut connection_rx).await.is_some());
        assert!(wait_for_ack(&mut connection_rx).await.is_some());

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
        assert_eq!(
            wait_for_new_data(&mut replicator_rx)
                .await
                .unwrap()
                .payload
                .len(),
            0
        );

        let reply = wait_for_new_watermarks(&mut connection_rx).await.unwrap();
        assert_eq!(reply.watermarks, vec![3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
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
                tracker_topic_offset: 0,
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
            RouterInMessage::WatermarksRequest(WatermarksRequest {
                topic: topic.to_owned(),
                watermarks: vec![],
                tracker_topic_offset: 0,
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

    async fn wait_for_new_watermarks(
        rx: &mut Receiver<RouterOutMessage>,
    ) -> Option<WatermarksReply> {
        tokio::time::delay_for(Duration::from_secs(1)).await;
        match rx.try_recv() {
            Ok(RouterOutMessage::WatermarksReply(reply)) => Some(reply),
            v => {
                error!("{:?}", v);
                None
            }
        }
    }
}

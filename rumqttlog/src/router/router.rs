use async_channel::{bounded, Receiver, Sender, TrySendError};
use std::collections::{HashMap, VecDeque};
use std::{io, mem};

use super::bytes::Bytes;
use super::commitlog::CommitLog;
use super::{Connection, RouterInMessage, RouterOutMessage};
use crate::router::commitlog::TopicLog;
use crate::router::{
    AcksReply, AcksRequest, DataReply, DataRequest, Disconnection, ReplicationAck,
    SubscriptionReply, SubscriptionRequest, TopicsReply, TopicsRequest,
};
use crate::router::{ConnectionAck, ConnectionType, Subscription};

use crate::router::watermarks::Watermarks;
use crate::{Config, ReplicationData};
use mqtt4bytes::{Packet, Publish, Subscribe, SubscribeReturnCodes};
use std::sync::Arc;
use thiserror::Error;
use tokio::stream::StreamExt;

#[derive(Error, Debug)]
#[error("...")]
pub enum Error {
    Io(#[from] io::Error),
}

type ConnectionId = usize;
type Topic = String;

pub struct Router {
    /// Router configuration
    _config: Arc<Config>,
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
    /// Subscriptions and matching topics maintained per connection
    subscriptions: Vec<Option<Subscription>>,
    /// Watermarks of all the replicas. Map[topic]List[u64]. Each index
    /// represents a router in the mesh
    /// Watermark 'n' implies data till n-1 is synced with the other node
    watermarks: Vec<Option<Watermarks>>,
    /// Waiter on a topic. These are used to wake connections/replicators
    /// which are caught up all the data on a topic. Map[topic]List[Connections Ids]
    data_waiters: HashMap<Topic, Vec<(ConnectionId, DataRequest)>>,
    /// Waiters on new topics
    topics_waiters: VecDeque<(ConnectionId, TopicsRequest)>,
    next_topics_waiters: VecDeque<(ConnectionId, TopicsRequest)>,
    /// Channel receiver to receive data from all the active connections and
    /// replicators. Each connection will have a tx handle which they use
    /// to send data and requests to router
    router_rx: Receiver<(ConnectionId, RouterInMessage)>,
}

/// Router is the central node where most of the state is held. Connections and
/// replicators ask router for data and router responds by putting data int
/// relevant connection handle
impl Router {
    pub fn new(config: Arc<Config>) -> (Self, Sender<(ConnectionId, RouterInMessage)>) {
        let (router_tx, router_rx) = bounded(1000);
        let topiclog = TopicLog::new();
        let commitlog = [
            CommitLog::new(config.clone(), 0),
            CommitLog::new(config.clone(), 1),
            CommitLog::new(config.clone(), 2),
        ];
        let mut connections = Vec::with_capacity(config.max_connections);
        let mut subscriptions = Vec::with_capacity(config.max_connections);
        let mut watermarks = Vec::with_capacity(config.max_connections);

        for _ in 0..config.max_connections {
            connections.push(None);
            subscriptions.push(None);
            watermarks.push(None);
        }

        let router = Router {
            _config: config.clone(),
            id: config.id,
            commitlog,
            topiclog,
            connections,
            subscriptions,
            watermarks,
            data_waiters: HashMap::new(),
            topics_waiters: VecDeque::with_capacity(100),
            next_topics_waiters: VecDeque::with_capacity(100),
            router_rx,
        };

        (router, router_tx)
    }

    pub async fn start(&mut self) {
        // All these methods will handle state and errors
        while let Some((id, data)) = self.router_rx.next().await {
            match data {
                RouterInMessage::Connect(connection) => self.handle_new_connection(connection),
                RouterInMessage::Publish(data) => {
                    self.handle_connection_publish(id, data);
                }
                RouterInMessage::Data(data) => self.handle_connection_data(id, data),
                RouterInMessage::ReplicationData(data) => self.handle_replication_data(id, data),
                RouterInMessage::ReplicationAcks(ack) => self.handle_replication_acks(id, ack),
                RouterInMessage::SubscriptionRequest(request) => {
                    self.handle_subscription_request(id, request)
                }
                RouterInMessage::TopicsRequest(request) => self.handle_topics_request(id, request),
                RouterInMessage::DataRequest(request) => self.handle_data_request(id, request),
                RouterInMessage::AcksRequest(request) => self.handle_acks_request(id, request),
                RouterInMessage::Disconnect(request) => self.handle_disconnection(id, request),
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
            return;
        }

        info!(
            "New Connection. In ID = {:?}, Router assigned ID = {:?}",
            connection.conn, id
        );

        if let Some(_) = mem::replace(&mut self.connections[id], Some(connection)) {
            warn!("Replacing an existing connection with same ID");
        }

        if let Some(_) = mem::replace(&mut self.subscriptions[id], Some(Subscription::new())) {
            warn!("Replacing an existing subscription with same ID");
        }

        if let Some(_) = mem::replace(&mut self.watermarks[id], Some(Watermarks::new())) {
            warn!("Replacing an existing watermarks with same ID");
        }

        let subscription_request = SubscriptionRequest;
        let topics_request = TopicsRequest::new();
        let acks_request = AcksRequest::new();
        self.handle_subscription_request(id, subscription_request);
        self.handle_topics_request(id, topics_request);
        self.handle_acks_request(id, acks_request);
    }

    fn handle_disconnection(&mut self, id: ConnectionId, disconnect: Disconnection) {
        info!("Cleaning ID [{}] = {:?} from router", disconnect.id, id);

        if mem::replace(&mut self.connections[id], None).is_none() {
            warn!("Weird, removing a non existent connection")
        }

        if mem::replace(&mut self.watermarks[id], None).is_none() {
            warn!("Weird, removing a non existent watermark")
        }

        if mem::replace(&mut self.subscriptions[id], None).is_none() {
            warn!("Weird, removing a non existent subscription")
        }

        // FIXME iterates through all the topics and all the (pending) requests remove the request
        for waiters in self.data_waiters.values_mut() {
            if let Some(index) = waiters.iter().position(|x| x.0 == id) {
                waiters.swap_remove(index);
            }
        }

        if let Some(index) = self.topics_waiters.iter().position(|x| x.0 == id) {
            self.topics_waiters.swap_remove_back(index);
        }
    }

    /// Handles new incoming data on a topic
    fn handle_connection_data(&mut self, id: ConnectionId, data: Vec<Packet>) {
        trace!(
            "{:11} {:14} Id = {} Count = {}",
            "data",
            "incoming",
            id,
            data.len()
        );
        let mut last_offset = (0, 0);
        let mut count = 0;
        for publish in data {
            match publish {
                Packet::Publish(publish) => {
                    if let Some(offset) = self.handle_connection_publish(id, publish) {
                        last_offset = offset;
                        count += 1;
                    }
                }
                Packet::Subscribe(subscribe) => {
                    self.handle_connection_subscribe(id, subscribe);
                }
                incoming => {
                    warn!("Packet = {:?} not supported by router yet", incoming);
                }
            }
        }

        trace!(
            "{:11} {:14} Id = {} Count = {}, Offset = {:?}",
            "data",
            "committed",
            id,
            count,
            last_offset
        );
    }

    fn handle_connection_subscribe(&mut self, id: ConnectionId, subscribe: Subscribe) {
        let topics = self.topiclog.topics();
        let subscriptions = self.subscriptions[id].as_mut().unwrap();
        let mut return_codes = Vec::new();
        for filter in subscribe.topics.iter() {
            return_codes.push(SubscribeReturnCodes::Success(filter.qos));
        }

        // A new subscription should match with all the existing topics and take a snapshot of current
        // offset of all the matched topics. Subscribers will receive data from that offset
        subscriptions.add_subscription(subscribe.topics, topics);

        // Update matched topic offsets to current offset of this topic's commitlog
        let commitlog = &self.commitlog[self.id];
        commitlog.seek_offsets_to_end(&mut subscriptions.topics);

        let watermarks = self.watermarks[id].as_mut().unwrap();
        watermarks.push_subscribe_ack(subscribe.pkid, return_codes);

        // Notification for suback
        self.fresh_acks_notification(id);
        // Notification for new subscription matched against existing topics
        self.fresh_subscription_notification(id);
    }

    fn handle_connection_publish(
        &mut self,
        id: ConnectionId,
        publish: Publish,
    ) -> Option<(u64, u64)> {
        if publish.payload.len() == 0 {
            error!("Empty publish. ID = {:?}, topic = {:?}", id, publish.topic);
            return None;
        }

        let Publish {
            pkid,
            topic,
            payload,
            qos,
            ..
        } = publish;

        let (is_new_topic, (base_offset, offset)) =
            match self.append_to_commitlog(id, &topic, payload) {
                Some(v) => v,
                None => return None,
            };

        if qos as u8 > 0 {
            let watermarks = self.watermarks[id].as_mut().unwrap();
            watermarks.push_publish_ack(pkid);
            // watermarks.update_pkid_offset_map(&topic, pkid, offset);
        }

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
        self.fresh_acks_notification(id);
        Some((base_offset, offset))
    }

    fn handle_replication_data(&mut self, id: ConnectionId, data: Vec<ReplicationData>) {
        trace!(
            "{:11} {:14} Id = {} Count = {}",
            "data",
            "replicacommit",
            id,
            data.len()
        );
        for data in data {
            let ReplicationData {
                pkid,
                topic,
                payload,
                ..
            } = data;
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
            let watermarks = self.watermarks[id].as_mut().unwrap();

            // TODO we can ignore offset mapping for replicated data
            watermarks.update_pkid_offset_map(&topic, pkid, 0);

            if is_new_topic {
                self.topiclog.append(&topic);
                self.fresh_topics_notification(id);
            }

            // we can probably handle multiple requests better
            self.fresh_data_notification(id, &topic);

            // Replicated data should be acked immediately when there are pending requests
            // in waiters
            self.fresh_acks_notification(id);
        }
    }

    fn handle_replication_acks(&mut self, _id: ConnectionId, acks: Vec<ReplicationAck>) {
        for ack in acks {
            // TODO: Take ReplicationAck and use connection ids in it for notifications
            // TODO: Using wrong id to make code compile. Loop over ids in ReplicationAck
            let watermarks = self.watermarks[0].as_mut().unwrap();
            watermarks.update_cluster_offsets(0, ack.offset);
            watermarks.commit(&ack.topic);
            self.fresh_acks_notification(0);
        }
    }

    /// Replies connection with existing topics that are matched against new
    /// subscription.
    fn handle_subscription_request(&mut self, id: ConnectionId, _request: SubscriptionRequest) {
        trace!("{:11} {:14} Id = {}", "subscr", "request", id);
        let subscription = self.subscriptions[id].as_mut().unwrap();
        let topics = match subscription.take_topics() {
            Some(topics) => topics,
            None => {
                trace!("{:11} {:14} Id = {}", "subscr", "register", id);
                subscription.register_pending_subscription_request();
                return;
            }
        };

        let reply = SubscriptionReply::new(topics);
        self.reply(id, RouterOutMessage::SubscriptionReply(reply));
    }

    fn handle_topics_request(&mut self, id: ConnectionId, request: TopicsRequest) {
        trace!(
            "{:11} {:14} Id = {}, Offset = {}",
            "topics",
            "request",
            id,
            request.offset
        );

        let reply = self.match_new_topics(id, &request);
        let reply = match reply {
            Some(r) => r,
            None => {
                // register the connection 'id' to notify when there are new topics
                self.register_topics_waiter(id, request);
                return;
            }
        };

        trace!(
            "{:11} {:14} Id = {}, Offset = {}, Count = {}",
            "topics",
            "response",
            id,
            reply.offset,
            reply.topics.len()
        );
        self.reply(id, RouterOutMessage::TopicsReply(reply));
    }

    fn handle_data_request(&mut self, id: ConnectionId, mut request: DataRequest) {
        trace!(
            "{:11} {:14} Id = {}, Topic = {}, Offsets = {:?}",
            "data",
            "request",
            id,
            request.topic,
            request.cursors
        );
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
                return;
            }
        };

        trace!(
            "{:11} {:14} Id = {}, Topic = {}, Offsets = {:?}, Count = {}",
            "data",
            "response",
            id,
            reply.topic,
            reply.cursors,
            reply.payload.len()
        );
        let reply = RouterOutMessage::DataReply(reply);
        self.reply(id, reply);
    }

    pub fn handle_acks_request(&mut self, id: ConnectionId, request: AcksRequest) {
        trace!("{:11} {:14} Id = {}", "acks", "request", id);
        let watermarks = self.watermarks[id].as_mut().unwrap();
        let acks = watermarks.acks();
        if acks.is_empty() {
            self.register_acks_waiter(id, request);
            return;
        }

        let reply = AcksReply::new(acks);
        trace!(
            "{:11} {:14} Id = {} Count = {}",
            "acks",
            "response",
            id,
            reply.acks.len()
        );
        let reply = RouterOutMessage::AcksReply(reply);
        self.reply(id, reply);
    }

    fn register_topics_waiter(&mut self, id: ConnectionId, request: TopicsRequest) {
        trace!("{:11} {:14} Id = {}", "topics", "register", id);
        let request = (id.to_owned(), request);
        self.topics_waiters.push_back(request);
    }

    /// Register data waiter
    fn register_data_waiter(&mut self, id: ConnectionId, request: DataRequest) {
        trace!(
            "{:11} {:14} Id = {}, Topic = {}",
            "data",
            "register",
            id,
            request.topic
        );
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

    fn register_acks_waiter(&mut self, id: ConnectionId, _request: AcksRequest) {
        trace!("{:11} {:14} Id = {}", "acks", "register", id);
        let watermarks = self.watermarks[id].as_mut().unwrap();
        watermarks.set_pending_acks_reply(true);
    }

    /// Send notifications to links which registered them. Id is only used to
    /// identify if new topics are from a replicator.
    /// New topic from one connection involves notifying other connections which
    /// are possibly interested in these topics. So this involves going through
    /// all the topic waiters
    fn fresh_topics_notification(&mut self, id: ConnectionId) {
        let replication_data = id < 10;
        for (link_id, request) in self.topics_waiters.drain(0..) {
            // Don't send replicated topic notifications to replication link. Replicator 1
            // should not track replicator 2's topics.
            // id 0-10 are reserved for replicators which are linked to other routers in the mesh
            if replication_data && link_id < 10 {
                self.next_topics_waiters.push_back((link_id, request));
                continue;
            }

            // Ignore connections with zero subscriptions.
            let subscription = self.subscriptions[link_id].as_mut().unwrap();
            if subscription.count() == 0 {
                self.next_topics_waiters.push_back((link_id, request));
                continue;
            }

            // Read more topics from last offset.
            // TODO This should always return in some topics? unwrap?
            let fresh_topics = self.topiclog.readv(request.offset, request.count);
            let (offset, topics) = match fresh_topics {
                Some(v) => v,
                None => {
                    self.next_topics_waiters.push_back((link_id, request));
                    continue;
                }
            };

            // Match new topics with existing subscriptions of this link and reply.
            // Even though there are new topics, it's possible that they didn't match
            // subscriptions held by this connection. Push TopicsRequest back to
            // next topics waiter queue in that case.
            let topics = match subscription.matched_topics(topics) {
                Some(topics) => topics,
                None => {
                    self.next_topics_waiters.push_back((link_id, request));
                    continue;
                }
            };

            let reply = TopicsReply::new(offset + topics.len(), topics);

            trace!(
                "{:11} {:14} Id = {}, Offset = {}, Count = {}",
                "topics",
                "notification",
                link_id,
                reply.offset,
                reply.topics.len()
            );
            self.connections[link_id]
                .as_mut()
                .unwrap()
                .reply(RouterOutMessage::TopicsReply(reply));

            // Note: When the reply is empty don't register notification on behalf of the link.
            // This will cause the channel to be full. Register the notification only when
            // link has made the request and reply doesn't contain any data
        }

        // A technique to optimize allocations. We are never allocating new buffers this way
        mem::swap(&mut self.topics_waiters, &mut self.next_topics_waiters);
    }

    /// This is a slight variant of publish notification. This also sends
    /// TopicsReply
    fn fresh_subscription_notification(&mut self, id: ConnectionId) {
        let subscription = self.subscriptions[id].as_mut().unwrap();

        if !subscription.pending_subscription_request() {
            return;
        }

        let topics = match subscription.take_topics() {
            Some(topics) => topics,
            None => {
                subscription.register_pending_subscription_request();
                return;
            }
        };

        let reply = SubscriptionReply::new(topics);
        self.connections[id]
            .as_mut()
            .unwrap()
            .reply(RouterOutMessage::SubscriptionReply(reply));
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

            trace!(
                "{:11} {:14} Id = {}, Topic = {}, Offsets = {:?}, Count = {}",
                "data",
                "notification",
                link_id,
                reply.topic,
                reply.cursors,
                reply.payload.len()
            );
            let reply = RouterOutMessage::DataReply(reply);
            self.reply(link_id, reply);

            // NOTE:
            // ----------------------
            // When the reply is empty don't register notification on behalf of the link.
            // This will cause the channel to be full. Register the notification only when
            // link has made the request and reply doesn't contain any data
        }
    }

    fn fresh_acks_notification(&mut self, id: ConnectionId) {
        let watermarks = self.watermarks[id].as_mut().unwrap();
        if watermarks.pending_acks_reply() {
            let acks = watermarks.acks();

            let reply = AcksReply::new(acks);
            watermarks.set_pending_acks_reply(false);
            trace!("{:11} {:14} Id = {}", "acks", "notification", id);
            let reply = RouterOutMessage::AcksReply(reply);
            self.reply(id, reply);
        }
    }

    /// Connections pull logs from both replication and connections where as replicator
    /// only pull logs from connections.
    /// Data from replicator and data from connection are separated for this reason
    fn append_to_commitlog(
        &mut self,
        id: ConnectionId,
        topic: &str,
        bytes: Bytes,
    ) -> Option<(bool, (u64, u64))> {
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

    /// Extracts new topics from topics log (from offset in TopicsRequest) and matches
    /// them against subscriptions of this connection. Returns a TopicsReply if there
    /// are matches
    fn match_new_topics(
        &mut self,
        id: ConnectionId,
        request: &TopicsRequest,
    ) -> Option<TopicsReply> {
        let (offset, topics) = match self.topiclog.readv(request.offset, request.count) {
            Some(v) => v,
            None => return None,
        };
        println!("{:?}, {:?}", offset, topics);

        let subscription = self.subscriptions[id].as_mut().unwrap();
        match subscription.matched_topics(&topics) {
            Some(topics) => Some(TopicsReply::new(offset + 1, topics)),
            None => None,
        }
    }

    /// Extracts data from native log. Returns None in case the
    /// log is caught up or encountered an error while reading data
    fn extract_connection_data(&mut self, request: &mut DataRequest) -> Option<DataReply> {
        let native_id = self.id;
        let topic = &request.topic;
        let commitlog = &mut self.commitlog[native_id];
        let max_count = request.max_count;
        let cursors = request.cursors;

        let (segment, offset) = cursors[native_id];
        debug!(
            "Pull native data. Topic = {}, seg = {}, offset = {}",
            topic, segment, offset
        );
        let mut reply = DataReply::new(request.topic.clone(), cursors, 0, Vec::new());

        match commitlog.readv(topic, segment, offset, max_count) {
            Ok(Some((jump, base_offset, record_offset, payload))) => {
                match jump {
                    Some(next) => reply.cursors[native_id] = (next, next),
                    None => reply.cursors[native_id] = (base_offset, record_offset + 1),
                }

                // Update reply's cursors only when read has returned some data
                // Move the reply to next segment if we are done with the current one
                if payload.is_empty() {
                    return None;
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
        let max_count = request.max_count;
        debug!(
            "Pull data. Topic = {}, cursors = {:?}",
            topic, request.cursors
        );

        let mut cursors = [(0, 0); 3];
        let mut payload = Vec::new();

        // Iterate through native and replica commitlogs to collect data (of a topic)
        for (i, commitlog) in self.commitlog.iter_mut().enumerate() {
            let (segment, offset) = request.cursors[i];
            match commitlog.readv(topic, segment, offset, max_count) {
                Ok(Some(v)) => {
                    let (jump, base_offset, record_offset, mut data) = v;
                    match jump {
                        Some(next) => cursors[i] = (next, next),
                        None => cursors[i] = (base_offset, record_offset + 1),
                    }

                    if data.is_empty() {
                        continue;
                    }
                    payload.append(&mut data);
                }
                Ok(None) => continue,
                Err(e) => {
                    error!("Failed to extract data from commitlog. Error = {:?}", e);
                }
            }
        }

        // When payload is empty due to read after current offset
        // because of uninitialized request, update request with
        // latest offsets and return None so that caller registers
        // the request with updated offsets
        match payload.is_empty() {
            true => None,
            false => Some(DataReply::new(request.topic.clone(), cursors, 0, payload)),
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
            match e {
                TrySendError::Full(e) => error!("Channel full. Id = {}, Message = {:?}", id, e),
                TrySendError::Closed(e) => info!("Channel closed. Id = {}, Message = {:?}", id, e),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::broker::Broker;
    use crate::router::*;
    use async_channel::Receiver;
    use std::time::Duration;

    #[tokio::test(core_threads = 1)]
    async fn router_doesnt_give_data_when_not_asked() {
        let mut broker = Broker::new().await;
        let (connection_id, connection_rx) = broker.connection("1").await;
        broker.write_to_commitlog(connection_id, "hello/world", vec![1, 2, 3], 1);
        broker.write_to_commitlog(connection_id, "hello/world", vec![4, 5, 6], 2);
        assert!(wait_for_new_data(&connection_rx).await.is_none());
    }

    #[tokio::test(core_threads = 1)]
    async fn router_registers_and_doesnt_repond_when_a_topic_is_caughtup() {
        let mut broker = Broker::new().await;
        let (connection_id, connection_rx) = broker.connection("1").await;
        broker.write_to_commitlog(connection_id, "hello/world", vec![1, 2, 3], 1);
        broker.write_to_commitlog(connection_id, "hello/world", vec![4, 5, 6], 2);
        wait_for_new_acks(&connection_rx).await.unwrap();

        broker.new_data_request(connection_id, "hello/world", [(0, 0); 3]);

        let reply = wait_for_new_data(&connection_rx).await.unwrap();
        assert_eq!(reply.cursors, [(0, 2), (0, 0), (0, 0)]);
        assert_eq!(reply.payload[0].as_ref(), &[1, 2, 3]);
        assert_eq!(reply.payload[1].as_ref(), &[4, 5, 6]);

        broker.new_data_request(connection_id, "hello/world", reply.cursors);
        assert!(wait_for_new_data(&connection_rx).await.is_none());
    }

    #[tokio::test(core_threads = 1)]
    async fn connection_reads_existing_connection_data() {
        let mut broker = Broker::new().await;
        let (connection_1_id, _connection_1_rx) = broker.connection("1").await;
        let (connection_2_id, connection_2_rx) = broker.connection("2").await;

        // write data from a native connection and read from connection
        broker.write_to_commitlog(connection_1_id, "hello/world", vec![1, 2, 3], 1);
        broker.write_to_commitlog(connection_1_id, "hello/world", vec![4, 5, 6], 2);

        // new data request from the replicator
        broker.new_data_request(connection_2_id, "hello/world", [(0, 0); 3]);
        let reply = wait_for_new_data(&connection_2_rx).await.unwrap();

        assert_eq!(reply.payload.len(), 2);
        assert_eq!(reply.payload[0].as_ref(), &[1, 2, 3]);
        assert_eq!(reply.payload[1].as_ref(), &[4, 5, 6]);
    }

    #[tokio::test(core_threads = 1)]
    async fn new_connection_data_notifies_connection() {
        let mut broker = Broker::new().await;
        let (connection_1_id, _connection_1_rx) = broker.connection("1").await;
        let (connection_2_id, connection_2_rx) = broker.connection("2").await;

        broker.new_data_request(connection_2_id, "hello/world", [(0, 0); 3]);
        assert!(wait_for_new_data(&connection_2_rx).await.is_none());

        // write data from a native connection and read from connection
        broker.write_to_commitlog(connection_1_id, "hello/world", vec![1, 2, 3], 1);
        broker.write_to_commitlog(connection_1_id, "hello/world", vec![4, 5, 6], 2);
        broker.write_to_commitlog(connection_1_id, "hello/world", vec![7, 8, 9], 3);

        // Initial notification is with 1 message
        let reply = wait_for_new_data(&connection_2_rx).await.unwrap();
        assert_eq!(reply.payload.len(), 1);
        assert_eq!(reply.payload[0].as_ref(), &[1, 2, 3]);

        // Subsequent request till catchup will result in a bulk
        broker.new_data_request(connection_2_id, "hello/world", reply.cursors);
        let reply = wait_for_new_data(&connection_2_rx).await.unwrap();
        assert_eq!(reply.payload.len(), 2);
        assert_eq!(reply.payload[0].as_ref(), &[4, 5, 6]);
        assert_eq!(reply.payload[1].as_ref(), &[7, 8, 9]);
    }

    #[tokio::test(core_threads = 1)]
    async fn new_topic_from_connection_replies_matching_connections() {
        let mut broker = Broker::new().await;
        let (connection_1_id, connection_1_rx) = broker.connection("1").await;
        let (connection_2_id, connection_2_rx) = broker.connection("2").await;

        // Send request for new topics. Router should reply with new topics when there are any
        broker.subscribe(connection_1_id, "hello/1/world", 1);
        broker.subscribe(connection_2_id, "#", 1);
        wait_for_new_acks(&connection_1_rx).await.unwrap();
        wait_for_new_acks(&connection_2_rx).await.unwrap();

        // Send new data to router to be written to commitlog
        for i in 0..20 {
            let topic = format!("hello/{}/world", i);
            broker.write_to_commitlog(connection_1_id, &topic, vec![1, 2, 3], 2);
        }

        // Notifications with 1 topic
        let reply = wait_for_new_topics(&connection_1_rx).await.unwrap();
        assert_eq!(reply.topics.len(), 1);
        assert_eq!(reply.topics[0].0, "hello/1/world");

        // Notifications with 1 topic
        let reply = wait_for_new_topics(&connection_2_rx).await.unwrap();
        let topics = reply.topics;
        let offset = reply.offset;
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].0, "hello/0/world");

        // Return all matching topics (a max of 10). hello/1/world to hello/10/world
        broker.new_topics_request(connection_2_id, offset);
        let reply = wait_for_new_topics(&connection_2_rx).await.unwrap();
        let topics = reply.topics;
        let offset = reply.offset;
        assert_eq!(topics.len(), 10);
        for (i, t) in topics.into_iter().enumerate() {
            assert_eq!(format!("hello/{}/world", i + 1), t.0);
            assert_eq!([(0, 0), (0, 0), (0, 0)], t.2);
        }

        // Remaining hello/11/world to hello/19/world
        broker.new_topics_request(connection_2_id, offset);
        let reply = wait_for_new_topics(&connection_2_rx).await.unwrap();
        let topics = reply.topics;
        assert_eq!(topics.len(), 9);
        // hello/1/world to hello/10/world
        for (i, t) in topics.into_iter().enumerate() {
            assert_eq!(format!("hello/{}/world", i + 11), t.0);
            assert_eq!([(0, 0), (0, 0), (0, 0)], t.2);
        }

        broker.new_topics_request(connection_1_id, 0);
        assert!(wait_for_new_topics(&connection_1_rx).await.is_none());
    }

    #[tokio::test(core_threads = 1)]
    async fn new_topic_from_connection_notifies_matching_connections() {
        let mut broker = Broker::new().await;
        let (connection_1_id, connection_1_rx) = broker.connection("1").await;
        let (connection_2_id, connection_2_rx) = broker.connection("2").await;

        broker.subscribe(connection_1_id, "hello/1/world", 1);
        broker.subscribe(connection_2_id, "#", 1);
        wait_for_new_acks(&connection_1_rx).await.unwrap();
        wait_for_new_acks(&connection_2_rx).await.unwrap();

        // Send new data to router to be written to commitlog
        for i in 0..20 {
            let topic = format!("hello/{}/world", i);
            broker.write_to_commitlog(connection_1_id, &topic, vec![1, 2, 3], 2);
        }

        // Returns only 1 matching topic for connection 1
        let topics = wait_for_new_topics(&connection_1_rx).await.unwrap().topics;
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].0, "hello/1/world");

        // Topics 'notification' replies with count = 1
        let reply = wait_for_new_topics(&connection_2_rx).await.unwrap();
        let topics = reply.topics;
        let offset = reply.offset;
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].0, "hello/0/world");

        // hello/1/world to hello/10/world
        broker.new_topics_request(connection_2_id, offset);
        let reply = wait_for_new_topics(&connection_2_rx).await.unwrap();
        let topics = reply.topics;
        let offset = reply.offset;
        assert_eq!(topics.len(), 10);
        for (i, t) in topics.into_iter().enumerate() {
            assert_eq!(format!("hello/{}/world", i + 1), t.0);
            assert_eq!([(0, 0), (0, 0), (0, 0)], t.2);
        }

        // remaining hello/11/world to hello/19/world
        broker.new_topics_request(connection_2_id, offset);
        let reply = wait_for_new_topics(&connection_2_rx).await.unwrap();
        let topics = reply.topics;
        assert_eq!(topics.len(), 9);
        // hello/1/world to hello/10/world
        for (i, t) in topics.into_iter().enumerate() {
            assert_eq!(format!("hello/{}/world", i + 11), t.0);
            assert_eq!([(0, 0), (0, 0), (0, 0)], t.2);
        }
    }

    #[tokio::test(core_threads = 1)]
    async fn new_subscription_notifies_with_existing_matching_topics() {
        let mut broker = Broker::new().await;
        let (connection_1_id, connection_1_rx) = broker.connection("1").await;

        // Write data of 15 topics
        for i in 0..15 {
            let topic = format!("hello/{}/world", i);
            broker.write_to_commitlog(connection_1_id, &topic, vec![1, 2, 3], 1);
        }

        // This should notify connection of existing topics which are matching
        // this subscription
        broker.subscribe(connection_1_id, "#", 2);

        wait_for_new_acks(&connection_1_rx).await.unwrap();
        let topics = wait_for_existing_topics(&connection_1_rx)
            .await
            .unwrap()
            .topics;

        assert_eq!(topics.len(), 15);
        for (i, t) in topics.into_iter().enumerate() {
            assert_eq!(format!("hello/{}/world", i), t.0);
            assert_eq!([(0, 1), (0, 0), (0, 0)], t.2);
        }
    }

    #[tokio::test(core_threads = 1)]
    async fn new_subscription_replies_with_existing_matching_topics() {
        let mut broker = Broker::new().await;
        let (connection_1_id, connection_1_rx) = broker.connection("1").await;

        // 15 existing matching topics
        for i in 0..15 {
            let topic = format!("hello/{}/world", i);
            broker.write_to_commitlog(connection_1_id, &topic, vec![1, 2, 3], 1);
        }

        // Send subscription. Matches and buffers matching topics. Doesn't reply yet
        broker.subscribe(connection_1_id, "#", 2);
        let o = wait_for_existing_topics(&connection_1_rx).await;
        assert!(o.is_none());

        // Replies after this request
        broker.new_subscription_request(connection_1_id);

        let topics = wait_for_existing_topics(&connection_1_rx)
            .await
            .unwrap()
            .topics;

        assert_eq!(topics.len(), 15);
        for (i, t) in topics.into_iter().enumerate() {
            assert_eq!(format!("hello/{}/world", i), t.0);
            assert_eq!([(0, 1), (0, 0), (0, 0)], t.2);
        }
    }

    #[tokio::test(core_threads = 1)]
    async fn new_subscription_should_not_return_already_tracked_topics() {}

    async fn wait_for_new_topics(rx: &Receiver<RouterOutMessage>) -> Option<TopicsReply> {
        tokio::time::delay_for(Duration::from_secs(1)).await;
        match rx.try_recv() {
            Ok(RouterOutMessage::TopicsReply(reply)) => Some(reply),
            _v => None,
        }
    }

    async fn wait_for_existing_topics(
        rx: &Receiver<RouterOutMessage>,
    ) -> Option<SubscriptionReply> {
        tokio::time::delay_for(Duration::from_secs(1)).await;
        match rx.try_recv() {
            Ok(RouterOutMessage::SubscriptionReply(reply)) => Some(reply),
            _v => None,
        }
    }

    async fn wait_for_new_data(rx: &Receiver<RouterOutMessage>) -> Option<DataReply> {
        tokio::time::delay_for(Duration::from_secs(1)).await;
        match rx.try_recv() {
            Ok(RouterOutMessage::DataReply(reply)) => Some(reply),
            _v => None,
        }
    }

    async fn wait_for_new_acks(rx: &Receiver<RouterOutMessage>) -> Option<AcksReply> {
        tokio::time::delay_for(Duration::from_secs(1)).await;
        match rx.try_recv() {
            Ok(RouterOutMessage::AcksReply(reply)) => Some(reply),
            _v => None,
        }
    }
}

#[cfg(test)]
mod broker {
    use super::{ConnectionId, Router};
    use crate::router::*;
    use crate::*;
    use async_channel::{Receiver, Sender};
    use mqtt4bytes::{QoS, Subscribe};
    use std::sync::Arc;

    // Broker is used to test router
    pub(crate) struct Broker {
        router_tx: Sender<(ConnectionId, RouterInMessage)>,
    }

    impl Broker {
        pub(crate) async fn new() -> Broker {
            let mut config = Config::default();
            config.id = 0;
            let (router, router_tx) = Router::new(Arc::new(config));
            tokio::task::spawn(async move {
                let mut router = router;
                router.start().await;
            });

            Broker { router_tx }
        }

        pub(crate) async fn connection(
            &mut self,
            id: &str,
        ) -> (ConnectionId, Receiver<RouterOutMessage>) {
            let (connection, link_rx) = Connection::new_remote(id, 5);
            let message = RouterInMessage::Connect(connection);
            self.router_tx.send((0, message)).await.unwrap();

            let connection_id = match link_rx.recv().await.unwrap() {
                RouterOutMessage::ConnectionAck(ConnectionAck::Success(id)) => id,
                o => panic!("Unexpected connection ack = {:?}", o),
            };

            (connection_id, link_rx)
        }

        // async fn replicator(&mut self, id: usize) -> (ConnectionId, Receiver<RouterOutMessage>) {
        //     let (connection, link_rx) = Connection::new_replica(id, 5);
        //     let message = RouterInMessage::Connect(connection);
        //     self.router_tx.send((0, message)).await.unwrap();

        //     let connection_id = match link_rx.recv().await.unwrap() {
        //         RouterOutMessage::ConnectionAck(ConnectionAck::Success(id)) => id,
        //         o => panic!("Unexpected connection ack = {:?}", o),
        //     };

        //     (connection_id, link_rx)
        // }

        pub(crate) fn write_to_commitlog(
            &mut self,
            id: usize,
            topic: &str,
            payload: Vec<u8>,
            pkid: u16,
        ) {
            let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
            publish.pkid = pkid;
            let message = RouterInMessage::Publish(publish);
            let message = (id, message);
            self.router_tx.try_send(message).unwrap();
        }

        pub(crate) fn subscribe(&mut self, id: usize, filter: &str, pkid: u16) {
            let mut subscribe = Subscribe::new(filter, QoS::AtLeastOnce);
            subscribe.pkid = pkid;
            let message = RouterInMessage::Data(vec![Packet::Subscribe(subscribe)]);
            let message = (id, message);
            self.router_tx.try_send(message).unwrap();
        }

        pub(crate) fn new_subscription_request(&mut self, id: usize) {
            let message = RouterInMessage::SubscriptionRequest(SubscriptionRequest);
            let message = (id, message);
            self.router_tx.try_send(message).unwrap();
        }

        pub(crate) fn new_topics_request(&mut self, id: usize, offset: usize) {
            let message = RouterInMessage::TopicsRequest(TopicsRequest::offset(offset));
            let message = (id, message);
            self.router_tx.try_send(message).unwrap();
        }

        pub(crate) fn new_data_request(
            &mut self,
            id: usize,
            topic: &str,
            offsets: [(u64, u64); 3],
        ) {
            let message =
                RouterInMessage::DataRequest(DataRequest::offsets(topic.to_owned(), offsets));
            let message = (id, message);
            self.router_tx.try_send(message).unwrap();
        }

        pub(crate) fn new_acks_request(&mut self, id: usize) {
            let message = (id, RouterInMessage::AcksRequest(AcksRequest::new()));
            self.router_tx.try_send(message).unwrap();
        }
    }
}

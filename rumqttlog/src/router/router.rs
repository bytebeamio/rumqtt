use std::sync::Arc;

use flume::{bounded, Receiver, RecvError, Sender};
use mqtt4bytes::{Packet, Publish, Subscribe, SubscribeReturnCodes};
use thiserror::Error;

use super::*;

use crate::logs::{DataLog, TopicsLog};
use crate::router::readyqueue::ReadyQueue;
use crate::router::slab::Slab;
use crate::router::watermarks::Watermarks;
use crate::waiters::{DataWaiters, TopicsWaiters};
use crate::{Config, DataRequest, Disconnection, ReplicationData};

#[derive(Error, Debug)]
#[error("...")]
pub enum RouterError {
    Recv(#[from] RecvError),
}

type ConnectionId = usize;

pub struct Router {
    /// Router configuration
    _config: Arc<Config>,
    /// Id of this router. Used to index native commitlog to store data from
    /// local connections
    id: ConnectionId,
    /// Data logs grouped by replica
    datalog: DataLog,
    /// Topic log
    topicslog: TopicsLog,
    /// Pre-allocated list of connections
    connections: Slab<Connection>,
    /// Subscriptions and matching topics maintained per connection
    trackers: Slab<Tracker>,
    /// Watermarks of a connection
    watermarks: Slab<Watermarks>,
    /// Connections with more pending requests and ready to make progress
    readyqueue: ReadyQueue,
    /// Waiter on a topic. These are used to wake connections/replicators
    /// which are caught up all the data on a topic. Map[topic]List[Connections Ids]
    data_waiters: DataWaiters,
    /// Waiters on new topics
    topics_waiters: TopicsWaiters,
    /// Channel receiver to receive data from all the active connections and
    /// replicators. Each connection will have a tx handle which they use
    /// to send data and requests to router
    router_rx: Receiver<(ConnectionId, Event)>,
}

impl Router {
    pub fn new(config: Arc<Config>) -> (Self, Sender<(ConnectionId, Event)>) {
        let (router_tx, router_rx) = bounded(1000);
        let id = config.id;
        let max_connections = config.max_connections;

        // Connection level information
        let connections = Slab::with_capacity(max_connections);
        let trackers = Slab::with_capacity(max_connections);
        let watermarks = Slab::with_capacity(max_connections);

        // Global data
        let datalog: DataLog = DataLog::new(id, config.clone());
        let topicslog = TopicsLog::new();

        // Waiters to notify new data or topics
        let data_waiters = DataWaiters::new();
        let topics_waiters = TopicsWaiters::new();
        let readyqueue = ReadyQueue::new();

        let router = Router {
            _config: config,
            id,
            datalog,
            topicslog,
            connections,
            trackers,
            watermarks,
            readyqueue,
            data_waiters,
            topics_waiters,
            router_rx,
        };

        (router, router_tx)
    }

    pub fn start(&mut self) -> Result<(), RouterError> {
        loop {
            // Block on incoming events if there are no connections
            // in ready queue.
            if self.readyqueue.is_empty() {
                let (id, data) = self.router_rx.recv()?;
                self.route(id, data);
            }

            // Poll 10 connections which are ready in ready queue
            for _ in 0..10 {
                match self.readyqueue.pop_front() {
                    Some(id) => self.connection_ready(id),
                    None => break,
                }
            }

            // Try reading 1000 events from connections in a non-blocking
            // fashion to accumulate data and handle subscriptions
            for _ in 0..1000 {
                // All these methods will handle state and errors
                match self.router_rx.try_recv() {
                    Ok((id, data)) => self.route(id, data),
                    Err(..) => break,
                }
            }
        }
    }

    fn route(&mut self, id: usize, data: Event) {
        match data {
            Event::Connect(connection) => self.handle_new_connection(connection),
            Event::Data(data) => self.handle_connection_data(id, data),
            Event::ReplicationData(data) => self.handle_replication_data(id, data),
            Event::ReplicationAcks(ack) => self.handle_replication_acks(id, ack),
            Event::Disconnect(request) => self.handle_disconnection(id, request),
            Event::Ready => self.connection_ready(id),
        }
    }

    fn handle_new_connection(&mut self, connection: Connection) {
        let id = match connection.conn.clone() {
            ConnectionType::Replicator(id) => {
                self.connections.insert_at(connection, id);
                id
            }
            ConnectionType::Device(did) => match self.connections.insert(connection) {
                Some(id) => {
                    self.trackers.insert(Tracker::new()).unwrap();
                    self.watermarks.insert(Watermarks::new()).unwrap();
                    self.readyqueue.push_back(id);
                    info!("Connection. In ID = {:?}, Router ID = {:?}", did, id);
                    id
                }
                None => {
                    error!("No space for new connection!!");
                    return;
                }
            },
        };

        let message = Notification::ConnectionAck(ConnectionAck::Success(id));
        notify(&mut self.connections, id, message);
    }

    fn handle_disconnection(&mut self, id: ConnectionId, disconnect: Disconnection) {
        info!("Cleaning ID [{}] = {:?} from router", disconnect.id, id);

        self.connections.remove(id);
        self.trackers.remove(id);
        self.watermarks.remove(id);
        self.data_waiters.remove(id);
        self.topics_waiters.remove(id);
        self.readyqueue.remove(id);
    }

    fn connection_ready(&mut self, id: ConnectionId) {
        let tracker = self.trackers.get_mut(id).unwrap();

        // Iterate through a max of 100 requests everytime a connection if polled.
        // This prevents a connection from unfairly taking up router's time preventing
        // other connections from making progress.
        for _ in 0..100 {
            match tracker.pop_request() {
                Some(request) => match request {
                    Request::Data(request) => {
                        let datalog = &mut self.datalog;
                        let waiters = &mut self.data_waiters;

                        // Get data from commitlog and register for notification if
                        // all the data is caught up.
                        if let Some(data) = handle_data_request(id, request, datalog, waiters) {
                            // If data is yielded by commitlog, register a new data request
                            // in the tracker with next offset and send data notification to
                            // the connection
                            tracker.register_data_request(data.topic.clone(), data.cursors);
                            let notification = Notification::Data(data);
                            let notified = notify(&mut self.connections, id, notification);

                            // Save notification to failed messages and dont schedule this
                            // connection again
                            if !notified {
                                info!("Notification error. Unschedule. Id = {}", id);
                                return;
                            }
                        }
                    }
                    Request::Topics(request) => {
                        let topicslog = &mut self.topicslog;
                        let waiters = &mut self.topics_waiters;

                        // Get topics from topics log and register for notification if
                        // all the topics are caught up
                        if let Some(data) = handle_topics_request(id, request, topicslog, waiters) {
                            // Register for new topics request if previous request is handled
                            tracker.track_matched_topics(data.topics);
                            tracker.register_topics_request(data.offset);
                        }
                    }
                    Request::Acks(_) => {
                        // Get acks from commitlog and register for notification if
                        // all the data is caught up.
                        let acks = self.watermarks.get_mut(id).unwrap();

                        // If acks are yielded, register a new acks request
                        // and send acks notification to the connection
                        if let Some(acks) = handle_acks_request(id, acks) {
                            tracker.register_acks_request();
                            let notification = Notification::Acks(acks);
                            let notified = notify(&mut self.connections, id, notification);

                            // Save notification to failed messages and dont schedule this
                            // connection again
                            if !notified {
                                info!("Notification error. Unschedule. Id = {}", id);
                                return;
                            }
                        }
                    }
                },
                None => {
                    // At this point, pending requests in tracker is 0. Early return
                    // here prevents connection from being added to ready queue again.
                    // New requests are added to tracker through notifications of
                    // previous requests. When first request is added back to tracker
                    // again, it's scheduled back on ready queue again.
                    return;
                }
            }
        }

        // If there are more requests in the tracker, add the connection back
        // to ready queue.
        self.readyqueue.push_back(id);
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

        let mut count = 0;
        for publish in data {
            match publish {
                Packet::Publish(publish) => {
                    self.handle_connection_publish(id, publish);
                    count += 1;
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
            "{:11} {:14} Id = {} Count = {}",
            "data",
            "committed",
            id,
            count,
        );
    }

    fn handle_connection_subscribe(&mut self, id: ConnectionId, subscribe: Subscribe) {
        let topics = self.topicslog.readv(0, 0);

        let tracker = self.trackers.get_mut(id).unwrap();
        let mut return_codes = Vec::new();
        for filter in subscribe.topics.iter() {
            return_codes.push(SubscribeReturnCodes::Success(filter.qos));
        }

        // A new subscription should match with all the existing topics and take a snapshot of current
        // offset of all the matched topics. Subscribers will receive data from the next offset
        match topics {
            Some((_, topics)) => {
                // Add subscription and get topics matching all the existing topics
                // in the (topics) commitlog ant seek them to next offset. Seeking is
                // necessary because new subscription should yield only subsequent data
                tracker.add_subscription_and_match(subscribe.topics, topics);

                // Add matching topics for data requests and register notifications for new topics
                // FIXME: Verify logic of self.id. Should this be seeked for replicators as well?
                while let Some(mut topic) = tracker.next_matched() {
                    self.datalog.seek_offsets_to_end(self.id, &mut topic);
                    tracker.register_data_request(topic.0, topic.2);
                }
            }
            None => {
                // Router did not receive data from any topics yet. Add subscription and
                // register topics request from offset 0
                tracker.add_subscription_and_match(subscribe.topics, &[]);
            }
        };

        // Update acks and triggers acks notification for suback
        let watermarks = self.watermarks.get_mut(id).unwrap();
        watermarks.push_subscribe_ack(subscribe.pkid, return_codes);
        self.fresh_acks_notification(id);
    }

    fn handle_connection_publish(
        &mut self,
        id: ConnectionId,
        publish: Publish,
    ) -> Option<(u64, u64)> {
        if publish.payload.is_empty() {
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

        let (is_new_topic, (base_offset, offset)) = match self.datalog.append(id, &topic, payload) {
            Some(v) => v,
            None => return None,
        };

        if qos as u8 > 0 {
            let watermarks = self.watermarks.get_mut(id).unwrap();
            watermarks.push_publish_ack(pkid);
            watermarks.update_pkid_offset_map(&topic, pkid, offset);
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
            self.topicslog.append(&topic);
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
                if payload.is_empty() {
                    error!("Empty publish. ID = {:?}, topic = {:?}", id, topic);
                    return;
                }

                if let Some((new_topic, _)) = self.datalog.append(id, &topic, payload) {
                    is_new_topic = new_topic;
                }
            }

            // TODO: Is this necessary for replicated data
            let watermarks = self.watermarks.get_mut(id).unwrap();

            // TODO we can ignore offset mapping for replicated data
            watermarks.update_pkid_offset_map(&topic, pkid, 0);

            if is_new_topic {
                self.topicslog.append(&topic);
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
            let watermarks = self.watermarks.get_mut(0).unwrap();
            watermarks.update_cluster_offsets(0, ack.offset);
            watermarks.commit(&ack.topic);
            self.fresh_acks_notification(0);
        }
    }

    /// Send notifications to links which registered them. Id is only used to
    /// identify if new topics are from a replicator.
    /// New topic from one connection involves notifying other connections which
    /// are possibly interested in these topics. So this involves going through
    /// all the topic waiters
    fn fresh_topics_notification(&mut self, id: ConnectionId) {
        let replication_data = id < 10;
        while let Some((link_id, request)) = self.topics_waiters.pop_front() {
            // Ignore connections with zero subscriptions.
            let tracker = self.trackers.get_mut(link_id).unwrap();
            if tracker.subscription_count() == 0 {
                // panic!("Topics request registration for 0 subscription connection");
                continue;
            }

            // Don't send replicated topic notifications to replication link. Replicator 1
            // should not track replicator 2's topics.
            // id 0-10 are reserved for replicators which are linked to other routers in the mesh
            if replication_data && link_id < 10 {
                self.topics_waiters.push_back(link_id, request);
                continue;
            }

            // Read more topics from last offset.
            let fresh_topics = self.topicslog.readv(request.offset, request.count);
            let (next_offset, topics) = fresh_topics.unwrap();

            // Match new topics with existing subscriptions of this link and reply.
            // Even though there are new topics, it's possible that they didn't match
            // subscriptions held by this connection. Push TopicsRequest back to
            // next topics waiter queue in that case.
            let count = tracker.track_matched_topics(topics);
            let pending = tracker.register_topics_request(next_offset);

            // Tracker is ready again. Add it back to ready queue
            if pending == 1 {
                self.readyqueue.push_back(link_id);
            }

            trace!(
                "{:11} {:14} Id = {}, Offset = {}, Count = {}",
                "topics",
                "notification",
                link_id,
                next_offset,
                count
            );
        }
    }

    /// Send data to links which registered them
    fn fresh_data_notification(&mut self, id: ConnectionId, topic: &str) {
        let waiters = match self.data_waiters.get_mut(topic) {
            Some(w) => w,
            None => return,
        };

        let replication_data = id < 10;
        while let Some((link_id, request)) = waiters.pop_front() {
            let is_replicator = link_id < 10;

            // Ignore new data notification if the current notification awaiting
            // link is a replicator link and data is also from a replicator
            let reply = if is_replicator && replication_data {
                None
            } else {
                self.datalog.handle_data_request(link_id, &request)
            };

            // Re-register this connection for notification
            let reply = match reply {
                Some(v) => v,
                None => {
                    waiters.push_back((link_id, request));
                    continue;
                }
            };

            let tracker = self.trackers.get_mut(link_id).unwrap();
            let pending = tracker.register_data_request(reply.topic.clone(), reply.cursors);

            // Tracker is ready again. Add it back to ready queue
            if pending == 1 {
                self.readyqueue.push_back(link_id);
            }

            trace!(
                "{:11} {:14} Id = {}, Topic = {}, Offsets = {:?}, Count = {}",
                "data",
                "notification",
                link_id,
                reply.topic,
                reply.cursors,
                reply.payload.len()
            );

            notify(&mut self.connections, link_id, Notification::Data(reply));
        }
    }

    fn fresh_acks_notification(&mut self, id: ConnectionId) {
        let watermarks = self.watermarks.get_mut(id).unwrap();

        // Unlike data and topics where notifications are routed
        // to other connections, acks are meant for the same connection.
        // We use watermark's own flag to determine if it's waiting for
        // a notification
        if watermarks.take_pending_acks_request().is_some() {
            // Take acks which are ready to be sent to the
            // connection
            let acks = watermarks.acks();
            debug_assert!(acks.len() > 0);

            trace!("{:11} {:14} Id = {}", "acks", "notification", id);

            // Notify connection with acks
            let reply = Acks::new(acks);
            let reply = Notification::Acks(reply);
            notify(&mut self.connections, id, reply);

            // Add next acks request to the tracker
            let tracker = self.trackers.get_mut(id).unwrap();
            let pending = tracker.register_acks_request();

            // Tracker is ready again. Add it back to ready queue
            if pending == 1 {
                self.readyqueue.push_back(id);
            }
        }
    }
}

fn handle_data_request(
    id: ConnectionId,
    request: DataRequest,
    datalog: &mut DataLog,
    waiters: &mut DataWaiters,
) -> Option<Data> {
    trace!(
        "{:11} {:14} Id = {}, Topic = {}, Offsets = {:?}",
        "data",
        "request",
        id,
        request.topic,
        request.cursors
    );

    let data = match datalog.handle_data_request(id, &request) {
        Some(data) => {
            trace!(
                "{:11} {:14} Id = {}, Topic = {}, Offsets = {:?}, Count = {}",
                "data",
                "response",
                id,
                data.topic,
                data.cursors,
                data.payload.len()
            );

            data
        }
        None => {
            trace!(
                "{:11} {:14} Id = {}, Topic = {}",
                "data",
                "register",
                id,
                request.topic
            );

            waiters.register(id, request);
            return None;
        }
    };

    Some(data)
}

fn handle_topics_request<'a>(
    id: ConnectionId,
    request: TopicsRequest,
    topicslog: &'a mut TopicsLog,
    waiters: &mut TopicsWaiters,
) -> Option<Topics<'a>> {
    trace!(
        "{:11} {:14} Id = {}, Offset = {}",
        "topics",
        "request",
        id,
        request.offset
    );

    let topics = match topicslog.readv(request.offset, request.count) {
        Some(topics) => {
            trace!(
                "{:11} {:14} Id = {}, Offset = {}, Count = {}",
                "topics",
                "response",
                id,
                topics.0,
                topics.1.len()
            );

            Topics::new(topics.0, topics.1)
        }
        None => {
            trace!("{:11} {:14} Id = {}", "topics", "register", id);
            waiters.push_back(id, request);
            return None;
        }
    };

    Some(topics)
}

fn handle_acks_request(id: ConnectionId, acks: &mut Watermarks) -> Option<Acks> {
    trace!("{:11} {:14} Id = {}", "acks", "request", id);
    let acks = match acks.handle_acks_request() {
        Some(acks) => {
            trace!(
                "{:11} {:14} Id = {} Count = {}",
                "acks",
                "response",
                id,
                acks.acks.len()
            );

            acks
        }
        None => {
            trace!("{:11} {:14} Id = {}", "acks", "register", id);
            acks.register_pending_acks_request();
            return None;
        }
    };

    Some(acks)
}

fn notify(connections: &mut Slab<Connection>, id: ConnectionId, reply: Notification) -> bool {
    let connection = match connections.get_mut(id) {
        Some(c) => c,
        None => {
            error!("Invalid id while replying = {:?}", id);
            return true;
        }
    };

    connection.notify(reply)
}

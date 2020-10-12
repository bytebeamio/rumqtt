use std::sync::Arc;

use jackiechan::{bounded, Receiver, RecvError, Sender, TryRecvError};
use mqtt4bytes::{Packet, Publish, Subscribe, SubscribeReturnCodes};
use thiserror::Error;

use super::connection::ConnectionType;
use super::readyqueue::ReadyQueue;
use super::slab::Slab;
use super::watermarks::Watermarks;
use super::*;

use crate::logs::{DataLog, TopicsLog};
use crate::waiters::{DataWaiters, TopicsWaiters};
use crate::{Config, DataRequest, Disconnection, ReplicationData};

#[derive(Error, Debug)]
#[error("...")]
pub enum RouterError {
    Recv(#[from] RecvError),
    Disconnected,
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

    /// Waits on incoming events when ready queue is empty.
    /// After pulling 1 event, tries to pull 500 more events
    /// before polling ready queue 100 times (connections)
    pub fn start(&mut self) -> Result<(), RouterError> {
        loop {
            // Block on incoming events if there are no connections
            // in ready queue.
            if self.readyqueue.is_empty() {
                let (id, data) = self.router_rx.recv()?;
                self.route(id, data);
            }

            // Try reading more from connections in a non-blocking
            // fashion to accumulate data and handle subscriptions.
            // Accumulating more data lets requests retrieve bigger
            // bulks which in turn increases efficiency
            for _ in 0..500 {
                // All these methods will handle state and errors
                match self.router_rx.try_recv() {
                    Ok((id, data)) => self.route(id, data),
                    Err(TryRecvError::Closed) => return Err(RouterError::Disconnected),
                    Err(TryRecvError::Empty) => break,
                }
            }

            // Poll 100 connections which are ready in ready queue
            for _ in 0..100 {
                match self.readyqueue.pop_front() {
                    Some(id) => self.connection_ready(id, 100),
                    None => break,
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
            Event::Ready => self.connection_ready(id, 100),
        }
    }

    fn handle_new_connection(&mut self, connection: Connection) {
        let id = match connection.conn.clone() {
            ConnectionType::Replicator(id) => {
                self.connections.insert_at(connection, id);
                self.trackers.insert_at(Tracker::new(), id);
                self.watermarks.insert_at(Watermarks::new(), id);
                self.readyqueue.push_back(id);
                id
            }
            ConnectionType::Device(did) => match self.connections.insert(connection) {
                Some(id) => {
                    self.trackers.insert(Tracker::new()).unwrap();
                    self.watermarks.insert(Watermarks::new()).unwrap();
                    self.readyqueue.push_back(id);
                    info!(
                        "{:11} {:14} Id = {}, Remote id = {}",
                        "connection", "new", id, did,
                    );

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
        info!(
            "{:11} {:14} Id = {}, Remote id = {}",
            "connection", "clean", id, disconnect.id,
        );

        self.connections.remove(id);
        self.trackers.remove(id);
        self.watermarks.remove(id);
        self.data_waiters.remove(id);
        self.topics_waiters.remove(id);
        self.readyqueue.remove(id);
    }

    fn connection_ready(&mut self, id: ConnectionId, max_iterations: usize) {
        trace!("{:11} {:14} Id = {}", "requests", "start", id,);
        let tracker = self.trackers.get_mut(id).unwrap();
        if tracker.busy_unschedule() {
            tracker.set_busy_unschedule(false);
        }

        // Iterate through a max of 'max_iterations' requests everytime a connection.
        // if polled. This prevents a connection from unfairly taking up router's time
        // preventing other connections from making progress.
        for _ in 0..max_iterations {
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
                            let pause = notify(&mut self.connections, id, notification);

                            // This connection might not be able to process next request. Don't schedule
                            if pause {
                                info!("Connection busy/closed. Unschedule. Id = {}", id);
                                tracker.set_busy_unschedule(true);
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
                        // Get acks from commitlog and register for notification if all
                        // the data is caught up.
                        let acks = self.watermarks.get_mut(id).unwrap();

                        // If acks are yielded, register a new acks request
                        // and send acks notification to the connection
                        if let Some(acks) = handle_acks_request(id, acks) {
                            tracker.register_acks_request();
                            let notification = Notification::Acks(acks);
                            let pause = notify(&mut self.connections, id, notification);

                            // This connection might not be able to process next request. Don't schedule
                            if pause {
                                info!("Connection busy/closed. Unschedule. Id = {}", id);
                                tracker.set_busy_unschedule(true);
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
                    trace!("{:11} {:14} Id = {}", "requests", "done", id);
                    tracker.set_empty_unschedule(true);
                    return;
                }
            }
        }

        // If there are more requests in the tracker, add the connection back
        // to ready queue.
        trace!("{:11} {:14} Id = {}", "requests", "pause", id,);
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

        for publish in data {
            match publish {
                Packet::Publish(publish) => self.handle_connection_publish(id, publish),
                Packet::Subscribe(subscribe) => self.handle_connection_subscribe(id, subscribe),
                incoming => {
                    warn!("Packet = {:?} not supported by router yet", incoming);
                }
            }
        }

        trace!("{:11} {:14} Id = {}", "data", "committed", id,);
    }

    fn handle_connection_subscribe(&mut self, id: ConnectionId, subscribe: Subscribe) {
        trace!("{:11} {:14} Id = {}", "subscribe", "", id);
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
                // in the (topics) commitlog ant seek them to next offset. Add subscriptions
                // and store matched topics interna. If this is the first subscription,
                // register topics request
                if tracker.add_subscription_and_match(subscribe.topics, topics) {
                    tracker.register_topics_request(topics.len());

                    // If connection is removed from ready queue because of 0 requests,
                    // but connection itself is ready for more notifications, add
                    // connection back to ready queue
                    if tracker.empty_unschedule() {
                        self.readyqueue.push_back(id);
                        tracker.set_empty_unschedule(false);
                    }
                }

                // Take matched topics above and seek their offsets. Seeking is
                // necessary because new subscription should yield only subsequent data
                // FIXME: Verify logic of self.id. Should this be seeked for replicators as well?
                while let Some(mut topic) = tracker.next_matched() {
                    self.datalog.seek_offsets_to_end(self.id, &mut topic);
                    tracker.register_data_request(topic.0, topic.2);

                    // If connection is removed from ready queue because of 0 requests,
                    // but connection itself is ready for more notifications, add
                    // connection back to ready queue
                    if tracker.empty_unschedule() {
                        self.readyqueue.push_back(id);
                        tracker.set_empty_unschedule(false);
                    }
                }
            }
            None => {
                // Router did not receive data from any topics yet. Add subscription and
                // register topics request from offset 0
                if tracker.add_subscription_and_match(subscribe.topics, &[]) {
                    tracker.register_topics_request(0);

                    // If connection is removed from ready queue because of 0 requests,
                    // but connection itself is ready for more notifications, add
                    // connection back to ready queue
                    if tracker.empty_unschedule() {
                        self.readyqueue.push_back(id);
                        tracker.set_empty_unschedule(false);
                    }
                }
            }
        };

        // Update acks and triggers acks notification for suback
        let watermarks = self.watermarks.get_mut(id).unwrap();
        watermarks.push_subscribe_ack(subscribe.pkid, return_codes);
        self.fresh_acks_notification(id);
    }

    fn handle_connection_publish(&mut self, id: ConnectionId, publish: Publish) {
        if publish.payload.is_empty() {
            error!("Empty publish. ID = {:?}, topic = {:?}", id, publish.topic);
            return;
        }

        let Publish {
            pkid,
            topic,
            payload,
            qos,
            ..
        } = publish;

        let (is_new_topic, (_segment, offset)) = match self.datalog.append(id, &topic, payload) {
            Some(v) => v,
            None => return,
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
        let waiters = &mut self.topics_waiters;

        while let Some((link_id, request)) = waiters.pop_front() {
            // Ignore connections with zero subscriptions.
            let tracker = self.trackers.get_mut(link_id).unwrap();
            if tracker.subscription_count() == 0 {
                // panic!("Topics request registration for 0 subscription connection");
                continue;
            }

            // Don't send replicated topic notifications to replication link. Replicator 1
            // should not track replicator 2's topics (This will lead to circular replication)
            // False notification. This connection should be notified in the next call of
            // this method. Push this request to next queue in waiter and swap at the end.
            // TODO: Split waiters into remote and replicator connections.
            // TODO: Ignore replicators for notifications from replicators
            if replication_data && link_id < 10 {
                waiters.push_back(link_id, request);
                continue;
            }

            trace!("{:11} {:14} Id = {}", "topics", "notification", link_id);

            // Match new topics with existing subscriptions of this link.
            // Even though there are new topics, it's possible that they didn't match
            // subscriptions held by this connection.
            // Register next topics request in the router
            tracker.register_topics_request(request.offset);

            // If connection is removed from ready queue because of 0 requests,
            // but connection itself is ready for more notifications, add
            // connection back to ready queue
            if tracker.empty_unschedule() {
                self.readyqueue.push_back(link_id);
                tracker.set_empty_unschedule(false);
            }
        }

        waiters.prepare_next();
    }

    /// Send data to links which registered them
    fn fresh_data_notification(&mut self, id: ConnectionId, topic: &str) {
        // There might not be any waiters on this topic
        // FIXME: Every notification trigger is a hashmap lookup
        let waiters = match self.data_waiters.get_mut(topic) {
            Some(waiters) => waiters,
            None => return,
        };

        let replication_data = id < 10;
        while let Some((link_id, request)) = waiters.pop_front() {
            let is_replicator = link_id < 10;

            // Ignore new data notification if the current notification awaiting
            // link is a replicator link and data is also from a replicator
            // False positive. This connection should be notified in the next call of
            // this method. Push this request to next queue in waiter and swap at the end.
            if is_replicator && replication_data {
                waiters.push_back(link_id, request);
                continue;
            }

            let tracker = self.trackers.get_mut(link_id).unwrap();
            tracker.register_data_request(request.topic, request.cursors);

            // If connection is removed from ready queue because of 0 requests,
            // but connection itself is ready for more notifications, add
            // connection back to ready queue
            if tracker.empty_unschedule() {
                self.readyqueue.push_back(link_id);
                tracker.set_empty_unschedule(false);
            }
        }

        waiters.prepare_next();
    }

    fn fresh_acks_notification(&mut self, id: ConnectionId) {
        let watermarks = self.watermarks.get_mut(id).unwrap();

        // Unlike data and topics where notifications are routed
        // to other connections, acks are meant for the same connection.
        // We use watermark's own flag to determine if it's waiting for
        // a notification
        if watermarks.take_pending_acks_request().is_some() {
            trace!("{:11} {:14} Id = {}", "acks", "notification", id);

            // Add next acks request to the tracker
            let tracker = self.trackers.get_mut(id).unwrap();
            tracker.register_acks_request();

            // If connection is removed from ready queue because of 0 requests,
            // but connection itself is ready for more notifications, add
            // connection back to ready queue
            if tracker.empty_unschedule() {
                self.readyqueue.push_back(id);
                tracker.set_empty_unschedule(false);
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
        "{:11} {:14} Id = {} Topic = {} Offsets = {:?}",
        "data",
        "request",
        id,
        request.topic,
        request.cursors
    );

    let data = match datalog.handle_data_request(id, &request) {
        Some(data) => {
            trace!(
                "{:11} {:14} Id = {} Topic = {} Offsets = {:?} Count = {}",
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
                "{:11} {:14} Id = {} Topic = {}",
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
        "{:11} {:14} Id = {} Offset = {}",
        "topics",
        "request",
        id,
        request.offset
    );

    let topics = match topicslog.readv(request.offset, request.count) {
        Some(topics) => {
            trace!(
                "{:11} {:14} Id = {} Offset = {} Count = {}",
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
            waiters.register(id, request);
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

/// Notifies and returns unschedule status if the connection is busy
fn notify(connections: &mut Slab<Connection>, id: ConnectionId, reply: Notification) -> bool {
    let connection = match connections.get_mut(id) {
        Some(c) => c,
        None => {
            error!("Invalid id while notifying = {:?}", id);
            return true;
        }
    };

    connection.notify(reply)
}

#[cfg(test)]
mod test {
    use super::*;
    use mqtt4bytes::{Publish, QoS, Subscribe};

    #[test]
    fn topics_notifications_does_not_create_infinite_loops() {
        let (mut router, _tx) = Router::new(Arc::new(Config::default()));

        // Instantiate replica connections with subscriptions and topic request notifications
        for i in 0..10 {
            add_new_replica_connection(&mut router, i);
            add_new_subscription(&mut router, i, "hello/world");
            router.topics_waiters.register(i, TopicsRequest::new());
        }

        // Instantiate replica connections with subscriptions and topic request notifications
        for i in 10..20 {
            let client_id = &format!("{}", i);
            add_new_remote_connection(&mut router, client_id);
            add_new_subscription(&mut router, i, "hello/world");
            router.topics_waiters.register(i, TopicsRequest::new());
        }

        router.topicslog.append("hello/world");

        // A new topic notification from a replicator. This is a false positive
        // for all the replicators
        router.fresh_topics_notification(1);

        // Next iteration of `fresh_topics_notification` will have all missed notifications
        for i in 0..10 {
            let request = router.topics_waiters.pop_front().unwrap();
            assert_eq!(i, request.0);
        }
    }

    #[test]
    fn data_notifications_does_not_create_infinite_loops() {
        let (mut router, _tx) = Router::new(Arc::new(Config::default()));

        // Instantiate replica connections with subscriptions and
        // data request notifications
        for i in 0..10 {
            add_new_replica_connection(&mut router, i);
            add_new_subscription(&mut router, i, "hello/world");

            let request = DataRequest::new("hello/world".to_owned());
            router.data_waiters.register(i, request);
        }

        // Instantiate replica connections with subscriptions and topic request notifications
        for i in 10..20 {
            let client_id = &format!("{}", i);
            add_new_remote_connection(&mut router, client_id);
            add_new_subscription(&mut router, i, "hello/world");
            router.topics_waiters.register(i, TopicsRequest::new());
        }

        let payload = Bytes::from(vec![1, 2, 3]);
        router.datalog.append(1, "hello/world", payload);

        // A new topic notification from a replicator. This is a false positive
        // for all the replicators
        router.fresh_data_notification(1, "hello/world");

        // Next iteration of `fresh_topics_notification` will have all missed notifications
        let waiters = router.data_waiters.get_mut("hello/world").unwrap();
        for i in 0..10 {
            let request = waiters.pop_front().unwrap();
            assert_eq!(i, request.0);
        }
    }

    /// Connection should not be part of next iteration of ready queue
    /// when previous notification fails
    #[test]
    fn failed_notification_connection_should_not_be_scheduled() {
        let mut config = Config::default();
        config.id = 0;

        let (mut router, _tx) = Router::new(Arc::new(config));
        let _rx = add_new_remote_connection(&mut router, "10");

        // Register 20 data requests in notifications
        for i in 0..20 {
            let topic = format!("hello/{}/world", i);
            let request = DataRequest::new(topic);
            router.data_waiters.register(10, request);
        }

        // Along with connection ack. 10th notification will fail
        for i in 1..=10 {
            // Write a publish to commitlog
            let publish = Publish::new("hello/world", QoS::AtLeastOnce, vec![1, 2, 3]);
            router.handle_connection_data(10, vec![Packet::Publish(publish)]);

            // connack + 8 pub acks. 9 acks before this iteration
            // implies 9 notifications + 1 pause notification = 10 notifications
            // channel full before this iteration
            if i == 9 {
                assert!(router.readyqueue.pop_front().is_none());
                break;
            } else {
                // Trigger 1 request. This will notify puback for above
                // data and puts connection back in ready queue.
                let id = router.readyqueue.pop_front().unwrap();
                router.connection_ready(id, 1);
            }
        }

        assert!(router.readyqueue.is_empty());
    }

    fn add_new_replica_connection(router: &mut Router, id: usize) {
        let (connection, _rx) = Connection::new_replica(id, 10);
        router.handle_new_connection(connection);
    }

    fn add_new_remote_connection(router: &mut Router, client_id: &str) -> Receiver<Notification> {
        let (connection, rx) = Connection::new_remote(client_id, 10);
        router.handle_new_connection(connection);
        rx
    }

    fn add_new_subscription(router: &mut Router, id: usize, topic: &str) {
        router.handle_connection_subscribe(id, Subscribe::new(topic, QoS::AtLeastOnce));
    }
}

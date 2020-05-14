use std::collections::HashMap;
use std::{io, mem};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use super::commitlog::CommitLog;
use super::{Connection, RouterInMessage, RouterOutMessage};
use crate::router::commitlog::TopicLog;
use crate::router::{DataReply, DataRequest, TopicsReply, TopicsRequest};
use crate::Config;
use mqtt4bytes::{Packet, Publish};
use thiserror::Error;
use tokio::sync::mpsc::error::TrySendError;
use super::bytes::Bytes;

#[derive(Error, Debug)]
#[error("...")]
pub enum Error {
    Mpsc(#[from] TrySendError<RouterOutMessage>),
    Io(#[from] io::Error),
}

pub struct Router {
    config: Config,
    /// Commit log by topic. Commit log stores all the of given topic. The
    /// details are very similar to what kafka does. Who know, we might
    /// even make the broker kafka compatible and directly feed it to databases
    commitlog: CommitLog,
    /// Messages directly received from conecitons should be separed from messages
    /// received by the router replication to make sure that linker of the current
    /// instance doesn't pull these again
    replicatedlog: CommitLog,
    /// Captures new topic just like commitlog
    topiclog: TopicLog,
    /// Client id -> Active connection map. Used to route data
    connections: HashMap<String, Connection>,
    /// Waiter on a topic. These are used to wake connections/replicators
    /// which are caught up all the data on a topic. Map[topic]List[Connections Ids]
    data_waiters: HashMap<String, Vec<(String, DataRequest)>>,
    /// Waiters on new topics
    topics_waiters: Vec<(String, TopicsRequest)>,
    /// Channel receiver to receive data from all the active connections and
    /// replicators. Each connection will have a tx handle which they use
    /// to send data and requests to router
    router_rx: Receiver<(String, RouterInMessage)>,
}

/// Router is the central node where most of the state is held. Connections and
/// replicators ask router for data and router responds by putting data into
/// relevant connection handle
impl Router {
    pub fn new(config: Config) -> (Self, Sender<(String, RouterInMessage)>) {
        let (router_tx, router_rx) = channel(1000);
        let commitlog = CommitLog::new(config.clone());
        let replicatedlog = CommitLog::new(config.clone());
        let topiclog = TopicLog::new();

        let router = Router {
            config,
            commitlog,
            replicatedlog,
            topiclog,
            connections: HashMap::new(),
            data_waiters: HashMap::new(),
            topics_waiters: Vec::new(),
            router_rx,
        };

        (router, router_tx)
    }

    pub async fn start(&mut self) {
        // All these methods will handle state and errors
        while let Some((id, data)) = self.router_rx.recv().await {
            match data {
                RouterInMessage::Connect(connection) => self.handle_new_connection(connection),
                RouterInMessage::Packet(packet) => self.handle_incoming_packet(&id, packet),
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
                        self.register_data_waiter(&id, request, &reply);
                    }
                    self.reply_data(&id, reply);
                }
                RouterInMessage::TopicsRequest(request) => {
                    let reply = self.extract_topics(&request);
                    // register this id to wake up when there are new topics.
                    // don't send a reply of empty topics
                    if reply.topics.is_empty() {
                        self.register_topics_waiter(&id, request);
                        continue;
                    }
                    self.reply_topics(&id, reply);
                },
            }
        }

        error!("Router stopped!!");
    }

    fn handle_new_connection(&mut self, connection: Connection) {
        let id = connection.connect.client_id.clone();
        info!("Connect. Id = {:?}", id);
        self.connections.insert(id.clone(), connection);
    }

    /// Handles
    fn handle_incoming_packet(&mut self, id: &str, packet: Packet) {
        match packet {
            Packet::Publish(publish) => {
                let Publish {
                    pkid,
                    topic,
                    bytes,
                    ..
                } = publish;

                if bytes.len() == 0 {
                    error!("Empty publish. Ignoring");
                    return
                }

                self.append_to_commitlog(id, &topic, pkid, bytes);

                // If there is a new unique append, send it to connection/linker waiting
                // on it. This is equivalent to hybrid of block and poll and we don't need
                // timers. Connections/Replicator will make a request and request fails as
                // there is no new data. Router caches the failed request on the topic.
                // If there is new data on this topic, router fulfills the last failed request.
                // This completely eliminates the need of polling
                if self.topiclog.unique_append(&topic) {
                    self.fresh_topics_notification(&id);
                }

                self.fresh_data_notification(&id, &topic);
            }
            _ => todo!(),
        }
    }

    /// Send notifications to links which registered them
    fn fresh_topics_notification(&mut self, id: &str) {
        // TODO too many indirection to get a link to the handle, probably directly
        // TODO clone a handle to the link and save it instead of saving ids?
        let waiters = mem::replace(&mut self.topics_waiters, Vec::new());
        for (link_id, request) in waiters {
            // don't send replicated topic notifications to linker
            let replication_data = id.starts_with("router-");
            if replication_data {
                continue;
            }

            // Send reply to the link which registered this notification
            let reply = self.extract_topics(&request);
            self.reply_topics(&link_id, reply);

            // NOTE:
            // ----------------------
            // When the reply is empty don't register notification on behalf of the link.
            // This will cause the channel to be full. Register the notification only when
            // link has made the request and reply doesn't contain any data
        }
    }

    /// Send data to links which registered them
    fn fresh_data_notification(&mut self, id: &str, topic: &str) {
        let waiters = match self.data_waiters.remove(topic) {
            Some(w) => w,
            None => return
        };

        for (link_id, request) in waiters {
            let replication_data = id.starts_with("router-");
            // don't send replicated data notifications to linker
            if replication_data {
                continue;
            }

            // Send reply to the link which registered this notification
            if let Some(reply) = self.extract_data(&request) {
                self.reply_data(&link_id, reply);
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
    fn append_to_commitlog(&mut self, id: &str, topic: &str, pkid: u16, bytes: Bytes) {
        let replication_data = id.starts_with("router-");

        if replication_data {
            debug!("Receiving data from {}, topic = {}", id, topic);
            if let Err(e) = self.replicatedlog.append(&topic, pkid, bytes) {
                error!("Commitlog append failed. Error = {:?}", e);
            }

            // don't trigger pending requests for replicate
        } else {
            if let Err(e) = self.commitlog.append(&topic, pkid, bytes) {
                error!("Commitlog append failed. Error = {:?}", e);
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
    fn reply_topics(&mut self, id: &str, reply: TopicsReply) {
        let connection = self.connections.get_mut(id).unwrap();
        let reply = RouterOutMessage::TopicsReply(reply);
        if let Err(e) = connection.handle.try_send(reply) {
            error!("Failed to topics refresh reply. Error = {:?}", e);
        }
    }

    fn register_topics_waiter(&mut self, id: &str, request: TopicsRequest) {
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
            count: o.4.len() as u64,
        };

        Some(reply)
    }

    /// Sends data to the link
    fn reply_data(&mut self, id: &str, reply: DataReply) {
        debug!(
            "Data reply.   Topic = {}, Segment = {}, offset = {}, size = {}",
            reply.topic,
            reply.segment,
            reply.offset,
            reply.payload.len(),
        );

        let connection = self.connections.get_mut(id).unwrap();
        let reply = RouterOutMessage::DataReply(reply);
        if let Err(e) = connection.handle.try_send(reply) {
            error!("Failed to data reply. Error = {:?}", e.to_string());
        }
    }

    /// Register data waiter
    fn register_data_waiter(&mut self, id: &str, mut request: DataRequest, reply: &DataReply) {
        request.segment = reply.segment;
        request.offset = reply.offset + 1;
        let request = (id.to_owned(), request);
        if let Some(waiters) = self.data_waiters.get_mut(&reply.topic) {
            waiters.push(request);
        } else {
            let mut waiters = Vec::new();
            waiters.push(request);
            self.data_waiters.insert(reply.topic.to_owned(), waiters);
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn new_data_on_existing_topic_should_to_reply_to_waiting_links() {}

    #[test]
    fn new_topic_should_to_reply_to_waiting_links() {}

    #[test]
    fn caught_up_topic_should_not_stop_other_topics_from_pollin() {}

    #[test]
    fn new_replicated_data_and_topics_should_not_notfiy_replicator() {}
}

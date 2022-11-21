use std::{
    collections::{HashMap, VecDeque},
    fmt,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    protocol::{
        ConnAck, PingResp, PubAck, PubAckProperties, PubComp, PubCompProperties, PubRec,
        PubRecProperties, PubRel, PubRelProperties, Publish, PublishProperties, SubAck,
        SubAckProperties, UnsubAck,
    },
    ConnectionId, Filter, RouterConfig, RouterId,
};

mod connection;
mod graveyard;
pub mod iobufs;
mod logs;
mod routing;
mod scheduler;
mod waiters;

pub use connection::Connection;
pub use routing::Router;
pub use waiters::Waiters;

use self::scheduler::Tracker;
pub const MAX_SCHEDULE_ITERATIONS: usize = 100;
pub const MAX_CHANNEL_CAPACITY: usize = 200;

pub(crate) type FilterIdx = usize;

#[derive(Debug)]
// TODO: Fix this
#[allow(clippy::large_enum_variant)]
pub enum Event {
    /// Client id and connection handle
    Connect {
        connection: connection::Connection,
        incoming: iobufs::Incoming,
        outgoing: iobufs::Outgoing,
    },
    /// New meter link
    NewMeter(flume::Sender<(ConnectionId, Meter)>),
    /// Request for meter
    GetMeter(GetMeter),
    /// Connection ready to receive more data
    Ready,
    /// Data for native commitlog
    DeviceData,
    /// Disconnection request
    Disconnect(Disconnection),
    /// Shadow
    Shadow(ShadowRequest),
    /// Get metrics of a connection or all connections
    Metrics(MetricsRequest),
}

/// Notification from router to connection
#[derive(Debug, Clone)]
pub enum Notification {
    /// Data reply
    Forward(Forward),
    /// Data reply
    ForwardWithProperties(Forward, PublishProperties),
    /// Acks reply for connection data
    DeviceAck(Ack),
    /// Data reply
    ReplicaData {
        cursor: (u64, u64),
        size: usize,
        payload: Bytes,
    },
    /// Acks reply for replication data
    ReplicaAcks {
        offset: (u64, u64),
        payload: Bytes,
    },
    /// All metrics
    Metrics(MetricsReply),
    /// Shadow
    Shadow(ShadowReply),
    Unschedule,
}

#[derive(Debug, Clone)]
pub struct Forward {
    pub cursor: (u64, u64),
    pub size: usize,
    pub publish: Publish,
}

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum Ack {
    ConnAck(ConnectionId, ConnAck),
    PubAck(PubAck),
    PubAckWithProperties(PubAck, PubAckProperties),
    SubAck(SubAck),
    SubAckWithProperties(SubAck, SubAckProperties),
    PubRec(PubRec),
    PubRecWithProperties(PubRec, PubRecProperties),
    PubRel(PubRel),
    PubRelWithProperties(PubRel, PubRelProperties),
    PubComp(PubComp),
    PubCompWithProperties(PubComp, PubCompProperties),
    UnsubAck(UnsubAck),
    PingResp(PingResp),
}

fn packetid(ack: &Ack) -> u16 {
    match ack {
        Ack::ConnAck(..) => 0,
        Ack::PubAck(puback) => puback.pkid,
        Ack::PubAckWithProperties(puback, _) => puback.pkid,
        Ack::SubAck(suback) => suback.pkid,
        Ack::SubAckWithProperties(suback, _) => suback.pkid,
        Ack::PubRel(pubrel) => pubrel.pkid,
        Ack::PubRelWithProperties(pubrel, _) => pubrel.pkid,
        Ack::PubRec(pubrec) => pubrec.pkid,
        Ack::PubRecWithProperties(pubrec, _) => pubrec.pkid,
        Ack::PubComp(pubcomp) => pubcomp.pkid,
        Ack::PubCompWithProperties(pubcomp, _) => pubcomp.pkid,
        Ack::UnsubAck(unsuback) => unsuback.pkid,
        Ack::PingResp(_) => 0,
    }
}

/// Request that connection/linker makes to extract data from commitlog
/// NOTE Connection can make one sweep request to get data from multiple topics
/// but we'll keep it simple for now as multiple requests in one message can
/// makes constant extraction size harder
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataRequest {
    /// Commitlog this request is pulling data from
    pub filter: Filter,
    pub filter_idx: FilterIdx,
    /// Qos of the outgoing data
    pub qos: u8,
    /// (segment, offset) tuples per replica (1 native and 2 replicas)
    pub cursor: (u64, u64),
    /// number of messages read from subscription
    pub read_count: usize,
    /// Maximum count of payload buffer per replica
    max_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcksRequest;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
    Data(DataRequest),
    Ack(AcksRequest),
}

/// A single message from connection to router
pub struct Message {
    /// Log to sweep
    pub topic: String,
    /// Qos of the topic
    pub qos: u8,
    /// Reply data chain
    pub payload: Bytes,
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Topic = {:?}, Payload size = {}",
            self.topic,
            self.payload.len()
        )
    }
}

/// A batch of messages from connection to router
pub struct Data {
    /// (segment, offset) tuples per replica (1 native and 2 replicas)
    pub offset: (u64, u64),
    /// Payload size
    pub size: usize,
    /// Reply data chain
    pub payload: Vec<Publish>,
}

impl fmt::Debug for Data {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Cursors = {:?}, Payload size = {}, Payload count = {}",
            self.offset,
            self.size,
            self.payload.len()
        )
    }
}

#[derive(Debug, Clone)]
pub struct Disconnection {
    pub id: String,
    pub execute_will: bool,
    pub pending: Vec<Notification>,
}

#[derive(Debug, Clone)]
pub struct ShadowRequest {
    pub filter: String,
}

#[derive(Debug, Clone)]
pub struct ShadowReply {
    pub topic: Bytes,
    pub payload: Bytes,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RouterMeter {
    pub router_id: RouterId,
    pub total_connections: usize,
    pub total_subscriptions: usize,
    pub total_publishes: usize,
    pub failed_publishes: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SubscriptionMeter {
    pub count: usize,
    pub total_size: usize,
    pub head_and_tail_id: (u64, u64),
    pub append_offset: (u64, u64),
    pub read_offset: usize,
}

#[derive(Debug, Default, Clone)]
pub struct IncomingMeter {
    pub publish_count: usize,
    pub subscribe_count: usize,
    pub total_size: usize,
}

#[derive(Debug, Default, Clone)]
pub struct OutgoingMeter {
    pub publish_count: usize,
    pub total_size: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConnectionEvents {
    events: VecDeque<String>,
}

#[derive(Debug, Clone)]
pub enum GetMeter {
    Router,
    Connection(String),
    Subscription(String),
}

#[derive(Debug, Clone)]
pub enum Meter {
    Router(usize, RouterMeter),
    Connection(String, Option<IncomingMeter>, Option<OutgoingMeter>),
    Subscription(String, Option<SubscriptionMeter>),
}

#[derive(Debug, Clone)]
pub enum MetricsRequest {
    Config,
    Router,
    ReadyQueue,
    Connection(String),
    Subscriptions,
    Subscription(Filter),
    Waiters(Filter),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricsReply {
    Config(RouterConfig),
    Router(RouterMeter),
    Connection(Option<(ConnectionEvents, Tracker)>),
    Subscriptions(HashMap<Filter, Vec<String>>),
    Subscription(Option<SubscriptionMeter>),
    Waiters(Option<VecDeque<(String, DataRequest)>>),
    ReadyQueue(VecDeque<ConnectionId>),
}

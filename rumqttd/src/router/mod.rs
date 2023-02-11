use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    protocol::{
        ConnAck, Packet, PingResp, PubAck, PubAckProperties, PubComp, PubCompProperties, PubRec,
        PubRecProperties, PubRel, PubRelProperties, Publish, PublishProperties, SubAck,
        SubAckProperties, UnsubAck,
    },
    ConnectionId, Filter, RouterConfig, RouterId, Topic,
};

mod alertlog;
mod connection;
mod graveyard;
pub mod iobufs;
mod logs;
mod routing;
mod scheduler;
mod waiters;

pub use alertlog::{Alert, AlertError, AlertEvent};
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
    NewMeter(flume::Sender<Vec<Meter>>),
    /// Request for meter
    GetMeter(GetMeter),
    /// New Alert link
    NewAlert(flume::Sender<(ConnectionId, Alert)>, Vec<Filter>),
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

type MaybePacket = Option<Packet>;

// We either get a Packet to write to buffer or we unschedule which is represented as `None`
impl From<Notification> for MaybePacket {
    fn from(notification: Notification) -> Self {
        let packet: Packet = match notification {
            Notification::Forward(forward) => Packet::Publish(forward.publish, None),
            Notification::DeviceAck(ack) => ack.into(),
            Notification::Unschedule => return None,
            v => {
                tracing::error!("Unexpected notification here, it cannot be converted into Packet, Notification: {:?}", v);
                return None;
            }
        };
        Some(packet)
    }
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

impl From<Ack> for Packet {
    fn from(value: Ack) -> Self {
        match value {
            Ack::ConnAck(_id, connack) => Packet::ConnAck(connack, None),
            Ack::PubAck(puback) => Packet::PubAck(puback, None),
            Ack::PubAckWithProperties(puback, prop) => Packet::PubAck(puback, Some(prop)),
            Ack::SubAck(suback) => Packet::SubAck(suback, None),
            Ack::SubAckWithProperties(suback, prop) => Packet::SubAck(suback, Some(prop)),
            Ack::PubRec(pubrec) => Packet::PubRec(pubrec, None),
            Ack::PubRecWithProperties(pubrec, prop) => Packet::PubRec(pubrec, Some(prop)),
            Ack::PubRel(pubrel) => Packet::PubRel(pubrel, None),
            Ack::PubRelWithProperties(pubrel, prop) => Packet::PubRel(pubrel, Some(prop)),
            Ack::PubComp(pubcomp) => Packet::PubComp(pubcomp, None),
            Ack::PubCompWithProperties(pubcomp, prop) => Packet::PubComp(pubcomp, Some(prop)),
            Ack::UnsubAck(unsuback) => Packet::UnsubAck(unsuback, None),
            Ack::PingResp(pingresp) => Packet::PingResp(pingresp),
        }
    }
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
struct Delta<T>
where
    T: std::ops::Sub<Output = T> + Default,
{
    last_read: T,
    curr: T,
}

impl<T> Delta<T>
where
    T: std::ops::Sub<Output = T> + Copy + Default,
{
    pub fn from(data: T) -> Self {
        Self {
            last_read: data,
            curr: data,
        }
    }

    pub fn set(&mut self) -> &mut T {
        &mut self.curr
    }

    pub fn get(&mut self) -> T {
        let diff = self.curr - self.last_read;
        self.last_read = self.curr;

        diff
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct RouterMeter {
    pub router_id: RouterId,
    pub total_connections: isize,
    pub total_subscriptions: usize,
    pub total_publishes: usize,
    pub failed_publishes: usize,
}

impl std::ops::Sub for RouterMeter {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        RouterMeter {
            router_id: self.router_id,
            total_connections: self.total_connections - rhs.total_connections,
            total_subscriptions: self.total_subscriptions - rhs.total_subscriptions,
            total_publishes: self.total_publishes - rhs.total_publishes,
            failed_publishes: self.failed_publishes - rhs.failed_publishes,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct SubscriptionMeter {
    pub count: usize,
    pub total_size: usize,
    pub head_and_tail_id: (u64, u64),
    pub append_offset: (u64, u64),
    pub read_offset: usize,
}

impl std::ops::Sub for SubscriptionMeter {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        SubscriptionMeter {
            count: self.count - rhs.count,
            total_size: self.total_size - rhs.total_size,
            head_and_tail_id: self.head_and_tail_id,
            append_offset: self.append_offset,
            read_offset: self.read_offset,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct MeterData {
    pub count: usize,
    pub size: usize,
}

#[derive(Debug, Default, Clone)]
pub struct IncomingMeter {
    publishes: HashMap<Topic, MeterData>,
    subscribes: HashSet<Filter>,
    total_publishes: MeterData,
}

impl IncomingMeter {
    pub fn register_publish(&mut self, publish: &Publish) -> Result<(), std::str::Utf8Error> {
        let meter = {
            let topic = std::str::from_utf8(&publish.topic)?.to_string();
            self.publishes.entry(topic).or_default()
        };
        meter.count += 1;
        meter.size += publish.len();

        self.total_publishes.count += 1;
        self.total_publishes.size += publish.len();

        Ok(())
    }

    pub fn get_topic_meters(&self) -> &HashMap<Topic, MeterData> {
        &self.publishes
    }

    pub fn register_subscription(&mut self, filter: Filter) -> bool {
        self.subscribes.insert(filter)
    }

    pub fn unregister_subscription(&mut self, filter: &Filter) -> bool {
        self.subscribes.remove(filter)
    }

    pub fn get_total_count(&self) -> usize {
        self.total_publishes.count
    }

    pub fn get_total_size(&self) -> usize {
        self.total_publishes.size
    }
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
    // Disable Connection meter temporarily.
    // MeterLink is used in a pull based data flow.
    // In a typical setup with many connections to
    // router, such data flow puts a lot of load on
    // the consumer of Connection meter.
    //
    // Connection meter can enabled when
    // - Connection data can be aggregated in a way
    //   that does not put load on consumer of
    //   connection meter
    // - MetersLink allows to consume data in a push
    //   based data flow (like .poll() interface
    //   instead of request-response)
    //
    // Connection(Option<String>),

    // Note: Associated data of None<String> type
    // means get all meters
    Subscription(Option<String>),
}

#[derive(Debug, Clone)]
pub enum Meter {
    Router(usize, RouterMeter),
    // Connection(String, Option<IncomingMeter>, Option<OutgoingMeter>),
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

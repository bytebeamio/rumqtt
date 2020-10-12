extern crate bytes;

pub(crate) mod connection;
mod readyqueue;
mod router;
mod slab;
mod tracker;
mod watermarks;

use connection::Connection;
pub use router::Router;
use tracker::Tracker;

use self::bytes::Bytes;
use mqtt4bytes::Packet;
use std::fmt;

/// Messages from connection to router
#[derive(Debug)]
pub enum Event {
    /// Client id and connection handle
    Connect(Connection),
    /// Connection ready to receive more data
    Ready,
    /// Data for native commitlog
    Data(Vec<Packet>),
    /// Data for commitlog of a replica
    ReplicationData(Vec<ReplicationData>),
    /// Replication acks
    ReplicationAcks(Vec<ReplicationAck>),
    /// Disconnection request
    Disconnect(Disconnection),
}

/// Requests for pull operations
#[derive(Debug)]
pub enum Request {
    /// Data request
    Data(DataRequest),
    /// Topics request
    Topics(TopicsRequest),
    /// Acks request
    Acks(AcksRequest),
}

/// Notification from router to connection
#[derive(Debug)]
pub enum Notification {
    /// Connection reply
    ConnectionAck(ConnectionAck),
    /// Data reply
    Data(Data),
    /// Watermarks reply
    Acks(Acks),
    /// Connection paused by router
    Pause,
}

/// Data sent router to be written to commitlog
#[derive(Debug)]
pub struct ReplicationData {
    /// Id of the packet that connection received
    pkid: u16,
    /// Topic of publish
    topic: String,
    /// Publish payload
    payload: Vec<Bytes>,
}

impl ReplicationData {
    pub fn with_capacity(pkid: u16, topic: String, cap: usize) -> ReplicationData {
        ReplicationData {
            pkid,
            topic,
            payload: Vec::with_capacity(cap),
        }
    }

    pub fn push(&mut self, payload: Bytes) {
        self.payload.push(payload)
    }
}

/// Replication connection frames this after receiving an ack from other replica
/// This is used by router to update watermarks, which is used to send acks
/// for replicated data
#[derive(Debug)]
pub struct ReplicationAck {
    topic: String,
    /// Packet id that router assigned
    offset: u64,
}

impl ReplicationAck {
    pub fn new(topic: String, offset: u64) -> ReplicationAck {
        ReplicationAck { topic, offset }
    }
}

/// Request that connection/linker makes to extract data from commitlog
/// NOTE Connection can make one sweep request to get data from multiple topics
/// but we'll keep it simple for now as multiple requests in one message can
/// makes constant extraction size harder
#[derive(Clone)]
pub struct DataRequest {
    /// Log to sweep
    pub(crate) topic: String,
    /// (segment, offset) tuples per replica (1 native and 2 replicas)
    pub(crate) cursors: [(u64, u64); 3],
    /// Maximum count of payload buffer per replica
    max_count: usize,
}

impl DataRequest {
    /// New data request with offsets starting from 0
    pub fn new(topic: String) -> DataRequest {
        DataRequest {
            topic,
            cursors: [(0, 0); 3],
            max_count: 100,
        }
    }

    pub fn with(topic: String, max_count: usize) -> DataRequest {
        DataRequest {
            topic,
            cursors: [(0, 0); 3],
            max_count,
        }
    }

    /// New data request with provided offsets
    pub fn offsets(topic: String, cursors: [(u64, u64); 3]) -> DataRequest {
        DataRequest {
            topic,
            cursors,
            max_count: 100,
        }
    }

    /// New data request with provided offsets
    pub fn offsets_with(topic: String, cursors: [(u64, u64); 3], max_count: usize) -> DataRequest {
        DataRequest {
            topic,
            cursors,
            max_count,
        }
    }
}

impl fmt::Debug for DataRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Topic = {}, cursors = {:?}, max_count = {}",
            self.topic, self.cursors, self.max_count
        )
    }
}

pub struct Data {
    /// Log to sweep
    pub topic: String,
    /// (segment, offset) tuples per replica (1 native and 2 replicas)
    pub cursors: [(u64, u64); 3],
    /// Payload size
    pub size: usize,
    /// Reply data chain
    pub payload: Vec<Bytes>,
}

impl Data {
    pub fn new(topic: String, cursors: [(u64, u64); 3], size: usize, payload: Vec<Bytes>) -> Data {
        Data {
            topic,
            cursors,
            size,
            payload,
        }
    }
}

impl fmt::Debug for Data {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Topic = {:?}, Cursors = {:?}, Payload size = {}, Payload count = {}",
            self.topic,
            self.cursors,
            self.size,
            self.payload.len()
        )
    }
}

#[derive(Debug, Clone)]
pub struct TopicsRequest {
    /// Start from this offset
    offset: usize,
    /// Maximum number of topics to read
    count: usize,
}

impl TopicsRequest {
    pub fn new() -> TopicsRequest {
        TopicsRequest {
            offset: 0,
            count: 10,
        }
    }

    pub fn set_count(&mut self, count: usize) {
        self.count = count;
    }

    pub fn offset(offset: usize) -> TopicsRequest {
        TopicsRequest { offset, count: 10 }
    }
}

pub struct Topics<'a> {
    offset: usize,
    topics: &'a [String],
}

impl<'a> Topics<'a> {
    pub fn new(offset: usize, topics: &'a [String]) -> Topics {
        Topics { offset, topics }
    }
}

#[derive(Clone, Debug)]
pub struct AcksRequest;

impl AcksRequest {
    pub fn new() -> AcksRequest {
        AcksRequest
    }
}

#[derive(Debug)]
pub struct Acks {
    /// packet ids that can be acked
    pub acks: Vec<(u16, Packet)>,
}

impl Acks {
    pub fn new(acks: Vec<(u16, Packet)>) -> Acks {
        Acks { acks }
    }

    pub fn len(&self) -> usize {
        self.acks.len()
    }
}

#[derive(Debug)]
pub enum ConnectionAck {
    /// Id assigned by the router for this connectiobackn
    Success(usize),
    /// Failure and reason for failure string
    Failure(String),
}

#[derive(Debug)]
pub struct Disconnection {
    id: String,
}

impl Disconnection {
    pub fn new(id: String) -> Disconnection {
        Disconnection { id }
    }
}

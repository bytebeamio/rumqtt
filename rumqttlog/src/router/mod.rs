extern crate bytes;

mod commitlog;
mod router;

pub use router::Router;

use self::bytes::Bytes;
use async_channel::{Sender, bounded, Receiver};
use rumqttc::Publish;
use std::fmt;
use std::collections::VecDeque;

/// Messages going into router
#[derive(Debug)]
pub enum RouterInMessage {
    /// Client id and connection handle
    Connect(Connection),
    /// Data for native commitlog
    ConnectionData(Vec<Publish>),
    /// Data for commitlog of a replica
    ReplicationData(Vec<ReplicationData>),
    /// Data request
    DataRequest(DataRequest),
    /// Topics request
    TopicsRequest(TopicsRequest),
    /// Watermarks request
    AcksRequest(AcksRequest),
    /// Disconnection request
    Disconnect(Disconnection)
}

/// Messages coming from router
#[derive(Debug)]
pub enum RouterOutMessage {
    /// Connection reply
    ConnectionAck(ConnectionAck),
    /// Data reply
    DataReply(DataReply),
    /// Topics reply
    TopicsReply(TopicsReply),
    /// Watermarks reply
    AcksReply(AcksReply),
}

/// Data sent router to be written to commitlog
#[derive(Debug)]
pub struct ReplicationData {
    /// Id of the packet that connection received
    pub pkid: u16,
    /// Topic of publish
    pub topic: String,
    /// Publish payload
    pub payload: Vec<Bytes>,
}

impl ReplicationData {
    pub fn with_capacity(pkid: u16, topic: String, cap: usize) -> ReplicationData {
        ReplicationData {
            pkid,
            topic,
            payload: Vec::with_capacity(cap)
        }
    }

    pub fn push(&mut self, payload: Bytes) {
        self.payload.push(payload)
    }
}

/// Acknowledgement after data is written to commitlog
/// Router sends this to connection for connection to maintain
/// mapping between packet id and router assigned id
#[derive(Debug)]
pub struct DataAck {
    /// Packet id that connection received
    pub pkid: u64,
    /// Packet id that router assigned
    pub offset: u64,
}

/// Request that connection/linker makes to extract data from commitlog
/// NOTE Connection can make one sweep request to get data from multiple topics
/// but we'll keep it simple for now as multiple requests in one message can
/// makes constant extraction size harder
#[derive(Debug, Clone)]
pub struct DataRequest {
    /// Log to sweep
    pub topic: String,
    /// (segment, offset) tuples per replica (1 native and 2 replicas)
    pub cursors: [(u64, u64); 3],
    /// Maximum size of payload buffer per replica
    pub max_size: usize,
    /// Maximum count of payload buffer per replica
    pub max_count: usize
}

impl DataRequest {
    /// New data request with offsets starting from 0
    pub fn new(topic: String) -> DataRequest {
        DataRequest {
            topic,
            cursors: [(0, 0); 3],
            max_size: 100 * 1024,
            max_count: 100
        }
    }

    pub fn with(topic: String, max_size: usize, max_count: usize) -> DataRequest {
        DataRequest {
            topic,
            cursors: [(0, 0); 3],
            max_size,
            max_count
        }
    }

    /// New data request with provided offsets
    pub fn offsets(topic: String, cursors: [(u64, u64); 3]) -> DataRequest {
        DataRequest {
            topic,
            cursors,
            max_size: 100 * 1024,
            max_count: 100
        }
    }

    /// New data request with provided offsets
    pub fn offsets_with(
        topic: String,
        cursors: [(u64, u64); 3],
        max_size: usize,
        max_count: usize
    ) -> DataRequest {
        DataRequest {
            topic,
            cursors,
            max_size,
            max_count
        }
    }
}

pub struct DataReply {
    /// Log to sweep
    pub topic: String,
    /// (segment, offset) tuples per replica (1 native and 2 replicas)
    pub cursors: [(u64, u64); 3],
    /// Payload size
    pub size: usize,
    /// Reply data chain
    pub payload: Vec<Bytes>,
}

impl fmt::Debug for DataReply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Topic = {:?}, Cursors = {:?}, Payload size = {}, Payload count = {}", self.topic, self.cursors, self.size, self.payload.len())
    }
}

#[derive(Debug, Clone)]
pub struct TopicsRequest {
    /// Start from this offset
    pub offset: usize,
    /// Maximum number of topics to read
    pub count: usize,
}

impl TopicsRequest {
    pub fn new() -> TopicsRequest {
        TopicsRequest {
            offset: 0,
            count: 10
        }
    }

    pub fn offset(offset: usize) -> TopicsRequest {
        TopicsRequest {
            offset,
            count: 10
        }
    }
}

#[derive(Debug)]
pub struct TopicsReply {
    /// Last topic offset
    pub offset: usize,
    /// list of new topics
    pub topics: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AcksRequest {
    /// Acks request topic
    pub(crate) topic: String,
    /// Router return acks after this offset
    /// Registers this request if it can't return acks
    /// yet due to incomplete replication
    pub(crate) offset: u64,
}

impl AcksRequest {
    pub fn new(topic: String, offset: u64) -> AcksRequest {
        AcksRequest {
            topic,
            offset
        }
    }
}

#[derive(Debug)]
pub struct AcksReply {
    pub topic: String,
    /// packet ids that can be acked
    pub pkids: VecDeque<u16>,
    /// offset till which pkids are returned
    pub offset: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum ConnectionType {
    Device(String),
    Replicator(usize),
}

/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
/// communicate with this connection
#[derive(Clone)]
pub struct Connection {
    /// Kind of connection. A replicator connection or a device connection
    /// Replicator connection are only created from inside this library.
    /// All the external connections are of 'device' type
    pub(crate) conn: ConnectionType,
    /// Handle which is given to router to allow router to comminicate with
    /// this connection
    pub handle: Sender<RouterOutMessage>,
}

impl Connection {
    pub fn new(id: &str, capacity: usize) -> (Connection, Receiver<RouterOutMessage>) {
        let (this_tx, this_rx) = bounded(capacity);

        let connection = Connection {
            conn: ConnectionType::Device(id.to_owned()),
            handle: this_tx,
        };

        (connection , this_rx)
    }
}

#[derive(Debug)]
pub enum ConnectionAck {
    /// Id assigned by the router for this connectiobackn
    Success(usize),
    /// Failure and reason for failure string
    Failure(String),
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.conn)
    }
}


#[derive(Debug)]
pub struct Disconnection {
    id: String,
}

impl Disconnection {
    pub fn new(id: String) -> Disconnection {
        Disconnection {
            id
        }
    }
}
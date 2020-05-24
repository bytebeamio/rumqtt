extern crate bytes;

/// NOTE: Don't let mqtt4bytes creep in here. By keeping MQTT specifics outside,
/// supporting mqtt4 and 5 with the same router becomes easy

mod commitlog;
mod router;

pub use router::Router;

use self::bytes::Bytes;
use std::fmt;
use tokio::sync::mpsc::Sender;

/// Router message to orchestrate data between connections. We can also
/// use this to send control signals to connections to modify their behavior
/// dynamically from the console
#[derive(Debug)]
pub enum RouterInMessage {
    /// Client id and connection handle
    Connect(Connection),
    /// Data which is written to commitlog
    Data(Data),
    /// Data request
    DataRequest(DataRequest),
    /// Topics request
    TopicsRequest(TopicsRequest),
}

/// Outgoing message from the router.
#[derive(Debug)]
pub enum RouterOutMessage {
    /// Connection reply
    ConnectionAck(ConnectionAck),
    /// Data reply
    DataReply(DataReply),
    /// Topics reply
    TopicsReply(TopicsReply),
}


/// Data which is sent router to be written to commitlog
#[derive(Debug)]
pub struct Data {
    /// Id of the packet that connection received
    pub pkid: u16,
    /// Topic of publish
    pub topic: String,
    /// Publish payload
    pub payload: Bytes
}

#[derive(Debug)]
pub struct DataAck {
    /// Packet id that connection received
    pub pkid: u16,
    /// Packet id that router assigned
    pub rpkid: u64
}

/// Request that connection/linker makes to extract data from commitlog
/// NOTE Connection can make one sweep request to get data from multiple topics
/// but we'll keep it simple for now as multiple requests in one message can
/// makes constant extraction size harder
#[derive(Debug, Clone)]
pub struct DataRequest {
    /// Log to sweep
    pub topic: String,
    /// Segment id of the log.
    pub segment: u64,
    /// Current offset. For requests, this is where sweeps
    /// start from. For reply, this is the last offset
    pub offset: u64,
    /// Request Size / Reply size
    pub size: u64,
}

#[derive(Debug)]
pub struct DataReply {
    /// Catch up status
    pub done: bool,
    /// Log to sweep
    pub topic: String,
    /// Segment id of the log.
    pub segment: u64,
    /// Current offset. For requests, this is where sweeps
    /// start from. For reply, this is the last offset
    pub offset: u64,
    /// Packet ids of replys
    pub pkids: Vec<u64>,
    /// Reply data chain
    pub payload: Vec<Bytes>,
}

#[derive(Debug, Clone)]
pub struct TopicsRequest {
    /// Start from this offset
    pub offset: usize,
    /// Maximum number of topics to read
    pub count: usize,
}

#[derive(Debug)]
pub struct TopicsReply {
    /// Catch up status
    pub done: bool,
    /// Last topic offset
    pub offset: usize,
    /// list of new topics
    pub topics: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum ConnectionType {
    Device(String),
    Replicator(usize)
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
    pub fn new(id: &str, handle: Sender<RouterOutMessage>) -> Connection {
        Connection {
            conn: ConnectionType::Device(id.to_owned()),
            handle
        }
    }
}

#[derive(Debug)]
pub enum ConnectionAck {
    /// Id assigned by the router for this connection
    Success(usize),
    /// Failure and reason for failure string
    Failure(String)
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.conn)
    }
}


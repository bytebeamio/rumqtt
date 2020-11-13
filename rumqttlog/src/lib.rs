#[macro_use]
extern crate log;

pub mod logs;
pub mod router;
pub mod storage;
pub mod volatile;
pub mod waiters;

use std::path::PathBuf;

pub use router::connection::Connection;
pub use router::{
    Acks, ConnectionAck, Data, DataRequest, Disconnection, Event, Message, MetricsReply,
    MetricsRequest, Notification, ReplicationData, Router,
};
pub use storage::segment::Segment;
pub use storage::Log;

pub use jackiechan::{bounded, Receiver, RecvError, SendError, Sender};
pub use mqtt4bytes::{Packet, Publish, QoS, Subscribe};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

pub type ConnectionId = usize;
pub type RouterId = usize;
pub type Topic = String;
pub type TopicId = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub id: usize,
    pub dir: PathBuf,
    pub max_segment_size: usize,
    pub max_segment_count: usize,
    pub max_connections: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            id: 255,
            dir: PathBuf::from("/tmp/timestone"),
            max_segment_size: 5 * 1024 * 1024,
            max_segment_count: 1024,
            max_connections: 1010,
        }
    }
}

pub trait IO: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> IO for T {}

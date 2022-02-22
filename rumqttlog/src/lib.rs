#![doc = include_str!("../README.md")]

#[macro_use]
extern crate log;

pub mod logs;
pub mod router;
pub mod waiters;

use std::path::PathBuf;

pub use router::connection::Connection;
pub use router::{
    ConnectionAck, Data, DataRequest, Disconnection, Event, Message, MetricsReply, MetricsRequest,
    Notification, Router,
};

pub use jackiechan::{bounded, Receiver, RecvError, RecvTimeoutError, SendError, Sender};
use serde::{Deserialize, Serialize};

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

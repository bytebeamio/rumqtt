#[macro_use]
extern crate log;

pub mod router;
pub mod storage;
pub mod volatile;

pub use mqtt4bytes;
pub use router::{CommitLog, Router, RouterInMessage, RouterOutMessage, DataRequest, DataReply};
pub use storage::segment::Segment;
pub use storage::Log;

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    id: u8,
    host: String,
    port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub id: u8,
    pub dir: PathBuf,
    pub max_segment_size: u64,
    pub max_record_count: u64,
    pub max_segment_count: usize,
    pub routers: Option<Vec<RouterConfig>>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            id: 255,
            dir: PathBuf::from("/tmp/timestone"),
            max_record_count: 1000_000,
            max_segment_size: 100 * 1024 * 1024,
            max_segment_count: 100,
            routers: None,
        }
    }
}

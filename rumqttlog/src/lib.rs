#[macro_use]
extern crate log;

pub mod router;
pub mod storage;
pub mod volatile;
pub mod mesh;
pub mod tracker;

pub use mqtt4bytes;
pub use router::{Router, RouterInMessage, RouterOutMessage, DataRequest, DataReply, Connection};
pub use storage::segment::Segment;
pub use storage::Log;
pub use async_channel::{bounded, Receiver, Sender};

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshConfig {
    id: u8,
    host: String,
    port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub id: u8,
    pub dir: PathBuf,
    pub max_segment_size: u64,
    pub max_segment_count: usize,
    pub routers: Option<Vec<MeshConfig>>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            id: 255,
            dir: PathBuf::from("/tmp/timestone"),
            max_segment_size: 5 * 1024 * 1024,
            max_segment_count: 1024,
            routers: None,
        }
    }
}

pub trait IO: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> IO for T {}

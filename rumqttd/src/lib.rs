use segments::Storage;

pub mod config;
mod link;
pub mod protocol;
mod router;
mod segments;
pub mod serialconfig;
mod server;

pub type ConnectionId = usize;
pub type RouterId = usize;
pub type NodeId = usize;
pub type Topic = String;
pub type Filter = String;
pub type TopicId = usize;
pub type Offset = (u64, u64);
pub type Cursor = (u64, u64);

pub use link::alerts;
pub use link::local;
pub use link::meters;

pub use router::{Alert, IncomingMeter, Meter, Notification, OutgoingMeter};
pub use server::Broker;

//! PING heartbeat message sent from outbound node to inbound node.
//!
//! Wire format: `PING <node_port> <unix_timestamp_ms>`

use std::time::SystemTime;

const PING_MESSAGE_IDENTIFIER: &str = "PING";

pub struct PingReplicationMessage {
    #[allow(dead_code)]
    master_id: u16,
}

impl PingReplicationMessage {
    /// Returns `true` if the given frame starts with the PING identifier.
    pub fn matches(frame: &str) -> bool {
        frame.starts_with(PING_MESSAGE_IDENTIFIER)
    }

    /// Builds a PING frame payload containing this node's port and current timestamp.
    pub fn new(master_id: u16) -> String {
        format!(
            "{} {} {}",
            PING_MESSAGE_IDENTIFIER,
            master_id,
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        )
    }
}

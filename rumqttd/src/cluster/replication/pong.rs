//! PONG heartbeat response sent back to the node that sent a PING.
//!
//! Wire format: `PONG <replica_id> <master|slave>`

use crate::cluster::config::ClusterNodeMode;

const PONG_MESSAGE_IDENTIFIER: &str = "PONG";

pub struct PongReplicationMessage;

impl PongReplicationMessage {
    /// Returns `true` if the given frame starts with the PONG identifier.
    #[allow(dead_code)]
    pub fn matches(frame: &str) -> bool {
        frame.starts_with(PONG_MESSAGE_IDENTIFIER)
    }

    /// Builds a PONG frame payload containing this node's port and mode.
    pub fn new(replica_id: u16, node_mode: &ClusterNodeMode) -> String {
        let mode_str = match node_mode {
            ClusterNodeMode::Master => "master",
            ClusterNodeMode::Slave => "slave",
        };
        format!("{} {} {}", PONG_MESSAGE_IDENTIFIER, replica_id, mode_str)
    }

    /// Parses a PONG frame payload, returning the replica id and node mode.
    pub fn parse(message: &str) -> Result<(u16, ClusterNodeMode), &'static str> {
        let mut parts = message.split_whitespace();

        match (parts.next(), parts.next(), parts.next()) {
            (Some(PONG_MESSAGE_IDENTIFIER), Some(id), Some(mode)) => {
                let replica_id = id.parse::<u16>().map_err(|_| "Invalid replica id")?;
                let node_mode = match mode {
                    "master" => ClusterNodeMode::Master,
                    "slave" => ClusterNodeMode::Slave,
                    _ => return Err("Invalid node mode"),
                };
                Ok((replica_id, node_mode))
            }
            (Some(PONG_MESSAGE_IDENTIFIER), Some(_), None) => Err("Missing node mode"),
            (Some(_), _, _) => Err("Not a PONG message"),
            _ => Err("PONG message too short"),
        }
    }
}

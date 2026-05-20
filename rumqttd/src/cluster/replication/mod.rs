//! Replication message formats for inter-node heartbeats.
//!
//! Nodes exchange PING and PONG messages to verify connectivity and
//! discover each other's role. See [`ping`] and [`pong`] for the
//! wire format of each message type.

pub mod ping;
pub mod pong;

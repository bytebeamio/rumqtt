use std::time::Instant;
use rumq_core::{PacketIdentifier, Publish};
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
}

#[derive(Debug)]
pub struct ClientState {
    /// Connection status of this client for handling persistent sessions
    pub status: ConnectionStatus,
    /// Time at which this client received last control packet
    pub last_control_at: Instant,
    /// Packet identifier of the last packet
    pub last_pkid: PacketIdentifier,
    /// For QoS 1. Stores outgoing publishes
    pub outgoing_pub: VecDeque<Publish>,
    /// For QoS 1. Stores incoming publishes
    pub incoming_pub: VecDeque<Publish>,
}

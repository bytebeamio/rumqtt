//! Result and event enums for the outbound subsystem.
//!
//! These types decouple the three concerns inside the outbound ping-pong loop:
//! reader-thread signalling, TCP connection management, and channel-drain
//! classification.

use crate::cluster::config::ClusterNodeMode;

/// Events sent from a reader thread to its writer thread ([`OutboundNode`]).
///
/// [`OutboundNode`]: super::outbound::OutboundNode
pub(crate) enum ReaderEvent {
    /// A well-formed PONG frame was received.
    Pong {
        replica_id: u16,
        node_mode: ClusterNodeMode,
    },
    /// A frame arrived but could not be parsed as a PONG.
    InvalidFrame(String),
    /// The underlying TCP connection was lost (read error).
    ConnectionLost,
}

/// Outcome of a single TCP connection attempt.
pub(crate) enum ConnectionResult {
    /// A live stream is now available; the reader thread has been spawned.
    Connected,
    /// Connection failed but attempts < max; caller should sleep and retry.
    RetryLater,
    /// Attempts >= max; caller should mark the node Disconnected and return.
    GaveUp,
}

/// Outcome of draining the reader-event channel after a ping interval.
pub(crate) enum DrainResult {
    /// At least one valid PONG was received (success already recorded).
    PongReceived,
    /// The reader signalled that the TCP connection dropped.
    ConnectionLost,
    /// No PONG arrived within the interval.
    NoPong,
}

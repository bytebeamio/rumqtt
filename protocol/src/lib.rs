#![allow(dead_code, unused)]

// pub mod fixedlen;
// pub mod shred;
// pub mod local;
pub mod v4;
// pub mod v5;

use std::str::Utf8Error;

use base::messages::*;

/// Error during serialization and deserialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("Invalid return code received as response for connect = {0}")]
    InvalidConnectReturnCode(u8),
    #[error("Invalid reason = {0}")]
    InvalidReason(u8),
    #[error("Invalid reason = {0}")]
    InvalidRemainingLength(usize),
    #[error("Invalid protocol used")]
    InvalidProtocol,
    #[error("Invalid protocol level {0}. Make sure right port is being used.")]
    InvalidProtocolLevel(u8),
    #[error("Invalid packet format")]
    IncorrectPacketFormat,
    #[error("Invalid packet type = {0}")]
    InvalidPacketType(u8),
    #[error("Invalid retain forward rule = {0}")]
    InvalidRetainForwardRule(u8),
    #[error("Invalid QoS level = {0}")]
    InvalidQoS(u8),
    #[error("Invalid subscribe reason code = {0}")]
    InvalidSubscribeReasonCode(u8),
    #[error("Packet received has id Zero")]
    PacketIdZero,
    #[error("Empty Subscription")]
    EmptySubscription,
    #[error("Subscription had id Zero")]
    SubscriptionIdZero,
    #[error("Payload size is incorrect")]
    PayloadSizeIncorrect,
    #[error("Payload is too long")]
    PayloadTooLong,
    #[error("Payload size has been exceeded by {0} bytes")]
    PayloadSizeLimitExceeded(usize),
    #[error("Payload is required")]
    PayloadRequired,
    #[error("Payload is required = {0}")]
    PayloadNotUtf8(#[from] Utf8Error),
    #[error("Topic not utf-8")]
    TopicNotUtf8,
    #[error("Promised boundary crossed, contains {0} bytes")]
    BoundaryCrossed(usize),
    #[error("Packet is malformed")]
    MalformedPacket,
    #[error("Remaining length is malformed")]
    MalformedRemainingLength,
    #[error("Invalid property type = {0}")]
    InvalidPropertyType(u8),
    /// More bytes required to frame packet. Argument
    /// implies minimum additional bytes required to
    /// proceed further
    // TODO: Remove this error variant because is not really required because now when there is insufficient bytes
    // we return `Frame::Pending`. Currently its been used by `check` function
    #[error("Insufficient number of bytes to frame packet, {0} more bytes required")]
    InsufficientBytes(usize),
    #[error("Connect packet is send more then once")]
    UnexpectedConnect,
    #[error("First packet should be connect")]
    NotConnectPacket,
    #[error("This packet is not yet supported")]
    NotSupportedYet,
}

pub trait Protocol: Clone {
    // NOTE: Do not take reference into the stream's buffer (i.e. methods like `split_to`, `split_off`)
    // and store it in returned `Packet`. Because the packet generated from this byte can persist for much longer
    // TODO: See if there is any way to enforce this by type system
    // fn check(&self, stream: &[u8], max_size: usize) -> Result<Frame, Error>;
    // fn frame(
    //     &self,
    //     stream: &mut &[u8],
    //     frames: &mut Vec<u8>,
    //     max_size: usize,
    // ) -> Result<Frame, Error>;
    fn read_connect(&self, stream: &mut &[u8], max_size: usize) -> Result<Connect, Error>;
    fn read_publish(&self, stream: &mut &[u8], max_size: usize) -> Result<Publish, Error>;
    fn write_connect(&self, connect: &Connect, buffer: &mut Vec<u8>) -> Result<usize, Error>;
    fn write_publish(&self, publish: &Publish, buffer: &mut Vec<u8>) -> Result<usize, Error>;
    fn read(&self, stream: &mut &[u8], max_size: usize) -> Result<Packet, Error>;
    fn write(&self, packet: Packet, write: &mut Vec<u8>) -> Result<usize, Error>;
}

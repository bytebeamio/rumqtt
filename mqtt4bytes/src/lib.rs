//! This is a low level (no_std) crate with the ability to assemble and disassemble MQTT 3.1.1
//! packets and is used by both client and broker. Uses 'bytes' crate internally

#![no_std]
use cfg_if::cfg_if;

extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

#[cfg(feature = "std")]
mod codec;
mod packets;
mod read;
mod topic;
mod write;

#[cfg(feature = "std")]
pub use codec::*;
pub use packets::*;
pub use read::*;
pub use topic::*;
pub use write::*;
use bytes::{Buf};

cfg_if! {
    if #[cfg(feature = "std")] {
        /// Serialization and deserialization errors
        #[derive(Debug, thiserror::Error)]
        pub enum Error {
            #[error("Invalid connect return code `{0}`")]
            InvalidConnectReturnCode(u8),
            #[error("Invalid protocol. Expecting 'MQTT' in payload")]
            InvalidProtocol,
            #[error("Invalid protocol level `{0}`")]
            InvalidProtocolLevel(u8),
            #[error("Incorrect packet format")]
            IncorrectPacketFormat,
            #[error("Unsupported Packet type `{0}`")]
            InvalidPacketType(u8),
            #[error("Unsupported QoS `{0}`")]
            InvalidQoS(u8),
            #[error("Invalid packet identifier = 0")]
            PacketIdZero,
            #[error("Payload size incorrect")]
            PayloadSizeIncorrect,
            #[error("Payload too long")]
            PayloadTooLong,
            #[error("Payload size limit exceeded")]
            PayloadSizeLimitExceeded,
            #[error("Payload required")]
            PayloadRequired,
            #[error("Topic name must only contain valid UTF-8")]
            TopicNotUtf8,
            #[error("Malformed remaining length")]
            MalformedRemainingLength,
            #[error("Trying to access wrong boundary")]
            BoundaryCrossed,
            #[error("EOF. Not enough data in buffer")]
            UnexpectedEof,
            #[error("I/O")]
            Io(#[from] std::io::Error),
        }
    } else {
        /// Serialization and deserialization errors
        pub enum Error {
            InvalidConnectReturnCode(u8),
            InvalidProtocol,
            InvalidProtocolLevel(u8),
            IncorrectPacketFormat,
            InvalidPacketType(u8),
            InvalidQoS(u8),
            PacketIdZero,
            PayloadSizeIncorrect,
            PayloadTooLong,
            PayloadSizeLimitExceeded,
            PayloadRequired,
            TopicNotUtf8,
            BoundaryCrossed,
            MalformedRemainingLength,
            UnexpectedEof,
        }
    }
}

/// Encapsulates all MQTT packet types
#[derive(Debug, Clone)]
pub enum Packet {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(PubAck),
    PubRec(PubRec),
    PubRel(PubRel),
    PubComp(PubComp),
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    PingReq,
    PingResp,
    Disconnect,
}



/// MQTT packet type
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Connect = 1,
    ConnAck,
    Publish,
    PubAck,
    PubRec,
    PubRel,
    PubComp,
    Subscribe,
    SubAck,
    Unsubscribe,
    UnsubAck,
    PingReq,
    PingResp,
    Disconnect,
}

/// Packet type from a byte
///
/// ```ignore
///          7                          3                          0
///          +--------------------------+--------------------------+
/// byte 1   | MQTT Control Packet Type | Flags for each type      |
///          +--------------------------+--------------------------+
///          |         Remaining Bytes Len  (1 - 4 bytes)          |
///          +-----------------------------------------------------+
///
/// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Figure_2.2_-
/// ```
pub fn packet_type(num: u8) -> Result<PacketType, Error> {
    match num {
        1 => Ok(PacketType::Connect),
        2 => Ok(PacketType::ConnAck),
        3 => Ok(PacketType::Publish),
        4 => Ok(PacketType::PubAck),
        5 => Ok(PacketType::PubRec),
        6 => Ok(PacketType::PubRel),
        7 => Ok(PacketType::PubComp),
        8 => Ok(PacketType::Subscribe),
        9 => Ok(PacketType::SubAck),
        10 => Ok(PacketType::Unsubscribe),
        11 => Ok(PacketType::UnsubAck),
        12 => Ok(PacketType::PingReq),
        13 => Ok(PacketType::PingResp),
        14 => Ok(PacketType::Disconnect),
        _ => Err(Error::InvalidPacketType(num)),
    }
}

/// Protocol type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    MQTT(u8),
}


/// Quality of service
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

struct FixedHeader {
    byte1: u8,
    header_len: usize,
    remaining_len: usize,
}

/// Maps a number to QoS
pub fn qos(num: u8) -> Result<QoS, Error> {
    match num {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        qos => Err(Error::InvalidQoS(qos)),
    }
}


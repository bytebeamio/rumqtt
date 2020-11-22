//! This is a low level (no_std) crate with the ability to assemble and disassemble MQTT 3.1.1
//! packets and is used by both client and broker. Uses 'bytes' crate internally

extern crate alloc;

mod packets;
mod read;
mod topic;

use bytes::Buf;
use core::fmt;
use core::fmt::{Display, Formatter};
pub use packets::*;
pub use read::*;
pub use topic::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidConnectReturnCode(u8),
    InvalidReason(u8),
    InvalidProtocol,
    InvalidProtocolLevel(u8),
    IncorrectPacketFormat,
    InvalidPacketType(u8),
    InvalidPropertyType(u8),
    InvalidRetainForwardRule(u8),
    InvalidQoS(u8),
    InvalidSubscribeReasonCode(u8),
    PacketIdZero,
    SubscriptionIdZero,
    PayloadSizeIncorrect,
    PayloadTooLong,
    PayloadSizeLimitExceeded,
    PayloadRequired,
    TopicNotUtf8,
    BoundaryCrossed(usize),
    MalformedPacket,
    MalformedRemainingLength,
    InsufficientBytes(usize),
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

/// Protocol type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    MQTT(u8),
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyType {
    PayloadFormatIndicator = 1,
    MessageExpiryInterval = 2,
    ContentType = 3,
    ResponseTopic = 8,
    CorrelationData = 9,
    SubscriptionIdentifier = 11,
    SessionExpiryInterval = 17,
    AssignedClientIdentifier = 18,
    ServerKeepAlive = 19,
    AuthenticationMethod = 21,
    AuthenticationData = 22,
    RequestProblemInformation = 23,
    WillDelayInterval = 24,
    RequestResponseInformation = 25,
    ResponseInformation = 26,
    ServerReference = 28,
    ReasonString = 31,
    ReceiveMaximum = 33,
    TopicAliasMaximum = 34,
    TopicAlias = 35,
    MaximumQos = 36,
    RetainAvailable = 37,
    UserProperty = 38,
    MaximumPacketSize = 39,
    WildcardSubscriptionAvailable = 40,
    SubscriptionIdentifierAvailable = 41,
    SharedSubscriptionAvailable = 42,
}

fn property(num: u8) -> Result<PropertyType, Error> {
    let property = match num {
        1 => PropertyType::PayloadFormatIndicator,
        2 => PropertyType::MessageExpiryInterval,
        3 => PropertyType::ContentType,
        8 => PropertyType::ResponseTopic,
        9 => PropertyType::CorrelationData,
        11 => PropertyType::SubscriptionIdentifier,
        17 => PropertyType::SessionExpiryInterval,
        18 => PropertyType::AssignedClientIdentifier,
        19 => PropertyType::ServerKeepAlive,
        21 => PropertyType::AuthenticationMethod,
        22 => PropertyType::AuthenticationData,
        23 => PropertyType::RequestProblemInformation,
        24 => PropertyType::WillDelayInterval,
        25 => PropertyType::RequestResponseInformation,
        26 => PropertyType::ResponseInformation,
        28 => PropertyType::ServerReference,
        31 => PropertyType::ReasonString,
        33 => PropertyType::ReceiveMaximum,
        34 => PropertyType::TopicAliasMaximum,
        35 => PropertyType::TopicAlias,
        36 => PropertyType::MaximumQos,
        37 => PropertyType::RetainAvailable,
        38 => PropertyType::UserProperty,
        39 => PropertyType::MaximumPacketSize,
        40 => PropertyType::WildcardSubscriptionAvailable,
        41 => PropertyType::SubscriptionIdentifierAvailable,
        42 => PropertyType::SharedSubscriptionAvailable,
        num => return Err(Error::InvalidPropertyType(num)),
    };

    Ok(property)
}

/// Quality of service
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

/// Packet type from a byte
///
/// ```ignore
///          7                          3                          0
///          +--------------------------+--------------------------+
/// byte 1   | MQTT Control Packet Type | Flags for each type      |
///          +--------------------------+--------------------------+
///          |         Remaining Bytes Len  (1/2/3/4 bytes)        |
///          +-----------------------------------------------------+
///
/// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Figure_2.2_-
/// ```
pub struct FixedHeader {
    /// First byte of the stream. Used to identify packet types and
    /// several flags
    byte1: u8,
    /// Length of fixed header. Byte 1 + (1..4) bytes. So fixed header
    /// len can vary from 2 bytes to 5 bytes
    /// 1..4 bytes are variable length encoded to represent remaining length
    fixed_len: usize,
    /// Remaining length of the packet. Doesn't include fixed header bytes
    /// Represents variable header + payload size
    remaining_len: usize,
}

impl FixedHeader {
    pub fn new(byte1: u8, remaining_len_len: usize, remaining_len: usize) -> FixedHeader {
        FixedHeader {
            byte1,
            fixed_len: remaining_len_len + 1,
            remaining_len,
        }
    }

    pub fn packet_type(&self) -> Result<PacketType, Error> {
        let num = self.byte1 >> 4;
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

    /// Returns the size of full packet (fixed header + variable header + payload)
    /// Fixed header is enough to get the size of a frame in the stream
    pub fn frame_length(&self) -> usize {
        self.fixed_len + self.remaining_len
    }
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

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Error = {:?}", self)
    }
}

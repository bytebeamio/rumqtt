// reexport valid_filter and valid_topic since they are identical in nature for
// both v3 and v5
use super::PublishProperties;
pub use crate::mqttbytes::{valid_filter, valid_topic};
use std::{str::Utf8Error, vec};

/// This module is the place where all the protocol specifics gets abstracted
/// out and creates a structures which are common across protocols. Since,
/// MQTT is the core protocol that this broker supports, a lot of structs closely
/// map to what MQTT specifies in its protocol
pub mod v5;

/// Quality of service
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
#[allow(clippy::enum_variant_names)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl Default for QoS {
    fn default() -> Self {
        Self::AtMostOnce
    }
}

/// Maps a number to QoS
pub fn qos(num: u8) -> Option<QoS> {
    match num {
        0 => Some(QoS::AtMostOnce),
        1 => Some(QoS::AtLeastOnce),
        2 => Some(QoS::ExactlyOnce),
        _ => None,
    }
}

pub(crate) fn validate_topic_name_and_alias(
    topic: impl AsRef<str>,
    properties: &Option<PublishProperties>,
) -> bool {
    let topic = topic.as_ref();
    let is_topic_empty = topic.is_empty();
    // The topic alias is considered valid only if it is greater than zero.
    // If it is not supplied, it is still considered valid because in that
    // case, the validity depends on the topic name itself.
    let is_topic_valid = valid_topic(topic);
    let is_alias_given = properties
        .as_ref()
        .map_or(false, |p| p.topic_alias.is_some());
    let is_alias_valid = properties
        .as_ref()
        .map_or(false, |p| p.topic_alias.map_or(false, |a| a > 0));

    (is_topic_valid || is_topic_empty)
        && (!is_topic_empty || is_alias_given)
        && (!is_alias_given || is_alias_valid)
}

/// Checks if topic matches a filter. topic and filter validation isn't done here.
///
/// **NOTE**: 'topic' is a misnomer in the arg. this can also be used to match 2 wild subscriptions
/// **NOTE**: make sure a topic is validated during a publish and filter is validated
/// during a subscribe
pub fn matches(topic: &str, filter: &str) -> bool {
    if !topic.is_empty() && topic[..1].contains('$') {
        return false;
    }

    let mut topics = topic.split('/');
    let mut filters = filter.split('/');

    for f in filters.by_ref() {
        // "#" being the last element is validated by the broker with 'valid_filter'
        if f == "#" {
            return true;
        }

        // filter still has remaining elements
        // filter = a/b/c/# should match topci = a/b/c
        // filter = a/b/c/d should not match topic = a/b/c
        let top = topics.next();
        match top {
            Some("#") => return false,
            Some(_) if f == "+" => continue,
            Some(t) if f != t => return false,
            Some(_) => continue,
            None => return false,
        }
    }

    // topic has remaining elements and filter's last element isn't "#"
    if topics.next().is_some() {
        return false;
    }

    true
}

/// Error during serialization and deserialization
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid return code received as response for connect = {0}")]
    InvalidConnectReturnCode(u8),
    #[error("Invalid reason = {0}")]
    InvalidReason(u8),
    #[error("Invalid remaining length = {0}")]
    InvalidRemainingLength(usize),
    #[error("Invalid protocol used")]
    InvalidProtocol,
    #[error("Invalid protocol level")]
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
    #[error("Max Payload size of {max:?} has been exceeded by packet of {pkt_size:?} bytes")]
    PayloadSizeLimitExceeded { pkt_size: usize, max: u32 },
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
    #[error("Insufficient number of bytes to frame packet, {0} more bytes required")]
    InsufficientBytes(usize),
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("Cannot send packet of size '{pkt_size:?}'. It's greater than the broker's maximum packet size of: '{max:?}'")]
    OutgoingPacketTooLarge { pkt_size: u32, max: u32 },
}

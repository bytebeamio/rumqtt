#![allow(dead_code, unused)]

pub mod v4;
pub mod v5;

use std::{io, str::Utf8Error, string::FromUtf8Error};

/// This module is the place where all the protocol specifics gets abstracted
/// out and creates a structures which are common across protocols. Since,
/// MQTT is the core protocol that this broker supports, a lot of structs closely
/// map to what MQTT specifies in its protocol
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::Notification;

// TODO: Handle the cases when there are no properties using Inner struct, so
// handling of properties can be made simplier internally

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Packet {
    Connect(
        Connect,
        Option<ConnectProperties>,
        Option<LastWill>,
        Option<LastWillProperties>,
        Option<Login>,
    ),
    ConnAck(ConnAck, Option<ConnAckProperties>),
    Publish(Publish, Option<PublishProperties>),
    PubAck(PubAck, Option<PubAckProperties>),
    PingReq(PingReq),
    PingResp(PingResp),
    Subscribe(Subscribe, Option<SubscribeProperties>),
    SubAck(SubAck, Option<SubAckProperties>),
    PubRec(PubRec, Option<PubRecProperties>),
    PubRel(PubRel, Option<PubRelProperties>),
    PubComp(PubComp, Option<PubCompProperties>),
    Unsubscribe(Unsubscribe, Option<UnsubscribeProperties>),
    UnsubAck(UnsubAck, Option<UnsubAckProperties>),
    Disconnect(Disconnect, Option<DisconnectProperties>),
}

//--------------------------- Connect packet -------------------------------
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PingReq;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PingResp;

/// Connection packet initiated by the client
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    /// Mqtt keep alive time
    pub keep_alive: u16,
    /// Client Id
    pub client_id: String,
    /// Clean session. Asks the broker to clear previous state
    pub clean_session: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectProperties {
    /// Expiry interval property after loosing connection
    pub session_expiry_interval: Option<u32>,
    /// Maximum simultaneous packets
    pub receive_maximum: Option<u16>,
    /// Maximum packet size
    pub max_packet_size: Option<u32>,
    /// Maximum mapping integer for a topic
    pub topic_alias_max: Option<u16>,
    pub request_response_info: Option<u8>,
    pub request_problem_info: Option<u8>,
    /// List of user properties
    pub user_properties: Vec<(String, String)>,
    /// Method of authentication
    pub authentication_method: Option<String>,
    /// Authentication data
    pub authentication_data: Option<Bytes>,
}

/// LastWill that broker forwards on behalf of the client
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastWill {
    pub topic: Bytes,
    pub message: Bytes,
    pub qos: QoS,
    pub retain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastWillProperties {
    pub delay_interval: Option<u32>,
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Bytes>,
    pub user_properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Login {
    pub username: String,
    pub password: String,
}

//--------------------------- ConnectAck packet -------------------------------

/// Return code in connack
// This contains return codes for both MQTT v311 and v5
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectReturnCode {
    Success,
    RefusedProtocolVersion,
    BadClientId,
    ServiceUnavailable,
    UnspecifiedError,
    MalformedPacket,
    ProtocolError,
    ImplementationSpecificError,
    UnsupportedProtocolVersion,
    ClientIdentifierNotValid,
    BadUserNamePassword,
    NotAuthorized,
    ServerUnavailable,
    ServerBusy,
    Banned,
    BadAuthenticationMethod,
    TopicNameInvalid,
    PacketTooLarge,
    QuotaExceeded,
    PayloadFormatInvalid,
    RetainNotSupported,
    QoSNotSupported,
    UseAnotherServer,
    ServerMoved,
    ConnectionRateExceeded,
}

/// Acknowledgement to connect packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    pub session_present: bool,
    pub code: ConnectReturnCode,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnAckProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_max: Option<u16>,
    pub max_qos: Option<u8>,
    pub retain_available: Option<u8>,
    pub max_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub topic_alias_max: Option<u16>,
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
    pub wildcard_subscription_available: Option<u8>,
    pub subscription_identifiers_available: Option<u8>,
    pub shared_subscription_available: Option<u8>,
    pub server_keep_alive: Option<u16>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Bytes>,
}

//--------------------------- Publish packet -------------------------------

/// Publish packet
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Publish {
    pub(crate) dup: bool,
    pub(crate) qos: QoS,
    pub(crate) pkid: u16,
    pub retain: bool,
    pub topic: Bytes,
    pub payload: Bytes,
}

impl Publish {
    // Constructor for publish. Used in local links as local links shouldn't
    // send qos 1 or 2 packets
    pub fn new<T: Into<Bytes>>(topic: T, payload: T, retain: bool) -> Publish {
        Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            pkid: 0,
            retain,
            topic: topic.into(),
            payload: payload.into(),
        }
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    /// Approximate length for meter
    pub fn len(&self) -> usize {
        let len = 2 + self.topic.len() + self.payload.len();
        match self.qos == QoS::AtMostOnce {
            true => len,
            false => len + 2,
        }
    }

    /// Serialization which is independent of MQTT
    pub fn serialize(&self) -> Bytes {
        let mut o = BytesMut::with_capacity(self.len() + 5);
        let dup = self.dup as u8;
        let qos = self.qos as u8;
        let retain = self.retain as u8;
        o.put_u8(0b0011_0000 | retain | qos << 1 | dup << 3);
        o.put_u16(self.pkid);
        o.put_u16(self.topic.len() as u16);
        o.extend_from_slice(&self.topic[..]);

        // TODO: Change segments to take Buf to prevent this copying
        o.extend_from_slice(&self.payload[..]);
        o.freeze()
    }

    /// Serialization which is independent of MQTT
    pub fn deserialize(mut o: Bytes) -> Publish {
        let header = o.get_u8();
        let qos_num = (header & 0b0110) >> 1;
        let qos = qos(qos_num).unwrap_or(QoS::AtMostOnce);
        let dup = (header & 0b1000) != 0;
        let retain = (header & 0b0001) != 0;

        let pkid = o.get_u16();
        let topic_len = o.get_u16();
        let topic = o.split_to(topic_len as usize);
        let payload = o;
        Publish {
            dup,
            qos,
            retain,
            topic,
            pkid,
            payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PublishProperties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub topic_alias: Option<u16>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Bytes>,
    pub user_properties: Vec<(String, String)>,
    pub subscription_identifiers: Vec<usize>,
    pub content_type: Option<String>,
}

//--------------------------- PublishAck packet -------------------------------

/// Return code in puback
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubAckReason {
    Success,
    NoMatchingSubscribers,
    UnspecifiedError,
    ImplementationSpecificError,
    NotAuthorized,
    TopicNameInvalid,
    PacketIdentifierInUse,
    QuotaExceeded,
    PayloadFormatInvalid,
}

/// Acknowledgement to QoS1 publish
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubAck {
    pub pkid: u16,
    pub reason: PubAckReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubAckProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

//--------------------------- Subscribe packet -------------------------------

/// Subscription packet
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscribe {
    pub pkid: u16,
    pub filters: Vec<Filter>,
}

///  Subscription filter
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Filter {
    pub path: String,
    pub qos: QoS,
    pub nolocal: bool,
    pub preserve_retain: bool,
    pub retain_forward_rule: RetainForwardRule,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetainForwardRule {
    OnEverySubscribe,
    OnNewSubscribe,
    Never,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeProperties {
    pub id: Option<usize>,
    pub user_properties: Vec<(String, String)>,
}

//--------------------------- SubscribeAck packet -------------------------------

/// Acknowledgement to subscribe
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    pub pkid: u16,
    pub return_codes: Vec<SubscribeReasonCode>,
}

impl SubAck {
    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn len(&self) -> usize {
        2 + self.return_codes.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeReasonCode {
    QoS0,
    QoS1,
    QoS2,
    Success(QoS),
    Failure,
    Unspecified,
    ImplementationSpecific,
    NotAuthorized,
    TopicFilterInvalid,
    PkidInUse,
    QuotaExceeded,
    SharedSubscriptionsNotSupported,
    SubscriptionIdNotSupported,
    WildcardSubscriptionsNotSupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAckProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

//--------------------------- Unsubscribe packet -------------------------------

/// Unsubscribe packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    pub pkid: u16,
    pub filters: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubscribeProperties {
    pub user_properties: Vec<(String, String)>,
}
//--------------------------- UnsubscribeAck packet -------------------------------

/// Acknowledgement to unsubscribe
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubAck {
    pub pkid: u16,
    pub reasons: Vec<UnsubAckReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UnsubAckReason {
    Success,
    NoSubscriptionExisted,
    UnspecifiedError,
    ImplementationSpecificError,
    NotAuthorized,
    TopicFilterInvalid,
    PacketIdentifierInUse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubAckProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

//--------------------------- Ping packet -------------------------------

struct Ping;
struct PingResponse;

//------------------------------------------------------------------------

//--------------------------- PubRec packet -------------------------------

/// Return code in connack
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PubRecReason {
    Success,
    NoMatchingSubscribers,
    UnspecifiedError,
    ImplementationSpecificError,
    NotAuthorized,
    TopicNameInvalid,
    PacketIdentifierInUse,
    QuotaExceeded,
    PayloadFormatInvalid,
}

/// Acknowledgement to QoS1 publish
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubRec {
    pub pkid: u16,
    pub reason: PubRecReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubRecProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

//------------------------------------------------------------------------

//--------------------------- PubComp packet -------------------------------

/// Return code in connack
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PubCompReason {
    Success,
    PacketIdentifierNotFound,
}

/// QoS2 Assured publish complete, in response to PUBREL packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubComp {
    pub pkid: u16,
    pub reason: PubCompReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubCompProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

//------------------------------------------------------------------------

//--------------------------- PubRel packet -------------------------------

/// Return code in connack
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PubRelReason {
    Success,
    PacketIdentifierNotFound,
}

/// QoS2 Publish release, in response to PUBREC packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubRel {
    pub pkid: u16,
    pub reason: PubRelReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubRelProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

//------------------------------------------------------------------------

//--------------------------- Disconnect packet -------------------------------
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disconnect {
    /// Disconnect Reason Code
    pub reason_code: DisconnectReasonCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DisconnectReasonCode {
    /// Close the connection normally. Do not send the Will Message.
    NormalDisconnection,
    /// The Client wishes to disconnect but requires that the Server also publishes its Will Message.
    DisconnectWithWillMessage,
    /// The Connection is closed but the sender either does not wish to reveal the reason, or none of the other Reason Codes apply.
    UnspecifiedError,
    /// The received packet does not conform to this specification.
    MalformedPacket,
    /// An unexpected or out of order packet was received.
    ProtocolError,
    /// The packet received is valid but cannot be processed by this implementation.
    ImplementationSpecificError,
    /// The request is not authorized.
    NotAuthorized,
    /// The Server is busy and cannot continue processing requests from this Client.
    ServerBusy,
    /// The Server is shutting down.
    ServerShuttingDown,
    /// The Connection is closed because no packet has been received for 1.5 times the Keepalive time.
    KeepAliveTimeout,
    /// Another Connection using the same ClientID has connected causing this Connection to be closed.
    SessionTakenOver,
    /// The Topic Filter is correctly formed, but is not accepted by this Sever.
    TopicFilterInvalid,
    /// The Topic Name is correctly formed, but is not accepted by this Client or Server.
    TopicNameInvalid,
    /// The Client or Server has received more than Receive Maximum publication for which it has not sent PUBACK or PUBCOMP.
    ReceiveMaximumExceeded,
    /// The Client or Server has received a PUBLISH packet containing a Topic Alias which is greater than the Maximum Topic Alias it sent in the CONNECT or CONNACK packet.
    TopicAliasInvalid,
    /// The packet size is greater than Maximum Packet Size for this Client or Server.
    PacketTooLarge,
    /// The received data rate is too high.
    MessageRateTooHigh,
    /// An implementation or administrative imposed limit has been exceeded.
    QuotaExceeded,
    /// The Connection is closed due to an administrative action.
    AdministrativeAction,
    /// The payload format does not match the one specified by the Payload Format Indicator.
    PayloadFormatInvalid,
    /// The Server has does not support retained messages.
    RetainNotSupported,
    /// The Client specified a QoS greater than the QoS specified in a Maximum QoS in the CONNACK.
    QoSNotSupported,
    /// The Client should temporarily change its Server.
    UseAnotherServer,
    /// The Server is moved and the Client should permanently change its server location.
    ServerMoved,
    /// The Server does not support Shared Subscriptions.
    SharedSubscriptionNotSupported,
    /// This connection is closed because the connection rate is too high.
    ConnectionRateExceeded,
    /// The maximum connection time authorized for this connection has been exceeded.
    MaximumConnectTime,
    /// The Server does not support Subscription Identifiers; the subscription is not accepted.
    SubscriptionIdentifiersNotSupported,
    /// The Server does not support Wildcard subscription; the subscription is not accepted.
    WildcardSubscriptionsNotSupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisconnectProperties {
    /// Session Expiry Interval in seconds
    pub session_expiry_interval: Option<u32>,

    /// Human readable reason for the disconnect
    pub reason_string: Option<String>,

    /// List of user properties
    pub user_properties: Vec<(String, String)>,

    /// String which can be used by the Client to identify another Server to use.
    pub server_reference: Option<String>,
}
//------------------------------------------------------------------------

/// Quality of service
#[repr(u8)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd)]
#[allow(clippy::enum_variant_names)]
pub enum QoS {
    #[default]
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

/// Maps a number to QoS
pub fn qos(num: u8) -> Option<QoS> {
    match num {
        0 => Some(QoS::AtMostOnce),
        1 => Some(QoS::AtLeastOnce),
        2 => Some(QoS::ExactlyOnce),
        qos => None,
    }
}

/// Checks if a topic or topic filter has wildcards
pub fn has_wildcards(s: &str) -> bool {
    s.contains('+') || s.contains('#')
}

/// Checks if a topic is valid
pub fn valid_topic(topic: &str) -> bool {
    if topic.contains('+') {
        return false;
    }

    if topic.contains('#') {
        return false;
    }

    true
}

/// Checks if the filter is valid
///
/// <https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106>
pub fn valid_filter(filter: &str) -> bool {
    if filter.is_empty() {
        return false;
    }

    let hirerarchy = filter.split('/').collect::<Vec<&str>>();
    if let Some((last, remaining)) = hirerarchy.split_last() {
        for entry in remaining.iter() {
            // # is not allowed in filter except as a last entry
            // invalid: sport/tennis#/player
            // invalid: sport/tennis/#/ranking
            if entry.contains('#') {
                return false;
            }

            // + must occupy an entire level of the filter
            // invalid: sport+
            if entry.len() > 1 && entry.contains('+') {
                return false;
            }
        }

        // only single '#" or '+' is allowed in last entry
        // invalid: sport/tennis#
        // invalid: sport/++
        if last.len() != 1 && (last.contains('#') || last.contains('+')) {
            return false;
        }
    }

    true
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
    #[error("Insufficient number of bytes to frame packet, {0} more bytes required")]
    InsufficientBytes(usize),
}

pub trait Protocol {
    fn read_mut(&mut self, stream: &mut BytesMut, max_size: usize) -> Result<Packet, Error>;
    fn write(&self, packet: Packet, write: &mut BytesMut) -> Result<usize, Error>;
}

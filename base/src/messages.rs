#![allow(dead_code, unused)]

use crate::xchg::Size;
use arrow::record_batch::RecordBatch;
use std::{io, slice::Iter, str::Utf8Error, string::FromUtf8Error};

#[derive(Debug, Clone, PartialEq)]
pub struct Data {
    pub topic: String,
    pub records: RecordBatch,
}

/// This module is the place where all the protocol specifics gets abstracted
/// out and creates a structures which are common across protocols. Since,
/// MQTT is the core protocol that this broker supports, a lot of structs closely
/// map to what MQTT specifies in its protocol

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Packet {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(PubAck),
    PingReq(PingReq),
    PingResp(PingResp),
    Subscribe(Subscribe),
    SubAck(SubAck),
    PubRec(PubRec),
    PubRel(PubRel),
    PubComp(PubComp),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    Disconnect(Disconnect),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Connect,
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

/// Approximate inmemory size of a packet.
/// Move this to protocol module later. IO should rely on Protocol trait to get
/// actual size of a packet
impl Packet {
    // pub fn length(&self) -> usize {
    //     match self {
    //         Packet::Connect(p) => v4::connect::len(p),
    //         Packet::ConnAck(_) => v4::connack::len(),
    //         Packet::Publish(p) => v4::publish::len(p),
    //         Packet::PubAck(p) => v4::puback::len(),
    //         Packet::PingReq(p) => 2,
    //         Packet::PingResp(p) => 2,
    //         Packet::Subscribe(p) => v4::subscribe::len(p),
    //         Packet::SubAck(p) => v4::suback::len(p),
    //         Packet::PubRec(p) => v4::pubrec::len(),
    //         Packet::PubRel(p) => v4::pubrel::len(),
    //         Packet::PubComp(p) => v4::pubcomp::len(),
    //         Packet::Unsubscribe(p) => v4::unsubscribe::len(p),
    //         Packet::UnsubAck(p) => v4::unsuback::len(p),
    //         Packet::Disconnect(p) => 2,
    //     }
    // }
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
    /// Login details
    pub login: Option<Login>,
    /// Last will details
    pub last_will: Option<LastWill>,
    /// Connect properties
    pub properties: Option<ConnectProperties>,
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
    pub authentication_data: Option<Vec<u8>>,
}

/// LastWill that broker forwards on behalf of the client
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastWill {
    pub topic: Vec<u8>,
    pub message: Vec<u8>,
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
    pub correlation_data: Option<Vec<u8>>,
    pub user_properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Login {
    pub username: String,
    pub password: String,
}

impl From<Packet> for Result<Connect, Error> {
    fn from(packet: Packet) -> Self {
        match packet {
            Packet::Connect(connect) => Ok(connect),
            packet => Err(Error::IncorrectPacketFormat),
        }
    }
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
    pub properties: Option<ConnAckProperties>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub authentication_data: Option<Vec<u8>>,
}

//--------------------------- Publish packet -------------------------------

/// Publish packet
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub pkid: u16,
    pub retain: bool,
    pub topic: Vec<u8>,
    pub payload: Vec<u8>,
    pub properties: Option<PublishProperties>,
}

impl Publish {
    // Constructor for publish. Used in local links as local links shouldn't
    // send qos 1 or 2 packets
    pub fn new<T: Into<Vec<u8>>>(topic: T, payload: T, retain: bool) -> Publish {
        Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            pkid: 0,
            retain,
            topic: topic.into(),
            payload: payload.into(),
            properties: None,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishProperties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub topic_alias: Option<u16>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
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
    pub properties: Option<PubAckProperties>,
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
    pub properties: Option<SubscribeProperties>,
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
    pub properties: Option<SubAckProperties>,
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
    pub properties: Option<UnsubscribeProperties>,
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
    pub properties: Option<UnsubAckProperties>,
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
    pub properties: Option<PubRecProperties>,
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
    pub properties: Option<PubCompProperties>,
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
    pub properties: Option<PubRelProperties>,
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
    pub properties: Option<DisconnectProperties>,
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
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QoS {
    #[default]
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl TryFrom<u8> for QoS {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            qos => Err(Error::InvalidQoS(qos)),
        }
    }
}

impl From<QoS> for u8 {
    fn from(value: QoS) -> u8 {
        value as u8
    }
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

    // rev() is used so we can easily get the last entry
    let mut hirerarchy = filter.split('/').rev();

    // split will never return an empty iterator
    // even if the pattern isn't matched, the original string will be there
    // so it is safe to just unwrap here!
    let last = hirerarchy.next().unwrap();

    // only single '#" or '+' is allowed in last entry
    // invalid: sport/tennis#
    // invalid: sport/++
    if last.len() != 1 && (last.contains('#') || last.contains('+')) {
        return false;
    }

    // remaining entries
    for entry in hirerarchy {
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

    true
}

/// Error during serialization and deserialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("Invalid packet format")]
    IncorrectPacketFormat,
    #[error("Invalid packet type = {0}")]
    InvalidPacketType(u8),
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
    #[error("Invalid property type = {0}")]
    InvalidPropertyType(u8),
    #[error("This packet is not yet supported")]
    NotSupportedYet,
}

// Define a trait for types that can be converted to RecordBatch
pub trait IntoRecordBatch {
    fn into_record_batch(&self) -> RecordBatch;
}

impl IntoRecordBatch for Data {
    fn into_record_batch(&self) -> RecordBatch {
        // Implement the conversion from Data to RecordBatch
        unimplemented!("Conversion from Data to RecordBatch not implemented")
    }
}

// ------------------------------------------------------------------------
// Size implementations
// ------------------------------------------------------------------------

impl Size for Data {
    fn size(&self) -> usize {
        let mut size = self.topic.len();
        size += self.records.get_array_memory_size();
        size
    }
}

impl Size for Publish {
    // Approximate size of a publish packet
    fn size(&self) -> usize {
        2 + self.topic.len() + self.payload.len()
    }
}

impl Size for SubAck {
    fn size(&self) -> usize {
        2 + self.return_codes.len()
    }
}

impl Size for UnsubAck {
    fn size(&self) -> usize {
        2 + self.reasons.len()
    }
}

// TODO: Validate size of each packet
impl Size for Packet {
    fn size(&self) -> usize {
        match self {
            Packet::Connect(connect) => connect.size(),
            Packet::ConnAck(connack) => connack.size(),
            Packet::Publish(publish) => publish.size(),
            Packet::PubAck(puback) => puback.size(),
            Packet::PubRec(pubrec) => pubrec.size(),
            Packet::PubRel(pubrel) => pubrel.size(),
            Packet::PubComp(pubcomp) => pubcomp.size(),
            Packet::PingReq(_) => 0,
            Packet::PingResp(_) => 0,
            Packet::Disconnect(disconnect) => disconnect.size(),
            Packet::Subscribe(subscribe) => subscribe.size(),
            Packet::SubAck(sub_ack) => sub_ack.size(),
            Packet::Unsubscribe(unsubscribe) => unsubscribe.size(),
            Packet::UnsubAck(unsub_ack) => unsub_ack.size(),
        }
    }
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

// Add these implementations before the Size impl for Packet

impl Size for Connect {
    fn size(&self) -> usize {
        let mut size = 2 + // fixed header
            2 + // keep alive
            self.client_id.len();
        
        if let Some(login) = &self.login {
            size += login.username.len() + login.password.len();
        }
        
        if let Some(last_will) = &self.last_will {
            size += last_will.topic.len() + last_will.message.len();
        }
        
        size
    }
}

impl Size for ConnAck {
    fn size(&self) -> usize {
        4 // Fixed size for ConnAck packet
    }
}

impl Size for PubAck {
    fn size(&self) -> usize {
        4 // Fixed size for PubAck packet (2 bytes fixed header + 2 bytes packet id)
    }
}

impl Size for PubRec {
    fn size(&self) -> usize {
        4 // Fixed size for PubRec packet
    }
}

impl Size for PubRel {
    fn size(&self) -> usize {
        4 // Fixed size for PubRel packet
    }
}

impl Size for PubComp {
    fn size(&self) -> usize {
        4 // Fixed size for PubComp packet
    }
}

impl Size for Disconnect {
    fn size(&self) -> usize {
        2 // Minimum size for Disconnect packet
    }
}

impl Size for Subscribe {
    fn size(&self) -> usize {
        let mut size = 2 + 2; // Fixed header + packet id
        for filter in &self.filters {
            size += 2 + filter.path.len() + 1; // topic length + topic + qos
        }
        size
    }
}

impl Size for Unsubscribe {
    fn size(&self) -> usize {
        let mut size = 2 + 2; // Fixed header + packet id
        for filter in &self.filters {
            size += 2 + filter.len(); // topic length + topic
        }
        size
    }
}

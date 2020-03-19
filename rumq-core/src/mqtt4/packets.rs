use derive_more::From;

use crate::mqtt4::QoS;
use std::fmt;

/// Packet identifier for packets types that require broker to acknowledge
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, From)]
pub struct PacketIdentifier(pub u16);

/// Mqtt protocol version
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    MQTT(u8),
}

/// Mqtt connect packet representation
#[derive(Clone, PartialEq)]
pub struct Connect {
    /// Mqtt protocol version
    pub protocol: Protocol,
    /// Mqtt keep alive time
    pub keep_alive: u16,
    /// Client Id
    pub client_id: String,
    /// Clean session. Asks the broker to clear previous state
    pub clean_session: bool,
    /// Will that broker needs to publish when the client disconnects
    pub last_will: Option<LastWill>,
    /// Username of the client
    pub username: Option<String>,
    /// Password of the client
    pub password: Option<String>,
}

/// Creates a new mqtt connect packet
pub fn connect<S: Into<String>>(id: S) -> Connect {
    Connect {
        protocol: Protocol::MQTT(4),
        keep_alive: 10,
        client_id: id.into(),
        clean_session: true,
        last_will: None,
        username: None,
        password: None,
    }
}

impl Connect {
    /// Sets username
    pub fn set_username<S: Into<String>>(&mut self, u: S) -> &mut Connect {
        self.username = Some(u.into());
        self
    }

    /// Sets password
    pub fn set_password<S: Into<String>>(&mut self, p: S) -> &mut Connect {
        self.password = Some(p.into());
        self
    }

    pub(crate) fn len(&self) -> usize {
        let mut len = 8 + "MQTT".len() + self.client_id.len();

        // lastwill len
        if let Some(ref last_will) = self.last_will {
            len += 4 + last_will.topic.len() + last_will.message.len();
        }

        // username len
        if let Some(ref username) = self.username {
            len += 2 + username.len();
        }

        // passwork len
        if let Some(ref password) = self.password {
            len += 2 + password.len();
        }

        len
    }
}

/// Connection return code sent by the server
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum ConnectReturnCode {
    Accepted = 0,
    RefusedProtocolVersion,
    RefusedIdentifierRejected,
    ServerUnavailable,
    BadUsernamePassword,
    NotAuthorized,
}

/// Connack packet
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Connack {
    pub session_present: bool,
    pub code: ConnectReturnCode,
}

/// Creates a new connack packet
pub fn connack(code: ConnectReturnCode, session_present: bool) -> Connack {
    Connack { code, session_present }
}

/// Last will of the connection
#[derive(Debug, Clone, PartialEq)]
pub struct LastWill {
    pub topic: String,
    pub message: String,
    pub qos: QoS,
    pub retain: bool,
}

/// Publish packet
#[derive(Clone, PartialEq)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic_name: String,
    pub pkid: Option<PacketIdentifier>,
    pub payload: Vec<u8>,
}

/// Creates a new publish packet
//TODO: maybe this should become private, or just removed and inserted into Publish::new()
pub fn publish<S: Into<String>, P: Into<Vec<u8>>>(topic: S, qos: QoS, payload: P) -> Publish {
    Publish {
        dup: false,
        qos,
        retain: false,
        pkid: None,
        topic_name: topic.into(),
        payload: payload.into(),
    }
}

impl Publish {
    pub fn new<S: Into<String>, P: Into<Vec<u8>>>(topic: S, qos: QoS, payload: P) -> Self {
        publish(topic, qos, payload)
    }

    /// Sets packet identifier
    pub fn set_pkid<P: Into<PacketIdentifier>>(&mut self, pkid: P) -> &mut Self {
        self.pkid = Some(pkid.into());
        self
    }
    pub fn set_retain(&mut self, retain: bool) -> &mut Self {
        self.retain = retain;
        self
    }
}

/// Subscriber packet
#[derive(Clone, PartialEq)]
pub struct Subscribe {
    pub pkid: PacketIdentifier,
    pub topics: Vec<SubscribeTopic>,
}

//TODO: maybe this should become private, or just removed and inserted into Subscribe::new()
/// Creates a new subscription packet
pub fn subscribe<S: Into<String>>(topic: S, qos: QoS) -> Subscribe {
    let topic = SubscribeTopic {
        topic_path: topic.into(),
        qos,
    };

    Subscribe {
        pkid: PacketIdentifier(0),
        topics: vec![topic],
    }
}

/// Creates an empty subscription packet
pub fn empty_subscribe() -> Subscribe {
    Subscribe {
        pkid: PacketIdentifier(0),
        topics: Vec::new(),
    }
}

impl Subscribe {
    pub fn new<S: Into<String>>(topic: S, qos: QoS) -> Self {
        subscribe(topic, qos)
    }
    pub fn add(&mut self, topic: String, qos: QoS) -> &mut Self {
        let topic = SubscribeTopic { topic_path: topic, qos };
        self.topics.push(topic);
        self
    }
}

/// Subscription topic
#[derive(Clone, PartialEq)]
pub struct SubscribeTopic {
    pub topic_path: String,
    pub qos: QoS,
}

/// Subscription return code sent by the broker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeReturnCodes {
    Success(QoS),
    Failure,
}

/// Subscription acknowledgement
#[derive(Debug, Clone, PartialEq)]
pub struct Suback {
    pub pkid: PacketIdentifier,
    pub return_codes: Vec<SubscribeReturnCodes>,
}

/// Creates a new subscription acknowledgement packet
pub fn suback(pkid: PacketIdentifier, return_codes: Vec<SubscribeReturnCodes>) -> Suback {
    Suback { pkid, return_codes }
}

/// Unsubscribe packet
#[derive(Debug, Clone, PartialEq)]
pub struct Unsubscribe {
    pub pkid: PacketIdentifier,
    pub topics: Vec<String>,
}

impl fmt::Debug for Publish {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Topic = {}, Qos = {:?}, Retain = {}, Pkid = {:?}, Payload Size = {}",
            self.topic_name,
            self.qos,
            self.retain,
            self.pkid,
            self.payload.len()
        )
    }
}

impl fmt::Debug for Connect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Protocol = {:?}, Keep alive = {:?}, Client id = {}, Clean session = {}",
            self.protocol, self.keep_alive, self.client_id, self.clean_session,
        )
    }
}

impl fmt::Debug for Subscribe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Filters = {:?}, Packet id = {:?}", self.pkid, self.topics)
    }
}

impl fmt::Debug for SubscribeTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Filter = {}, Qos = {:?}", self.topic_path, self.qos)
    }
}

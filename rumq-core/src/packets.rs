use derive_more::From;

use crate::QoS;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, From)]
pub struct PacketIdentifier(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    MQTT(u8),
}

#[derive(Debug, Clone, PartialEq, Getters, Setters)]
#[get = "pub"]
pub struct Connect {
    /// Mqtt protocol version
    pub(crate) protocol: Protocol,
    /// Mqtt keep alive time
    #[set = "pub"]
    pub(crate) keep_alive: u16,
    /// Client Id
    pub(crate) client_id: String,
    /// Clean session. Asks the broker to clear previous state
    #[set = "pub"]
    pub(crate) clean_session: bool,
    /// Will that broker needs to publish when the client disconnects
    pub(crate) last_will: Option<LastWill>,
    /// Username of the client
    pub(crate) username: Option<String>,
    /// Password of the client
    pub(crate) password: Option<String>,
}

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
    pub fn set_username<S: Into<String>>(&mut self, u: S) -> &mut Connect {
        self.username = Some(u.into());
        self
    }

    pub fn set_password<S: Into<String>>(&mut self, p: S) -> &mut Connect {
        self.password = Some(p.into());
        self
    }

    pub fn len(&self) -> usize {
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

#[derive(Debug, Clone, Copy, PartialEq, Getters)]
#[get = "pub"]
pub struct Connack {
    pub(crate) session_present: bool,
    pub(crate) code: ConnectReturnCode,
}

pub fn connack(code: ConnectReturnCode, session_present: bool) -> Connack {
    Connack { code, session_present }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LastWill {
    pub(crate) topic: String,
    pub(crate) message: String,
    pub(crate) qos: QoS,
    pub(crate) retain: bool,
}

#[derive(Debug, Clone, PartialEq, Getters, Setters)]
#[get = "pub"]
pub struct Publish {
    #[set = "pub"]
    pub(crate) dup: bool,
    #[set = "pub"]
    pub(crate) qos: QoS,
    #[set = "pub"]
    pub(crate) retain: bool,
    pub(crate) topic_name: String,
    pub(crate) pkid: Option<PacketIdentifier>,
    pub payload: Arc<Vec<u8>>,
}

pub fn publish<S: Into<String>, P: Into<Vec<u8>>>(topic: S, payload: P) -> Publish {
    Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        pkid: None,
        topic_name: topic.into(),
        payload: Arc::new(payload.into()),
    }
}

impl Publish {
    pub fn set_pkid<P: Into<PacketIdentifier>>(&mut self, pkid: P) -> &mut Self {
        self.pkid = Some(pkid.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Getters, Setters)]
#[get = "pub"]
pub struct Subscribe {
    #[set = "pub"]
    pub(crate) pkid: PacketIdentifier,
    pub(crate) topics: Vec<SubscribeTopic>,
}

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

pub fn empty_subscribe() -> Subscribe {
    Subscribe {
        pkid: PacketIdentifier(0),
        topics: Vec::new(),
    }
}

impl Subscribe {
    pub fn add(&mut self, topic: String, qos: QoS) -> &mut Self {
        let topic = SubscribeTopic { topic_path: topic, qos };
        self.topics.push(topic);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Getters)]
#[get = "pub"]
pub struct SubscribeTopic {
    pub(crate) topic_path: String,
    pub(crate) qos: QoS,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeReturnCodes {
    Success(QoS),
    Failure,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Suback {
    pub(crate) pkid: PacketIdentifier,
    pub(crate) return_codes: Vec<SubscribeReturnCodes>,
}

pub fn suback(pkid: PacketIdentifier, return_codes: Vec<SubscribeReturnCodes>) -> Suback {
    Suback { pkid, return_codes }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Unsubscribe {
    pub(crate) pkid: PacketIdentifier,
    pub(crate) topics: Vec<String>,
}

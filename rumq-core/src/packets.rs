#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PacketIdentifier(pub u16);

pub mod connect {
    use super::lastwill::LastWill;
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Protocol {
        MQTT(u8),
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct Connect {
        pub(crate) protocol: Protocol,
        pub(crate) keep_alive: u16,
        pub(crate) client_id: String,
        pub(crate) clean_session: bool,
        pub(crate) last_will: Option<LastWill>,
        pub(crate) username: Option<String>,
        pub(crate) password: Option<String>,
    }

    pub fn new<S>(id: S) -> Connect
    where
        S: Into<String>,
    {
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
        pub fn set_keep_alive(mut self, keep_alive: u16) -> Connect {
            self.keep_alive = keep_alive;
            self
        }

        pub fn set_clean_session(mut self, c: bool) -> Connect {
            self.clean_session = c;
            self
        }

        pub fn set_username<S>(mut self, u: S) -> Connect
        where
            S: Into<String>,
        {
            self.username = Some(u.into());
            self
        }

        pub fn set_password<S>(mut self, p: S) -> Connect
        where
            S: Into<String>,
        {
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
}

pub mod connack {
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

    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct Connack {
        pub(crate) session_present: bool,
        pub(crate) code: ConnectReturnCode,
    }
}

pub mod lastwill {
    use crate::QoS;

    #[derive(Debug, Clone, PartialEq)]
    pub struct LastWill {
        pub(crate) topic: String,
        pub(crate) message: String,
        pub(crate) qos: QoS,
        pub(crate) retain: bool,
    }
}

pub mod publish {
    use super::PacketIdentifier;
    use crate::QoS;
    use std::sync::Arc;

    #[derive(Debug, Clone, PartialEq)]
    pub struct Publish {
        pub(crate) dup: bool,
        pub(crate) qos: QoS,
        pub(crate) retain: bool,
        pub(crate) topic_name: String,
        pub(crate) pkid: Option<PacketIdentifier>,
        pub(crate) payload: Arc<Vec<u8>>,
    }
}

pub mod subscribe {
    use super::PacketIdentifier;
    use crate::QoS;

    #[derive(Debug, Clone, PartialEq)]
    pub struct Subscribe {
        pub(crate) pkid: PacketIdentifier,
        pub(crate) topics: Vec<SubscribeTopic>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct SubscribeTopic {
        pub(crate) topic_path: String,
        pub(crate) qos: QoS,
    }
}

pub mod suback {
    use super::PacketIdentifier;
    use crate::QoS;

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
}

pub mod unsubscribe {
    use super::PacketIdentifier;

    #[derive(Debug, Clone, PartialEq)]
    pub struct Unsubscribe {
        pub(crate) pkid: PacketIdentifier,
        pub(crate) topics: Vec<String>,
    }
}

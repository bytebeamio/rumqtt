use crate::packets::*;
use crate::{Error, FixedHeader};
use bytes::buf::Buf;
use bytes::Bytes;
use core::fmt::Debug;

use crate::{qos, Protocol, QoS};
use alloc::string::String;
use alloc::vec::Vec;

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

/// Connection packet initiated by the client
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

fn extract_last_will(connect_flags: u8, mut bytes: &mut Bytes) -> Result<Option<LastWill>, Error> {
    let last_will = match connect_flags & 0b100 {
        0 if (connect_flags & 0b0011_1000) != 0 => {
            return Err(Error::IncorrectPacketFormat);
        }
        0 => None,
        _ => {
            let will_topic = read_mqtt_string(&mut bytes)?;
            let will_message = read_mqtt_string(&mut bytes)?;
            let will_qos = qos((connect_flags & 0b11000) >> 3)?;
            Some(LastWill {
                topic: will_topic,
                message: will_message,
                qos: will_qos,
                retain: (connect_flags & 0b0010_0000) != 0,
            })
        }
    };

    Ok(last_will)
}

fn extract_username_password(
    connect_flags: u8,
    mut bytes: &mut Bytes,
) -> Result<(Option<String>, Option<String>), Error> {
    let username = match connect_flags & 0b1000_0000 {
        0 => None,
        _ => Some(read_mqtt_string(&mut bytes)?),
    };

    let password = match connect_flags & 0b0100_0000 {
        0 => None,
        _ => Some(read_mqtt_string(&mut bytes)?),
    };

    Ok((username, password))
}

impl Connect {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Connect, Error> {
        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);
        let protocol_name = read_mqtt_string(&mut bytes)?;
        let protocol_level = bytes.get_u8();
        if protocol_name != "MQTT" {
            return Err(Error::InvalidProtocol);
        }

        let protocol = match protocol_level {
            4 => Protocol::MQTT(4),
            num => return Err(Error::InvalidProtocolLevel(num)),
        };

        let connect_flags = bytes.get_u8();
        let keep_alive = bytes.get_u16();
        let clean_session = (connect_flags & 0b10) != 0;
        let client_id = read_mqtt_string(&mut bytes)?;
        let last_will = extract_last_will(connect_flags, &mut bytes)?;
        let (username, password) = extract_username_password(connect_flags, &mut bytes)?;

        let connect = Connect {
            protocol,
            keep_alive,
            client_id,
            clean_session,
            last_will,
            username,
            password,
        };

        Ok(connect)
    }
}

/// Acknowledgement to connect packet
#[derive(Debug, Clone, PartialEq)]
pub struct ConnAck {
    pub session_present: bool,
    pub code: ConnectReturnCode,
}

impl ConnAck {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);

        if fixed_header.remaining_len != 2 {
            return Err(Error::PayloadSizeIncorrect);
        }

        let flags = bytes.get_u8();
        let return_code = bytes.get_u8();

        let session_present = (flags & 0x01) == 1;
        let code = connect_return(return_code)?;
        let connack = ConnAck {
            session_present,
            code,
        };

        Ok(connack)
    }
}

/// Publish packet
#[derive(Clone, PartialEq)]
pub struct Publish {
    pub qos: QoS,
    pub pkid: u16,
    pub topic: String,
    pub payload: Bytes,
    pub dup: bool,
    pub retain: bool,
    pub bytes: Bytes,
}

impl Publish {
    pub(crate) fn assemble(fixed_header: FixedHeader, bytes: Bytes) -> Result<Self, Error> {
        let mut payload = bytes.clone();
        let qos = qos((fixed_header.byte1 & 0b0110) >> 1)?;
        let dup = (fixed_header.byte1 & 0b1000) != 0;
        let retain = (fixed_header.byte1 & 0b0001) != 0;

        let variable_header_index = fixed_header.header_len;
        payload.advance(variable_header_index);
        let topic = read_mqtt_string(&mut payload)?;

        // Packet identifier exists where QoS > 0
        let pkid = match qos {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce | QoS::ExactlyOnce => payload.get_u16(),
        };

        if qos != QoS::AtMostOnce && pkid == 0 {
            return Err(Error::PacketIdZero);
        }

        let publish = Publish {
            qos,
            pkid,
            topic,
            payload,
            dup,
            retain,
            bytes,
        };

        Ok(publish)
    }
}

/// Acknowledgement to QoS1 publish
#[derive(Debug, Clone, PartialEq)]
pub struct PubAck {
    pub pkid: u16,
}

impl PubAck {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        if fixed_header.remaining_len != 2 {
            return Err(Error::PayloadSizeIncorrect);
        }

        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        let puback = PubAck { pkid };

        Ok(puback)
    }
}

/// Acknowledgement to QoS2 publish
#[derive(Debug, Clone, PartialEq)]
pub struct PubRec {
    pub pkid: u16,
}

impl PubRec {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        if fixed_header.remaining_len != 2 {
            return Err(Error::PayloadSizeIncorrect);
        }

        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        let pubrec = PubRec { pkid };

        Ok(pubrec)
    }
}

/// Acknowledgement to pubrec
#[derive(Debug, Clone, PartialEq)]
pub struct PubRel {
    pub pkid: u16,
}

impl PubRel {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        if fixed_header.remaining_len != 2 {
            return Err(Error::PayloadSizeIncorrect);
        }

        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        let pubrel = PubRel { pkid };

        Ok(pubrel)
    }
}

/// Acknowledgement to pubrel
#[derive(Debug, Clone, PartialEq)]
pub struct PubComp {
    pub pkid: u16,
}

impl PubComp {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        if fixed_header.remaining_len != 2 {
            return Err(Error::PayloadSizeIncorrect);
        }

        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        let pubcomp = PubComp { pkid };

        Ok(pubcomp)
    }
}

/// Subscription packet
#[derive(Clone, PartialEq)]
pub struct Subscribe {
    pub pkid: u16,
    pub topics: Vec<SubscribeTopic>,
}

impl Subscribe {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();

        // variable header size = 2 (packet identifier)
        let mut payload_bytes = fixed_header.remaining_len - 2;
        let mut topics = Vec::new();

        while payload_bytes > 0 {
            let topic_filter = read_mqtt_string(&mut bytes)?;
            let requested_qos = bytes.get_u8();
            payload_bytes -= topic_filter.len() + 3;
            topics.push(SubscribeTopic {
                topic_path: topic_filter,
                qos: qos(requested_qos)?,
            });
        }

        let subscribe = Subscribe { pkid, topics };

        Ok(subscribe)
    }
}

/// Acknowledgement to subscribe
#[derive(Debug, Clone, PartialEq)]
pub struct SubAck {
    pub pkid: u16,
    pub return_codes: Vec<SubscribeReturnCodes>,
}

impl SubAck {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);

        let pkid = bytes.get_u16();
        let mut payload_bytes = fixed_header.remaining_len - 2;
        let mut return_codes = Vec::with_capacity(payload_bytes);

        while payload_bytes > 0 {
            let return_code = bytes.get_u8();
            if return_code >> 7 == 1 {
                return_codes.push(SubscribeReturnCodes::Failure)
            } else {
                return_codes.push(SubscribeReturnCodes::Success(qos(return_code & 0x3)?));
            }
            payload_bytes -= 1
        }
        let suback = SubAck { pkid, return_codes };

        Ok(suback)
    }
}

/// Unsubscribe packet
#[derive(Debug, Clone, PartialEq)]
pub struct Unsubscribe {
    pub pkid: u16,
    pub topics: Vec<String>,
}

impl Unsubscribe {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        let mut payload_bytes = fixed_header.remaining_len - 2;
        let mut topics = Vec::with_capacity(1);

        while payload_bytes > 0 {
            let topic_filter = read_mqtt_string(&mut bytes)?;
            payload_bytes -= topic_filter.len() + 2;
            topics.push(topic_filter);
        }

        let unsubscribe = Unsubscribe { pkid, topics };

        Ok(unsubscribe)
    }
}

/// Acknowledgement to unsubscribe
#[derive(Debug, Clone, PartialEq)]
pub struct UnsubAck {
    pub pkid: u16,
}

impl UnsubAck {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        if fixed_header.remaining_len != 2 {
            return Err(Error::PayloadSizeIncorrect);
        }

        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        let unsuback = UnsubAck { pkid };

        Ok(unsuback)
    }
}

/// Reads a string from bytes stream
fn read_mqtt_string(stream: &mut Bytes) -> Result<String, Error> {
    let len = stream.get_u16() as usize;
    // Invalid packets which reached this point (simulated invalid packets actually triggered this)
    // should not cause the split to cross boundaries
    if len > stream.len() {
        return Err(Error::BoundaryCrossed);
    }

    let s = stream.split_to(len);
    match String::from_utf8(s.to_vec()) {
        Ok(v) => Ok(v),
        Err(_e) => Err(Error::TopicNotUtf8),
    }
}

/// Connection return code type
fn connect_return(num: u8) -> Result<ConnectReturnCode, Error> {
    match num {
        0 => Ok(ConnectReturnCode::Accepted),
        1 => Ok(ConnectReturnCode::BadUsernamePassword),
        2 => Ok(ConnectReturnCode::NotAuthorized),
        3 => Ok(ConnectReturnCode::RefusedIdentifierRejected),
        4 => Ok(ConnectReturnCode::RefusedProtocolVersion),
        5 => Ok(ConnectReturnCode::ServerUnavailable),
        num => Err(Error::InvalidConnectReturnCode(num)),
    }
}

#[cfg(test)]
mod test {
    use crate::*;
    use alloc::borrow::ToOwned;
    use alloc::vec;
    use bytes::{Bytes, BytesMut};
    use pretty_assertions::assert_eq;

    #[test]
    fn connect_stitching_works_correctlyl() {
        let mut stream = bytes::BytesMut::new();
        let packetstream = &[
            0x10,
            39, // packet type, flags and remaining len
            0x00,
            0x04,
            b'M',
            b'Q',
            b'T',
            b'T',
            0x04,        // variable header
            0b1100_1110, // variable header. +username, +password, -will retain, will qos=1, +last_will, +clean_session
            0x00,
            0x0a, // variable header. keep alive = 10 sec
            0x00,
            0x04,
            b't',
            b'e',
            b's',
            b't', // payload. client_id
            0x00,
            0x02,
            b'/',
            b'a', // payload. will topic = '/a'
            0x00,
            0x07,
            b'o',
            b'f',
            b'f',
            b'l',
            b'i',
            b'n',
            b'e', // payload. variable header. will msg = 'offline'
            0x00,
            0x04,
            b'r',
            b'u',
            b'm',
            b'q', // payload. username = 'rumq'
            0x00,
            0x02,
            b'm',
            b'q', // payload. password = 'mq'
            0xDE,
            0xAD,
            0xBE,
            0xEF, // extra packets in the stream
        ];

        stream.extend_from_slice(&packetstream[..]);
        let packet = mqtt_read(&mut stream, 100).unwrap();
        let packet = match packet {
            Packet::Connect(connect) => connect,
            packet => panic!("Invalid packet = {:?}", packet),
        };

        assert_eq!(
            packet,
            Connect {
                protocol: Protocol::MQTT(4),
                keep_alive: 10,
                client_id: "test".to_owned(),
                clean_session: true,
                last_will: Some(LastWill {
                    topic: "/a".to_owned(),
                    message: "offline".to_owned(),
                    retain: false,
                    qos: QoS::AtLeastOnce
                }),
                username: Some("rumq".to_owned()),
                password: Some("mq".to_owned())
            }
        );
    }

    #[test]
    fn connack_stitching_works_correctly() {
        let mut stream = bytes::BytesMut::new();
        let packetstream = &[
            0b0010_0000,
            0x02, // packet type, flags and remaining len
            0x01,
            0x00, // variable header. connack flags, connect return code
            0xDE,
            0xAD,
            0xBE,
            0xEF, // extra packets in the stream
        ];

        stream.extend_from_slice(&packetstream[..]);
        let packet = mqtt_read(&mut stream, 100).unwrap();
        let packet = match packet {
            Packet::ConnAck(packet) => packet,
            packet => panic!("Invalid packet = {:?}", packet),
        };

        assert_eq!(
            packet,
            ConnAck {
                session_present: true,
                code: ConnectReturnCode::Accepted
            }
        );
    }

    #[test]
    fn qos1_publish_stitching_works_correctly() {
        let stream = &[
            0b0011_0010,
            11, // packet type, flags and remaining len
            0x00,
            0x03,
            b'a',
            b'/',
            b'b', // variable header. topic name = 'a/b'
            0x00,
            0x0a, // variable header. pkid = 10
            0xF1,
            0xF2,
            0xF3,
            0xF4, // publish payload
            0xDE,
            0xAD,
            0xBE,
            0xEF, // extra packets in the stream
        ];

        let bytes = &[
            0b0011_0010,
            11, // packet type, flags and remaining len
            0x00,
            0x03,
            b'a',
            b'/',
            b'b', // variable header. topic name = 'a/b'
            0x00,
            0x0a, // variable header. pkid = 10
            0xF1,
            0xF2,
            0xF3,
            0xF4, // publish payload
        ];

        let mut stream = BytesMut::from(&stream[..]);
        let bytes = Bytes::from(&bytes[..]);

        let packet = mqtt_read(&mut stream, 100).unwrap();
        let packet = match packet {
            Packet::Publish(packet) => packet,
            packet => panic!("Invalid packet = {:?}", packet),
        };

        let payload = &[0xF1, 0xF2, 0xF3, 0xF4];
        assert_eq!(
            packet,
            Publish {
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic: "a/b".to_owned(),
                pkid: 10,
                payload: Bytes::from(&payload[..]),
                bytes
            }
        );
    }

    #[test]
    fn qos0_publish_stitching_works_correctly() {
        let stream = &[
            0b0011_0000,
            7, // packet type, flags and remaining len
            0x00,
            0x03,
            b'a',
            b'/',
            b'b', // variable header. topic name = 'a/b'
            0x01,
            0x02, // payload
            0xDE,
            0xAD,
            0xBE,
            0xEF, // extra packets in the stream
        ];
        let bytes = &[
            0b0011_0000,
            7, // packet type, flags and remaining len
            0x00,
            0x03,
            b'a',
            b'/',
            b'b', // variable header. topic name = 'a/b'
            0x01,
            0x02, // payloa/home/tekjar/.local/share/JetBrains/Toolbox/bin/cliond
        ];
        let mut stream = BytesMut::from(&stream[..]);
        let bytes = Bytes::from(&bytes[..]);

        let packet = mqtt_read(&mut stream, 100).unwrap();
        let packet = match packet {
            Packet::Publish(packet) => packet,
            packet => panic!("Invalid packet = {:?}", packet),
        };

        assert_eq!(
            packet,
            Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: "a/b".to_owned(),
                pkid: 0,
                payload: Bytes::from(&[0x01, 0x02][..]),
                bytes
            }
        );
    }

    #[test]
    fn puback_stitching_works_correctly() {
        let stream = &[
            0b0100_0000,
            0x02, // packet type, flags and remaining len
            0x00,
            0x0A, // fixed header. packet identifier = 10
            0xDE,
            0xAD,
            0xBE,
            0xEF, // extra packets in the stream
        ];
        let mut stream = BytesMut::from(&stream[..]);

        let packet = mqtt_read(&mut stream, 100).unwrap();
        let packet = match packet {
            Packet::PubAck(packet) => packet,
            packet => panic!("Invalid packet = {:?}", packet),
        };

        assert_eq!(packet, PubAck { pkid: 10 });
    }

    #[test]
    fn subscribe_stitching_works_correctly() {
        let stream = &[
            0b1000_0010,
            20, // packet type, flags and remaining len
            0x01,
            0x04, // variable header. pkid = 260
            0x00,
            0x03,
            b'a',
            b'/',
            b'+', // payload. topic filter = 'a/+'
            0x00, // payload. qos = 0
            0x00,
            0x01,
            b'#', // payload. topic filter = '#'
            0x01, // payload. qos = 1
            0x00,
            0x05,
            b'a',
            b'/',
            b'b',
            b'/',
            b'c', // payload. topic filter = 'a/b/c'
            0x02, // payload. qos = 2
            0xDE,
            0xAD,
            0xBE,
            0xEF, // extra packets in the stream
        ];
        let mut stream = BytesMut::from(&stream[..]);

        let packet = mqtt_read(&mut stream, 100).unwrap();
        let packet = match packet {
            Packet::Subscribe(packet) => packet,
            packet => panic!("Invalid packet = {:?}", packet),
        };

        assert_eq!(
            packet,
            Subscribe {
                pkid: 260,
                topics: vec![
                    SubscribeTopic {
                        topic_path: "a/+".to_owned(),
                        qos: QoS::AtMostOnce
                    },
                    SubscribeTopic {
                        topic_path: "#".to_owned(),
                        qos: QoS::AtLeastOnce
                    },
                    SubscribeTopic {
                        topic_path: "a/b/c".to_owned(),
                        qos: QoS::ExactlyOnce
                    }
                ]
            }
        );
    }

    #[test]
    fn suback_stitching_works_correctly() {
        let stream = vec![
            0x90, 4, // packet type, flags and remaining len
            0x00, 0x0F, // variable header. pkid = 15
            0x01, 0x80, // payload. return codes [success qos1, failure]
            0xDE, 0xAD, 0xBE, 0xEF, // extra packets in the stream
        ];
        let mut stream = BytesMut::from(&stream[..]);

        let packet = mqtt_read(&mut stream, 100).unwrap();
        let packet = match packet {
            Packet::SubAck(packet) => packet,
            packet => panic!("Invalid packet = {:?}", packet),
        };

        assert_eq!(
            packet,
            SubAck {
                pkid: 15,
                return_codes: vec![
                    SubscribeReturnCodes::Success(QoS::AtLeastOnce),
                    SubscribeReturnCodes::Failure
                ]
            }
        );
    }
}

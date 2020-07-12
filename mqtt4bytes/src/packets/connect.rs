use alloc::string::String;
use crate::*;
use super::*;
use bytes::{Bytes, Buf};
use core::fmt;


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


impl Connect {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Connect, Error> {
        let variable_header_index = fixed_header.fixed_len;
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

    pub fn new<S: Into<String>>(id: S) -> Connect {
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

        // password len
        if let Some(ref password) = self.password {
            len += 2 + password.len();
        }

        len
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = self.len();
        buffer.reserve(len);
        buffer.put_u8(0b0001_0000);
        write_remaining_length(buffer, len)?;
        write_mqtt_string(buffer, "MQTT");
        buffer.put_u8(0x04);

        let mut connect_flags = 0;
        if self.clean_session {
            connect_flags |= 0x02;
        }

        match &self.last_will {
            Some(w) if w.retain => connect_flags |= 0x04 | (w.qos as u8) << 3 | 0x20,
            Some(w) => connect_flags |= 0x04 | (w.qos as u8) << 3,
            None => (),
        }

        if self.password.is_some() {
            connect_flags |= 0x40;
        }

        if self.username.is_some() {
            connect_flags |= 0x80;
        }

        buffer.put_u8(connect_flags);
        buffer.put_u16( self.keep_alive);
        write_mqtt_string(buffer, &self.client_id);

        if let Some(ref last_will) = self.last_will {
            write_mqtt_string(buffer, &last_will.topic);
            write_mqtt_string(buffer, &last_will.message);
        }

        if let Some(ref username) = self.username {
            write_mqtt_string(buffer, username);
        }
        if let Some(ref password) = self.password {
            write_mqtt_string(buffer, password);
        }

        Ok(len)
    }
}

/// LastWill that broker forwards on behalf of the client
#[derive(Debug, Clone, PartialEq)]
pub struct LastWill {
    pub topic: String,
    pub message: String,
    pub qos: QoS,
    pub retain: bool,
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

impl fmt::Debug for Connect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Protocol = {:?}, Keep alive = {:?}, Client id = {}, Clean session = {}",
            self.protocol, self.keep_alive, self.client_id, self.clean_session,
        )
    }
}

#[cfg(test)]
mod test {
    use crate::*;
    use alloc::vec;
    use alloc::borrow::ToOwned;
    use pretty_assertions::assert_eq;
    use bytes::BytesMut;

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
    fn write_connect_mqtt_packet_works() {
        let connect = Connect {
            protocol: Protocol::MQTT(4),
            keep_alive: 10,
            client_id: "test".to_owned(),
            clean_session: true,
            last_will: Some(LastWill {
                topic: "/a".to_owned(),
                message: "offline".to_owned(),
                retain: false,
                qos: QoS::AtLeastOnce,
            }),
            username: Some("rust".to_owned()),
            password: Some("mq".to_owned()),
        };

        let mut buf = BytesMut::new();
        connect.write(&mut buf).unwrap();

        assert_eq!(
            buf,
            vec![
                0x10,
                39,
                0x00,
                0x04,
                b'M',
                b'Q',
                b'T',
                b'T',
                0x04,
                0b1100_1110, // +username, +password, -will retain, will qos=1, +last_will, +clean_session
                0x00,
                0x0a, // 10 sec
                0x00,
                0x04,
                b't',
                b'e',
                b's',
                b't', // client_id
                0x00,
                0x02,
                b'/',
                b'a', // will topic = '/a'
                0x00,
                0x07,
                b'o',
                b'f',
                b'f',
                b'l',
                b'i',
                b'n',
                b'e', // will msg = 'offline'
                0x00,
                0x04,
                b'r',
                b'u',
                b's',
                b't', // username = 'rust'
                0x00,
                0x02,
                b'm',
                b'q' // password = 'mq'
            ]
        );
    }
}

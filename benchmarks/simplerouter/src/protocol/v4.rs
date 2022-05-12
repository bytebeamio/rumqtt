#![allow(dead_code)]

use super::*;

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub(crate) mod connect {
    use super::*;
    use bytes::Bytes;

    /// Connection packet initiated by the client
    #[derive(Debug, Clone, PartialEq)]
    pub struct Connect {
        /// Mqtt keep alive time
        pub keep_alive: u16,
        /// Client Id
        pub client_id: String,
        /// Clean session. Asks the broker to clear previous state
        pub clean_session: bool,
        /// Will that broker needs to publish when the client disconnects
        pub last_will: Option<LastWill>,
        /// Login credentials
        pub login: Option<Login>,
    }

    impl Connect {
        pub fn new<S: Into<String>>(id: S) -> Connect {
            Connect {
                keep_alive: 10,
                client_id: id.into(),
                clean_session: true,
                last_will: None,
                login: None,
            }
        }

        pub fn len(&self) -> usize {
            let mut len = 2 + "MQTT".len() // protocol name
                              + 1            // protocol version
                              + 1            // connect flags
                              + 2; // keep alive

            len += 2 + self.client_id.len();

            // last will len
            if let Some(last_will) = &self.last_will {
                len += last_will.len();
            }

            // username and password len
            if let Some(login) = &self.login {
                len += login.len();
            }

            len
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Connect, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);

            // Variable header
            let protocol_name = read_mqtt_bytes(&mut bytes)?;
            let protocol_name = std::str::from_utf8(&protocol_name)?.to_owned();
            let protocol_level = read_u8(&mut bytes)?;
            if protocol_name != "MQTT" {
                return Err(Error::InvalidProtocol);
            }

            if protocol_level != 4 {
                return Err(Error::InvalidProtocolLevel(protocol_level));
            }

            connect_v4_part(bytes)
        }

        pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
            let len = self.len();
            buffer.put_u8(0b0001_0000);
            let count = write_remaining_length(buffer, len)?;
            write_mqtt_string(buffer, "MQTT");
            buffer.put_u8(0x04);

            let flags_index = 1 + count + 2 + 4 + 1;

            let mut connect_flags = 0;
            if self.clean_session {
                connect_flags |= 0x02;
            }

            buffer.put_u8(connect_flags);
            buffer.put_u16(self.keep_alive);
            write_mqtt_string(buffer, &self.client_id);

            if let Some(last_will) = &self.last_will {
                connect_flags |= last_will.write(buffer)?;
            }

            if let Some(login) = &self.login {
                connect_flags |= login.write(buffer);
            }

            // update connect flags
            buffer[flags_index] = connect_flags;
            Ok(len)
        }
    }

    pub(crate) fn connect_v4_part(mut bytes: Bytes) -> Result<Connect, Error> {
        let connect_flags = read_u8(&mut bytes)?;
        let clean_session = (connect_flags & 0b10) != 0;
        let keep_alive = read_u16(&mut bytes)?;

        let client_id = read_mqtt_bytes(&mut bytes)?;
        let client_id = std::str::from_utf8(&client_id)?.to_owned();
        let last_will = LastWill::read(connect_flags, &mut bytes)?;
        let login = Login::read(connect_flags, &mut bytes)?;

        let connect = Connect {
            keep_alive,
            client_id,
            clean_session,
            last_will,
            login,
        };

        Ok(connect)
    }

    /// LastWill that broker forwards on behalf of the client
    #[derive(Debug, Clone, PartialEq)]
    pub struct LastWill {
        pub topic: String,
        pub message: Bytes,
        pub qos: QoS,
        pub retain: bool,
    }

    impl LastWill {
        pub fn _new(
            topic: impl Into<String>,
            payload: impl Into<Vec<u8>>,
            qos: QoS,
            retain: bool,
        ) -> LastWill {
            LastWill {
                topic: topic.into(),
                message: Bytes::from(payload.into()),
                qos,
                retain,
            }
        }

        fn len(&self) -> usize {
            let mut len = 0;
            len += 2 + self.topic.len() + 2 + self.message.len();
            len
        }

        fn read(connect_flags: u8, bytes: &mut Bytes) -> Result<Option<LastWill>, Error> {
            let last_will = match connect_flags & 0b100 {
                0 if (connect_flags & 0b0011_1000) != 0 => {
                    return Err(Error::IncorrectPacketFormat);
                }
                0 => None,
                _ => {
                    let will_topic = read_mqtt_bytes(bytes)?;
                    let will_topic = std::str::from_utf8(&will_topic)?.to_owned();
                    let will_message = read_mqtt_bytes(bytes)?;
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

        fn write(&self, buffer: &mut BytesMut) -> Result<u8, Error> {
            let mut connect_flags = 0;

            connect_flags |= 0x04 | (self.qos as u8) << 3;
            if self.retain {
                connect_flags |= 0x20;
            }

            write_mqtt_string(buffer, &self.topic);
            write_mqtt_bytes(buffer, &self.message);
            Ok(connect_flags)
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct Login {
        username: String,
        password: String,
    }

    impl Login {
        pub fn new<S: Into<String>>(u: S, p: S) -> Login {
            Login {
                username: u.into(),
                password: p.into(),
            }
        }

        fn read(connect_flags: u8, bytes: &mut Bytes) -> Result<Option<Login>, Error> {
            let username = match connect_flags & 0b1000_0000 {
                0 => String::new(),
                _ => {
                    let username = read_mqtt_bytes(bytes)?;
                    std::str::from_utf8(&username)?.to_owned()
                }
            };

            let password = match connect_flags & 0b0100_0000 {
                0 => String::new(),
                _ => {
                    let password = read_mqtt_bytes(bytes)?;
                    std::str::from_utf8(&password)?.to_owned()
                }
            };

            if username.is_empty() && password.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Login { username, password }))
            }
        }

        fn len(&self) -> usize {
            let mut len = 0;

            if !self.username.is_empty() {
                len += 2 + self.username.len();
            }

            if !self.password.is_empty() {
                len += 2 + self.password.len();
            }

            len
        }

        fn write(&self, buffer: &mut BytesMut) -> u8 {
            let mut connect_flags = 0;
            if !self.username.is_empty() {
                connect_flags |= 0x80;
                write_mqtt_string(buffer, &self.username);
            }

            if !self.password.is_empty() {
                connect_flags |= 0x40;
                write_mqtt_string(buffer, &self.password);
            }

            connect_flags
        }
    }
}

pub(crate) mod connack {
    use super::*;
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    /// Return code in connack
    #[derive(Debug, Clone, Copy, PartialEq)]
    #[repr(u8)]
    pub enum ConnectReturnCode {
        Success = 0,
        RefusedProtocolVersion,
        BadClientId,
        ServiceUnavailable,
        BadUserNamePassword,
        NotAuthorized,
    }

    /// Acknowledgement to connect packet
    #[derive(Debug, Clone, PartialEq)]
    pub struct ConnAck {
        pub session_present: bool,
        pub code: ConnectReturnCode,
    }

    impl ConnAck {
        pub fn new(code: ConnectReturnCode, session_present: bool) -> ConnAck {
            ConnAck {
                code,
                session_present,
            }
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);

            let flags = read_u8(&mut bytes)?;
            let return_code = read_u8(&mut bytes)?;

            let session_present = (flags & 0x01) == 1;
            let code = connect_return(return_code)?;
            let connack = ConnAck {
                session_present,
                code,
            };

            Ok(connack)
        }
    }

    pub fn write(
        code: ConnectReturnCode,
        session_present: bool,
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        // sesssion present + code
        let len = 1 + 1;
        buffer.put_u8(0x20);

        let count = write_remaining_length(buffer, len)?;
        buffer.put_u8(session_present as u8);
        buffer.put_u8(code as u8);

        Ok(1 + count + len)
    }

    /// Connection return code type
    fn connect_return(num: u8) -> Result<ConnectReturnCode, Error> {
        match num {
            0 => Ok(ConnectReturnCode::Success),
            1 => Ok(ConnectReturnCode::RefusedProtocolVersion),
            2 => Ok(ConnectReturnCode::BadClientId),
            3 => Ok(ConnectReturnCode::ServiceUnavailable),
            4 => Ok(ConnectReturnCode::BadUserNamePassword),
            5 => Ok(ConnectReturnCode::NotAuthorized),
            num => Err(Error::InvalidConnectReturnCode(num)),
        }
    }
}

pub(crate) mod publish {
    use super::*;
    use bytes::{BufMut, Bytes, BytesMut};

    #[derive(Debug, Clone, PartialEq)]
    pub struct Publish {
        pub fixed_header: FixedHeader,
        pub raw: Bytes,
    }

    impl Publish {
        //         pub fn new<S: Into<String>, P: Into<Vec<u8>>>(topic: S, qos: QoS, payload: P) -> Publish {
        //             Publish {
        //                 dup: false,
        //                 qos,
        //                 retain: false,
        //                 pkid: 0,
        //                 topic: topic.into(),
        //                 payload: Bytes::from(payload.into()),
        //             }
        //         }

        //         pub fn from_bytes<S: Into<String>>(topic: S, qos: QoS, payload: Bytes) -> Publish {
        //             Publish {
        //                 dup: false,
        //                 qos,
        //                 retain: false,
        //                 pkid: 0,
        //                 topic: topic.into(),
        //                 payload,
        //             }
        //         }

        //         pub fn len(&self) -> usize {
        //             let mut len = 2 + self.topic.len();
        //             if self.qos != QoS::AtMostOnce && self.pkid != 0 {
        //                 len += 2;
        //             }

        //             len += self.payload.len();
        //             len
        //         }

        pub fn view_meta(&self) -> Result<(&str, u8, u16, bool, bool), Error> {
            let qos = (self.fixed_header.byte1 & 0b0110) >> 1;
            let dup = (self.fixed_header.byte1 & 0b1000) != 0;
            let retain = (self.fixed_header.byte1 & 0b0001) != 0;

            // FIXME: Remove indexes and use get method
            let stream = &self.raw[self.fixed_header.fixed_header_len..];
            let topic_len = view_u16(stream)? as usize;

            let stream = &stream[2..];
            let topic = view_str(stream, topic_len)?;

            let pkid = match qos {
                0 => 0,
                1 => {
                    let stream = &stream[topic_len..];
                    view_u16(stream)?
                }
                v => return Err(Error::InvalidQoS(v)),
            };

            if qos == 1 && pkid == 0 {
                return Err(Error::PacketIdZero);
            }

            Ok((topic, qos, pkid, dup, retain))
        }

        pub fn view_topic(&self) -> Result<&str, Error> {
            // FIXME: Remove indexes
            let stream = &self.raw[self.fixed_header.fixed_header_len..];
            let topic_len = view_u16(stream)? as usize;

            let stream = &stream[2..];
            let topic = view_str(stream, topic_len)?;
            Ok(topic)
        }

        pub fn take_topic_and_payload(mut self) -> Result<(Bytes, Bytes), Error> {
            let qos = (self.fixed_header.byte1 & 0b0110) >> 1;

            let variable_header_index = self.fixed_header.fixed_header_len;
            self.raw.advance(variable_header_index);
            let topic = read_mqtt_bytes(&mut self.raw)?;

            match qos {
                0 => (),
                1 => self.raw.advance(2),
                v => return Err(Error::InvalidQoS(v)),
            };

            let payload = self.raw;
            Ok((topic, payload))
        }

        pub fn read(fixed_header: FixedHeader, bytes: Bytes) -> Result<Self, Error> {
            let publish = Publish {
                fixed_header,
                raw: bytes,
            };

            Ok(publish)
        }
    }

    pub struct PublishBytes(pub Bytes);

    impl From<PublishBytes> for Result<Publish, Error> {
        fn from(raw: PublishBytes) -> Self {
            let fixed_header = check(raw.0.iter(), 100 * 1024 * 1024)?;
            Ok(Publish {
                fixed_header,
                raw: raw.0,
            })
        }
    }

    pub fn write(
        topic: &str,
        qos: QoS,
        pkid: u16,
        dup: bool,
        retain: bool,
        payload: &[u8],
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        let mut len = 2 + topic.len();
        if qos != QoS::AtMostOnce {
            len += 2;
        }

        len += payload.len();

        let dup = dup as u8;
        let qos = qos as u8;
        let retain = retain as u8;

        buffer.put_u8(0b0011_0000 | retain | qos << 1 | dup << 3);

        let count = write_remaining_length(buffer, len)?;
        write_mqtt_string(buffer, topic);

        if qos != 0 {
            if pkid == 0 {
                return Err(Error::PacketIdZero);
            }

            buffer.put_u16(pkid);
        }

        buffer.extend_from_slice(payload);

        // TODO: Returned length is wrong in other packets. Fix it
        Ok(1 + count + len)
    }
}

pub(crate) mod puback {
    use super::*;
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    /// Acknowledgement to QoS1 publish
    #[derive(Debug, Clone, PartialEq)]
    pub struct PubAck {
        pub pkid: u16,
    }

    impl PubAck {
        pub fn new(pkid: u16) -> PubAck {
            PubAck { pkid }
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);
            let pkid = read_u16(&mut bytes)?;

            // No reason code or properties if remaining length == 2
            if fixed_header.remaining_len == 2 {
                return Ok(PubAck { pkid });
            }

            // No properties len or properties if remaining len > 2 but < 4
            if fixed_header.remaining_len < 4 {
                return Ok(PubAck { pkid });
            }

            let puback = PubAck { pkid };

            Ok(puback)
        }
    }

    pub fn write(pkid: u16, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = 2; // pkid
        buffer.put_u8(0x40);

        let count = write_remaining_length(buffer, len)?;
        buffer.put_u16(pkid);
        Ok(1 + count + len)
    }
}

pub(crate) mod subscribe {
    use super::*;
    use bytes::{Buf, Bytes};

    /// Subscription packet
    #[derive(Debug, Clone, PartialEq)]
    pub struct Subscribe {
        pub pkid: u16,
        pub filters: Vec<SubscribeFilter>,
    }

    impl Subscribe {
        pub fn new<S: Into<String>>(path: S, qos: QoS) -> Subscribe {
            let filter = SubscribeFilter {
                path: path.into(),
                qos,
            };

            let filters = vec![filter];
            Subscribe { pkid: 0, filters }
        }

        pub fn add(&mut self, path: String, qos: QoS) -> &mut Self {
            let filter = SubscribeFilter { path, qos };

            self.filters.push(filter);
            self
        }

        pub fn len(&self) -> usize {
            let len = 2 + self.filters.iter().fold(0, |s, t| s + t.len()); // len of pkid + vec![subscribe filter len]
            len
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);

            let pkid = read_u16(&mut bytes)?;

            // variable header size = 2 (packet identifier)
            let mut filters = Vec::new();

            while bytes.has_remaining() {
                let path = read_mqtt_bytes(&mut bytes)?;
                let path = std::str::from_utf8(&path)?.to_owned();
                let options = read_u8(&mut bytes)?;
                let requested_qos = options & 0b0000_0011;

                filters.push(SubscribeFilter {
                    path,
                    qos: qos(requested_qos)?,
                });
            }

            let subscribe = Subscribe { pkid, filters };

            Ok(subscribe)
        }
    }

    pub fn write(
        filters: Vec<SubscribeFilter>,
        pkid: u16,
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        let len = 2 + filters.iter().fold(0, |s, t| s + t.len()); // len of pkid + vec![subscribe filter len]
                                                                  // write packet type
        buffer.put_u8(0x82);

        // write remaining length
        let remaining_len_bytes = write_remaining_length(buffer, len)?;

        // write packet id
        buffer.put_u16(pkid);

        // write filters
        for filter in filters.iter() {
            filter.write(buffer);
        }

        Ok(1 + remaining_len_bytes + len)
    }

    ///  Subscription filter
    #[derive(Debug, Clone, PartialEq)]
    pub struct SubscribeFilter {
        pub path: String,
        pub qos: QoS,
    }

    impl SubscribeFilter {
        pub fn new(path: String, qos: QoS) -> SubscribeFilter {
            SubscribeFilter { path, qos }
        }

        pub fn len(&self) -> usize {
            // filter len + filter + options
            2 + self.path.len() + 1
        }

        fn write(&self, buffer: &mut BytesMut) {
            let mut options = 0;
            options |= self.qos as u8;

            write_mqtt_string(buffer, self.path.as_str());
            buffer.put_u8(options);
        }
    }
}

pub(crate) mod suback {
    use std::convert::{TryFrom, TryInto};

    use super::*;
    use bytes::{Buf, Bytes};

    /// Acknowledgement to subscribe
    #[derive(Debug, Clone, PartialEq)]
    pub struct SubAck {
        pub pkid: u16,
        pub return_codes: Vec<SubscribeReasonCode>,
    }

    impl SubAck {
        pub fn new(pkid: u16, return_codes: Vec<SubscribeReasonCode>) -> SubAck {
            SubAck { pkid, return_codes }
        }

        pub fn len(&self) -> usize {
            2 + self.return_codes.len()
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);
            let pkid = read_u16(&mut bytes)?;

            if !bytes.has_remaining() {
                return Err(Error::MalformedPacket);
            }

            let mut return_codes = Vec::new();
            while bytes.has_remaining() {
                let return_code = read_u8(&mut bytes)?;
                return_codes.push(return_code.try_into()?);
            }

            let suback = SubAck { pkid, return_codes };
            Ok(suback)
        }
    }

    pub fn write(
        return_codes: Vec<SubscribeReasonCode>,
        pkid: u16,
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        let len = 2 + return_codes.len();
        buffer.put_u8(0x90);

        let remaining_len_bytes = write_remaining_length(buffer, len)?;
        buffer.put_u16(pkid);

        let p: Vec<u8> = return_codes
            .iter()
            .map(|&code| match code {
                SubscribeReasonCode::Success(qos) => qos as u8,
                SubscribeReasonCode::Failure => 0x80,
            })
            .collect();

        buffer.extend_from_slice(&p);
        Ok(1 + remaining_len_bytes + len)
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SubscribeReasonCode {
        Success(QoS),
        Failure,
    }

    impl TryFrom<u8> for SubscribeReasonCode {
        type Error = Error;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            let v = match value {
                0 => SubscribeReasonCode::Success(QoS::AtMostOnce),
                1 => SubscribeReasonCode::Success(QoS::AtLeastOnce),
                128 => SubscribeReasonCode::Failure,
                v => return Err(Error::InvalidSubscribeReasonCode(v)),
            };

            Ok(v)
        }
    }

    pub fn codes(c: Vec<u8>) -> Vec<SubscribeReasonCode> {
        c.into_iter()
            .map(|v| {
                let qos = qos(v).unwrap();
                SubscribeReasonCode::Success(qos)
            })
            .collect()
    }
}

pub(crate) mod pingresp {
    use super::*;

    pub fn write(payload: &mut BytesMut) -> Result<usize, Error> {
        payload.put_slice(&[0xD0, 0x00]);
        Ok(2)
    }
}

pub(crate) mod pingreq {
    use super::*;

    pub fn write(payload: &mut BytesMut) -> Result<usize, Error> {
        payload.put_slice(&[0xC0, 0x00]);
        Ok(2)
    }
}

/// Reads a stream of bytes and extracts next MQTT packet out of it
pub fn read_mut(stream: &mut BytesMut, max_size: usize) -> Result<Packet, Error> {
    let fixed_header = check(stream.iter(), max_size)?;

    // Test with a stream with exactly the size to check border panics
    let packet = stream.split_to(fixed_header.frame_length());
    let packet_type = fixed_header.packet_type()?;

    if fixed_header.remaining_len == 0 {
        // no payload packets
        return match packet_type {
            PacketType::PingReq => Ok(Packet::PingReq),
            PacketType::PingResp => Ok(Packet::PingResp),
            PacketType::Disconnect => Ok(Packet::Disconnect),
            _ => Err(Error::PayloadRequired),
        };
    }

    let packet = packet.freeze();
    let packet = match packet_type {
        PacketType::Connect => Packet::Connect(connect::Connect::read(fixed_header, packet)?),
        PacketType::ConnAck => Packet::ConnAck(connack::ConnAck::read(fixed_header, packet)?),
        PacketType::Publish => Packet::Publish(publish::Publish::read(fixed_header, packet)?),
        PacketType::PubAck => Packet::PubAck(puback::PubAck::read(fixed_header, packet)?),
        PacketType::Subscribe => {
            Packet::Subscribe(subscribe::Subscribe::read(fixed_header, packet)?)
        }
        PacketType::SubAck => Packet::SubAck(suback::SubAck::read(fixed_header, packet)?),
        PacketType::PingReq => Packet::PingReq,
        PacketType::PingResp => Packet::PingResp,
        PacketType::Disconnect => Packet::Disconnect,
        v => return Err(Error::UnsupportedPacket(v)),
    };

    Ok(packet)
}

/// Reads a stream of bytes and extracts next MQTT packet out of it
pub fn read(stream: &mut Bytes, max_size: usize) -> Result<Packet, Error> {
    let fixed_header = check(stream.iter(), max_size)?;

    // Test with a stream with exactly the size to check border panics
    let packet = stream.split_to(fixed_header.frame_length());
    let packet_type = fixed_header.packet_type()?;

    if fixed_header.remaining_len == 0 {
        // no payload packets
        return match packet_type {
            PacketType::PingReq => Ok(Packet::PingReq),
            PacketType::PingResp => Ok(Packet::PingResp),
            PacketType::Disconnect => Ok(Packet::Disconnect),
            _ => Err(Error::PayloadRequired),
        };
    }

    let packet = match packet_type {
        PacketType::Connect => Packet::Connect(connect::Connect::read(fixed_header, packet)?),
        PacketType::ConnAck => Packet::ConnAck(connack::ConnAck::read(fixed_header, packet)?),
        PacketType::Publish => Packet::Publish(publish::Publish::read(fixed_header, packet)?),
        PacketType::PubAck => Packet::PubAck(puback::PubAck::read(fixed_header, packet)?),
        PacketType::Subscribe => {
            Packet::Subscribe(subscribe::Subscribe::read(fixed_header, packet)?)
        }
        PacketType::SubAck => Packet::SubAck(suback::SubAck::read(fixed_header, packet)?),
        PacketType::PingReq => Packet::PingReq,
        PacketType::PingResp => Packet::PingResp,
        PacketType::Disconnect => Packet::Disconnect,
        v => return Err(Error::UnsupportedPacket(v)),
    };

    Ok(packet)
}

#[derive(Clone, Debug, PartialEq)]
pub enum Packet {
    Connect(connect::Connect),
    Publish(publish::Publish),
    ConnAck(connack::ConnAck),
    PubAck(puback::PubAck),
    PingReq,
    PingResp,
    Subscribe(subscribe::Subscribe),
    SubAck(suback::SubAck),
    Disconnect,
}

use crate::mqtt4::*;
use byteorder::ReadBytesExt;
use std::io::Read;

/// Mqtt awareness on top of `Read`
pub trait MqttRead: ReadBytesExt {
    fn mqtt_read(&mut self) -> Result<Packet, Error> {
        let packet_type = self.read_u8()?;
        let remaining_len = self.read_remaining_length()?;

        self.deserialize(packet_type, remaining_len)
    }

    fn read_packet_type_and_remaining_length(&mut self) -> Result<(u8, usize), Error> {
        let packet_type = self.read_u8()?;
        let remaining_len = self.read_remaining_length()?;
        Ok((packet_type, remaining_len))
    }

    fn deserialize(&mut self, byte1: u8, remaining_len: usize) -> Result<Packet, Error> {
        let kind = packet_type(byte1 >> 4)?;

        if remaining_len == 0 {
            // no payload packets
            return match kind {
                PacketType::Pingreq => Ok(Packet::Pingreq),
                PacketType::Pingresp => Ok(Packet::Pingresp),
                PacketType::Disconnect => Ok(Packet::Disconnect),
                _ => Err(Error::PayloadRequired),
            };
        }

        match kind {
            PacketType::Connect => Ok(Packet::Connect(self.read_connect()?)),
            PacketType::Connack => Ok(Packet::Connack(self.read_connack(remaining_len)?)),
            PacketType::Publish => Ok(Packet::Publish(self.read_publish(byte1, remaining_len)?)),
            PacketType::Puback if remaining_len != 2 => Err(Error::PayloadSizeIncorrect),
            PacketType::Puback => {
                let pkid = self.read_u16::<byteorder::BigEndian>()?;
                Ok(Packet::Puback(PacketIdentifier(pkid)))
            }
            PacketType::Pubrec if remaining_len != 2 => Err(Error::PayloadSizeIncorrect),
            PacketType::Pubrec => {
                let pkid = self.read_u16::<byteorder::BigEndian>()?;
                Ok(Packet::Pubrec(PacketIdentifier(pkid)))
            }
            PacketType::Pubrel if remaining_len != 2 => Err(Error::PayloadSizeIncorrect),
            PacketType::Pubrel => {
                let pkid = self.read_u16::<byteorder::BigEndian>()?;
                Ok(Packet::Pubrel(PacketIdentifier(pkid)))
            }
            PacketType::Pubcomp if remaining_len != 2 => Err(Error::PayloadSizeIncorrect),
            PacketType::Pubcomp => {
                let pkid = self.read_u16::<byteorder::BigEndian>()?;
                Ok(Packet::Pubcomp(PacketIdentifier(pkid)))
            }
            PacketType::Subscribe => Ok(Packet::Subscribe(self.read_subscribe(remaining_len)?)),
            PacketType::Suback => Ok(Packet::Suback(self.read_suback(remaining_len)?)),
            PacketType::Unsubscribe => {
                Ok(Packet::Unsubscribe(self.read_unsubscribe(remaining_len)?))
            }
            PacketType::Unsuback if remaining_len != 2 => Err(Error::PayloadSizeIncorrect),
            PacketType::Unsuback => {
                let pkid = self.read_u16::<byteorder::BigEndian>()?;
                Ok(Packet::Unsuback(PacketIdentifier(pkid)))
            }
            PacketType::Pingreq => Err(Error::IncorrectPacketFormat),
            PacketType::Pingresp => Err(Error::IncorrectPacketFormat),
            PacketType::Disconnect => Err(Error::IncorrectPacketFormat),
        }
    }

    fn read_connect(&mut self) -> Result<Connect, Error> {
        let protocol_name = self.read_mqtt_string()?;
        let protocol_level = self.read_u8()?;

        if protocol_name != "MQTT" {
            return Err(Error::InvalidProtocolLevel(protocol_name, protocol_level));
        }

        let protocol = match protocol_level {
            4 => Protocol::MQTT(4),
            _ => return Err(Error::InvalidProtocolLevel(protocol_name, protocol_level)),
        };

        let connect_flags = self.read_u8()?;
        let keep_alive = self.read_u16::<byteorder::BigEndian>()?;
        let client_id = self.read_mqtt_string()?;

        let last_will = match connect_flags & 0b100 {
            0 => {
                if (connect_flags & 0b00111000) != 0 {
                    return Err(Error::IncorrectPacketFormat);
                }
                None
            }
            _ => {
                let will_topic = self.read_mqtt_string()?;
                let will_message = self.read_mqtt_string()?;
                let will_qos = qos((connect_flags & 0b11000) >> 3)?;
                Some(LastWill {
                    topic: will_topic,
                    message: will_message,
                    qos: will_qos,
                    retain: (connect_flags & 0b00100000) != 0,
                })
            }
        };

        let username = match connect_flags & 0b10000000 {
            0 => None,
            _ => Some(self.read_mqtt_string()?),
        };

        let password = match connect_flags & 0b01000000 {
            0 => None,
            _ => Some(self.read_mqtt_string()?),
        };

        Ok(Connect {
            protocol,
            keep_alive,
            client_id,
            clean_session: (connect_flags & 0b10) != 0,
            last_will,
            username,
            password,
        })
    }

    fn read_connack(&mut self, remaining_len: usize) -> Result<Connack, Error> {
        // check remaining number of bytes (ignoring fixed header length). fixed header
        if remaining_len != 2 {
            return Err(Error::PayloadSizeIncorrect);
        }
        let flags = self.read_u8()?;
        let return_code = self.read_u8()?;
        Ok(Connack {
            session_present: (flags & 0x01) == 1,
            code: connect_return(return_code)?,
        })
    }

    fn read_publish(&mut self, byte1: u8, remaining_len: usize) -> Result<Publish, Error> {
        let qos = qos((byte1 & 0b0110) >> 1)?;
        let dup = (byte1 & 0b1000) != 0;
        let retain = (byte1 & 0b0001) != 0;

        let topic_name = self.read_mqtt_string()?;

        // Packet identifier exists where QoS > 0
        let pkid = match qos {
            QoS::AtMostOnce => None,
            QoS::AtLeastOnce | QoS::ExactlyOnce => {
                Some(PacketIdentifier(self.read_u16::<byteorder::BigEndian>()?))
            }
        };

        // variable header len = len of topic (2 bytes) + topic.len() + [optional packet id (2 bytes)]
        let variable_header_len = match qos {
            QoS::AtMostOnce => 2 + topic_name.len(),
            QoS::AtLeastOnce | QoS::ExactlyOnce => 2 + topic_name.len() + 2,
        };

        let payload_len = remaining_len - variable_header_len;

        // read publish payload into the buffer
        let mut payload = Vec::with_capacity(payload_len);
        let mut s = self.take(payload_len as u64);
        s.read_to_end(&mut payload)?;
        Ok(Publish {
            dup,
            qos,
            retain,
            topic_name,
            pkid,
            payload: payload,
        })
    }

    fn read_subscribe(&mut self, remaining_len: usize) -> Result<Subscribe, Error> {
        let pkid = self.read_u16::<byteorder::BigEndian>()?;

        // variable header size = 2
        // variable header + payload - variable header
        let mut payload_bytes = remaining_len - 2;
        let mut topics = Vec::with_capacity(1);

        while payload_bytes > 0 {
            let topic_filter = self.read_mqtt_string()?;
            let requested_qos = self.read_u8()?;
            payload_bytes -= topic_filter.len() + 3;
            topics.push(SubscribeTopic {
                topic_path: topic_filter,
                qos: qos(requested_qos)?,
            });
        }

        Ok(Subscribe {
            pkid: PacketIdentifier(pkid),
            topics,
        })
    }

    fn read_suback(&mut self, remaining_len: usize) -> Result<Suback, Error> {
        let pkid = self.read_u16::<byteorder::BigEndian>()?;
        let mut payload_bytes = remaining_len - 2;
        let mut return_codes = Vec::with_capacity(payload_bytes);

        while payload_bytes > 0 {
            let return_code = self.read_u8()?;
            if return_code >> 7 == 1 {
                return_codes.push(SubscribeReturnCodes::Failure)
            } else {
                return_codes.push(SubscribeReturnCodes::Success(qos(return_code & 0x3)?));
            }
            payload_bytes -= 1
        }

        Ok(Suback {
            pkid: PacketIdentifier(pkid),
            return_codes,
        })
    }

    fn read_unsubscribe(&mut self, remaining_len: usize) -> Result<Unsubscribe, Error> {
        let pkid = self.read_u16::<byteorder::BigEndian>()?;
        let mut payload_bytes = remaining_len - 2;
        let mut topics = Vec::with_capacity(1);

        while payload_bytes > 0 {
            let topic_filter = self.read_mqtt_string()?;
            payload_bytes -= topic_filter.len() + 2;
            topics.push(topic_filter);
        }

        Ok(Unsubscribe {
            pkid: PacketIdentifier(pkid),
            topics,
        })
    }

    fn read_mqtt_string(&mut self) -> Result<String, Error> {
        let len = self.read_u16::<byteorder::BigEndian>()? as usize;
        let mut data = Vec::with_capacity(len);

        let mut s = self.take(len as u64);
        s.read_to_end(&mut data)?;

        Ok(String::from_utf8(data)?)
    }

    fn header_len(&self, remaining_len: usize) -> usize {
        if remaining_len >= 2_097_152 {
            4 + 1
        } else if remaining_len >= 16_384 {
            3 + 1
        } else if remaining_len >= 128 {
            2 + 1
        } else {
            1 + 1
        }
    }

    fn read_remaining_length(&mut self) -> Result<usize, Error> {
        let mut mult: usize = 1;
        let mut len: usize = 0;
        let mut done = false;

        while !done {
            let byte = self.read_u8()? as usize;
            len += (byte & 0x7F) * mult;
            mult *= 0x80;

            if mult > 0x80 * 0x80 * 0x80 * 0x80 {
                return Err(Error::MalformedRemainingLength);
            }
            done = (byte & 0x80) == 0
        }

        Ok(len)
    }
}

/// Implement MattRead for every AsyncReadExt type (and hence AsyncRead type)
impl<R: ReadBytesExt + ?Sized> MqttRead for R {}

#[cfg(test)]
mod test {
    use super::MqttRead;
    use crate::mqtt4::{Connack, Connect, Packet, Publish, Suback, Subscribe, Unsubscribe};
    use crate::mqtt4::{
        ConnectReturnCode, LastWill, PacketIdentifier, Protocol, QoS, SubscribeReturnCodes,
        SubscribeTopic,
    };
    use std::io::Cursor;

    #[test]
    fn read_packet_connect_mqtt_protocol() {
        let mut stream = Cursor::new(vec![
            0x10, 39, // packet type, flags and remaining len
            0x00, 0x04, 'M' as u8, 'Q' as u8, 'T' as u8, 'T' as u8, 0x04, // variable header
            0b11001110, // variable header. +username, +password, -will retain, will qos=1, +last_will, +clean_session
            0x00, 0x0a, // variable header. keep alive = 10 sec
            0x00, 0x04, 't' as u8, 'e' as u8, 's' as u8, 't' as u8, // payload. client_id
            0x00, 0x02, '/' as u8, 'a' as u8, // payload. will topic = '/a'
            0x00, 0x07, 'o' as u8, 'f' as u8, 'f' as u8, 'l' as u8, 'i' as u8, 'n' as u8,
            'e' as u8, // payload. variable header. will msg = 'offline'
            0x00, 0x04, 'r' as u8, 'u' as u8, 'm' as u8,
            'q' as u8, // payload. username = 'rumq'
            0x00, 0x02, 'm' as u8, 'q' as u8, // payload. password = 'mq'
            0xDE, 0xAD, 0xBE, 0xEF, // extra packets in the stream
        ]);

        let packet = stream.mqtt_read().unwrap();

        assert_eq!(
            packet,
            Packet::Connect(Connect {
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
            })
        );
    }

    #[test]
    fn read_packet_connack_works() {
        let mut stream = Cursor::new(vec![
            0b00100000, 0x02, // packet type, flags and remaining len
            0x01, 0x00, // variable header. connack flags, connect return code
            0xDE, 0xAD, 0xBE, 0xEF, // extra packets in the stream
        ]);
        let packet = stream.mqtt_read().unwrap();

        assert_eq!(
            packet,
            Packet::Connack(Connack {
                session_present: true,
                code: ConnectReturnCode::Accepted
            })
        );
    }

    #[test]
    fn read_packet_publish_qos1_works() {
        let mut stream = Cursor::new(vec![
            0b00110010, 11, // packet type, flags and remaining len
            0x00, 0x03, 'a' as u8, '/' as u8,
            'b' as u8, // variable header. topic name = 'a/b'
            0x00, 0x0a, // variable header. pkid = 10
            0xF1, 0xF2, 0xF3, 0xF4, // publish payload
            0xDE, 0xAD, 0xBE, 0xEF, // extra packets in the stream
        ]);

        let packet = stream.mqtt_read().unwrap();

        assert_eq!(
            packet,
            Packet::Publish(Publish {
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic_name: "a/b".to_owned(),
                pkid: Some(PacketIdentifier(10)),
                payload: vec![0xF1, 0xF2, 0xF3, 0xF4]
            })
        );
    }

    #[test]
    fn read_packet_publish_qos0_works() {
        let mut stream = Cursor::new(vec![
            0b00110000, 7, // packet type, flags and remaining len
            0x00, 0x03, 'a' as u8, '/' as u8,
            'b' as u8, // variable header. topic name = 'a/b'
            0x01, 0x02, // payload
            0xDE, 0xAD, 0xBE, 0xEF, // extra packets in the stream
        ]);

        let packet = stream.mqtt_read().unwrap();

        assert_eq!(
            packet,
            Packet::Publish(Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic_name: "a/b".to_owned(),
                pkid: None,
                payload: vec![0x01, 0x02]
            })
        );
    }

    #[test]
    fn read_packet_puback_works() {
        let mut stream = Cursor::new(vec![
            0b01000000, 0x02, // packet type, flags and remaining len
            0x00, 0x0A, // fixed header. packet identifier = 10
            0xDE, 0xAD, 0xBE, 0xEF, // extra packets in the stream
        ]);
        let packet = stream.mqtt_read().unwrap();

        assert_eq!(packet, Packet::Puback(PacketIdentifier(10)));
    }

    #[test]
    fn read_packet_subscribe_works() {
        let mut stream = Cursor::new(vec![
            0b10000010, 20, // packet type, flags and remaining len
            0x01, 0x04, // variable header. pkid = 260
            0x00, 0x03, 'a' as u8, '/' as u8, '+' as u8, // payload. topic filter = 'a/+'
            0x00,      // payload. qos = 0
            0x00, 0x01, '#' as u8, // payload. topic filter = '#'
            0x01,      // payload. qos = 1
            0x00, 0x05, 'a' as u8, '/' as u8, 'b' as u8, '/' as u8,
            'c' as u8, // payload. topic filter = 'a/b/c'
            0x02,      // payload. qos = 2
            0xDE, 0xAD, 0xBE, 0xEF, // extra packets in the stream
        ]);

        let packet = stream.mqtt_read().unwrap();

        assert_eq!(
            packet,
            Packet::Subscribe(Subscribe {
                pkid: PacketIdentifier(260),
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
            })
        );
    }

    #[test]
    fn read_packet_unsubscribe_works() {
        let mut stream = Cursor::new(vec![
            0b10100010, 17, // packet type, flags and remaining len
            0x00, 0x0F, // variable header. pkid = 15
            0x00, 0x03, 'a' as u8, '/' as u8, '+' as u8, // payload. topic filter = 'a/+'
            0x00, 0x01, '#' as u8, // pyaload. topic filter = '#'
            0x00, 0x05, 'a' as u8, '/' as u8, 'b' as u8, '/' as u8,
            'c' as u8, // payload. topic filter = 'a/b/c'
            0xDE, 0xAD, 0xBE, 0xEF, // extra packets in the stream
        ]);

        let packet = stream.mqtt_read().unwrap();

        assert_eq!(
            packet,
            Packet::Unsubscribe(Unsubscribe {
                pkid: PacketIdentifier(15),
                topics: vec!["a/+".to_owned(), "#".to_owned(), "a/b/c".to_owned()]
            })
        );
    }

    #[test]
    fn read_packet_suback_works() {
        let mut stream = Cursor::new(vec![
            0x90, 4, // packet type, flags and remaining len
            0x00, 0x0F, // variable header. pkid = 15
            0x01, 0x80, // payload. return codes [success qos1, failure]
            0xDE, 0xAD, 0xBE, 0xEF, // extra packets in the stream
        ]);

        let packet = stream.mqtt_read().unwrap();

        assert_eq!(
            packet,
            Packet::Suback(Suback {
                pkid: PacketIdentifier(15),
                return_codes: vec![
                    SubscribeReturnCodes::Success(QoS::AtLeastOnce),
                    SubscribeReturnCodes::Failure
                ]
            })
        );
    }
}

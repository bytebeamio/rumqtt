use crate::{Error, Packet, QoS, SubscribeReturnCodes};
use bytes::buf::BufMut;
use bytes::BytesMut;

use alloc::vec::Vec;

pub fn mqtt_write(packet: Packet, payload: &mut BytesMut) -> Result<(), Error> {
    match packet {
        Packet::Connect(packet) => {
            let len = packet.len();
            payload.reserve(len);
            payload.put_u8(0b0001_0000);
            write_remaining_length(payload, len)?;
            write_mqtt_string(payload, "MQTT");
            payload.put_u8(0x04);

            let mut connect_flags = 0;
            if packet.clean_session {
                connect_flags |= 0x02;
            }

            match &packet.last_will {
                Some(w) if w.retain => connect_flags |= 0x04 | (w.qos as u8) << 3 | 0x20,
                Some(w) => connect_flags |= 0x04 | (w.qos as u8) << 3,
                None => (),
            }

            if packet.password.is_some() {
                connect_flags |= 0x40;
            }

            if packet.username.is_some() {
                connect_flags |= 0x80;
            }

            payload.put_u8(connect_flags);
            payload.put_u16(packet.keep_alive);
            write_mqtt_string(payload, &packet.client_id);

            if let Some(ref last_will) = packet.last_will {
                write_mqtt_string(payload, &last_will.topic);
                write_mqtt_string(payload, &last_will.message);
            }

            if let Some(ref username) = packet.username {
                write_mqtt_string(payload, username);
            }
            if let Some(ref password) = packet.password {
                write_mqtt_string(payload, password);
            }

            Ok(())
        }
        Packet::ConnAck(packet) => {
            payload.reserve(4);
            let session_present = packet.session_present as u8;
            let code = packet.code as u8;
            payload.put_u8(0x20);
            payload.put_u8(0x02);
            payload.put_u8(session_present);
            payload.put_u8(code);
            Ok(())
        }
        Packet::Publish(packet) => {
            payload.reserve(packet.topic.len() + packet.payload.len() + 10);
            payload.put_u8(0b0011_0000 | packet.retain as u8 | ((packet.qos as u8) << 1) | ((packet.dup as u8) << 3));
            let mut len = packet.topic.len() + 2 + packet.payload.len();
            if packet.qos != QoS::AtMostOnce && packet.pkid != 0 {
                len += 2;
            }

            write_remaining_length(payload, len)?;
            write_mqtt_string(payload, packet.topic.as_str());
            if packet.qos != QoS::AtMostOnce {
                let pkid = packet.pkid;
                if pkid == 0 {
                    return Err(Error::PacketIdZero);
                }

                payload.put_u16(pkid);
            }

            payload.extend_from_slice(&packet.payload);
            Ok(())
        }
        Packet::PubAck(packet) => {
            payload.reserve(4);
            payload.put_u8(0x40);
            payload.put_u8(0x02);
            payload.put_u16(packet.pkid);
            Ok(())
        }
        Packet::PubRec(packet) => {
            payload.reserve(4);
            let o: &[u8] = &[0x50, 0x02];
            payload.put_slice(o);
            payload.put_u16(packet.pkid);
            Ok(())
        }
        Packet::PubRel(packet) => {
            payload.reserve(4);
            let o: &[u8] = &[0x62, 0x02];
            payload.put_slice(o);
            payload.put_u16(packet.pkid);
            Ok(())
        }
        Packet::PubComp(packet) => {
            payload.reserve(4);
            let o: &[u8] = &[0x70, 0x02];
            payload.put_slice(o);
            payload.put_u16(packet.pkid);
            Ok(())
        }
        Packet::Subscribe(packet) => {
            let len = 2 + packet.topics.iter().fold(0, |s, ref t| s + t.topic_path.len() + 3);
            payload.reserve(len + 8);
            payload.put_u8(0x82);
            write_remaining_length(payload, len)?;
            payload.put_u16(packet.pkid);
            for topic in packet.topics.iter() {
                write_mqtt_string(payload, topic.topic_path.as_str());
                payload.put_u8(topic.qos as u8);
            }
            Ok(())
        }
        Packet::SubAck(packet) => {
            payload.reserve(10);
            payload.put_u8(0x90);
            write_remaining_length(payload, packet.return_codes.len() + 2)?;
            payload.put_u16(packet.pkid);
            let p: Vec<u8> = packet
                .return_codes
                .iter()
                .map(|&code| match code {
                    SubscribeReturnCodes::Success(qos) => qos as u8,
                    SubscribeReturnCodes::Failure => 0x80,
                })
                .collect();

            payload.extend_from_slice(&p);
            Ok(())
        }
        Packet::Unsubscribe(packet) => {
            let len = 2 + packet.topics.iter().fold(0, |s, ref topic| s + topic.len() + 2);
            payload.reserve(len + 8);
            payload.put_u8(0xA2);
            write_remaining_length(payload, len)?;
            payload.put_u16(packet.pkid);
            for topic in packet.topics.iter() {
                write_mqtt_string(payload, topic.as_str());
            }
            Ok(())
        }
        Packet::UnsubAck(packet) => {
            payload.reserve(4);
            let o: &[u8] = &[0xB0, 0x02];
            payload.put_slice(o);
            payload.put_u16(packet.pkid);
            Ok(())
        }
        Packet::PingReq => {
            payload.reserve(2);
            let mut payload = BytesMut::with_capacity(2);
            payload.put_u8(0xc0);
            payload.put_u8(0);
            Ok(())
        }
        Packet::PingResp => {
            payload.reserve(2);
            let mut payload = BytesMut::with_capacity(2);
            payload.put_u8(0xd0);
            payload.put_u8(0);
            Ok(())
        }
        Packet::Disconnect => {
            payload.reserve(2);
            let o: &[u8] = &[0xe0, 0];
            payload.put_slice(o);
            Ok(())
        }
    }
}

fn write_mqtt_string(stream: &mut BytesMut, string: &str) {
    stream.put_u16(string.len() as u16);
    stream.extend_from_slice(string.as_bytes());
}

fn write_remaining_length(stream: &mut BytesMut, len: usize) -> Result<(), Error> {
    if len > 268_435_455 {
        return Err(Error::PayloadTooLong);
    }

    let mut done = false;
    let mut x = len;

    while !done {
        let mut byte = (x % 128) as u8;
        x /= 128;
        if x > 0 {
            byte |= 128;
        }

        stream.put_u8(byte);
        done = x == 0;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::mqtt_write;
    use crate::{ConnAck, Connect, Packet, Publish, Subscribe};
    use crate::{ConnectReturnCode, LastWill, Protocol, QoS, SubscribeTopic};
    use alloc::borrow::ToOwned;
    use alloc::vec;
    use bytes::{Bytes, BytesMut};

    #[test]
    fn write_packet_connect_mqtt_protocol_works() {
        let connect = Packet::Connect(Connect {
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
        });

        let mut buf = BytesMut::new();
        mqtt_write(connect, &mut buf).unwrap();

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

    #[test]
    fn write_packet_connack_works() {
        let connack = Packet::ConnAck(ConnAck {
            session_present: true,
            code: ConnectReturnCode::Accepted,
        });

        let mut buf = BytesMut::new();
        mqtt_write(connack, &mut buf).unwrap();
        assert_eq!(buf, vec![0b0010_0000, 0x02, 0x01, 0x00]);
    }

    #[test]
    fn write_packet_publish_at_least_once_works() {
        let publish = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "a/b".to_owned(),
            pkid: 10,
            payload: Bytes::from(vec![0xF1, 0xF2, 0xF3, 0xF4]),
            bytes: Bytes::new(),
        });

        let mut buf = BytesMut::new();
        mqtt_write(publish, &mut buf).unwrap();

        assert_eq!(
            buf,
            vec![0b0011_0010, 11, 0x00, 0x03, b'a', b'/', b'b', 0x00, 0x0a, 0xF1, 0xF2, 0xF3, 0xF4]
        );
    }

    #[test]
    fn write_packet_publish_at_most_once_works() {
        let publish = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "a/b".to_owned(),
            pkid: 0,
            payload: Bytes::from(vec![0xE1, 0xE2, 0xE3, 0xE4]),
            bytes: Bytes::new(),
        });

        let mut buf = BytesMut::new();
        mqtt_write(publish, &mut buf).unwrap();

        assert_eq!(buf, vec![0b0011_0000, 9, 0x00, 0x03, b'a', b'/', b'b', 0xE1, 0xE2, 0xE3, 0xE4]);
    }

    #[test]
    fn write_packet_subscribe_works() {
        let subscribe = Packet::Subscribe(Subscribe {
            pkid: 260,
            topics: vec![
                SubscribeTopic {
                    topic_path: "a/+".to_owned(),
                    qos: QoS::AtMostOnce,
                },
                SubscribeTopic {
                    topic_path: "#".to_owned(),
                    qos: QoS::AtLeastOnce,
                },
                SubscribeTopic {
                    topic_path: "a/b/c".to_owned(),
                    qos: QoS::ExactlyOnce,
                },
            ],
        });

        let mut buf = BytesMut::new();
        mqtt_write(subscribe, &mut buf).unwrap();
        assert_eq!(
            buf,
            vec![
                0b1000_0010,
                20,
                0x01,
                0x04, // pkid = 260
                0x00,
                0x03,
                b'a',
                b'/',
                b'+', // topic filter = 'a/+'
                0x00, // qos = 0
                0x00,
                0x01,
                b'#', // topic filter = '#'
                0x01, // qos = 1
                0x00,
                0x05,
                b'a',
                b'/',
                b'b',
                b'/',
                b'c', // topic filter = 'a/b/c'
                0x02  // qos = 2
            ]
        );
    }
}

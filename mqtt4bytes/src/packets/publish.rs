use crate::*;
use super::*;
use alloc::string::String;
use bytes::{Bytes, Buf};
use alloc::vec::Vec;
use core::fmt;

/// Publish packet
#[derive(Clone, PartialEq)]
pub struct Publish {
    pub qos: QoS,
    pub pkid: u16,
    pub topic: String,
    pub payload: Bytes,
    pub dup: bool,
    pub retain: bool,
}

impl Publish {
    pub fn new<S: Into<String>, P: Into<Vec<u8>>>(topic: S, qos: QoS, payload: P) -> Publish {
        Publish {
            dup: false,
            qos,
            retain: false,
            pkid: 0,
            topic: topic.into(),
            payload: Bytes::from(payload.into()),
        }
    }

    pub fn raw(self) -> Result<PublishRaw, Error> {
        PublishRaw::from_bytes(self.topic, self.qos, self.payload)
    }

    pub fn from_bytes<S: Into<String>>(topic: S, qos: QoS, payload: Bytes) -> Publish {
        Publish {
            dup: false,
            qos,
            retain: false,
            pkid: 0,
            topic: topic.into(),
            payload,
        }

    }

    pub fn set_pkid(&mut self, pkid: u16) -> &mut Self {
        self.pkid = pkid;
        self
    }

    pub fn set_retain(&mut self, retain: bool) -> &mut Self {
        self.retain = retain;
        self
    }

    pub fn set_dup(&mut self, dup: bool) -> &mut Self {
        self.dup = dup;
        self
    }

    pub(crate) fn assemble(fixed_header: FixedHeader, bytes: Bytes) -> Result<Self, Error> {
        let mut payload = bytes.clone();
        let qos = qos((fixed_header.byte1 & 0b0110) >> 1)?;
        let dup = (fixed_header.byte1 & 0b1000) != 0;
        let retain = (fixed_header.byte1 & 0b0001) != 0;

        let variable_header_index = fixed_header.fixed_len;
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
        };

        Ok(publish)
    }

    pub fn write(&self, payload: &mut BytesMut) -> Result<usize, Error> {
        let dup = self.dup as u8;
        let qos = self.qos as u8;
        let retain = self.retain as u8;
        payload.put_u8(0b0011_0000 | retain | qos << 1 | dup << 3);

        // for publish, variable header = 2 + topic + packet id (optional) + payload
        let mut remaining_len = self.topic.len() + 2 + self.payload.len();
        if self.qos != QoS::AtMostOnce && self.pkid != 0 {
            remaining_len += 2;
        }

        let remaining_len_bytes = write_remaining_length(payload, remaining_len)?;
        write_mqtt_string(payload, self.topic.as_str());
        if self.qos != QoS::AtMostOnce {
            let pkid = self.pkid;
            if pkid == 0 {
                return Err(Error::PacketIdZero);
            }

            payload.put_u16(pkid);
        }

        payload.extend_from_slice(&self.payload);
        Ok(1 + remaining_len_bytes + remaining_len)
    }
}


impl fmt::Debug for Publish {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Topic = {}, Qos = {:?}, Retain = {}, Pkid = {:?}, Payload Size = {}",
            self.topic,
            self.qos,
            self.retain,
            self.pkid,
            self.payload.len()
        )
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use alloc::borrow::ToOwned;
    use bytes::{Bytes, BytesMut};
    use pretty_assertions::assert_eq;
    use alloc::vec;

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

        let mut stream = BytesMut::from(&stream[..]);
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

        let mut stream = BytesMut::from(&stream[..]);
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
            }
        );
    }

    #[test]
    fn write_packet_publish_at_least_once_works() {
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "a/b".to_owned(),
            pkid: 10,
            payload: Bytes::from(vec![0xF1, 0xF2, 0xF3, 0xF4]),
        };

        let mut buf = BytesMut::new();
        publish.write(&mut buf).unwrap();

        assert_eq!(
            buf,
            vec![
                0b0011_0010,
                11,
                0x00,
                0x03,
                b'a',
                b'/',
                b'b',
                0x00,
                0x0a,
                0xF1,
                0xF2,
                0xF3,
                0xF4
            ]
        );
    }

    #[test]
    fn write_packet_publish_at_most_once_works() {
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "a/b".to_owned(),
            pkid: 0,
            payload: Bytes::from(vec![0xE1, 0xE2, 0xE3, 0xE4]),
        };

        let mut buf = BytesMut::new();
        publish.write(&mut buf).unwrap();

        assert_eq!(
            buf,
            vec![
                0b0011_0000,
                9,
                0x00,
                0x03,
                b'a',
                b'/',
                b'b',
                0xE1,
                0xE2,
                0xE3,
                0xE4
            ]
        );
    }
}


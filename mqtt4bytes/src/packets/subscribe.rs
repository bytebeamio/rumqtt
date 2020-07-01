use crate::*;
use super::*;
use alloc::string::String;
use alloc::vec::Vec;
use bytes::{Bytes, Buf};
use core::fmt;

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

    pub fn new<S: Into<String>>(topic: S, qos: QoS) -> Subscribe {
        let topic = SubscribeTopic {
            topic_path: topic.into(),
            qos,
        };

        let mut topics = Vec::new();
        topics.push(topic);
        Subscribe {
            pkid: 0,
            topics,
        }
    }

    pub fn empty_subscribe() -> Subscribe {
        Subscribe {
            pkid: 0,
            topics: Vec::new(),
        }
    }

    pub fn add(&mut self, topic: String, qos: QoS) -> &mut Self {
        let topic = SubscribeTopic {
            topic_path: topic,
            qos,
        };
        self.topics.push(topic);
        self
    }
}

///  Subscription filter
#[derive(Clone, PartialEq)]
pub struct SubscribeTopic {
    pub topic_path: String,
    pub qos: QoS,
}

impl fmt::Debug for Subscribe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Filters = {:?}, Packet id = {:?}",
            self.pkid, self.topics
        )
    }
}

impl fmt::Debug for SubscribeTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Filter = {}, Qos = {:?}", self.topic_path, self.qos)
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use alloc::borrow::ToOwned;
    use alloc::vec;
    use bytes::{BytesMut};
    use pretty_assertions::assert_eq;

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
}

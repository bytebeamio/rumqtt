use std::convert::TryInto;

use crate::protocol::{Publish, QoS};
use crate::Persistant;
use bytes::{BufMut, Bytes, BytesMut};

impl Persistant for Bytes {
    fn serialize(self) -> Vec<u8> {
        self.into_iter().collect()
    }

    fn deserialize(input: &[u8]) -> Self {
        let mut ret = BytesMut::new();
        ret.put_slice(input);
        ret.freeze()
    }

    fn size(&self) -> usize {
        // For bytes len returns number of bytes in the given `Bytes`
        self.len()
    }
}

impl Persistant for Publish {
    fn serialize(self) -> Vec<u8> {
        let mut o = BytesMut::with_capacity(self.size() + 5);
        o.put_u16(self.pkid);
        o.put_u16(self.topic.len() as u16);
        o.extend_from_slice(&self.topic[..]);

        // TODO: Change segments to take Buf to prevent this copying
        o.extend_from_slice(&self.payload[..]);
        o.to_vec()
    }

    fn deserialize(input: &[u8]) -> Self {
        let pkid_t: [u8; 2] = input[..2].try_into().unwrap();
        let pkid: u16 = u16::from_be_bytes(pkid_t);
        let topic_len_t = input[2..4].try_into().unwrap();
        let topic_len = u16::from_be_bytes(topic_len_t);
        let (topic, payload) = input[4..].split_at(topic_len as usize);
        Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: Bytes::from(topic.to_owned()),
            pkid,
            payload: Bytes::from(payload.to_owned()),
        }
    }

    fn size(&self) -> usize {
        4 + self.topic.len() + self.payload.len()
    }
}

impl Persistant for Vec<u8> {
    fn serialize(self) -> Vec<u8> {
        self
    }

    fn deserialize(input: &[u8]) -> Self {
        input.into()
    }

    fn size(&self) -> usize {
        // For bytes len returns number of bytes in the given `Bytes`
        self.len()
    }
}

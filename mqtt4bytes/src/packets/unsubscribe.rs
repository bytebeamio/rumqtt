use super::*;
use crate::*;
use alloc::string::String;
use alloc::vec::Vec;
use bytes::{Buf, Bytes};

/// Unsubscribe packet
#[derive(Debug, Clone, PartialEq)]
pub struct Unsubscribe {
    pub pkid: u16,
    pub topics: Vec<String>,
}

impl Unsubscribe {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_len;
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

    pub fn new<S: Into<String>>(topic: S) -> Unsubscribe {
        let mut topics = Vec::new();
        topics.push(topic.into());
        Unsubscribe { pkid: 0, topics }
    }

    pub fn write(&self, payload: &mut BytesMut) -> Result<usize, Error> {
        let remaining_len = 2 + self
            .topics
            .iter()
            .fold(0, |s, ref topic| s + topic.len() + 2);
        payload.reserve(remaining_len + 8);
        payload.put_u8(0xA2);
        let remaining_len_bytes = write_remaining_length(payload, remaining_len)?;
        payload.put_u16(self.pkid);
        for topic in self.topics.iter() {
            write_mqtt_string(payload, topic.as_str());
        }
        Ok(1 + remaining_len_bytes + remaining_len)
    }
}

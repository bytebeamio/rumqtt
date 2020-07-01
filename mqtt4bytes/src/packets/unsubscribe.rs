use crate::*;
use super::*;
use bytes::{Bytes, Buf};
use alloc::vec::Vec;
use alloc::string::String;

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

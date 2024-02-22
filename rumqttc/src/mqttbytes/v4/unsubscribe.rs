use bytes::{Buf, Bytes};
use tokio::sync::oneshot::Sender;

use super::*;
use crate::Pkid;

/// Unsubscribe packet
#[derive(Debug)]
pub struct Unsubscribe {
    pub pkid: u16,
    pub topics: Vec<String>,
    pub pkid_tx: Option<Sender<Pkid>>,
}

// TODO: figure out if this is even required
impl Clone for Unsubscribe {
    fn clone(&self) -> Self {
        Self {
            pkid: self.pkid,
            topics: self.topics.clone(),
            pkid_tx: None,
        }
    }
}

impl PartialEq for Unsubscribe {
    fn eq(&self, other: &Self) -> bool {
        self.pkid == other.pkid && self.topics == other.topics
    }
}

impl Eq for Unsubscribe {}

impl Unsubscribe {
    pub fn new<S: Into<String>>(topic: S) -> Unsubscribe {
        Unsubscribe {
            pkid: 0,
            topics: vec![topic.into()],
            pkid_tx: None,
        }
    }

    fn len(&self) -> usize {
        // len of pkid + vec![subscribe topics len]
        2 + self.topics.iter().fold(0, |s, t| s + t.len() + 2)
    }

    pub fn size(&self) -> usize {
        let len = self.len();
        let remaining_len_size = len_len(len);

        1 + remaining_len_size + len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);

        let pkid = read_u16(&mut bytes)?;
        let mut payload_bytes = fixed_header.remaining_len - 2;
        let mut topics = Vec::with_capacity(1);

        while payload_bytes > 0 {
            let topic_filter = read_mqtt_string(&mut bytes)?;
            payload_bytes -= topic_filter.len() + 2;
            topics.push(topic_filter);
        }

        let unsubscribe = Unsubscribe {
            pkid,
            topics,
            pkid_tx: None,
        };
        Ok(unsubscribe)
    }

    pub fn write(&self, payload: &mut BytesMut) -> Result<usize, Error> {
        let remaining_len = self.len();

        payload.put_u8(0xA2);
        let remaining_len_bytes = write_remaining_length(payload, remaining_len)?;
        payload.put_u16(self.pkid);

        for topic in self.topics.iter() {
            write_mqtt_string(payload, topic.as_str());
        }
        Ok(1 + remaining_len_bytes + remaining_len)
    }

    pub fn place_pkid_tx(&mut self, pkid_tx: Sender<Pkid>) {
        self.pkid_tx = Some(pkid_tx)
    }
}

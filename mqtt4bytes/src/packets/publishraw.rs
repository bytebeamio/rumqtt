use bytes::{BytesMut, BufMut};
use alloc::vec::Vec;
use alloc::string::String;
use crate::*;
use super::*;

#[derive(Clone, PartialEq)]
pub struct PublishRaw {
    pub header: BytesMut,
    pub qos: QoS,
    pub pkid: u16,
    pub payload: Bytes
}

impl PublishRaw {
    pub fn new<S: Into<String>, P: Into<Vec<u8>>>(topic: S, qos: QoS, payload: P) -> Result<PublishRaw, Error> {
        PublishRaw::from_bytes(topic, qos, Bytes::from(payload.into()))
    }

    pub fn from_bytes<S: Into<String>>(topic: S, qos: QoS, payload: Bytes) -> Result<PublishRaw, Error> {
        let dup = false as u8;
        let qos_raw = qos as u8;
        let retain = false as u8;
        let topic = topic.into();

        // for publish, variable header = 2 + topic + packet id (optional) + payload
        let mut remaining_len = topic.len() + 2 + payload.len();
        if qos != QoS::AtMostOnce {
            remaining_len += 2;
        }

        let mut header = BytesMut::with_capacity(4 + remaining_len);
        header.put_u8(0b0011_0000 | retain | qos_raw << 1 | dup << 3);
        write_remaining_length(&mut header, remaining_len)?;

        write_mqtt_string(&mut header, topic.as_str());
        if qos != QoS::AtMostOnce {
            header.put_u16(0);
        }

        Ok(PublishRaw {
            header,
            qos,
            pkid: 0,
            payload: Bytes::from(payload)
        })
    }

    pub fn set_pkid(&mut self, pkid: u16) -> &mut Self {
        if self.qos != QoS::AtMostOnce {
            self.pkid = pkid;
            let len = self.header.len();
            self.header.truncate(len - 2);
            self.header.put_u16(pkid);
        }

        self
    }

    pub fn set_retain(&mut self, retain: bool) -> &mut Self {
        self.header[0] |= retain as u8;
        self
    }

    pub fn set_dup(&mut self, dup: bool) -> &mut Self {
        self.header[0] |= (dup as u8) << 3;
        self
    }


    pub fn write(&self, payload: &mut BytesMut) -> Result<usize, Error> {
        let len = self.header.len() + self.payload.len();
        payload.extend_from_slice(&self.header);
        payload.extend_from_slice(&self.payload);
        Ok(len)
    }
}



impl fmt::Debug for PublishRaw {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "raw publish")

    }
}

#[cfg(test)]
mod test {
    use super::*;
    use alloc::vec;

    #[test]
    fn write_packet_publish_at_least_once_works() {
        let mut publish = PublishRaw::new("a/b", QoS::AtLeastOnce, vec![0xF1, 0xF2, 0xF3, 0xF4]).unwrap();
        publish.set_pkid(10);

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
}
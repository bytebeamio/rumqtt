use super::*;
use crate::*;
use bytes::{BufMut, BytesMut};

/// Raw publish is used to perform serialization in current
/// thread rather the eventloop.
#[derive(Clone, PartialEq)]
pub struct PublishRaw {
    pub header: BytesMut,
    pub payload: Bytes,
    // Packet properties which are part of the header
    pub qos: QoS,
    pub pkid: u16,
}

impl PublishRaw {
    pub fn from_publish(publish: Publish) -> Result<PublishRaw, Error> {
        let len = PublishRaw::len(&publish.topic, publish.qos, &publish.payload);
        // FIX: allocating only len - payload.len() is resulting in perf degrade
        let mut header = BytesMut::with_capacity(len);
        header.put_u8(
            0x30 | (publish.retain as u8) | (publish.qos as u8) << 1 | (publish.dup as u8) << 3,
        );

        write_remaining_length(&mut header, len)?;
        write_mqtt_string(&mut header, &publish.topic);

        if publish.qos != QoS::AtMostOnce {
            header.put_u16(publish.pkid);
        }

        Ok(PublishRaw {
            header,
            qos: publish.qos,
            pkid: publish.pkid,
            payload: publish.payload,
        })
    }

    pub(crate) fn len(topic: &str, qos: QoS, payload: &Bytes) -> usize {
        let mut len = 2 + topic.len();
        if qos != QoS::AtMostOnce {
            len += 2;
        }

        len += payload.len();
        len
    }

    pub fn set_pkid(&mut self, pkid: u16) -> &mut Self {
        if self.qos != QoS::AtMostOnce {
            self.pkid = pkid;
            self.header.truncate(self.header.len() - 2);
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
        if self.qos != QoS::AtMostOnce && self.pkid == 0 {
            return Err(Error::PacketIdZero);
        }

        let len = self.header.len() + self.payload.len();
        payload.extend_from_slice(&self.header);
        payload.extend_from_slice(&self.payload);
        Ok(len)
    }
}

impl fmt::Debug for PublishRaw {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Qos = {:?}, Pkid = {}, Payload Size = {}",
            self.qos,
            self.pkid,
            self.payload.len()
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use alloc::vec;

    #[test]
    fn write_packet_publish_at_least_once_works() {
        let publish = Publish::new("a/b", QoS::AtLeastOnce, vec![0xF1, 0xF2, 0xF3, 0xF4]);
        let mut publish = publish.raw().unwrap();
        publish.set_pkid(10);
        publish.set_retain(true);
        publish.set_dup(true);

        let mut buf = BytesMut::new();
        publish.write(&mut buf).unwrap();

        assert_eq!(
            &buf[..],
            vec![
                0b0011_1011,
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

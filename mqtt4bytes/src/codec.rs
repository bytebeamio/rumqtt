use crate::Error;
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::read::mqtt_read;
use crate::{mqtt_write, Packet};

pub struct MqttCodec {
    max_payload_size: usize,
}

impl MqttCodec {
    pub fn new(max_payload_size: usize) -> Self {
        MqttCodec { max_payload_size }
    }
}

impl Decoder for MqttCodec {
    type Item = Packet;
    type Error = crate::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Packet>, crate::Error> {
        // `decode` might be called with `buf.len == 0`. We should return Ok(None)
        if buf.len() < 2 {
            return Ok(None);
        }

        // Find ways to reserve `buf` better to optimize allocations
        let packet = match mqtt_read(buf, self.max_payload_size) {
            Ok(len) => len,
            Err(Error::UnexpectedEof) => return Ok(None),
            Err(e) => return Err(e),
        };

        Ok(Some(packet))
    }
}

impl Encoder<Packet> for MqttCodec {
    type Error = crate::Error;

    fn encode(&mut self, packet: Packet, buf: &mut BytesMut) -> Result<(), crate::Error> {
        mqtt_write(packet, buf)?;
        Ok(())
    }
}

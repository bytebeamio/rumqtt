use bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;

use super::{Error, Packet};

/// MQTT v4 codec
pub struct Codec {
    /// Maximum packet size
    pub max_incoming_size: usize,
}

impl Decoder for Codec {
    type Item = Packet;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.remaining() == 0 {
            return Ok(None);
        }

        let packet = Packet::read(src, self.max_incoming_size)?;
        Ok(Some(packet))
    }
}

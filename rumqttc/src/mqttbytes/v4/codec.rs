use bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::{Error, Packet};

/// MQTT v4 codec
#[derive(Debug, Clone)]
pub struct Codec {
    /// Maximum packet size allowed by client
    pub max_incoming_size: usize,
    /// Maximum packet size allowed by broker
    pub max_outgoing_size: usize,
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

impl Encoder<Packet> for Codec {
    type Error = Error;
    
    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.write(dst, self.max_outgoing_size)?;

        Ok(())
    } 
}

use bytes::BytesMut;
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
        match Packet::read(src, self.max_incoming_size) {
            Ok(packet) => Ok(Some(packet)),
            Err(Error::InsufficientBytes(b)) => {
                // Get more packets to construct the incomplete packet
                src.reserve(b);
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}

impl Encoder<Packet> for Codec {
    type Error = Error;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.write(dst, self.max_outgoing_size)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use tokio_util::codec::Encoder;

    use super::Codec;
    use crate::{mqttbytes::Error, Packet, Publish, QoS};

    #[test]
    fn outgoing_max_packet_size_check() {
        let mut buf = BytesMut::new();
        let mut codec = Codec {
            max_incoming_size: 100,
            max_outgoing_size: 200,
        };

        let mut small_publish = Publish::new("hello/world", QoS::AtLeastOnce, vec![1; 100]);
        small_publish.pkid = 1;
        codec
            .encode(Packet::Publish(small_publish), &mut buf)
            .unwrap();

        let large_publish = Publish::new("hello/world", QoS::AtLeastOnce, vec![1; 265]);
        match codec.encode(Packet::Publish(large_publish), &mut buf) {
            Err(Error::OutgoingPacketTooLarge {
                pkt_size: 281,
                max: 200,
            }) => {}
            _ => unreachable!(),
        }
    }
}

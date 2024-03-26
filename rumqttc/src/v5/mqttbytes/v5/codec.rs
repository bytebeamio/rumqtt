use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use super::{Error, Packet};

/// MQTT v5 codec
#[derive(Default, Debug, Clone)]
pub struct Codec {
    /// Maximum packet size allowed by client
    pub max_incoming_size: Option<u32>,
    /// Maximum packet size allowed by broker
    pub max_outgoing_size: Option<u32>,
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
        item.write(dst, self.max_outgoing_size).map(drop)
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};
    use tokio_util::codec::{Decoder, Encoder};

    use super::Codec;
    use crate::v5::{
        mqttbytes::{Error, QoS},
        Packet, Publish,
    };

    #[test]
    fn outgoing_max_packet_size_check() {
        let mut buf = BytesMut::new();
        let mut codec = Codec {
            max_incoming_size: Some(100),
            max_outgoing_size: Some(200),
        };

        let mut small_publish = Publish::new("hello/world", QoS::AtLeastOnce, vec![1; 100], None);
        small_publish.pkid = 1;
        codec
            .encode(Packet::Publish(small_publish), &mut buf)
            .unwrap();

        let large_publish = Publish::new("hello/world", QoS::AtLeastOnce, vec![1; 265], None);
        match codec.encode(Packet::Publish(large_publish), &mut buf) {
            Err(Error::OutgoingPacketTooLarge {
                pkt_size: 282,
                max: 200,
            }) => {}
            _ => unreachable!(),
        }
    }

    #[test]
    fn encode_decode_multiple_packets() {
        let mut buf = BytesMut::new();
        let mut codec = Codec::default();
        let publish = Packet::Publish(Publish::new(
            "hello/world",
            QoS::AtMostOnce,
            vec![1; 10],
            None,
        ));

        // Encode a fixed number of publications into `buf`
        for _ in 0..100 {
            codec
                .encode(publish.clone(), &mut buf)
                .expect("failed to encode");
        }

        // Decode a fixed number of packets from `buf`
        for _ in 0..100 {
            let result = codec.decode(&mut buf).expect("failed to encode");
            assert!(matches!(result, Some(p) if p == publish));
        }

        assert_eq!(buf.remaining(), 0);
    }

    #[test]
    fn decode_insufficient() {
        let mut buf = BytesMut::new();
        let mut codec = Codec::default();
        let publish = Packet::Publish(Publish::new(
            "hello/world",
            QoS::AtMostOnce,
            vec![1; 100],
            None,
        ));

        // Encode packet into `buf`
        codec
            .encode(publish.clone(), &mut buf)
            .expect("failed to encode");
        let result = codec.decode(&mut buf);
        assert!(matches!(result, Ok(Some(p)) if p == publish));

        buf.resize(buf.remaining() / 2, 0);

        let result = codec.decode(&mut buf);
        assert!(matches!(result, Ok(None)));
    }
}

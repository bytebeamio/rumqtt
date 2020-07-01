use crate::*;
use bytes::{Bytes, Buf};

/// Acknowledgement to QoS1 publish
#[derive(Debug, Clone, PartialEq)]
pub struct PubAck {
    pub pkid: u16,
}

impl PubAck {
    pub fn new(pkid: u16) -> PubAck {
        PubAck { pkid }
    }

    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        if fixed_header.remaining_len != 2 {
            return Err(Error::PayloadSizeIncorrect);
        }

        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        let puback = PubAck { pkid };

        Ok(puback)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::{BytesMut};
    use pretty_assertions::assert_eq;

    #[test]
    fn puback_stitching_works_correctly() {
        let stream = &[
            0b0100_0000,
            0x02, // packet type, flags and remaining len
            0x00,
            0x0A, // fixed header. packet identifier = 10
            0xDE,
            0xAD,
            0xBE,
            0xEF, // extra packets in the stream
        ];
        let mut stream = BytesMut::from(&stream[..]);

        let packet = mqtt_read(&mut stream, 100).unwrap();
        let packet = match packet {
            Packet::PubAck(packet) => packet,
            packet => panic!("Invalid packet = {:?}", packet),
        };

        assert_eq!(packet, PubAck { pkid: 10 });
    }
}


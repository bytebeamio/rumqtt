use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Acknowledgement to QoS1 publish
#[derive(Debug, Clone, PartialEq)]
pub struct PubRel {
    pub pkid: u16,
}

impl PubRel {
    pub fn new(pkid: u16) -> PubRel {
        PubRel { pkid }
    }

    fn len(&self) -> usize {
        let len = 2; // pkid
        len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);
        let pkid = read_u16(&mut bytes)?;
        if fixed_header.remaining_len == 2 {
            return Ok(PubRel { pkid });
        }

        if fixed_header.remaining_len < 4 {
            return Ok(PubRel { pkid });
        }

        let puback = PubRel { pkid };

        Ok(puback)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = self.len();
        buffer.put_u8(0x62);
        let count = write_remaining_length(buffer, len)?;
        buffer.put_u16(self.pkid);
        Ok(1 + count + len)
    }
}

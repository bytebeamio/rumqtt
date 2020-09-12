use crate::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

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

        let variable_header_index = fixed_header.fixed_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        let puback = PubAck { pkid };

        Ok(puback)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let o: &[u8] = &[0x40, 0x02];
        buffer.put_slice(o);
        buffer.put_u16(self.pkid);
        Ok(4)
    }
}

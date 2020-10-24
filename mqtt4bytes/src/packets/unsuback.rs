use crate::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Acknowledgement to unsubscribe
#[derive(Debug, Clone, PartialEq)]
pub struct UnsubAck {
    pub pkid: u16,
}

impl UnsubAck {
    pub fn new(pkid: u16) -> UnsubAck {
        UnsubAck { pkid }
    }

    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        if fixed_header.remaining_len != 2 {
            return Err(Error::PayloadSizeIncorrect);
        }

        let variable_header_index = fixed_header.fixed_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        let unsuback = UnsubAck { pkid };

        Ok(unsuback)
    }

    pub fn write(&self, payload: &mut BytesMut) -> Result<usize, Error> {
        payload.put_slice(&[0xB0, 0x02]);
        payload.put_u16(self.pkid);
        Ok(4)
    }
}

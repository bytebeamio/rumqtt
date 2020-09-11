use crate::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Acknowledgement to QoS2 publish
#[derive(Debug, Clone, PartialEq)]
pub struct PubRec {
    pub pkid: u16,
}

impl PubRec {
    pub fn new(pkid: u16) -> PubRec {
        PubRec { pkid }
    }

    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        if fixed_header.remaining_len != 2 {
            return Err(Error::PayloadSizeIncorrect);
        }

        let variable_header_index = fixed_header.fixed_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        let pubrec = PubRec { pkid };

        Ok(pubrec)
    }

    pub fn write(&self, payload: &mut BytesMut) -> Result<usize, Error> {
        let o: &[u8] = &[0x50, 0x02];
        payload.put_slice(o);
        payload.put_u16(self.pkid);
        Ok(4)
    }
}

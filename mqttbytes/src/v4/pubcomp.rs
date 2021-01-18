use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Return code in connack
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum PubCompReason {
    Success = 0,
    PacketIdentifierNotFound = 146,
}

/// Acknowledgement to QoS1 publish
#[derive(Debug, Clone, PartialEq)]
pub struct PubComp {
    pub pkid: u16,
    pub reason: PubCompReason,
}

impl PubComp {
    pub fn new(pkid: u16) -> PubComp {
        PubComp {
            pkid,
            reason: PubCompReason::Success,
        }
    }

    fn len(&self) -> usize {
        let len = 2 + 1; // pkid + reason

        // TODO: Verify
        if self.reason == PubCompReason::Success {
            return 2;
        }

        len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);
        let pkid = read_u16(&mut bytes)?;

        if fixed_header.remaining_len == 2 {
            return Ok(PubComp {
                pkid,
                reason: PubCompReason::Success,
            });
        }

        let ack_reason = read_u8(&mut bytes)?;
        if fixed_header.remaining_len < 4 {
            return Ok(PubComp {
                pkid,
                reason: reason(ack_reason)?,
            });
        }

        let puback = PubComp {
            pkid,
            reason: reason(ack_reason)?,
        };

        Ok(puback)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = self.len();
        buffer.put_u8(0x70);
        let count = write_remaining_length(buffer, len)?;
        buffer.put_u16(self.pkid);
        // TODO: Verify
        if self.reason == PubCompReason::Success {
            return Ok(4);
        }

        buffer.put_u8(self.reason as u8);
        Ok(1 + count + len)
    }
}

/// Connection return code type
fn reason(num: u8) -> Result<PubCompReason, Error> {
    let code = match num {
        0 => PubCompReason::Success,
        146 => PubCompReason::PacketIdentifierNotFound,
        num => return Err(Error::InvalidConnectReturnCode(num)),
    };

    Ok(code)
}

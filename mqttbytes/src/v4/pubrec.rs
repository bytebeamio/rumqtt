use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Return code in connack
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum PubRecReason {
    Success = 0,
    NoMatchingSubscribers = 16,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
}

/// Acknowledgement to QoS1 publish
#[derive(Debug, Clone, PartialEq)]
pub struct PubRec {
    pub pkid: u16,
    pub reason: PubRecReason,
}

impl PubRec {
    pub fn new(pkid: u16) -> PubRec {
        PubRec {
            pkid,
            reason: PubRecReason::Success,
        }
    }

    fn len(&self) -> usize {
        let len = 2 + 1; // pkid + reason

        // TODO: Verify
        // if self.reason == PubRecReason::Success && self.properties.is_none() {
        if self.reason == PubRecReason::Success {
            return 2;
        }

        // Unlike other packets, property length can be ignored if there are
        // no properties in acks
        len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);
        let pkid = read_u16(&mut bytes)?;
        if fixed_header.remaining_len == 2 {
            return Ok(PubRec {
                pkid,
                reason: PubRecReason::Success,
            });
        }

        let ack_reason = read_u8(&mut bytes)?;
        if fixed_header.remaining_len < 4 {
            return Ok(PubRec {
                pkid,
                reason: reason(ack_reason)?,
            });
        }

        let puback = PubRec {
            pkid,
            reason: reason(ack_reason)?,
        };

        Ok(puback)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = self.len();
        buffer.put_u8(0x50);
        let count = write_remaining_length(buffer, len)?;
        buffer.put_u16(self.pkid);

        // TODO: Verify
        // if self.reason == PubRecReason::Success && self.properties.is_none() {
        if self.reason == PubRecReason::Success {
            return Ok(4);
        }

        buffer.put_u8(self.reason as u8);
        Ok(1 + count + len)
    }
}


/// Connection return code type
fn reason(num: u8) -> Result<PubRecReason, Error> {
    let code = match num {
        0 => PubRecReason::Success,
        16 => PubRecReason::NoMatchingSubscribers,
        128 => PubRecReason::UnspecifiedError,
        131 => PubRecReason::ImplementationSpecificError,
        135 => PubRecReason::NotAuthorized,
        144 => PubRecReason::TopicNameInvalid,
        145 => PubRecReason::PacketIdentifierInUse,
        151 => PubRecReason::QuotaExceeded,
        153 => PubRecReason::PayloadFormatInvalid,
        num => return Err(Error::InvalidConnectReturnCode(num)),
    };

    Ok(code)
}

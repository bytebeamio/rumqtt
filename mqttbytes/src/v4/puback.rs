use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Return code in connack
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum PubAckReason {
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
pub struct PubAck {
    pub pkid: u16,
    pub reason: PubAckReason,
}

impl PubAck {
    pub fn new(pkid: u16) -> PubAck {
        PubAck {
            pkid,
            reason: PubAckReason::Success,
        }
    }

    fn len(&self) -> usize {
        let len = 2 + 1; // pkid + reason

        // TODO: Verify modified code
        // if self.reason == PubAckReason::Success && self.properties.is_none() {
        if self.reason == PubAckReason::Success {
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

        // No reason code or properties if remaining length == 2
        if fixed_header.remaining_len == 2 {
            return Ok(PubAck {
                pkid,
                reason: PubAckReason::Success,
            });
        }

        // No properties len or properties if remaining len > 2 but < 4
        let ack_reason = read_u8(&mut bytes)?;
        if fixed_header.remaining_len < 4 {
            return Ok(PubAck {
                pkid,
                reason: reason(ack_reason)?,
            });
        }

        let puback = PubAck {
            pkid,
            reason: reason(ack_reason)?,
        };

        Ok(puback)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = self.len();
        buffer.put_u8(0x40);

        let count = write_remaining_length(buffer, len)?;
        buffer.put_u16(self.pkid);

        // TODO: Verify modified code
        // if self.reason == PubAckReason::Success && self.properties.is_none() {
        if self.reason == PubAckReason::Success {
            return Ok(4);
        }

        buffer.put_u8(self.reason as u8);
        Ok(1 + count + len)
    }
}


/// Connection return code type
fn reason(num: u8) -> Result<PubAckReason, Error> {
    let code = match num {
        0 => PubAckReason::Success,
        16 => PubAckReason::NoMatchingSubscribers,
        128 => PubAckReason::UnspecifiedError,
        131 => PubAckReason::ImplementationSpecificError,
        135 => PubAckReason::NotAuthorized,
        144 => PubAckReason::TopicNameInvalid,
        145 => PubAckReason::PacketIdentifierInUse,
        151 => PubAckReason::QuotaExceeded,
        153 => PubAckReason::PayloadFormatInvalid,
        num => return Err(Error::InvalidConnectReturnCode(num)),
    };

    Ok(code)
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;

    #[test]
    fn puback_encoding_works() {
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
        let fixed_header = parse_fixed_header(stream.iter()).unwrap();
        let ack_bytes = stream.split_to(fixed_header.frame_length()).freeze();
        let packet = PubAck::read(fixed_header, ack_bytes).unwrap();

        assert_eq!(
            packet,
            PubAck {
                pkid: 10,
                reason: PubAckReason::Success,
            }
        );
    }
}


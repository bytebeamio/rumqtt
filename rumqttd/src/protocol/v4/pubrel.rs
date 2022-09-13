use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

fn len() -> usize {
    // pkid
    2
}

pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<PubRel, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);
    let pkid = read_u16(&mut bytes)?;
    if fixed_header.remaining_len == 2 {
        return Ok(PubRel {
            pkid,
            reason: PubRelReason::Success,
        });
    }

    if fixed_header.remaining_len < 4 {
        return Ok(PubRel {
            pkid,
            reason: PubRelReason::Success,
        });
    }

    let puback = PubRel {
        pkid,
        reason: PubRelReason::Success,
    };

    Ok(puback)
}

pub fn write(pubrel: &PubRel, buffer: &mut BytesMut) -> Result<usize, Error> {
    let len = len();
    buffer.put_u8(0x62);
    let count = write_remaining_length(buffer, len)?;
    buffer.put_u16(pubrel.pkid);
    Ok(1 + count + len)
}

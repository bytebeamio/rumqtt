use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

fn len() -> usize {
    // pkid
    2
}

pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<PubRec, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);
    let pkid = read_u16(&mut bytes)?;
    if fixed_header.remaining_len == 2 {
        return Ok(PubRec {
            pkid,
            reason: PubRecReason::Success,
        });
    }

    if fixed_header.remaining_len < 4 {
        return Ok(PubRec {
            pkid,
            reason: PubRecReason::Success,
        });
    }

    let puback = PubRec {
        pkid,
        reason: PubRecReason::Success,
    };

    Ok(puback)
}

pub fn write(pubrec: &PubRec, buffer: &mut BytesMut) -> Result<usize, Error> {
    let len = len();
    buffer.put_u8(0x50);
    let count = write_remaining_length(buffer, len)?;
    buffer.put_u16(pubrec.pkid);
    Ok(1 + count + len)
}

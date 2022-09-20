use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

fn len() -> usize {
    // pkid
    2
}

pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<PubAck, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);

    if fixed_header.remaining_len != 2 {
        return Err(Error::InvalidRemainingLength(fixed_header.remaining_len));
    }

    let pkid = read_u16(&mut bytes)?;
    let puback = PubAck {
        pkid,
        reason: PubAckReason::Success,
    };

    Ok(puback)
}

pub fn write(puback: &PubAck, buffer: &mut BytesMut) -> Result<usize, Error> {
    let len = len();
    buffer.put_u8(0x40);
    let count = write_remaining_length(buffer, len)?;
    buffer.put_u16(puback.pkid);
    Ok(1 + count + len)
}

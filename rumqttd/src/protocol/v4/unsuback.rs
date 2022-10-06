use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<UnsubAck, Error> {
    if fixed_header.remaining_len != 2 {
        return Err(Error::PayloadSizeIncorrect);
    }

    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);
    let pkid = read_u16(&mut bytes)?;
    let unsuback = UnsubAck {
        pkid,
        reasons: vec![],
    };

    Ok(unsuback)
}

pub fn write(unsuback: &UnsubAck, buffer: &mut BytesMut) -> Result<usize, Error> {
    buffer.put_slice(&[0xB0, 0x02]);
    buffer.put_u16(unsuback.pkid);
    Ok(4)
}

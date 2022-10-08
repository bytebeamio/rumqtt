use super::*;
use bytes::{BufMut, BytesMut};

// In v4 there are no Reason Code and properties
pub fn write(_disconnect: &Disconnect, payload: &mut BytesMut) -> Result<usize, Error> {
    payload.put_slice(&[0xE0, 0x00]);
    Ok(2)
}

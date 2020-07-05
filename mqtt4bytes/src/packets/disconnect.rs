use crate::*;
use bytes::{BytesMut, BufMut};

pub struct Disconnect;

impl Disconnect {
    pub fn write(&self, payload: &mut BytesMut) -> Result<usize, Error> {
        payload.put_slice(&[0xE0, 0x00]);
        Ok(2)
    }
}

use super::*;
use bytes::{BufMut, BytesMut};

pub struct PingResp;

impl PingResp {
    pub fn write(&self, payload: &mut BytesMut) -> Result<usize, MqttError> {
        payload.put_slice(&[0xD0, 0x00]);
        Ok(2)
    }
}

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PingReq;

impl PingReq {
    pub fn write(payload: &mut Vec<u8>) -> Result<usize, Error> {
        payload.extend_from_slice(&[0xC0, 0x00]);
        Ok(2)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PingResp;

impl PingResp {
    pub fn write(payload: &mut Vec<u8>) -> Result<usize, Error> {
        payload.extend_from_slice(&[0xD0, 0x00]);
        Ok(2)
    }
}

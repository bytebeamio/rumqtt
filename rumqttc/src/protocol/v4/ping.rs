use super::*;

pub mod pingreq {
    use super::*;

    pub fn write(payload: &mut Vec<u8>) -> Result<usize, Error> {
        payload.extend_from_slice(&[0xC0, 0x00]);
        Ok(2)
    }
}

pub mod pingresp {
    use super::*;

    pub fn write(buffer: &mut Vec<u8>) -> Result<usize, Error> {
        buffer.extend_from_slice(&[0xD0, 0x00]);
        Ok(2)
    }
}

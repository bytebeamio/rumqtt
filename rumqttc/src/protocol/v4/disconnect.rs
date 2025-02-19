use super::*;

// In v4 there are no Reason Code and properties
pub fn write(_disconnect: &Disconnect, payload: &mut Vec<u8>) -> Result<usize, Error> {
    payload.extend_from_slice(&[0xE0, 0x00]);
    Ok(2)
}

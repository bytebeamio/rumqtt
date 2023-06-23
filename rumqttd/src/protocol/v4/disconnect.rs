use super::*;
use bytes::{BufMut, BytesMut};

// In v4 there are no Reason Code and properties
pub fn write(_disconnect: &Disconnect, _payload: &mut BytesMut) -> Result<usize, Error> {
    // The MQTT v4 (3.1.1) specification only mentions that DISCONNECT packages are sent
    // from the client to the server (see section 3.14).
    Ok(0)
}

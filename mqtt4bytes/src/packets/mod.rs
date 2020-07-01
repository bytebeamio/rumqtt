mod connect;
mod connack;
mod publish;
mod puback;
mod pubrec;
mod pubrel;
mod pubcomp;
mod subscribe;
mod suback;
mod unsubscribe;
mod unsuback;
mod disconnect;

pub use connect::*;
pub use connack::*;
pub use publish::*;
pub use puback::*;
pub use pubrec::*;
pub use pubrel::*;
pub use pubcomp::*;
pub use subscribe::*;
pub use suback::*;
pub use unsubscribe::*;
pub use unsuback::*;
pub use disconnect::*;

use crate::*;
use bytes::Bytes;
use alloc::string::String;

/// Reads a string from bytes stream
fn read_mqtt_string(stream: &mut Bytes) -> Result<String, Error> {
    let len = stream.get_u16() as usize;
    // Invalid packets which reached this point (simulated invalid packets actually triggered this)
    // should not cause the split to cross boundaries
    if len > stream.len() {
        return Err(Error::BoundaryCrossed);
    }

    let s = stream.split_to(len);
    match String::from_utf8(s.to_vec()) {
        Ok(v) => Ok(v),
        Err(_e) => Err(Error::TopicNotUtf8),
    }
}

mod ping;
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
pub use ping::*;

use crate::*;
use bytes::{Bytes, BytesMut, BufMut};
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

/// Serializes a string to stream
fn write_mqtt_string(stream: &mut BytesMut, string: &str) {
    stream.put_u16(string.len() as u16);
    stream.extend_from_slice(string.as_bytes());
}

/// Writes remaining length to stream and returns number of bytes for remaining length
fn write_remaining_length(stream: &mut BytesMut, len: usize) -> Result<usize, Error> {
    if len > 268_435_455 {
        return Err(Error::PayloadTooLong);
    }

    let mut done = false;
    let mut x = len;
    let mut count = 0;

    while !done {
        let mut byte = (x % 128) as u8;
        x /= 128;
        if x > 0 {
            byte |= 128;
        }

        stream.put_u8(byte);
        count += 1;
        done = x == 0;
    }

    Ok(count)
}

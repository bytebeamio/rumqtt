use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{
    convert::{TryFrom, TryInto},
    slice::Iter,
    str::Utf8Error,
};

#[derive(Debug, Clone)]
pub struct Shred;

impl Protocol for Shred {
    /// Reads a stream of bytes and extracts next MQTT packet out of it
    fn read(&mut self, s: &mut BytesMut, max_packet_size: usize) -> Result<Frame, Error> {
        let data = BytesMut::from(&s[..]);
        s.clear();
        Ok(Frame::Passthrough(data))
    }

    fn write(&self, packet: Packet, buffer: &mut BytesMut) -> Result<usize, Error> {
        let size = match packet {
            Packet::PubAck(puback, None) => {
                buffer.put_u16(puback.pkid);
                2
            }
            _ => unreachable!(),
        };

        Ok(size)
    }
}

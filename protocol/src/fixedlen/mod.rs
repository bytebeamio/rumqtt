use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{
    convert::{TryFrom, TryInto},
    slice::Iter,
    str::Utf8Error,
};

#[derive(Debug, Clone)]
pub struct FL;

impl Protocol for FL {
    /// Reads a stream of bytes and extracts next MQTT packet out of it
    fn read(&mut self, stream: &mut BytesMut, max_packet_size: usize) -> Result<Frame, Error> {
        let frame_len = match check(&stream) {
            Check::Ready(packet_size) => packet_size,
            Check::Pending(pending_size) => return Ok(Frame::Pending(pending_size)),
        };

        // Don't let rogue connections attack with huge payloads.
        // Disconnect them before reading all that data
        if frame_len > max_packet_size {
            return Err(Error::PayloadSizeLimitExceeded(frame_len));
        }

        let remaining_len = stream.get_u16();
        let pkid = stream.get_u16();
        let packet_len = remaining_len as usize - 2;

        let payload = Bytes::copy_from_slice(&stream[..packet_len]);
        stream.advance(packet_len);
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            pkid,
            retain: false,
            topic: "dummy".into(),
            payload,
        };

        let packet = Packet::Publish(publish, None);
        Ok(Frame::Ready(packet))
    }

    fn write(&self, packet: Packet, buffer: &mut BytesMut) -> Result<usize, Error> {
        let size = match packet {
            Packet::Publish(publish, None) => {
                let remaining_len = publish.payload.len() + 2;
                buffer.put_u16(remaining_len as u16);
                buffer.put_u16(publish.pkid);
                buffer.put_slice(&publish.payload);
                4 + publish.payload.len()
            }
            Packet::PubAck(puback, None) => {
                buffer.put_u16(puback.pkid);
                2
            }
            _ => unreachable!(),
        };

        Ok(size)
    }
}

enum Check {
    // Number of pending bytes to create a frame
    Pending(usize),
    // Size of the frame
    Ready(usize),
}

/// Checks if the stream has enough bytes to frame a packet and returns fixed header
/// only if a packet can be framed with existing bytes in the `stream`.
/// The passed stream doesn't modify parent stream's cursor. If this function
/// returned an error, next `check` on the same parent stream is forced start
/// with cursor at 0 again (Iter is owned. Only Iter's cursor is changed internally)
fn check(stream: &[u8]) -> Check {
    // Create fixed header if there are enough bytes in the stream
    // to frame full packet
    let stream_len = stream.len();
    if stream_len < 2 {
        return Check::Pending(2 - stream_len);
    }

    let mut buf = [0; 2];

    buf.copy_from_slice(&stream[0..2]);
    let remaining_len = u16::from_be_bytes(buf);
    let frame_len = 2 + remaining_len as usize;

    // dbg!(stream_len, frame_len);
    if stream_len < frame_len {
        return Check::Pending(frame_len - stream_len);
    }

    Check::Ready(frame_len)
}

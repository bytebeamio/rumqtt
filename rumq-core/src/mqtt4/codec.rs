use bytes::buf::Buf;
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use std::io;
use std::io::{Cursor, ErrorKind::TimedOut, ErrorKind::UnexpectedEof, ErrorKind::WouldBlock};

use crate::mqtt4::Packet;
use crate::mqtt4::{MqttRead, MqttWrite};
use crate::Error;

pub struct MqttCodec {
    max_payload_size: usize,
}

impl MqttCodec {
    pub fn new(max_payload_size: usize) -> Self {
        MqttCodec { max_payload_size }
    }
}

impl Decoder for MqttCodec {
    type Item = Packet;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Packet>, Error> {
        // NOTE: `decode` might be called with `buf.len == 0`. We should return
        // Ok(None) in those cases or else the internal `read_exact` call will return UnexpectedEOF
        if buf.len() < 2 {
            return Ok(None);
        }

        // TODO: Better implementation by taking packet type and remaining len into account here
        // Maybe the next `decode` call can wait for publish size byts to be filled in `buf` before being
        // invoked again
        // TODO: Find how the underlying implementation invokes this method. Is there an
        // implementation with size awareness?
        let mut buf_ref = buf.as_ref();
        let (packet_type, remaining_len) = match buf_ref.read_packet_type_and_remaining_length() {
            Ok(len) => len,
            // Not being able to fill `buf_ref` entirely is also UnexpectedEof
            // This would be a problem if `buf` len is 2 and if the packet is not ping or other 2
            // byte len, `read_packet_type_and_remaining_length` call tries reading more than 2 bytes
            // from `buf` and results in Ok(0) which translates to Eof error when target buffer is
            // not completely full
            // https://doc.rust-lang.org/stable/std/io/trait.Read.html#tymethod.read
            // https://doc.rust-lang.org/stable/src/std/io/mod.rs.html#501-944
            Err(Error::Io(e)) if e.kind() == TimedOut || e.kind() == WouldBlock || e.kind() == UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        if remaining_len > self.max_payload_size {
            return Err(Error::PayloadSizeLimitExceeded);
        }

        let header_len = buf_ref.header_len(remaining_len);
        let len = header_len + remaining_len;

        // NOTE: It's possible that `decode` got called before `buf` has full bytes
        // necessary to frame raw bytes into a packet. In that case return Ok(None)
        // and the next time decode` gets called, there will be more bytes in `buf`,
        // hopefully enough to frame the packet
        if buf.len() < len {
            return Ok(None);
        }

        let packet = buf_ref.deserialize(packet_type, remaining_len)?;
        buf.advance(len);
        Ok(Some(packet))
    }
}

impl Encoder for MqttCodec {
    type Item = Packet;
    type Error = io::Error;

    fn encode(&mut self, msg: Packet, buf: &mut BytesMut) -> Result<(), io::Error> {
        let mut stream = Cursor::new(Vec::new());

        // TODO: Implement `write_packet` for `&mut BytesMut`
        if let Err(_) = stream.mqtt_write(&msg) {
            return Err(io::Error::new(io::ErrorKind::Other, "Unable to encode!"));
        }

        buf.extend(stream.get_ref());
        Ok(())
    }
}

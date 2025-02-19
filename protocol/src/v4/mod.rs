use super::*;
use std::{
    convert::{TryFrom, TryInto},
    default::Default,
    mem::size_of,
    slice::Iter,
    str::Utf8Error,
};

pub mod connack;
pub mod connect;
pub mod disconnect;
pub mod ping;
pub mod puback;
pub mod pubcomp;
pub mod publish;
pub mod pubrec;
pub mod pubrel;
pub mod suback;
pub mod subscribe;
pub mod unsuback;
pub mod unsubscribe;

pub fn packet_type_to_u8(value: PacketType) -> u8 {
    value as u8
}

pub fn u8_to_packet_type(value: u8) -> Result<PacketType, Error> {
    match value {
        1 => Ok(PacketType::Connect),
        2 => Ok(PacketType::ConnAck),
        3 => Ok(PacketType::Publish),
        4 => Ok(PacketType::PubAck),
        5 => Ok(PacketType::PubRec),
        6 => Ok(PacketType::PubRel),
        7 => Ok(PacketType::PubComp),
        8 => Ok(PacketType::Subscribe),
        9 => Ok(PacketType::SubAck),
        10 => Ok(PacketType::Unsubscribe),
        11 => Ok(PacketType::UnsubAck),
        12 => Ok(PacketType::PingReq),
        13 => Ok(PacketType::PingResp),
        14 => Ok(PacketType::Disconnect),
        _ => Err(Error::InvalidPacketType(value)),
    }
}

pub fn qos_to_u8(value: QoS) -> u8 {
    value as u8
}

pub fn u8_to_qos(value: u8) -> Result<QoS, Error> {
    match value {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        _ => Err(Error::InvalidQoS(value)),
    }
}

#[derive(Debug, Clone)]
pub enum Frame {
    /// Requires more bytes to frame. Arg implies minimum additional bytes required
    Pending(usize),
    /// Packet is ready to be processed. Tuple contains packet type and size
    Ready(PacketType, usize),
}

/// Packet type from a byte
///
/// ```ignore
///          7                          3                          0
///          +--------------------------+--------------------------+
/// byte 1   | MQTT Control Packet Type | Flags for each type      |
///          +--------------------------+--------------------------+
///          |         Remaining Bytes Len  (1/2/3/4 bytes)        |
///          +-----------------------------------------------------+
///
/// <https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc385349207>
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub struct FixedHeader {
    /// First byte of the stream. Used to identify packet types and
    /// several flags
    pub byte1: u8,
    /// Length of fixed header. Byte 1 + (1..4) bytes. So fixed header
    /// len can vary from 2 bytes to 5 bytes
    /// 1..4 bytes are variable length encoded to represent remaining length
    pub fixed_header_len: usize,
    /// Remaining length of the packet. Doesn't include fixed header bytes
    /// Represents variable header + payload size
    pub remaining_len: usize,
}

impl FixedHeader {
    pub fn new(byte1: u8, remaining_len_len: usize, remaining_len: usize) -> FixedHeader {
        FixedHeader {
            byte1,
            fixed_header_len: remaining_len_len + 1,
            remaining_len,
        }
    }

    pub fn packet_type(&self) -> Result<PacketType, Error> {
        let num = self.byte1 >> 4;
        u8_to_packet_type(num)
    }

    /// Returns the size of full packet (fixed header + variable header + payload)
    /// Fixed header is enough to get the size of a frame in the stream
    pub fn frame_length(&self) -> usize {
        self.fixed_header_len + self.remaining_len
    }
}

/// Checks if the stream has enough bytes to frame a packet and returns fixed header
/// only if a packet can be framed with existing bytes in the `stream`.
/// The passed stream doesn't modify parent stream's cursor. If this function
/// returned an error, next `check` on the same parent stream is forced start
/// with cursor at 0 again (Iter is owned. Only Iter's cursor is changed internally)
pub fn check(stream: &[u8], max_packet_size: usize) -> Result<FixedHeader, Error> {
    // Create fixed header if there are enough bytes in the stream
    // to frame full packet
    let stream_len = stream.len();
    let fixed_header = parse_fixed_header(stream)?;

    // Don't let rogue connections attack with huge payloads.
    // Disconnect them before reading all that data
    if fixed_header.remaining_len > max_packet_size {
        return Err(Error::PayloadSizeLimitExceeded(fixed_header.remaining_len));
    }

    // If the current call fails due to insufficient bytes in the stream,
    // after calculating remaining length, we extend the stream
    let frame_length = fixed_header.frame_length();
    if stream_len < frame_length {
        return Err(Error::InsufficientBytes(frame_length - stream_len));
    }

    Ok(fixed_header)
}

/// Parses fixed header
pub fn parse_fixed_header(mut stream: &[u8]) -> Result<FixedHeader, Error> {
    // At least 2 bytes are necessary to frame a packet
    let stream_len = stream.len();
    if stream_len < 2 {
        return Err(Error::InsufficientBytes(2 - stream_len));
    }

    let byte1 = stream[0];
    let (len_len, len) = length(&stream[1..])?;

    Ok(FixedHeader::new(byte1, len_len, len))
}

/// Parses variable byte integer in the stream and returns the length
/// and number of bytes that make it. Used for remaining length calculation
/// as well as for calculating property lengths
pub fn length(stream: &[u8]) -> Result<(usize, usize), Error> {
    let mut len: usize = 0;
    let mut len_len = 0;
    let mut done = false;
    let mut shift = 0;

    // Use continuation bit at position 7 to continue reading next
    // byte to frame 'length'.
    // Stream 0b1xxx_xxxx 0b1yyy_yyyy 0b1zzz_zzzz 0b0www_wwww will
    // be framed as number 0bwww_wwww_zzz_zzzz_yyy_yyyy_xxx_xxxx
    for byte in stream {
        len_len += 1;
        let byte = *byte as usize;
        len += (byte & 0x7F) << shift;

        // stop when continue bit is 0
        done = (byte & 0x80) == 0;
        if done {
            break;
        }

        shift += 7;

        // Only a max of 4 bytes allowed for remaining length
        // more than 4 shifts (0, 7, 14, 21) implies bad length
        if shift > 21 {
            return Err(Error::MalformedRemainingLength);
        }
    }

    // Not enough bytes to frame remaining length. wait for
    // one more byte
    if !done {
        return Err(Error::InsufficientBytes(1));
    }

    Ok((len_len, len))
}

/// After collecting enough bytes to frame a packet (packet's frame())
/// , It's possible that content itself in the stream is wrong. Like expected
/// packet id or qos not being present. In cases where `read_mqtt_string` or
/// `read_mqtt_bytes` exhausted remaining length but packet framing expects to
/// parse qos next, these pre checks will prevent `bytes` crashes
pub fn read_u16(stream: &mut &[u8]) -> Result<u16, Error> {
    if stream.len() < 2 {
        return Err(Error::MalformedPacket);
    }
    let (u16_from_stream, rest) = stream.split_at(size_of::<u16>());
    *stream = rest;

    let u16_from_stream = u16::from_be_bytes(u16_from_stream.try_into().unwrap());

    Ok(u16_from_stream)
}

fn read_u8(stream: &mut &[u8]) -> Result<u8, Error> {
    let Some((&u8_from_stream, rest)) = stream.split_first() else {
        // stream was empty
        return Err(Error::MalformedPacket);
    };

    *stream = rest;

    Ok(u8_from_stream)
}

/// Reads a series of bytes with a length from a byte stream
fn read_mqtt_bytes(stream: &mut &[u8]) -> Result<Vec<u8>, Error> {
    let len = read_u16(stream)? as usize;

    // Prevent attacks with wrong remaining length. This method is used in
    // `packet.assembly()` with (enough) bytes to frame packet. Ensures that
    // reading variable len string or bytes doesn't cross promised boundary
    // with `read_fixed_header()`
    if len > stream.len() {
        return Err(Error::BoundaryCrossed(len));
    }

    let (read_bytes, rest) = stream.split_at(len);

    *stream = rest;

    // TODO(swanx): remove this to_vec if possible
    Ok(read_bytes.to_vec())
}

/// Reads a string from bytes stream
fn read_mqtt_string(stream: &mut &[u8]) -> Result<String, Error> {
    let s = read_mqtt_bytes(stream)?;
    match String::from_utf8(s) {
        Ok(v) => Ok(v),
        Err(_e) => Err(Error::TopicNotUtf8),
    }
}

/// Serializes bytes to stream (including length)
fn write_mqtt_bytes(stream: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len() as u16;
    stream.extend_from_slice(&len.to_be_bytes());
    stream.extend_from_slice(bytes);
}

/// Serializes a string to stream
fn write_mqtt_string(stream: &mut Vec<u8>, string: &str) {
    write_mqtt_bytes(stream, string.as_bytes());
}

/// Writes remaining length to stream and returns number of bytes for remaining length
pub fn write_remaining_length(stream: &mut Vec<u8>, len: usize) -> Result<usize, Error> {
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

        stream.push(byte);
        count += 1;
        done = x == 0;
    }

    Ok(count)
}

/// Return number of remaining length bytes required for encoding length
fn len_len(len: usize) -> usize {
    if len >= 2_097_152 {
        4
    } else if len >= 16_384 {
        3
    } else if len >= 128 {
        2
    } else {
        1
    }
}

pub fn qos(num: u8) -> Option<QoS> {
    match num {
        0 => Some(QoS::AtMostOnce),
        1 => Some(QoS::AtLeastOnce),
        2 => Some(QoS::ExactlyOnce),
        qos => None,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct V4;

impl V4 {
    fn fixed_header(&self, stream: &[u8], max_size: usize) -> Result<FixedHeader, Error> {
        // Your implementation here
        let fixed_header = match check(stream, max_size) {
            Ok(v) => v,
            Err(e) => return Err(e)?,
        };

        Ok(fixed_header)
    }
}

// TODO: Cleanup unnecessary `self` and `BytesMut` where not required
// TODO: We pass `max_size` to almost all methods, maybe it can be moved into V4 struct
impl Protocol for V4 {
    // fn check(&self, stream: &[u8], max_size: usize) -> Result<Frame, Error> {
    //     let fixed_header = match self.fixed_header(stream, max_size) {
    //         Ok(v) => v,
    //         Err(Error::InsufficientBytes(i)) => return Ok(Frame::Pending(i)),
    //         Err(e) => return Err(e)?,
    //     };

    //     let frame_type = fixed_header.packet_type()?;
    //     let frame_length = fixed_header.frame_length();
    //     Ok(Frame::Ready(frame_type, frame_length))
    // }

    // fn frame(
    //     &self,
    //     stream: &mut &[u8],
    //     frames: &mut Vec<u8>,
    //     max_size: usize,
    // ) -> Result<Frame, Error> {
    //     let fixed_header = self.fixed_header(stream, max_size)?;
    //     let frame_length = fixed_header.frame_length();

    //     // TODO: Test with a stream with exactly the size to check border panics

    //     let (frame, rest) = stream.split_at(frame_length);
    //     frames.extend_from_slice(frame);
    //     *stream = rest;

    //     let frame_type = fixed_header.packet_type()?;
    //     let frame_length = fixed_header.frame_length();
    //     Ok(Frame::Ready(frame_type, frame_length))
    // }

    // TODO: Keep adding more as required, need to add relevant function signature to `Protocol`
    // trait
    derive_read!(publish, PacketType::Publish, Publish);
    derive_read!(connect, PacketType::Connect, Connect);
    derive_write!(publish, Publish);
    derive_write!(connect, Connect);

    /// Reads a stream of bytes and extracts next MQTT packet out of it
    fn read(&self, stream: &mut &[u8], max_size: usize) -> Result<Packet, Error> {
        let fixed_header = self.fixed_header(stream, max_size)?;

        let frame_length = fixed_header.frame_length();
        let packet_type = fixed_header.packet_type()?;
        let (packet, rest) = stream.split_at(frame_length);
        *stream = rest;

        if fixed_header.remaining_len == 0 {
            // no payload packets
            let packet = match packet_type {
                PacketType::PingReq => Packet::PingReq(PingReq),
                PacketType::PingResp => Packet::PingResp(PingResp),
                PacketType::Disconnect => Packet::Disconnect(Disconnect {
                    reason_code: DisconnectReasonCode::NormalDisconnection,
                    properties: None,
                }),
                _ => return Err(Error::PayloadRequired),
            };

            return Ok(packet);
        }

        let packet = match packet_type {
            PacketType::Connect => {
                let connect = connect::read(fixed_header, packet)?;
                Packet::Connect(connect)
            }
            PacketType::ConnAck => Packet::ConnAck(connack::read(fixed_header, packet)?),
            PacketType::Publish => Packet::Publish(publish::read(fixed_header, packet)?),
            PacketType::PubAck => Packet::PubAck(puback::read(fixed_header, packet)?),
            PacketType::Subscribe => Packet::Subscribe(subscribe::read(fixed_header, packet)?),
            PacketType::SubAck => Packet::SubAck(suback::read(fixed_header, packet)?),
            PacketType::Unsubscribe => {
                Packet::Unsubscribe(unsubscribe::read(fixed_header, packet)?)
            }
            PacketType::UnsubAck => Packet::UnsubAck(unsuback::read(fixed_header, packet)?),
            PacketType::PingReq => Packet::PingReq(PingReq),
            PacketType::PingResp => Packet::PingResp(PingResp),
            PacketType::PubRec => Packet::PubRec(pubrec::read(fixed_header, packet)?),
            PacketType::PubRel => Packet::PubRel(pubrel::read(fixed_header, packet)?),
            PacketType::PubComp => Packet::PubComp(pubcomp::read(fixed_header, packet)?),
            // v4 Disconnect packet gets handled in the previous check, this branch gets hit when
            // Disconnect packet has properties which is only valid for v5
            PacketType::Disconnect => return Err(Error::InvalidProtocol),
            _ => unreachable!(),
        };

        Ok(packet)
    }

    fn write(&self, packet: Packet, buffer: &mut Vec<u8>) -> Result<usize, Error> {
        let size = match packet {
            Packet::Connect(connect) => connect::write(&connect, buffer)?,
            Packet::ConnAck(connack) => connack::write(&connack, buffer)?,
            Packet::Publish(publish) => publish::write(&publish, buffer)?,
            Packet::PubAck(puback) => puback::write(&puback, buffer)?,
            Packet::Subscribe(subscribe) => subscribe::write(&subscribe, buffer)?,
            Packet::SubAck(suback) => suback::write(&suback, buffer)?,
            Packet::PubRec(pubrec) => pubrec::write(&pubrec, buffer)?,
            Packet::PubRel(pubrel) => pubrel::write(&pubrel, buffer)?,
            Packet::PubComp(pubcomp) => pubcomp::write(&pubcomp, buffer)?,
            Packet::Unsubscribe(unsubscribe) => unsubscribe::write(&unsubscribe, buffer)?,
            Packet::UnsubAck(unsuback) => unsuback::write(&unsuback, buffer)?,
            Packet::Disconnect(disconnect) => disconnect::write(&disconnect, buffer)?,
            Packet::PingReq(pingreq) => ping::pingreq::write(buffer)?,
            Packet::PingResp(pingresp) => ping::pingresp::write(buffer)?,
            _ => unreachable!(
                "This branch only matches for packets with Properties, which is not possible in v4",
            ),
        };
        Ok(size)
    }
}

#[macro_export]
macro_rules! derive_read {
    ($func_name:ident, $packet_type:pat, $packet:ty) => {
        paste::paste! {
            fn [<read_$func_name>](&self, stream: &mut &[u8], max_size: usize) -> Result<$packet, Error> {
                let fixed_header = self.fixed_header(stream, max_size)?;
                let packet_type = fixed_header.packet_type()?;
                let (packet, rest) = stream.split_at(fixed_header.frame_length());
                *stream = rest;

                match packet_type {
                    $packet_type => {
                        // overloading function name as module name
                        let publish = $func_name::read(fixed_header, packet)?;
                        Ok(publish)
                    }
                    _ => Err(Error::IncorrectPacketFormat),
                }
            }
        }
    };
}

#[macro_export]
macro_rules! derive_write {
    ($packet_name:ident, $packet:ty) => {
        paste::paste! {
            fn [<write_ $packet_name>](
                &self,
                packet: &$packet,
                buffer: &mut Vec<u8>,
            ) -> Result<usize, Error> {
                let size = $packet_name::write(packet, buffer)?;
                Ok(size)
            }
        }
    };
}

impl Default for V4 {
    fn default() -> Self {
        // Provide a default implementation for V4
        V4 {
            // Initialize fields with default values
        }
    }
}

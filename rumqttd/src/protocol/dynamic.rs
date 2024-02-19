use crate::{router::Ack, SupportedProtocol};

use super::{v4::V4, v5::V5, *};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{
    convert::{TryFrom, TryInto},
    slice::Iter,
    str::Utf8Error,
};

/// MQTT packet type
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Connect = 1,
    ConnAck,
    Publish,
    PubAck,
    PubRec,
    PubRel,
    PubComp,
    Subscribe,
    SubAck,
    Unsubscribe,
    UnsubAck,
    PingReq,
    PingResp,
    Disconnect,
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
        match num {
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
            _ => Err(Error::InvalidPacketType(num)),
        }
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
pub fn check(stream: Iter<u8>, max_packet_size: usize) -> Result<FixedHeader, Error> {
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
fn parse_fixed_header(mut stream: Iter<u8>) -> Result<FixedHeader, Error> {
    // At least 2 bytes are necessary to frame a packet
    let stream_len = stream.len();
    if stream_len < 2 {
        return Err(Error::InsufficientBytes(2 - stream_len));
    }

    let byte1 = stream.next().unwrap();
    let (len_len, len) = length(stream)?;

    Ok(FixedHeader::new(*byte1, len_len, len))
}

/// Parses variable byte integer in the stream and returns the length
/// and number of bytes that make it. Used for remaining length calculation
/// as well as for calculating property lengths
pub fn length(stream: Iter<u8>) -> Result<(usize, usize), Error> {
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
pub fn read_u16(stream: &mut Bytes) -> Result<u16, Error> {
    if stream.len() < 2 {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u16())
}

fn read_u8(stream: &mut Bytes) -> Result<u8, Error> {
    if stream.is_empty() {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u8())
}

/// Reads a series of bytes with a length from a byte stream
fn read_mqtt_bytes(stream: &mut Bytes) -> Result<Bytes, Error> {
    let len = read_u16(stream)? as usize;

    // Prevent attacks with wrong remaining length. This method is used in
    // `packet.assembly()` with (enough) bytes to frame packet. Ensures that
    // reading variable len string or bytes doesn't cross promised boundary
    // with `read_fixed_header()`
    if len > stream.len() {
        return Err(Error::BoundaryCrossed(len));
    }

    Ok(stream.split_to(len))
}

/// Reads a string from bytes stream
fn read_mqtt_string(stream: &mut Bytes) -> Result<String, Error> {
    let s = read_mqtt_bytes(stream)?;
    match String::from_utf8(s.to_vec()) {
        Ok(v) => Ok(v),
        Err(_e) => Err(Error::TopicNotUtf8),
    }
}

#[derive(Debug, Clone)]
/// A protocol which dynamically determines how to read the packets.
/// After the connect packet the following packets will be read as determined by
/// the protocol level sent in connect (assuming it's valid & supported).
pub struct Dynamic {
    protocol: CurrentProtocol,
    supported_protocol: SupportedProtocol,
    v4: V4,
    v5: V5,
}

#[derive(Debug, Clone, PartialEq)]
enum CurrentProtocol {
    Unkown,
    V4,
    V5,
}

impl Dynamic {
    pub fn new(sub_protocol: SupportedProtocol) -> Self {
        Self {
            protocol: CurrentProtocol::Unkown,
            supported_protocol: sub_protocol,
            v4: V4,
            v5: V5,
        }
    }

    fn protocol_supported(&self) -> bool {
        match self.supported_protocol {
            SupportedProtocol::All => true,
            SupportedProtocol::V4 => self.protocol == CurrentProtocol::V4,
            SupportedProtocol::V5 => self.protocol == CurrentProtocol::V5,
        }
    }
}

impl Protocol for Dynamic {
    /// Reads a stream of bytes and extracts next MQTT packet out of it
    fn read_mut(&mut self, stream: &mut BytesMut, max_size: usize) -> Result<Packet, Error> {
        /// Protocol is unknown and we recieved a connect packet, attempt to determine what protocol level is requested.
        /// Technically we will read the connect packet twice, once to determine the protocol level and once when actually parsing it
        /// in the specified protocol, could maybe be improved.
        if self.protocol == CurrentProtocol::Unkown {
            let fixed_header = check(stream.iter(), max_size)?;

            let packet_type = fixed_header.packet_type()?;
            /// Protocol is currently unknown, we only want to peek the packet if it is a connect.
            /// We should never not get a connect packet here, except for maybe a ping?
            if packet_type == PacketType::Connect {
                let mut stream_clone = stream.iter();
                let mut packet: Bytes = stream_clone
                    .take(fixed_header.frame_length())
                    .copied()
                    .collect();
                let variable_header_index = fixed_header.fixed_header_len;
                packet.advance(variable_header_index);

                // Variable header
                let protocol_name = read_mqtt_string(&mut packet)?;
                let protocol_level = read_u8(&mut packet)?;

                if protocol_name != "MQTT" {
                    return Err(Error::InvalidProtocol);
                }

                self.protocol = match protocol_level {
                    4 => CurrentProtocol::V4,
                    5 => CurrentProtocol::V5,
                    _ => return Err(Error::InvalidProtocolLevel(protocol_level)),
                }
            }
        }

        match self.protocol {
            CurrentProtocol::V4 if self.protocol_supported() => self.v4.read_mut(stream, max_size),
            CurrentProtocol::V5 if self.protocol_supported() => self.v5.read_mut(stream, max_size),
            _ => Err(Error::UnsupportedProtocolLevel),
        }
    }

    fn write(&self, packet: Packet, buffer: &mut BytesMut) -> Result<usize, Error> {
        match self.protocol {
            CurrentProtocol::V4 if self.protocol_supported() => self.v4.write(packet, buffer),
            CurrentProtocol::V5 if self.protocol_supported() => self.v5.write(packet, buffer),
            _ => Err(Error::UnsupportedProtocolLevel),
        }
    }
}

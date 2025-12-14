#![allow(dead_code)]
use std::{slice::Iter, str::Utf8Error};

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub mod v4;
pub mod v5;

/// Checks if the filter is valid
///
/// <https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106>
pub fn valid_filter(filter: &str) -> bool {
    if filter.is_empty() {
        return false;
    }

    let hirerarchy = filter.split('/').collect::<Vec<&str>>();
    if let Some((last, remaining)) = hirerarchy.split_last() {
        for entry in remaining.iter() {
            // # is not allowed in filter except as a last entry
            // invalid: sport/tennis#/player
            // invalid: sport/tennis/#/ranking
            if entry.contains('#') {
                return false;
            }

            // + must occupy an entire level of the filter
            // invalid: sport+
            if entry.len() > 1 && entry.contains('+') {
                return false;
            }
        }

        // only single '#" or '+' is allowed in last entry
        // invalid: sport/tennis#
        // invalid: sport/++
        if last.len() != 1 && (last.contains('#') || last.contains('+')) {
            return false;
        }
    }
    true
}

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

/// Error during serialization and deserialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("Expected connect packet, received = {0:?}")]
    NotConnect(PacketType),
    #[error("Received an unexpected connect packet")]
    UnexpectedConnect,
    #[error("Invalid return code received as response for connect = {0}")]
    InvalidConnectReturnCode(u8),
    #[error("Invalid reason = {0}")]
    InvalidReason(u8),
    #[error("Invalid protocol used")]
    InvalidProtocol,
    #[error("Invalid protocol level = {0}")]
    InvalidProtocolLevel(u8),
    #[error("Invalid packet format")]
    IncorrectPacketFormat,
    #[error("Invalid packet type = {0}")]
    InvalidPacketType(u8),
    #[error("Packet type unsupported = {0:?}")]
    UnsupportedPacket(PacketType),
    #[error("Invalid retain forward rule = {0}")]
    InvalidRetainForwardRule(u8),
    #[error("Invalid QoS level = {0}")]
    InvalidQoS(u8),
    #[error("Invalid subscribe reason code = {0}")]
    InvalidSubscribeReasonCode(u8),
    #[error("Packet received has id Zero")]
    PacketIdZero,
    #[error("Subscription had id Zero")]
    SubscriptionIdZero,
    #[error("Payload size is incorrect")]
    PayloadSizeIncorrect,
    #[error("Payload is too long")]
    PayloadTooLong,
    #[error("Payload size has been exceeded by {0} bytes")]
    PayloadSizeLimitExceeded(usize),
    #[error("Payload is required")]
    PayloadRequired,
    #[error("Topic not utf-8 = {0}")]
    TopicNotUtf8(#[from] Utf8Error),
    #[error("Promised boundary crossed, contains {0} bytes")]
    BoundaryCrossed(usize),
    #[error("Packet is malformed")]
    MalformedPacket,
    #[error("Remaining length is malformed")]
    MalformedRemainingLength,
    /// More bytes required to frame packet. Argument
    /// implies minimum additional bytes required to
    /// proceed further
    #[error("Insufficient number of bytes to frame packet, {0} more bytes required")]
    InsufficientBytes(usize),
    #[error("Property does not exist = {0}")]
    InvalidPropertyType(u8),
}

/// Quality of service
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
}

/// Maps a number to QoS
pub fn qos(num: u8) -> Result<QoS, Error> {
    match num {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        qos => Err(Error::InvalidQoS(qos)),
    }
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

/// Returns big endian u16 view from next 2 bytes
pub fn view_u16(stream: &[u8]) -> Result<u16, Error> {
    let v = stream.get(0..2).ok_or(Error::MalformedPacket)?;

    Ok(((v[0] as u16) << 8) | (v[1] as u16))
}

/// Returns big endian u16 view from next 2 bytes
pub fn view_str(stream: &[u8], end: usize) -> Result<&str, Error> {
    let v = match stream.get(0..end) {
        Some(v) => v,
        None => return Err(Error::BoundaryCrossed(stream.len())),
    };

    let v = std::str::from_utf8(v)?;
    Ok(v)
}

/// After collecting enough bytes to frame a packet (packet's frame())
/// , It's possible that content itself in the stream is wrong. Like expected
/// packet id or qos not being present. In cases where `read_mqtt_string` or
/// `read_mqtt_bytes` exhausted remaining length but packet framing expects to
/// parse qos next, these pre checks will prevent `bytes` crashes
fn read_u32(stream: &mut Bytes) -> Result<u32, Error> {
    if stream.len() < 4 {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u32())
}

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

/// Serializes bytes to stream (including length)
fn write_mqtt_bytes(stream: &mut BytesMut, bytes: &[u8]) {
    stream.put_u16(bytes.len() as u16);
    stream.extend_from_slice(bytes);
}

/// Serializes a string to stream
pub fn write_mqtt_string(stream: &mut BytesMut, string: &str) {
    write_mqtt_bytes(stream, string.as_bytes());
}

/// Writes remaining length to stream and returns number of bytes for remaining length
pub fn write_remaining_length(stream: &mut BytesMut, len: usize) -> Result<usize, Error> {
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

pub enum Connect {
    V4(v4::connect::Connect),
    V5(v5::connect::Connect),
}

#[derive(Debug)]
pub enum Packet {
    V4(v4::Packet),
    V5(v5::Packet),
}

pub(crate) fn read_first_connect(stream: &mut BytesMut, max_size: usize) -> Result<Connect, Error> {
    let fixed_header = check(stream.iter(), max_size)?;

    // Test with a stream with exactly the size to check border panics
    let packet = stream.split_to(fixed_header.frame_length());
    match fixed_header.packet_type()? {
        PacketType::Connect => {}
        p => return Err(Error::NotConnect(p)),
    }
    let mut packet = packet.freeze();

    let variable_header_index = fixed_header.fixed_header_len;
    packet.advance(variable_header_index);

    // Variable header
    let protocol_name = read_mqtt_bytes(&mut packet)?;
    let protocol_name = std::str::from_utf8(&protocol_name)?.to_owned();
    let protocol_level = read_u8(&mut packet)?;
    if protocol_name != "MQTT" {
        return Err(Error::InvalidProtocol);
    }

    match protocol_level {
        4 => Ok(Connect::V4(v4::connect::connect_v4_part(packet)?)),
        5 => Ok(Connect::V5(v5::connect::connect_v5_part(packet)?)),
        _ => Err(Error::InvalidProtocolLevel(protocol_level)),
    }
}

use derive_more::From;
use std::io;
use std::string::FromUtf8Error;

mod asyncdeserialize;
mod asyncserialize;
mod codec;
mod deserialize;
mod packets;
mod serialize;
mod topic;

pub use asyncdeserialize::AsyncMqttRead;
pub use asyncserialize::AsyncMqttWrite;
pub use deserialize::MqttRead;
pub use packets::*;
pub use serialize::MqttWrite;
pub use topic::*;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Connect = 1,
    Connack,
    Publish,
    Puback,
    Pubrec,
    Pubrel,
    Pubcomp,
    Subscribe,
    Suback,
    Unsubscribe,
    Unsuback,
    Pingreq,
    Pingresp,
    Disconnect,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    Connect(Connect),
    Connack(Connack),
    Publish(Publish),
    Puback(PacketIdentifier),
    Pubrec(PacketIdentifier),
    Pubrel(PacketIdentifier),
    Pubcomp(PacketIdentifier),
    Subscribe(Subscribe),
    Suback(Suback),
    Unsubscribe(Unsubscribe),
    Unsuback(PacketIdentifier),
    Pingreq,
    Pingresp,
    Disconnect,
}

///          7                          3                          0
///          +--------------------------+--------------------------+
/// byte 1   | MQTT Control Packet Type | Flags for each type      |
///          +--------------------------+--------------------------+
///          |         Remaining Bytes Len  (1 - 4 bytes)          |
///          +-----------------------------------------------------+
///
/// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Figure_2.2_-

pub fn qos(num: u8) -> Result<QoS, Error> {
    match num {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        _ => Err(Error::UnsupportedQoS),
    }
}

pub fn packet_type(num: u8) -> Result<PacketType, Error> {
    match num {
        1 => Ok(PacketType::Connect),
        2 => Ok(PacketType::Connack),
        3 => Ok(PacketType::Publish),
        4 => Ok(PacketType::Puback),
        5 => Ok(PacketType::Pubrec),
        6 => Ok(PacketType::Pubrel),
        7 => Ok(PacketType::Pubcomp),
        8 => Ok(PacketType::Subscribe),
        9 => Ok(PacketType::Suback),
        10 => Ok(PacketType::Unsubscribe),
        11 => Ok(PacketType::Unsuback),
        12 => Ok(PacketType::Pingreq),
        13 => Ok(PacketType::Pingresp),
        14 => Ok(PacketType::Disconnect),
        _ => Err(Error::UnsupportedPacketType(num)),
    }
}

pub fn connect_return(num: u8) -> Result<ConnectReturnCode, Error> {
    match num {
        0 => Ok(ConnectReturnCode::Accepted),
        1 => Ok(ConnectReturnCode::BadUsernamePassword),
        2 => Ok(ConnectReturnCode::NotAuthorized),
        3 => Ok(ConnectReturnCode::RefusedIdentifierRejected),
        4 => Ok(ConnectReturnCode::RefusedProtocolVersion),
        5 => Ok(ConnectReturnCode::ServerUnavailable),
        _ => Err(Error::InvalidConnectReturnCode(num)),
    }
}

#[derive(Debug, From)]
pub enum Error {
    InvalidConnectReturnCode(u8),
    InvalidProtocolLevel(String, u8),
    IncorrectPacketFormat,
    UnsupportedQoS,
    UnsupportedPacketType(u8),
    PayloadSizeIncorrect,
    PayloadTooLong,
    PayloadRequired,
    TopicNameMustNotContainNonUtf8(FromUtf8Error),
    MalformedRemainingLength,
    Io(io::Error),
}

use bytes::buf::Buf;
use bytes::BytesMut;
use std::io::{Cursor, ErrorKind::TimedOut, ErrorKind::UnexpectedEof, ErrorKind::WouldBlock};
use tokio_util::codec::{Decoder, Encoder};

pub struct MqttCodec;

impl MqttCodec {
    pub fn new() -> Self {
        MqttCodec
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

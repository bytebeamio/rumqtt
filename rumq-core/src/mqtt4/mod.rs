mod asyncdeserialize;
mod asyncserialize;
mod deserialize;
mod packets;
mod serialize;
mod topic;

pub mod codec;

pub use asyncdeserialize::AsyncMqttRead;
pub use asyncserialize::AsyncMqttWrite;
pub use deserialize::MqttRead;
pub use packets::*;
pub use serialize::MqttWrite;
pub use topic::*;

use crate::Error;

/// Quality of service
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PacketType {
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

/// Encapsulates all the possible mqtt packets
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

//          7                          3                          0
//          +--------------------------+--------------------------+
// byte 1   | MQTT Control Packet Type | Flags for each type      |
//          +--------------------------+--------------------------+
//          |         Remaining Bytes Len  (1 - 4 bytes)          |
//          +-----------------------------------------------------+
//
// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Figure_2.2_-

fn qos(num: u8) -> Result<QoS, Error> {
    match num {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        _ => Err(Error::UnsupportedQoS),
    }
}

fn packet_type(num: u8) -> Result<PacketType, Error> {
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

fn connect_return(num: u8) -> Result<ConnectReturnCode, Error> {
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

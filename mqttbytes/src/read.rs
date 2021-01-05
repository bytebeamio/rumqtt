use crate::*;
use bytes::BytesMut;
use packets::*;

/// Reading connect packet is a separate method as it's used to bootstrap
/// protocol version of the connection. `read` method uses this information
/// to parse packets of the resulting version
pub fn read_connect(stream: &mut BytesMut, max_packet_size: usize) -> Result<Connect, Error> {
    let fixed_header = check(stream.iter(), max_packet_size)?;

    // Test with a stream with exactly the size to check border panics
    let packet = stream.split_to(fixed_header.frame_length());
    let packet_type = fixed_header.packet_type()?;

    let packet = packet.freeze();
    let connect = match packet_type {
        PacketType::Connect => Connect::read(fixed_header, packet)?,
        v => return Err(Error::NotConnect(v)),
    };

    Ok(connect)
}

/// Reads a stream of bytes and extracts next MQTT packet out of it
pub fn read(stream: &mut BytesMut, max_size: usize) -> Result<Packet, Error> {
    let fixed_header = check(stream.iter(), max_size)?;

    // Test with a stream with exactly the size to check border panics
    let packet = stream.split_to(fixed_header.frame_length());
    let packet_type = fixed_header.packet_type()?;

    if fixed_header.remaining_len == 0 {
        // no payload packets
        return match packet_type {
            PacketType::PingReq => Ok(Packet::PingReq),
            PacketType::PingResp => Ok(Packet::PingResp),
            PacketType::Disconnect => Ok(Packet::Disconnect),
            _ => Err(Error::PayloadRequired),
        };
    }

    let packet = packet.freeze();
    let packet = match packet_type {
        PacketType::Connect => return Err(Error::UnexpectedConnect),
        PacketType::ConnAck => Packet::ConnAck(ConnAck::read(fixed_header, packet)?),
        PacketType::Publish => Packet::Publish(Publish::read(fixed_header, packet)?),
        PacketType::PubAck => Packet::PubAck(PubAck::read(fixed_header, packet)?),
        PacketType::PubRec => Packet::PubRec(PubRec::read(fixed_header, packet)?),
        PacketType::PubRel => Packet::PubRel(PubRel::read(fixed_header, packet)?),
        PacketType::PubComp => Packet::PubComp(PubComp::read(fixed_header, packet)?),
        PacketType::Subscribe => Packet::Subscribe(Subscribe::read(fixed_header, packet)?),
        PacketType::SubAck => Packet::SubAck(SubAck::read(fixed_header, packet)?),
        PacketType::Unsubscribe => Packet::Unsubscribe(Unsubscribe::read(fixed_header, packet)?),
        PacketType::UnsubAck => Packet::UnsubAck(UnsubAck::read(fixed_header, packet)?),
        PacketType::PingReq => Packet::PingReq,
        PacketType::PingResp => Packet::PingResp,
        PacketType::Disconnect => Packet::Disconnect,
    };

    Ok(packet)
}

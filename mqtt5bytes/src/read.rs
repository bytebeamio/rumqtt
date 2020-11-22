use crate::*;
use bytes::BytesMut;
use core::slice::Iter;

/// Reads a stream of bytes and extracts MQTT packets
pub fn mqtt_read(stream: &mut BytesMut, max_packet_size: usize) -> Result<Packet, Error> {
    let fixed_header = check(stream, max_packet_size)?;
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
        PacketType::Connect => Packet::Connect(Connect::assemble(fixed_header, packet)?),
        PacketType::ConnAck => Packet::ConnAck(ConnAck::assemble(fixed_header, packet)?),
        PacketType::Publish => Packet::Publish(Publish::assemble(fixed_header, packet)?),
        PacketType::PubAck => Packet::PubAck(PubAck::assemble(fixed_header, packet)?),
        PacketType::PubRec => Packet::PubRec(PubRec::assemble(fixed_header, packet)?),
        PacketType::PubRel => Packet::PubRel(PubRel::assemble(fixed_header, packet)?),
        PacketType::PubComp => Packet::PubComp(PubComp::assemble(fixed_header, packet)?),
        PacketType::Subscribe => Packet::Subscribe(Subscribe::assemble(fixed_header, packet)?),
        PacketType::SubAck => Packet::SubAck(SubAck::assemble(fixed_header, packet)?),
        PacketType::Unsubscribe => {
            Packet::Unsubscribe(Unsubscribe::assemble(fixed_header, packet)?)
        }
        PacketType::UnsubAck => Packet::UnsubAck(UnsubAck::assemble(fixed_header, packet)?),
        PacketType::PingReq => Packet::PingReq,
        PacketType::PingResp => Packet::PingResp,
        PacketType::Disconnect => Packet::Disconnect,
    };

    Ok(packet)
}

/// Checks if the stream has enough bytes to frame a packet and returns fixed header
pub fn check(stream: &mut BytesMut, max_packet_size: usize) -> Result<FixedHeader, Error> {
    let stream_len = stream.len();
    if stream_len < 2 {
        return Err(Error::InsufficientBytes(2));
    }

    // Read the initial bytes necessary from the stream with out mutating the stream cursor
    let (byte1, remaining_len_len, remaining_len) = parse_fixed_header(stream.iter())?;
    let fixed_header = FixedHeader::new(byte1, remaining_len_len, remaining_len);

    // Don't let rogue connections attack with huge payloads. Disconnect them before reading all
    // that data
    if fixed_header.remaining_len > max_packet_size {
        return Err(Error::PayloadSizeLimitExceeded);
    }

    // If the current call fails due to insufficient bytes in the stream, after calculating
    // remaining length, we extend the stream
    let frame_length = fixed_header.frame_length();
    if stream_len < frame_length {
        return Err(Error::InsufficientBytes(frame_length - stream_len));
    }

    Ok(fixed_header)
}

/// Parses fixed header. Doesn't modify the source
fn parse_fixed_header(mut stream: Iter<u8>) -> Result<(u8, usize, usize), Error> {
    let byte1 = stream.next().unwrap();
    let (remaining_len_len, remaining_len) = length(stream)?;
    Ok((*byte1, remaining_len_len, remaining_len))
}

/// Parses variable byte integer in the stream to calculate length
/// of the trailing stream. Used for remaining length calculation
/// as well as for calculating property lengths
pub(crate) fn length(stream: Iter<u8>) -> Result<(usize, usize), Error> {
    let mut remaining_len: usize = 0;
    let mut remaining_len_len = 0;
    let mut done = false;
    let mut shift = 0;
    for byte in stream {
        remaining_len_len += 1;
        let byte = *byte as usize;
        remaining_len += (byte & 0x7F) << shift;

        // stop when continue bit is 0
        done = (byte & 0x80) == 0;
        if done {
            break;
        }

        shift += 7;
        if shift > 21 {
            return Err(Error::MalformedRemainingLength);
        }
    }

    // TODO: Fix stream length + 1 in mqtt4bytes and write a test
    if !done {
        return Err(Error::InsufficientBytes(1));
    }

    Ok((remaining_len_len, remaining_len))
}

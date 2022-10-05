use super::*;
use bytes::{BufMut, BytesMut};

// In v4 there are no Reason Code and properties
pub fn write(_disconnect: Disconnect, payload: &mut BytesMut) -> Result<usize, Error> {
    payload.put_slice(&[0xE0, 0x00]);
    Ok(2)
}

pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Disconnect, Error> {
    let packet_type = fixed_header.byte1 >> 4;
    let flags = fixed_header.byte1 & 0b0000_1111;

    bytes.advance(fixed_header.fixed_header_len);

    if packet_type != PacketType::Disconnect as u8 {
        return Err(Error::InvalidPacketType(packet_type));
    };

    if flags != 0x00 {
        return Err(Error::MalformedPacket);
    };

    if fixed_header.remaining_len == 0 {
        return Ok(Disconnect {
            reason_code: DisconnectReasonCode::NormalDisconnection,
        });
    }

    let reason_code = read_u8(&mut bytes)?;

    let disconnect = Disconnect {
        reason_code: reason_code.try_into()?,
    };

    Ok(disconnect)
}

use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub fn len(unsuback: &UnsubAck, properties: &Option<UnsubAckProperties>) -> usize {
    let mut len = 2 + unsuback.reasons.len();

    if let Some(p) = properties {
        let properties_len = properties::len(p);
        let properties_len_len = len_len(properties_len);
        len += properties_len_len + properties_len;
    } else {
        // just 1 byte representing 0 len
        len += 1;
    }

    len
}

pub fn read(
    fixed_header: FixedHeader,
    mut bytes: Bytes,
) -> Result<(UnsubAck, Option<UnsubAckProperties>), Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);

    let pkid = read_u16(&mut bytes)?;
    let properties = properties::read(&mut bytes)?;

    if !bytes.has_remaining() {
        return Err(Error::MalformedPacket);
    }

    let mut reasons = Vec::new();
    while bytes.has_remaining() {
        let r = read_u8(&mut bytes)?;
        reasons.push(reason(r)?);
    }

    let unsuback = UnsubAck { pkid, reasons };

    Ok((unsuback, properties))
}

pub fn write(
    unsuback: &UnsubAck,
    properties: &Option<UnsubAckProperties>,
    buffer: &mut BytesMut,
) -> Result<usize, Error> {
    buffer.put_u8(0xB0);
    let remaining_len = len(unsuback, properties);
    let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

    buffer.put_u16(unsuback.pkid);

    if let Some(p) = &properties {
        properties::write(p, buffer)?;
    } else {
        write_remaining_length(buffer, 0)?;
    }

    let p: Vec<u8> = unsuback.reasons.iter().map(|&c| code(c)).collect();
    buffer.extend_from_slice(&p);
    Ok(1 + remaining_len_bytes + remaining_len)
}

mod properties {
    use super::*;
    pub fn len(properties: &UnsubAckProperties) -> usize {
        let mut len = 0;

        if let Some(reason) = &properties.reason_string {
            len += 1 + 2 + reason.len();
        }

        for (key, value) in properties.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        len
    }

    pub fn read(mut bytes: &mut Bytes) -> Result<Option<UnsubAckProperties>, Error> {
        let mut reason_string = None;
        let mut user_properties = Vec::new();

        let (properties_len_len, properties_len) = length(bytes.iter())?;
        bytes.advance(properties_len_len);
        if properties_len == 0 {
            return Ok(None);
        }

        let mut cursor = 0;
        // read until cursor reaches property length. properties_len = 0 will skip this loop
        while cursor < properties_len {
            let prop = read_u8(bytes)?;
            cursor += 1;

            match property(prop)? {
                PropertyType::ReasonString => {
                    let reason = read_mqtt_string(bytes)?;
                    cursor += 2 + reason.len();
                    reason_string = Some(reason);
                }
                PropertyType::UserProperty => {
                    let key = read_mqtt_string(bytes)?;
                    let value = read_mqtt_string(bytes)?;
                    cursor += 2 + key.len() + 2 + value.len();
                    user_properties.push((key, value));
                }
                _ => return Err(Error::InvalidPropertyType(prop)),
            }
        }

        Ok(Some(UnsubAckProperties {
            reason_string,
            user_properties,
        }))
    }

    pub fn write(properties: &UnsubAckProperties, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = len(properties);
        write_remaining_length(buffer, len)?;

        if let Some(reason) = &properties.reason_string {
            buffer.put_u8(PropertyType::ReasonString as u8);
            write_mqtt_string(buffer, reason);
        }

        for (key, value) in properties.user_properties.iter() {
            buffer.put_u8(PropertyType::UserProperty as u8);
            write_mqtt_string(buffer, key);
            write_mqtt_string(buffer, value);
        }

        Ok(())
    }
}

/// Connection return code type
fn reason(num: u8) -> Result<UnsubAckReason, Error> {
    let code = match num {
        0x00 => UnsubAckReason::Success,
        0x11 => UnsubAckReason::NoSubscriptionExisted,
        0x80 => UnsubAckReason::UnspecifiedError,
        0x83 => UnsubAckReason::ImplementationSpecificError,
        0x87 => UnsubAckReason::NotAuthorized,
        0x8F => UnsubAckReason::TopicFilterInvalid,
        0x91 => UnsubAckReason::PacketIdentifierInUse,
        num => return Err(Error::InvalidSubscribeReasonCode(num)),
    };

    Ok(code)
}

fn code(reason: UnsubAckReason) -> u8 {
    match reason {
        UnsubAckReason::Success => 0x00,
        UnsubAckReason::NoSubscriptionExisted => 0x11,
        UnsubAckReason::UnspecifiedError => 0x80,
        UnsubAckReason::ImplementationSpecificError => 0x83,
        UnsubAckReason::NotAuthorized => 0x87,
        UnsubAckReason::TopicFilterInvalid => 0x8F,
        UnsubAckReason::PacketIdentifierInUse => 0x91,
    }
}

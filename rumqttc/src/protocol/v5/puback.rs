use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

fn len(puback: &PubAck, properties: &Option<PubAckProperties>) -> usize {
    let mut len = 2 + 1; // pkid + reason

    // If there are no properties, sending reason code is optional
    if puback.reason == PubAckReason::Success && properties.is_none() {
        return 2;
    }

    if let Some(p) = properties {
        let properties_len = properties::len(p);
        let properties_len_len = len_len(properties_len);
        len += properties_len_len + properties_len;
    } else {
        // just 1 byte representing 0 len properties
        len += 1;
    }

    len
}

pub fn read(
    fixed_header: FixedHeader,
    mut bytes: Bytes,
) -> Result<(PubAck, Option<PubAckProperties>), Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);
    let pkid = read_u16(&mut bytes)?;

    // No reason code or properties if remaining length == 2
    if fixed_header.remaining_len == 2 {
        return Ok((
            PubAck {
                pkid,
                reason: PubAckReason::Success,
            },
            None,
        ));
    }

    // No properties len or properties if remaining len > 2 but < 4
    let ack_reason = read_u8(&mut bytes)?;
    if fixed_header.remaining_len < 4 {
        return Ok((
            PubAck {
                pkid,
                reason: reason(ack_reason)?,
            },
            None,
        ));
    }

    let puback = PubAck {
        pkid,
        reason: reason(ack_reason)?,
    };

    let properties = properties::read(&mut bytes)?;
    Ok((puback, properties))
}

pub fn write(
    puback: &PubAck,
    properties: &Option<PubAckProperties>,
    buffer: &mut BytesMut,
) -> Result<usize, Error> {
    let len = len(puback, properties);
    buffer.put_u8(0x40);

    let count = write_remaining_length(buffer, len)?;
    buffer.put_u16(puback.pkid);

    // Reason code is optional with success if there are no properties
    if puback.reason == PubAckReason::Success && properties.is_none() {
        return Ok(4);
    }

    buffer.put_u8(code(puback.reason));
    if let Some(p) = properties {
        properties::write(p, buffer)?;
    } else {
        write_remaining_length(buffer, 0)?;
    }

    Ok(1 + count + len)
}

mod properties {
    use super::*;

    pub fn len(properties: &PubAckProperties) -> usize {
        let mut len = 0;

        if let Some(reason) = &properties.reason_string {
            len += 1 + 2 + reason.len();
        }

        for (key, value) in properties.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        len
    }

    pub fn read(mut bytes: &mut Bytes) -> Result<Option<PubAckProperties>, Error> {
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

        Ok(Some(PubAckProperties {
            reason_string,
            user_properties,
        }))
    }

    pub fn write(properties: &PubAckProperties, buffer: &mut BytesMut) -> Result<(), Error> {
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
fn reason(num: u8) -> Result<PubAckReason, Error> {
    let code = match num {
        0 => PubAckReason::Success,
        16 => PubAckReason::NoMatchingSubscribers,
        128 => PubAckReason::UnspecifiedError,
        131 => PubAckReason::ImplementationSpecificError,
        135 => PubAckReason::NotAuthorized,
        144 => PubAckReason::TopicNameInvalid,
        145 => PubAckReason::PacketIdentifierInUse,
        151 => PubAckReason::QuotaExceeded,
        153 => PubAckReason::PayloadFormatInvalid,
        num => return Err(Error::InvalidConnectReturnCode(num)),
    };

    Ok(code)
}

// TODO: Is typecasting significantly faster than functions?
fn code(reason: PubAckReason) -> u8 {
    match reason {
        PubAckReason::Success => 0,
        PubAckReason::NoMatchingSubscribers => 16,
        PubAckReason::UnspecifiedError => 128,
        PubAckReason::ImplementationSpecificError => 131,
        PubAckReason::NotAuthorized => 135,
        PubAckReason::TopicNameInvalid => 144,
        PubAckReason::PacketIdentifierInUse => 145,
        PubAckReason::QuotaExceeded => 151,
        PubAckReason::PayloadFormatInvalid => 153,
    }
}

use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::convert::{TryFrom, TryInto};

pub fn len(suback: &SubAck, properties: &Option<SubAckProperties>) -> usize {
    let mut len = 2 + suback.return_codes.len();

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
) -> Result<(SubAck, Option<SubAckProperties>), Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);

    let pkid = read_u16(&mut bytes)?;
    let properties = properties::read(&mut bytes)?;

    if !bytes.has_remaining() {
        return Err(Error::MalformedPacket);
    }

    let mut return_codes = Vec::new();
    while bytes.has_remaining() {
        let return_code = read_u8(&mut bytes)?;
        return_codes.push(reason(return_code)?);
    }

    let suback = SubAck { pkid, return_codes };

    Ok((suback, properties))
}

pub fn write(
    suback: &SubAck,
    properties: &Option<SubAckProperties>,
    buffer: &mut BytesMut,
) -> Result<usize, Error> {
    buffer.put_u8(0x90);
    let remaining_len = len(suback, properties);
    let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

    buffer.put_u16(suback.pkid);

    if let Some(p) = properties {
        properties::write(p, buffer)?;
    } else {
        write_remaining_length(buffer, 0)?;
    }

    let p: Vec<u8> = suback.return_codes.iter().map(|&c| code(c)).collect();

    buffer.extend_from_slice(&p);
    Ok(1 + remaining_len_bytes + remaining_len)
}

mod properties {
    use super::*;

    pub fn len(properties: &SubAckProperties) -> usize {
        let mut len = 0;

        if let Some(reason) = &properties.reason_string {
            len += 1 + 2 + reason.len();
        }

        for (key, value) in properties.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        len
    }

    pub fn read(mut bytes: &mut Bytes) -> Result<Option<SubAckProperties>, Error> {
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

        Ok(Some(SubAckProperties {
            reason_string,
            user_properties,
        }))
    }

    pub fn write(properties: &SubAckProperties, buffer: &mut BytesMut) -> Result<(), Error> {
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

fn reason(code: u8) -> Result<SubscribeReasonCode, Error> {
    let v = match code {
        0 => SubscribeReasonCode::QoS0,
        1 => SubscribeReasonCode::QoS1,
        2 => SubscribeReasonCode::QoS2,
        128 => SubscribeReasonCode::Unspecified,
        131 => SubscribeReasonCode::ImplementationSpecific,
        135 => SubscribeReasonCode::NotAuthorized,
        143 => SubscribeReasonCode::TopicFilterInvalid,
        145 => SubscribeReasonCode::PkidInUse,
        151 => SubscribeReasonCode::QuotaExceeded,
        158 => SubscribeReasonCode::SharedSubscriptionsNotSupported,
        161 => SubscribeReasonCode::SubscriptionIdNotSupported,
        162 => SubscribeReasonCode::WildcardSubscriptionsNotSupported,
        v => return Err(Error::InvalidSubscribeReasonCode(v)),
    };

    Ok(v)
}

fn code(value: SubscribeReasonCode) -> u8 {
    match value {
        SubscribeReasonCode::Success(qos) => qos as u8,
        SubscribeReasonCode::Failure => 0x80,
        SubscribeReasonCode::QoS0 => 0,
        SubscribeReasonCode::QoS1 => 1,
        SubscribeReasonCode::QoS2 => 2,
        SubscribeReasonCode::Unspecified => 128,
        SubscribeReasonCode::ImplementationSpecific => 131,
        SubscribeReasonCode::NotAuthorized => 135,
        SubscribeReasonCode::TopicFilterInvalid => 143,
        SubscribeReasonCode::PkidInUse => 145,
        SubscribeReasonCode::QuotaExceeded => 151,
        SubscribeReasonCode::SharedSubscriptionsNotSupported => 158,
        SubscribeReasonCode::SubscriptionIdNotSupported => 161,
        SubscribeReasonCode::WildcardSubscriptionsNotSupported => 162,
    }
}

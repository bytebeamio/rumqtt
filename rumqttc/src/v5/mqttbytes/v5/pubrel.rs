use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

fn len(pubrel: &PubRel, properties: &Option<PubRelProperties>) -> usize {
    let mut len = 2 + 1; // pkid + reason

    // The Reason Code and Property Length can be omitted if the Reason Code is 0x00 (Success)
    // and there are no Properties. In this case the PUBREL has a Remaining Length of 2.
    // <https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901144>
    if pubrel.reason == PubRelReason::Success && properties.is_none() {
        return 2;
    }

    if let Some(p) = properties {
        let properties_len = properties::len(p);
        let properties_len_len = len_len(properties_len);
        len += properties_len_len + properties_len;
    } else {
        len += 1;
    }

    len
}

pub fn read(
    fixed_header: FixedHeader,
    mut bytes: Bytes,
) -> Result<(PubRel, Option<PubRelProperties>), Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);
    let pkid = read_u16(&mut bytes)?;
    if fixed_header.remaining_len == 2 {
        return Ok((
            PubRel {
                pkid,
                reason: PubRelReason::Success,
            },
            None,
        ));
    }

    let ack_reason = read_u8(&mut bytes)?;
    if fixed_header.remaining_len < 4 {
        return Ok((
            PubRel {
                pkid,
                reason: reason(ack_reason)?,
            },
            None,
        ));
    }

    let puback = PubRel {
        pkid,
        reason: reason(ack_reason)?,
    };

    let properties = properties::read(&mut bytes)?;
    Ok((puback, properties))
}

pub fn write(
    pubrel: &PubRel,
    properties: &Option<PubRelProperties>,
    buffer: &mut BytesMut,
) -> Result<usize, Error> {
    let len = len(pubrel, properties);
    buffer.put_u8(0x62);
    let count = write_remaining_length(buffer, len)?;
    buffer.put_u16(pubrel.pkid);

    // If there are no properties during success, sending reason code is optional
    if pubrel.reason == PubRelReason::Success && properties.is_none() {
        return Ok(4);
    }

    buffer.put_u8(code(pubrel.reason));

    if let Some(p) = properties {
        properties::write(p, buffer)?;
    } else {
        write_remaining_length(buffer, 0)?;
    }

    Ok(1 + count + len)
}

mod properties {
    use super::*;

    pub fn len(properties: &PubRelProperties) -> usize {
        let mut len = 0;

        if let Some(reason) = &properties.reason_string {
            len += 1 + 2 + reason.len();
        }

        for (key, value) in properties.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        len
    }

    pub fn read(bytes: &mut Bytes) -> Result<Option<PubRelProperties>, Error> {
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

        Ok(Some(PubRelProperties {
            reason_string,
            user_properties,
        }))
    }

    pub fn write(properties: &PubRelProperties, buffer: &mut BytesMut) -> Result<(), Error> {
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
fn reason(num: u8) -> Result<PubRelReason, Error> {
    let code = match num {
        0 => PubRelReason::Success,
        146 => PubRelReason::PacketIdentifierNotFound,
        num => return Err(Error::InvalidConnectReturnCode(num)),
    };

    Ok(code)
}

fn code(reason: PubRelReason) -> u8 {
    match reason {
        PubRelReason::Success => 0,
        PubRelReason::PacketIdentifierNotFound => 146,
    }
}

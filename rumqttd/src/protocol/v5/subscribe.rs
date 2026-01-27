use super::*;
use bytes::{Buf, Bytes};
use core::fmt;

pub fn len(subscribe: &Subscribe, properties: &Option<SubscribeProperties>) -> usize {
    let mut len = 2 + subscribe.filters.iter().fold(0, |s, t| s + filter::len(t));

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
) -> Result<(Subscribe, Option<SubscribeProperties>), Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);

    let pkid = read_u16(&mut bytes)?;
    let properties = properties::read(&mut bytes)?;

    // variable header size = 2 (packet identifier)
    let mut filters = Vec::new();

    while bytes.has_remaining() {
        let path = read_mqtt_string(&mut bytes)?;
        let options = read_u8(&mut bytes)?;
        let requested_qos = options & 0b0000_0011;

        let nolocal = ((options >> 2) & 0b0000_0001);
        let nolocal = nolocal != 0;

        let preserve_retain = ((options >> 3) & 0b0000_0001);
        let preserve_retain = preserve_retain != 0;

        let retain_forward_rule = (options >> 4) & 0b0000_0011;
        let retain_forward_rule = match retain_forward_rule {
            0 => RetainForwardRule::OnEverySubscribe,
            1 => RetainForwardRule::OnNewSubscribe,
            2 => RetainForwardRule::Never,
            r => return Err(Error::InvalidRetainForwardRule(r)),
        };

        filters.push(Filter {
            path,
            qos: qos(requested_qos).ok_or(Error::InvalidQoS(requested_qos))?,
            nolocal,
            preserve_retain,
            retain_forward_rule,
        });
    }

    match filters.len() {
        0 => Err(Error::EmptySubscription),
        _ => Ok((Subscribe { pkid, filters }, properties)),
    }
}

pub fn write(
    subscribe: &Subscribe,
    properties: &Option<SubscribeProperties>,
    buffer: &mut BytesMut,
) -> Result<usize, Error> {
    // write packet type
    buffer.put_u8(0x82);

    // write remaining length
    let remaining_len = len(subscribe, properties);
    let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

    // write packet id
    buffer.put_u16(subscribe.pkid);

    if let Some(p) = properties {
        properties::write(p, buffer)?;
    } else {
        write_remaining_length(buffer, 0)?;
    }

    // write filters
    for f in subscribe.filters.iter() {
        filter::write(f, buffer);
    }

    Ok(1 + remaining_len_bytes + remaining_len)
}

mod filter {
    use super::*;

    pub fn len(filter: &Filter) -> usize {
        // filter len + filter + options
        2 + filter.path.len() + 1
    }

    pub fn read(bytes: &mut Bytes) -> Result<Vec<Filter>, Error> {
        // variable header size = 2 (packet identifier)
        let mut filters = Vec::new();

        while bytes.has_remaining() {
            let path = read_mqtt_string(bytes)?;
            let options = read_u8(bytes)?;
            let requested_qos = options & 0b0000_0011;

            let nolocal = (options >> 2) & 0b0000_0001;
            let nolocal = nolocal != 0;

            let preserve_retain = (options >> 3) & 0b0000_0001;
            let preserve_retain = preserve_retain != 0;

            let retain_forward_rule = (options >> 4) & 0b0000_0011;
            let retain_forward_rule = match retain_forward_rule {
                0 => RetainForwardRule::OnEverySubscribe,
                1 => RetainForwardRule::OnNewSubscribe,
                2 => RetainForwardRule::Never,
                r => return Err(Error::InvalidRetainForwardRule(r)),
            };

            filters.push(Filter {
                path,
                qos: qos(requested_qos).ok_or(Error::InvalidQoS(requested_qos))?,
                nolocal,
                preserve_retain,
                retain_forward_rule,
            });
        }

        Ok(filters)
    }

    pub fn write(filter: &Filter, buffer: &mut BytesMut) {
        let mut options = 0;
        options |= filter.qos as u8;

        if filter.nolocal {
            options |= 0b0000_0100;
        }

        if filter.preserve_retain {
            options |= 0b0000_1000;
        }

        options |= match filter.retain_forward_rule {
            RetainForwardRule::OnEverySubscribe => 0b0000_0000,
            RetainForwardRule::OnNewSubscribe => 0b0001_0000,
            RetainForwardRule::Never => 0b0010_0000,
        };

        write_mqtt_string(buffer, filter.path.as_str());
        buffer.put_u8(options);
    }
}

mod properties {
    use super::*;

    pub fn len(properties: &SubscribeProperties) -> usize {
        let mut len = 0;

        if let Some(id) = &properties.id {
            len += 1 + len_len(*id);
        }

        for (key, value) in properties.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        len
    }

    pub fn read(mut bytes: &mut Bytes) -> Result<Option<SubscribeProperties>, Error> {
        let mut id = None;
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
                PropertyType::SubscriptionIdentifier => {
                    let (id_len, sub_id) = length(bytes.iter())?;
                    // TODO: Validate 1 +. Tests are working either way
                    cursor += 1 + id_len;
                    bytes.advance(id_len);
                    id = Some(sub_id)
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

        Ok(Some(SubscribeProperties {
            id,
            user_properties,
        }))
    }

    pub fn write(properties: &SubscribeProperties, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = len(properties);
        write_remaining_length(buffer, len)?;

        if let Some(id) = &properties.id {
            buffer.put_u8(PropertyType::SubscriptionIdentifier as u8);
            write_remaining_length(buffer, *id)?;
        }

        for (key, value) in properties.user_properties.iter() {
            buffer.put_u8(PropertyType::UserProperty as u8);
            write_mqtt_string(buffer, key);
            write_mqtt_string(buffer, value);
        }

        Ok(())
    }
}

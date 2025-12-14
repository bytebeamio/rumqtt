use super::*;
use bytes::{Buf, Bytes};
use core::fmt;

pub fn len(publish: &Publish, properties: &Option<PublishProperties>) -> usize {
    let mut len = 2 + publish.topic.len();
    if publish.qos != QoS::AtMostOnce && publish.pkid != 0 {
        len += 2;
    }

    if let Some(p) = properties {
        let properties_len = properties::len(p);
        let properties_len_len = len_len(properties_len);
        len += properties_len_len + properties_len;
    } else {
        // just 1 byte representing 0 len
        len += 1;
    }

    len += publish.payload.len();
    len
}

pub fn read(
    fixed_header: FixedHeader,
    mut bytes: Bytes,
) -> Result<(Publish, Option<PublishProperties>), Error> {
    let qos_num = (fixed_header.byte1 & 0b0110) >> 1;
    let qos = qos(qos_num).ok_or(Error::InvalidQoS(qos_num))?;
    let dup = (fixed_header.byte1 & 0b1000) != 0;
    let retain = (fixed_header.byte1 & 0b0001) != 0;

    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);
    let topic = read_mqtt_bytes(&mut bytes)?;

    // Packet identifier exists where QoS > 0
    let pkid = match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce | QoS::ExactlyOnce => read_u16(&mut bytes)?,
    };

    if qos != QoS::AtMostOnce && pkid == 0 {
        return Err(Error::PacketIdZero);
    }

    let properties = properties::read(&mut bytes)?;
    let publish = Publish {
        dup,
        retain,
        qos,
        pkid,
        topic,
        payload: bytes,
    };

    Ok((publish, properties))
}

pub fn write(
    publish: &Publish,
    properties: &Option<PublishProperties>,
    buffer: &mut BytesMut,
) -> Result<usize, Error> {
    let len = len(publish, properties);

    let dup = publish.dup as u8;
    let qos = publish.qos as u8;
    let retain = publish.retain as u8;
    buffer.put_u8(0b0011_0000 | retain | (qos << 1) | (dup << 3));

    let count = write_remaining_length(buffer, len)?;
    write_mqtt_bytes(buffer, &publish.topic);

    if publish.qos != QoS::AtMostOnce {
        let pkid = publish.pkid;
        if pkid == 0 {
            return Err(Error::PacketIdZero);
        }

        buffer.put_u16(pkid);
    }

    if let Some(p) = properties {
        properties::write(p, buffer)?;
    } else {
        write_remaining_length(buffer, 0)?;
    }

    buffer.extend_from_slice(&publish.payload);

    Ok(1 + count + len)
}

mod properties {
    use super::*;

    pub fn len(properties: &PublishProperties) -> usize {
        let mut len = 0;

        if properties.payload_format_indicator.is_some() {
            len += 1 + 1;
        }

        if properties.message_expiry_interval.is_some() {
            len += 1 + 4;
        }

        if properties.topic_alias.is_some() {
            len += 1 + 2;
        }

        if let Some(topic) = &properties.response_topic {
            len += 1 + 2 + topic.len()
        }

        if let Some(data) = &properties.correlation_data {
            len += 1 + 2 + data.len()
        }

        for (key, value) in properties.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        for id in properties.subscription_identifiers.iter() {
            len += 1 + len_len(*id);
        }

        if let Some(typ) = &properties.content_type {
            len += 1 + 2 + typ.len()
        }

        len
    }

    pub fn read(mut bytes: &mut Bytes) -> Result<Option<PublishProperties>, Error> {
        let mut payload_format_indicator = None;
        let mut message_expiry_interval = None;
        let mut topic_alias = None;
        let mut response_topic = None;
        let mut correlation_data = None;
        let mut user_properties = Vec::new();
        let mut subscription_identifiers = Vec::new();
        let mut content_type = None;

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
                PropertyType::PayloadFormatIndicator => {
                    payload_format_indicator = Some(read_u8(bytes)?);
                    cursor += 1;
                }
                PropertyType::MessageExpiryInterval => {
                    message_expiry_interval = Some(read_u32(bytes)?);
                    cursor += 4;
                }
                PropertyType::TopicAlias => {
                    topic_alias = Some(read_u16(bytes)?);
                    cursor += 2;
                }
                PropertyType::ResponseTopic => {
                    let topic = read_mqtt_string(bytes)?;
                    cursor += 2 + topic.len();
                    response_topic = Some(topic);
                }
                PropertyType::CorrelationData => {
                    let data = read_mqtt_bytes(bytes)?;
                    cursor += 2 + data.len();
                    correlation_data = Some(data);
                }
                PropertyType::UserProperty => {
                    let key = read_mqtt_string(bytes)?;
                    let value = read_mqtt_string(bytes)?;
                    cursor += 2 + key.len() + 2 + value.len();
                    user_properties.push((key, value));
                }
                PropertyType::SubscriptionIdentifier => {
                    let (id_len, id) = length(bytes.iter())?;
                    cursor += 1 + id_len;
                    bytes.advance(id_len);
                    subscription_identifiers.push(id);
                }
                PropertyType::ContentType => {
                    let typ = read_mqtt_string(bytes)?;
                    cursor += 2 + typ.len();
                    content_type = Some(typ);
                }
                _ => return Err(Error::InvalidPropertyType(prop)),
            }
        }

        Ok(Some(PublishProperties {
            payload_format_indicator,
            message_expiry_interval,
            topic_alias,
            response_topic,
            correlation_data,
            user_properties,
            subscription_identifiers,
            content_type,
        }))
    }

    pub fn write(properties: &PublishProperties, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = len(properties);
        write_remaining_length(buffer, len)?;

        if let Some(payload_format_indicator) = properties.payload_format_indicator {
            buffer.put_u8(PropertyType::PayloadFormatIndicator as u8);
            buffer.put_u8(payload_format_indicator);
        }

        if let Some(message_expiry_interval) = properties.message_expiry_interval {
            buffer.put_u8(PropertyType::MessageExpiryInterval as u8);
            buffer.put_u32(message_expiry_interval);
        }

        if let Some(topic_alias) = properties.topic_alias {
            buffer.put_u8(PropertyType::TopicAlias as u8);
            buffer.put_u16(topic_alias);
        }

        if let Some(topic) = &properties.response_topic {
            buffer.put_u8(PropertyType::ResponseTopic as u8);
            write_mqtt_string(buffer, topic);
        }

        if let Some(data) = &properties.correlation_data {
            buffer.put_u8(PropertyType::CorrelationData as u8);
            write_mqtt_bytes(buffer, data);
        }

        for (key, value) in properties.user_properties.iter() {
            buffer.put_u8(PropertyType::UserProperty as u8);
            write_mqtt_string(buffer, key);
            write_mqtt_string(buffer, value);
        }

        for id in properties.subscription_identifiers.iter() {
            buffer.put_u8(PropertyType::SubscriptionIdentifier as u8);
            write_remaining_length(buffer, *id)?;
        }

        if let Some(typ) = &properties.content_type {
            buffer.put_u8(PropertyType::ContentType as u8);
            write_mqtt_string(buffer, typ);
        }

        Ok(())
    }
}

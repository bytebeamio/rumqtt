use super::*;
use alloc::string::String;
use alloc::vec::Vec;
use bytes::{Buf, Bytes};
use core::fmt;

/// Publish packet
#[derive(Clone, PartialEq)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: String,
    pub pkid: u16,
    pub properties: Option<PublishProperties>,
    pub payload: Bytes,
}

impl Publish {
    pub fn new<S: Into<String>, P: Into<Vec<u8>>>(topic: S, qos: QoS, payload: P) -> Publish {
        Publish {
            dup: false,
            qos,
            retain: false,
            pkid: 0,
            topic: topic.into(),
            properties: None,
            payload: Bytes::from(payload.into()),
        }
    }

    pub fn from_bytes<S: Into<String>>(topic: S, qos: QoS, payload: Bytes) -> Publish {
        Publish {
            dup: false,
            qos,
            retain: false,
            pkid: 0,
            topic: topic.into(),
            properties: None,
            payload,
        }
    }

    pub fn len(&self) -> usize {
        let mut len = 2 + self.topic.len();
        if self.qos != QoS::AtMostOnce && self.pkid != 0 {
            len += 2;
        }

        match &self.properties {
            Some(properties) => {
                let properties_len = properties.len();
                let properties_len_len = len_len(properties_len);
                len += properties_len_len + properties_len;
            }
            None => {
                // just 1 byte representing 0 len
                len += 1;
            }
        }

        len += self.payload.len();
        len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let qos = qos((fixed_header.byte1 & 0b0110) >> 1)?;
        let dup = (fixed_header.byte1 & 0b1000) != 0;
        let retain = (fixed_header.byte1 & 0b0001) != 0;

        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);
        let topic = read_mqtt_string(&mut bytes)?;

        // Packet identifier exists where QoS > 0
        let pkid = match qos {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce | QoS::ExactlyOnce => read_u16(&mut bytes)?,
        };

        if qos != QoS::AtMostOnce && pkid == 0 {
            return Err(Error::PacketIdZero);
        }

        let publish = Publish {
            dup,
            retain,
            qos,
            pkid,
            topic,
            properties: PublishProperties::extract(&mut bytes)?,
            payload: bytes,
        };

        Ok(publish)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = self.len();

        let dup = self.dup as u8;
        let qos = self.qos as u8;
        let retain = self.retain as u8;
        buffer.put_u8(0b0011_0000 | retain | qos << 1 | dup << 3);

        let count = write_remaining_length(buffer, len)?;
        write_mqtt_string(buffer, self.topic.as_str());

        if self.qos != QoS::AtMostOnce {
            let pkid = self.pkid;
            if pkid == 0 {
                return Err(Error::PacketIdZero);
            }

            buffer.put_u16(pkid);
        }

        match &self.properties {
            Some(properties) => properties.write(buffer)?,
            None => {
                write_remaining_length(buffer, 0)?;
            }
        };

        buffer.extend_from_slice(&self.payload);

        // TODO: Returned length is wrong in other packets. Fix it
        Ok(1 + count + len)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublishProperties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub topic_alias: Option<u16>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Bytes>,
    pub user_properties: Vec<(String, String)>,
    pub subscription_identifiers: Vec<usize>,
    pub content_type: Option<String>,
}

impl PublishProperties {
    fn len(&self) -> usize {
        let mut len = 0;

        if self.payload_format_indicator.is_some() {
            len += 1 + 1;
        }

        if self.message_expiry_interval.is_some() {
            len += 1 + 4;
        }

        if self.topic_alias.is_some() {
            len += 1 + 2;
        }

        if let Some(topic) = &self.response_topic {
            len += 1 + 2 + topic.len()
        }

        if let Some(data) = &self.correlation_data {
            len += 1 + 2 + data.len()
        }

        for (key, value) in self.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        for id in self.subscription_identifiers.iter() {
            len += 1 + len_len(*id);
        }

        if let Some(typ) = &self.content_type {
            len += 1 + 2 + typ.len()
        }

        len
    }

    fn extract(mut bytes: &mut Bytes) -> Result<Option<PublishProperties>, Error> {
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
            let prop = read_u8(&mut bytes)?;
            cursor += 1;

            match property(prop)? {
                PropertyType::PayloadFormatIndicator => {
                    payload_format_indicator = Some(read_u8(&mut bytes)?);
                    cursor += 1;
                }
                PropertyType::MessageExpiryInterval => {
                    message_expiry_interval = Some(read_u32(&mut bytes)?);
                    cursor += 4;
                }
                PropertyType::TopicAlias => {
                    topic_alias = Some(read_u16(&mut bytes)?);
                    cursor += 2;
                }
                PropertyType::ResponseTopic => {
                    let topic = read_mqtt_string(&mut bytes)?;
                    cursor += 2 + topic.len();
                    response_topic = Some(topic);
                }
                PropertyType::CorrelationData => {
                    let data = read_mqtt_bytes(&mut bytes)?;
                    cursor += 2 + data.len();
                    correlation_data = Some(data);
                }
                PropertyType::UserProperty => {
                    let key = read_mqtt_string(&mut bytes)?;
                    let value = read_mqtt_string(&mut bytes)?;
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
                    let typ = read_mqtt_string(&mut bytes)?;
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

    fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = self.len();
        write_remaining_length(buffer, len)?;

        if let Some(payload_format_indicator) = self.payload_format_indicator {
            buffer.put_u8(PropertyType::PayloadFormatIndicator as u8);
            buffer.put_u8(payload_format_indicator);
        }

        if let Some(message_expiry_interval) = self.message_expiry_interval {
            buffer.put_u8(PropertyType::MessageExpiryInterval as u8);
            buffer.put_u32(message_expiry_interval);
        }

        if let Some(topic_alias) = self.topic_alias {
            buffer.put_u8(PropertyType::TopicAlias as u8);
            buffer.put_u16(topic_alias);
        }

        if let Some(topic) = &self.response_topic {
            buffer.put_u8(PropertyType::ResponseTopic as u8);
            write_mqtt_string(buffer, topic);
        }

        if let Some(data) = &self.correlation_data {
            buffer.put_u8(PropertyType::CorrelationData as u8);
            write_mqtt_bytes(buffer, data);
        }

        for (key, value) in self.user_properties.iter() {
            buffer.put_u8(PropertyType::UserProperty as u8);
            write_mqtt_string(buffer, key);
            write_mqtt_string(buffer, value);
        }

        for id in self.subscription_identifiers.iter() {
            buffer.put_u8(PropertyType::SubscriptionIdentifier as u8);
            write_remaining_length(buffer, *id)?;
        }

        if let Some(typ) = &self.content_type {
            buffer.put_u8(PropertyType::ContentType as u8);
            write_mqtt_string(buffer, typ);
        }

        Ok(())
    }
}

impl fmt::Debug for Publish {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Topic = {}, Qos = {:?}, Retain = {}, Pkid = {:?}, Payload Size = {}",
            self.topic,
            self.qos,
            self.retain,
            self.pkid,
            self.payload.len()
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use alloc::vec;
    use bytes::{Bytes, BytesMut};
    use pretty_assertions::assert_eq;

    fn sample_v5() -> Publish {
        let publish_properties = PublishProperties {
            payload_format_indicator: Some(1),
            message_expiry_interval: Some(4321),
            topic_alias: Some(100),
            response_topic: Some("topic".to_owned()),
            correlation_data: Some(Bytes::from(vec![1, 2, 3, 4])),
            user_properties: vec![("test".to_owned(), "test".to_owned())],
            subscription_identifiers: vec![120, 121],
            content_type: Some("test".to_owned()),
        };

        Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: "test".to_string(),
            pkid: 42,
            properties: Some(publish_properties),
            payload: Bytes::from(vec![b't', b'e', b's', b't']),
        }
    }

    fn sample_bytes() -> Vec<u8> {
        vec![
            0x34, // payload type
            0x3e, // remaining len
            0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // topic name
            0x00, 0x2a, // pkid
            0x31, // properties len
            0x01, 0x01, // payload format indicator
            0x02, 0x00, 0x00, 0x10, 0xe1, // message_expiry_interval
            0x23, 0x00, 0x64, // topic alias
            0x08, 0x00, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, // response topic
            0x09, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04, // correlation_data
            0x26, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x04, 0x74, 0x65, 0x73,
            0x74, // user properties
            0x0b, 0x78, // subscription_identifier
            0x0b, 0x79, // subscription_identifier
            0x03, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // content_type
            0x74, 0x65, 0x73, 0x74, // payload
        ]
    }

    #[test]
    fn publish_parsing_works() {
        let mut stream = bytes::BytesMut::new();
        let packetstream = &sample_bytes();
        stream.extend_from_slice(&packetstream[..]);

        let fixed_header = parse_fixed_header(stream.iter()).unwrap();
        let publish_bytes = stream.split_to(fixed_header.frame_length()).freeze();
        let publish = Publish::read(fixed_header, publish_bytes).unwrap();
        assert_eq!(publish, sample_v5());
    }

    #[test]
    fn publish_encoding_works() {
        let publish = sample_v5();
        let mut buf = BytesMut::new();
        publish.write(&mut buf).unwrap();
        assert_eq!(&buf[..], sample_bytes());
    }

    #[test]
    fn missing_properties_are_encoded() {}
}

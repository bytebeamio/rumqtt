use super::*;
use alloc::vec::Vec;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::convert::{TryFrom, TryInto};

/// Acknowledgement to subscribe
#[derive(Debug, Clone, PartialEq)]
pub struct SubAck {
    pub pkid: u16,
    pub return_codes: Vec<SubscribeReasonCode>,
    pub properties: Option<SubAckProperties>,
}

impl SubAck {
    pub fn new(pkid: u16, return_codes: Vec<SubscribeReasonCode>) -> SubAck {
        SubAck {
            pkid,
            return_codes,
            properties: None,
        }
    }

    pub fn len(&self) -> usize {
        let mut len = 2 + self.return_codes.len();

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

        len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);

        let pkid = read_u16(&mut bytes)?;
        let properties = SubAckProperties::extract(&mut bytes)?;

        if !bytes.has_remaining() {
            return Err(Error::MalformedPacket);
        }

        let mut return_codes = Vec::new();
        while bytes.has_remaining() {
            let return_code = read_u8(&mut bytes)?;
            return_codes.push(return_code.try_into()?);
        }

        let suback = SubAck {
            pkid,
            return_codes,
            properties,
        };

        Ok(suback)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        buffer.put_u8(0x90);
        let remaining_len = self.len();
        let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

        buffer.put_u16(self.pkid);

        match &self.properties {
            Some(properties) => properties.write(buffer)?,
            None => {
                write_remaining_length(buffer, 0)?;
            }
        };

        let p: Vec<u8> = self.return_codes.iter().map(|code| *code as u8).collect();
        buffer.extend_from_slice(&p);
        Ok(1 + remaining_len_bytes + remaining_len)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubAckProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl SubAckProperties {
    pub fn len(&self) -> usize {
        let mut len = 0;

        if let Some(reason) = &self.reason_string {
            len += 1 + 2 + reason.len();
        }

        for (key, value) in self.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        len
    }

    pub fn extract(mut bytes: &mut Bytes) -> Result<Option<SubAckProperties>, Error> {
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
            let prop = read_u8(&mut bytes)?;
            cursor += 1;

            match property(prop)? {
                PropertyType::ReasonString => {
                    let reason = read_mqtt_string(&mut bytes)?;
                    cursor += 2 + reason.len();
                    reason_string = Some(reason);
                }
                PropertyType::UserProperty => {
                    let key = read_mqtt_string(&mut bytes)?;
                    let value = read_mqtt_string(&mut bytes)?;
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

    fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = self.len();
        write_remaining_length(buffer, len)?;

        if let Some(reason) = &self.reason_string {
            buffer.put_u8(PropertyType::ReasonString as u8);
            write_mqtt_string(buffer, reason);
        }

        for (key, value) in self.user_properties.iter() {
            buffer.put_u8(PropertyType::UserProperty as u8);
            write_mqtt_string(buffer, key);
            write_mqtt_string(buffer, value);
        }

        Ok(())
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeReasonCode {
    QoS0 = 0,
    QoS1 = 1,
    QoS2 = 2,
    Unspecified = 128,
    ImplementationSpecific = 131,
    NotAuthorized = 135,
    TopicFilterInvalid = 143,
    PkidInUse = 145,
    QuotaExceeded = 151,
    SharedSubscriptionsNotSupported = 158,
    SubscriptionIdNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
}

impl TryFrom<u8> for SubscribeReasonCode {
    type Error = crate::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let v = match value {
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
            v => return Err(crate::Error::InvalidSubscribeReasonCode(v)),
        };

        Ok(v)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use alloc::vec;
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;

    fn sample() -> SubAck {
        let properties = SubAckProperties {
            reason_string: Some("test".to_owned()),
            user_properties: vec![("test".to_owned(), "test".to_owned())],
        };

        SubAck {
            pkid: 42,
            return_codes: vec![
                SubscribeReasonCode::QoS0,
                SubscribeReasonCode::QoS1,
                SubscribeReasonCode::QoS2,
                SubscribeReasonCode::Unspecified,
            ],
            properties: Some(properties),
        }
    }

    fn sample_bytes() -> Vec<u8> {
        vec![
            0x90, // packet type
            0x1b, // remaining len
            0x00, 0x2a, // pkid
            0x14, // properties len
            0x1f, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x26, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74,
            0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // user properties
            0x00, 0x01, 0x02, 0x80, // return codes
        ]
    }

    #[test]
    fn suback_parsing_works() {
        let mut stream = BytesMut::new();
        let packetstream = &sample_bytes();

        stream.extend_from_slice(&packetstream[..]);
        let fixed_header = parse_fixed_header(stream.iter()).unwrap();
        let suback_bytes = stream.split_to(fixed_header.frame_length()).freeze();
        let suback = SubAck::read(fixed_header, suback_bytes).unwrap();
        assert_eq!(suback, sample());
    }

    #[test]
    fn suback_encoding_works() {
        let publish = sample();
        let mut buf = BytesMut::new();
        publish.write(&mut buf).unwrap();

        // println!("{:X?}", buf);
        // println!("{:#04X?}", &buf[..]);
        assert_eq!(&buf[..], sample_bytes());
    }
}

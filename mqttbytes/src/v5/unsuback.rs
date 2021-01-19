use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

//// Return code in connack
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum UnsubAckReason {
    Success = 0x00,
    NoSubscriptionExisted = 0x11,
    UnspecifiedError = 0x80,
    ImplementationSpecificError = 0x83,
    NotAuthorized = 0x87,
    TopicFilterInvalid = 0x8F,
    PacketIdentifierInUse = 0x91,
}

/// Acknowledgement to unsubscribe
#[derive(Debug, Clone, PartialEq)]
pub struct UnsubAck {
    pub pkid: u16,
    pub reasons: Vec<UnsubAckReason>,
    pub properties: Option<UnsubAckProperties>,
}

impl UnsubAck {
    pub fn new(pkid: u16) -> UnsubAck {
        UnsubAck {
            pkid,
            reasons: Vec::new(),
            properties: None,
        }
    }

    pub fn len(&self) -> usize {
        let mut len = 2 + self.reasons.len();

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
        let properties = UnsubAckProperties::extract(&mut bytes)?;

        if !bytes.has_remaining() {
            return Err(Error::MalformedPacket);
        }

        let mut reasons = Vec::new();
        while bytes.has_remaining() {
            let r = read_u8(&mut bytes)?;
            reasons.push(reason(r)?);
        }

        let unsuback = UnsubAck {
            pkid,
            reasons,
            properties,
        };

        Ok(unsuback)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        buffer.put_u8(0xB0);
        let remaining_len = self.len();
        let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

        buffer.put_u16(self.pkid);

        match &self.properties {
            Some(properties) => properties.write(buffer)?,
            None => {
                write_remaining_length(buffer, 0)?;
            }
        };

        let p: Vec<u8> = self.reasons.iter().map(|code| *code as u8).collect();
        buffer.extend_from_slice(&p);
        Ok(1 + remaining_len_bytes + remaining_len)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnsubAckProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl UnsubAckProperties {
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

    pub fn extract(mut bytes: &mut Bytes) -> Result<Option<UnsubAckProperties>, Error> {
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

        Ok(Some(UnsubAckProperties {
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

#[cfg(test)]
mod test {
    use super::*;
    use alloc::vec;
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;

    fn sample() -> UnsubAck {
        let properties = UnsubAckProperties {
            reason_string: Some("test".to_owned()),
            user_properties: vec![("test".to_owned(), "test".to_owned())],
        };

        UnsubAck {
            pkid: 10,
            reasons: vec![
                UnsubAckReason::NotAuthorized,
                UnsubAckReason::TopicFilterInvalid,
            ],
            properties: Some(properties),
        }
    }

    fn sample_bytes() -> Vec<u8> {
        vec![
            0xb0, // packet type
            0x19, // remaining len
            0x00, 0x0a, // pkid
            0x14, // properties len
            0x1f, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // reason string
            0x26, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x04, 0x74, 0x65, 0x73,
            0x74, // user properties
            0x87, 0x8f, // reasons
        ]
    }

    #[test]
    fn unsuback_parsing_works() {
        let mut stream = BytesMut::new();
        let packetstream = &sample_bytes();

        stream.extend_from_slice(&packetstream[..]);
        let fixed_header = parse_fixed_header(stream.iter()).unwrap();
        let unsuback_bytes = stream.split_to(fixed_header.frame_length()).freeze();
        let unsuback = UnsubAck::read(fixed_header, unsuback_bytes).unwrap();
        assert_eq!(unsuback, sample());
    }

    #[test]
    fn unsuback_encoding_works() {
        let publish = sample();
        let mut buf = BytesMut::new();
        publish.write(&mut buf).unwrap();

        // println!("{:X?}", buf);
        // println!("{:#04X?}", &buf[..]);
        assert_eq!(&buf[..], sample_bytes());
    }
}

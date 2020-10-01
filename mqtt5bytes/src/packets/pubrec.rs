use super::*;
use crate::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Return code in connack
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum PubRecReason {
    Success = 0,
    NoMatchingSubscribers = 16,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
}

/// Acknowledgement to QoS1 publish
#[derive(Debug, Clone, PartialEq)]
pub struct PubRec {
    pub pkid: u16,
    pub reason: PubRecReason,
    pub properties: Option<PubRecProperties>,
}

impl PubRec {
    pub fn new(pkid: u16) -> PubRec {
        PubRec {
            pkid,
            reason: PubRecReason::Success,
            properties: None,
        }
    }

    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        if fixed_header.remaining_len == 2 {
            return Ok(PubRec {
                pkid,
                reason: PubRecReason::Success,
                properties: None,
            });
        }

        let ack_reason = bytes.get_u8();
        if fixed_header.remaining_len < 4 {
            return Ok(PubRec {
                pkid,
                reason: reason(ack_reason)?,
                properties: None,
            });
        }

        let properties = PubRecProperties::extract(&mut bytes)?;
        let puback = PubRec {
            pkid,
            reason: reason(ack_reason)?,
            properties,
        };

        Ok(puback)
    }

    fn len(&self) -> usize {
        let mut len = 2 + 1; // pkid + reason

        if self.reason == PubRecReason::Success && self.properties.is_none() {
            return 2;
        }

        if let Some(properties) = &self.properties {
            let properties_len = properties.len();
            let properties_len_len = remaining_len_len(properties_len);
            len += properties_len_len + properties_len;
        }
        // Unlike other packets, property length can be ignored if there are
        // no properties in acks
        len
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = self.len();
        buffer.reserve(len);
        buffer.put_u8(0x50);
        let count = write_remaining_length(buffer, len)?;
        buffer.put_u16(self.pkid);
        if self.reason == PubRecReason::Success && self.properties.is_none() {
            return Ok(4);
        }

        buffer.put_u8(self.reason as u8);

        if let Some(properties) = &self.properties {
            properties.write(buffer)?;
        }

        Ok(1 + count + len)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PubRecProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl PubRecProperties {
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

    pub fn extract(mut bytes: &mut Bytes) -> Result<Option<PubRecProperties>, Error> {
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
            let prop = bytes.get_u8();
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

        Ok(Some(PubRecProperties {
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
fn reason(num: u8) -> Result<PubRecReason, Error> {
    let code = match num {
        0 => PubRecReason::Success,
        16 => PubRecReason::NoMatchingSubscribers,
        128 => PubRecReason::UnspecifiedError,
        131 => PubRecReason::ImplementationSpecificError,
        135 => PubRecReason::NotAuthorized,
        144 => PubRecReason::TopicNameInvalid,
        145 => PubRecReason::PacketIdentifierInUse,
        151 => PubRecReason::QuotaExceeded,
        153 => PubRecReason::PayloadFormatInvalid,
        num => return Err(Error::InvalidConnectReturnCode(num)),
    };

    Ok(code)
}

#[cfg(test)]
mod test {
    use crate::*;
    use alloc::vec;
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;

    fn sample() -> PubRec {
        let properties = PubRecProperties {
            reason_string: Some("test".to_owned()),
            user_properties: vec![("test".to_owned(), "test".to_owned())],
        };

        PubRec {
            pkid: 42,
            reason: PubRecReason::NoMatchingSubscribers,
            properties: Some(properties),
        }
    }

    fn sample_bytes() -> Vec<u8> {
        vec![
            0x50, // payload type
            0x18, // remaining length
            0x00, 0x2a, // packet id
            0x10, // reason
            0x14, // properties len
            0x1f, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // reason_string
            0x26, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x04, 0x74, 0x65, 0x73,
            0x74, // user properties
        ]
    }

    #[test]
    fn puback_parsing_works_correctly() {
        let mut stream = bytes::BytesMut::new();
        let packetstream = &sample_bytes();
        stream.extend_from_slice(&packetstream[..]);
        let packet = mqtt_read(&mut stream, 200).unwrap();
        let packet = match packet {
            Packet::PubRec(pubrec) => pubrec,
            packet => panic!("Invalid packet = {:?}", packet),
        };

        let puback = sample();
        assert_eq!(packet, puback);
    }

    #[test]
    fn puback_encoding_works_correctly() {
        let pubrec = sample();
        let mut buf = BytesMut::new();
        pubrec.write(&mut buf).unwrap();
        assert_eq!(&buf[..], sample_bytes());
    }
}

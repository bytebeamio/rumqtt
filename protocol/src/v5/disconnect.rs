use std::convert::{TryFrom, TryInto};

use bytes::{BufMut, Bytes, BytesMut};

use super::*;

use super::{property, PropertyType};

fn len(disconnect: &Disconnect, properties: &Option<DisconnectProperties>) -> usize {
    if disconnect.reason_code == DisconnectReasonCode::NormalDisconnection && properties.is_none() {
        return 2; // Packet type + 0x00
    }

    let mut length = 0;
    if let Some(properties) = &properties {
        length += 1; // Disconnect Reason Code
        let properties_len = properties::len(properties);
        let properties_len_len = len_len(properties_len);
        length += properties_len_len + properties_len;
    } else {
        length += 1;
    }

    length
}

pub fn read(
    fixed_header: FixedHeader,
    mut bytes: Bytes,
) -> Result<(Disconnect, Option<DisconnectProperties>), Error> {
    let packet_type = fixed_header.byte1 >> 4;
    let flags = fixed_header.byte1 & 0b0000_1111;

    bytes.advance(fixed_header.fixed_header_len);

    if packet_type != PacketType::Disconnect as u8 {
        return Err(Error::InvalidPacketType(packet_type));
    };

    if flags != 0x00 {
        return Err(Error::MalformedPacket);
    };

    if fixed_header.remaining_len == 0 {
        return Ok((
            Disconnect {
                reason_code: DisconnectReasonCode::NormalDisconnection,
            },
            None,
        ));
    }

    let reason_code = read_u8(&mut bytes)?;

    let disconnect = Disconnect {
        reason_code: reason(reason_code)?,
    };
    let properties = properties::read(&mut bytes)?;

    Ok((disconnect, properties))
}

pub fn write(
    disconnect: &Disconnect,
    properties: &Option<DisconnectProperties>,
    buffer: &mut BytesMut,
) -> Result<usize, Error> {
    buffer.put_u8(0xE0);

    let length = len(disconnect, properties);

    if length == 2 {
        buffer.put_u8(0x00);
        return Ok(length);
    }

    let len_len = write_remaining_length(buffer, length)?;

    buffer.put_u8(code(disconnect.reason_code));

    if let Some(properties) = &properties {
        properties::write(properties, buffer)?;
    } else {
        write_remaining_length(buffer, 0)?;
    }

    Ok(1 + len_len + length)
}

mod properties {
    use super::*;

    pub fn len(properties: &DisconnectProperties) -> usize {
        let mut length = 0;

        if properties.session_expiry_interval.is_some() {
            length += 1 + 4;
        }

        if let Some(reason) = &properties.reason_string {
            length += 1 + 2 + reason.len();
        }

        for (key, value) in properties.user_properties.iter() {
            length += 1 + 2 + key.len() + 2 + value.len();
        }

        if let Some(server_reference) = &properties.server_reference {
            length += 1 + 2 + server_reference.len();
        }

        length
    }

    pub fn read(mut bytes: &mut Bytes) -> Result<Option<DisconnectProperties>, Error> {
        let (properties_len_len, properties_len) = length(bytes.iter())?;

        bytes.advance(properties_len_len);

        if properties_len == 0 {
            return Ok(None);
        }

        let mut session_expiry_interval = None;
        let mut reason_string = None;
        let mut user_properties = Vec::new();
        let mut server_reference = None;

        let mut cursor = 0;

        // read until cursor reaches property length. properties_len = 0 will skip this loop
        while cursor < properties_len {
            let prop = read_u8(bytes)?;
            cursor += 1;

            match property(prop)? {
                PropertyType::SessionExpiryInterval => {
                    session_expiry_interval = Some(read_u32(bytes)?);
                    cursor += 4;
                }
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
                PropertyType::ServerReference => {
                    let reference = read_mqtt_string(bytes)?;
                    cursor += 2 + reference.len();
                    server_reference = Some(reference);
                }
                _ => return Err(Error::InvalidPropertyType(prop)),
            }
        }

        let properties = DisconnectProperties {
            session_expiry_interval,
            reason_string,
            user_properties,
            server_reference,
        };

        Ok(Some(properties))
    }

    pub fn write(properties: &DisconnectProperties, buffer: &mut BytesMut) -> Result<(), Error> {
        let length = len(properties);
        write_remaining_length(buffer, length)?;

        if let Some(session_expiry_interval) = properties.session_expiry_interval {
            buffer.put_u8(PropertyType::SessionExpiryInterval as u8);
            buffer.put_u32(session_expiry_interval);
        }

        if let Some(reason) = &properties.reason_string {
            buffer.put_u8(PropertyType::ReasonString as u8);
            write_mqtt_string(buffer, reason);
        }

        for (key, value) in properties.user_properties.iter() {
            buffer.put_u8(PropertyType::UserProperty as u8);
            write_mqtt_string(buffer, key);
            write_mqtt_string(buffer, value);
        }

        if let Some(reference) = &properties.server_reference {
            buffer.put_u8(PropertyType::ServerReference as u8);
            write_mqtt_string(buffer, reference);
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn disconnect1_parsing_works() {
        let mut buffer = bytes::BytesMut::new();
        let packet_bytes = [
            0xE0, // Packet type
            0x00, // Remaining length
        ];
        let expected = Disconnect {
            reason_code: DisconnectReasonCode::NormalDisconnection,
        };

        buffer.extend_from_slice(&packet_bytes[..]);

        let fixed_header = parse_fixed_header(buffer.iter()).unwrap();
        let disconnect_bytes = buffer.split_to(fixed_header.frame_length()).freeze();
        let (disconnect, properties) = read(fixed_header, disconnect_bytes).unwrap();

        assert_eq!(disconnect, expected);
    }

    #[test]
    fn disconnect1_encoding_works() {
        let mut buffer = BytesMut::new();
        let disconnect = Disconnect {
            reason_code: DisconnectReasonCode::NormalDisconnection,
        };
        let expected = [
            0xE0, // Packet type
            0x00, // Remaining length
        ];

        write(&disconnect, &None, &mut buffer).unwrap();

        assert_eq!(&buffer[..], &expected);
    }

    fn sample2() -> (Disconnect, Option<DisconnectProperties>) {
        let properties = DisconnectProperties {
            // TODO: change to 2137 xD
            session_expiry_interval: Some(1234),
            reason_string: Some("test".to_owned()),
            user_properties: vec![("test".to_owned(), "test".to_owned())],
            server_reference: Some("test".to_owned()),
        };

        (
            Disconnect {
                reason_code: DisconnectReasonCode::UnspecifiedError,
            },
            Some(properties),
        )
    }

    fn sample_bytes2() -> Vec<u8> {
        vec![
            0xE0, // Packet type
            0x22, // Remaining length
            0x80, // Disconnect Reason Code
            0x20, // Properties length
            0x11, 0x00, 0x00, 0x04, 0xd2, // Session expiry interval
            0x1F, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // Reason string
            0x26, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x04, 0x74, 0x65, 0x73,
            0x74, // User properties
            0x1C, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // server reference
        ]
    }

    #[test]
    fn disconnect2_parsing_works() {
        let mut buffer = bytes::BytesMut::new();
        let packet_bytes = sample_bytes2();
        let expected = sample2();

        buffer.extend_from_slice(&packet_bytes[..]);

        let fixed_header = parse_fixed_header(buffer.iter()).unwrap();
        let disconnect_bytes = buffer.split_to(fixed_header.frame_length()).freeze();
        let disconnect = read(fixed_header, disconnect_bytes).unwrap();

        assert_eq!(disconnect, expected);
    }

    #[test]
    fn disconnect2_encoding_works() {
        let mut buffer = BytesMut::new();

        let (disconnect, properties) = sample2();
        let expected = sample_bytes2();

        write(&disconnect, &properties, &mut buffer).unwrap();

        assert_eq!(&buffer[..], &expected);
    }
}

fn reason(code: u8) -> Result<DisconnectReasonCode, Error> {
    let v = match code {
        0x00 => DisconnectReasonCode::NormalDisconnection,
        0x04 => DisconnectReasonCode::DisconnectWithWillMessage,
        0x80 => DisconnectReasonCode::UnspecifiedError,
        0x81 => DisconnectReasonCode::MalformedPacket,
        0x82 => DisconnectReasonCode::ProtocolError,
        0x83 => DisconnectReasonCode::ImplementationSpecificError,
        0x87 => DisconnectReasonCode::NotAuthorized,
        0x89 => DisconnectReasonCode::ServerBusy,
        0x8B => DisconnectReasonCode::ServerShuttingDown,
        0x8D => DisconnectReasonCode::KeepAliveTimeout,
        0x8E => DisconnectReasonCode::SessionTakenOver,
        0x8F => DisconnectReasonCode::TopicFilterInvalid,
        0x90 => DisconnectReasonCode::TopicNameInvalid,
        0x93 => DisconnectReasonCode::ReceiveMaximumExceeded,
        0x94 => DisconnectReasonCode::TopicAliasInvalid,
        0x95 => DisconnectReasonCode::PacketTooLarge,
        0x96 => DisconnectReasonCode::MessageRateTooHigh,
        0x97 => DisconnectReasonCode::QuotaExceeded,
        0x98 => DisconnectReasonCode::AdministrativeAction,
        0x99 => DisconnectReasonCode::PayloadFormatInvalid,
        0x9A => DisconnectReasonCode::RetainNotSupported,
        0x9B => DisconnectReasonCode::QoSNotSupported,
        0x9C => DisconnectReasonCode::UseAnotherServer,
        0x9D => DisconnectReasonCode::ServerMoved,
        0x9E => DisconnectReasonCode::SharedSubscriptionNotSupported,
        0x9F => DisconnectReasonCode::ConnectionRateExceeded,
        0xA0 => DisconnectReasonCode::MaximumConnectTime,
        0xA1 => DisconnectReasonCode::SubscriptionIdentifiersNotSupported,
        0xA2 => DisconnectReasonCode::WildcardSubscriptionsNotSupported,
        other => return Err(Error::InvalidConnectReturnCode(other)),
    };
    Ok(v)
}

fn code(reason: DisconnectReasonCode) -> u8 {
    match reason {
        DisconnectReasonCode::NormalDisconnection => 0x00,
        DisconnectReasonCode::DisconnectWithWillMessage => 0x04,
        DisconnectReasonCode::UnspecifiedError => 0x80,
        DisconnectReasonCode::MalformedPacket => 0x81,
        DisconnectReasonCode::ProtocolError => 0x82,
        DisconnectReasonCode::ImplementationSpecificError => 0x83,
        DisconnectReasonCode::NotAuthorized => 0x87,
        DisconnectReasonCode::ServerBusy => 0x89,
        DisconnectReasonCode::ServerShuttingDown => 0x8B,
        DisconnectReasonCode::KeepAliveTimeout => 0x8D,
        DisconnectReasonCode::SessionTakenOver => 0x8E,
        DisconnectReasonCode::TopicFilterInvalid => 0x8F,
        DisconnectReasonCode::TopicNameInvalid => 0x90,
        DisconnectReasonCode::ReceiveMaximumExceeded => 0x93,
        DisconnectReasonCode::TopicAliasInvalid => 0x94,
        DisconnectReasonCode::PacketTooLarge => 0x95,
        DisconnectReasonCode::MessageRateTooHigh => 0x96,
        DisconnectReasonCode::QuotaExceeded => 0x97,
        DisconnectReasonCode::AdministrativeAction => 0x98,
        DisconnectReasonCode::PayloadFormatInvalid => 0x99,
        DisconnectReasonCode::RetainNotSupported => 0x9A,
        DisconnectReasonCode::QoSNotSupported => 0x9B,
        DisconnectReasonCode::UseAnotherServer => 0x9C,
        DisconnectReasonCode::ServerMoved => 0x9D,
        DisconnectReasonCode::SharedSubscriptionNotSupported => 0x9E,
        DisconnectReasonCode::ConnectionRateExceeded => 0x9F,
        DisconnectReasonCode::MaximumConnectTime => 0xA0,
        DisconnectReasonCode::SubscriptionIdentifiersNotSupported => 0xA1,
        DisconnectReasonCode::WildcardSubscriptionsNotSupported => 0xA2,
    }
}

use super::*;

pub fn len() -> usize {
    // pkid
    2
}

pub fn read(fixed_header: FixedHeader, mut bytes: &[u8]) -> Result<PubAck, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes = &bytes[variable_header_index..];

    if fixed_header.remaining_len != 2 {
        return Err(Error::InvalidRemainingLength(fixed_header.remaining_len));
    }

    let pkid = read_u16(&mut bytes)?;
    let puback = PubAck {
        pkid,
        reason: PubAckReason::Success,
        properties: None,
    };

    Ok(puback)
}

pub fn write(puback: &PubAck, buffer: &mut Vec<u8>) -> Result<usize, Error> {
    let len = len();
    buffer.push(0x40);
    let count = write_remaining_length(buffer, len)?;
    buffer.extend_from_slice(&puback.pkid.to_be_bytes());
    Ok(1 + count + len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        fn generate_puback_properties()(
            reason_string: Option<String>,
            user_properties: Vec<(String, String)>
        ) -> PubAckProperties {
            PubAckProperties {
                reason_string,
                user_properties,
            }
        }
    }

    fn generate_puback_reason() -> impl Strategy<Value = PubAckReason> {
        prop_oneof![
            Just(PubAckReason::Success),
            Just(PubAckReason::NoMatchingSubscribers),
            Just(PubAckReason::UnspecifiedError),
            Just(PubAckReason::ImplementationSpecificError),
            Just(PubAckReason::NotAuthorized),
            Just(PubAckReason::TopicNameInvalid),
            Just(PubAckReason::PacketIdentifierInUse),
            Just(PubAckReason::QuotaExceeded),
            Just(PubAckReason::PayloadFormatInvalid),
        ]
    }

    prop_compose! {
        fn generate_puback_packet()(
            pkid: u16,
            reason in generate_puback_reason(),
            properties in proptest::option::of(generate_puback_properties()),
        ) -> PubAck {
            PubAck {
                pkid,
                reason,
                properties,
            }
        }
    }

    proptest! {
        #[test]
        fn test_roundtrip(
            packet in generate_puback_packet()
        ) {
            let mut buf = vec![];
            write(&packet, &mut buf).unwrap();
            let fixed_header = parse_fixed_header(&buf).unwrap();
            let parsed_packet = read(fixed_header, &buf).unwrap();
            prop_assert_eq!(parsed_packet, PubAck {
                pkid: packet.pkid,
                reason: PubAckReason::Success,
                properties: None,
            });
        }
    }
}

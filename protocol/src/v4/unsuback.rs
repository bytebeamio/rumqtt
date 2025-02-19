use super::*;

pub fn len(unsuback: &UnsubAck) -> usize {
    2 + unsuback.reasons.len()
}

pub fn read(fixed_header: FixedHeader, bytes: &[u8]) -> Result<UnsubAck, Error> {
    if fixed_header.remaining_len != 2 {
        return Err(Error::PayloadSizeIncorrect);
    }

    let variable_header_index = fixed_header.fixed_header_len;
    let mut bytes = &bytes[variable_header_index..];
    let pkid = read_u16(&mut bytes)?;
    let unsuback = UnsubAck {
        pkid,
        // TODO: See if this is correct
        reasons: vec![],
        properties: None,
    };

    Ok(unsuback)
}

pub fn write(unsuback: &UnsubAck, buffer: &mut Vec<u8>) -> Result<usize, Error> {
    buffer.extend_from_slice(&[0xB0, 0x02]);
    buffer.extend_from_slice(&unsuback.pkid.to_be_bytes());
    Ok(4)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        fn generate_unsuback_properties()(
            reason_string: Option<String>,
            user_properties: Vec<(String, String)>
        ) -> UnsubAckProperties {
            UnsubAckProperties {
                reason_string,
                user_properties,
            }
        }
    }

    fn generate_unsuback_reason() -> impl Strategy<Value = UnsubAckReason> {
        prop_oneof![
            Just(UnsubAckReason::Success),
            Just(UnsubAckReason::NoSubscriptionExisted),
            Just(UnsubAckReason::UnspecifiedError),
            Just(UnsubAckReason::ImplementationSpecificError),
            Just(UnsubAckReason::NotAuthorized),
            Just(UnsubAckReason::TopicFilterInvalid),
            Just(UnsubAckReason::PacketIdentifierInUse),
        ]
    }

    prop_compose! {
        fn generate_unsuback_packet()(
            pkid: u16,
            reasons in prop::collection::vec(generate_unsuback_reason(), 0..100),
            properties in prop::option::of(generate_unsuback_properties()),
        ) -> UnsubAck {
            UnsubAck {
                pkid,
                reasons,
                properties,
            }
        }
    }

    proptest! {
        #[test]
        fn test_roundtrip(
            packet in generate_unsuback_packet()
        ) {
            let mut buf = vec![];
            write(&packet, &mut buf).unwrap();
            let fixed_header = parse_fixed_header(&buf).unwrap();
            let parsed = read(fixed_header, &buf).unwrap();
            prop_assert_eq!(parsed, UnsubAck {
                pkid: packet.pkid,
                reasons: vec![],
                properties: None,
            })
        }
    }
}

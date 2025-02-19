use super::*;

pub fn len() -> usize {
    // pkid
    2
}

pub fn read(fixed_header: FixedHeader, mut bytes: &[u8]) -> Result<PubRec, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes = &bytes[variable_header_index..];
    let pkid = read_u16(&mut bytes)?;
    if fixed_header.remaining_len == 2 {
        return Ok(PubRec {
            pkid,
            reason: PubRecReason::Success,
            properties: None,
        });
    }

    if fixed_header.remaining_len < 4 {
        return Ok(PubRec {
            pkid,
            reason: PubRecReason::Success,
            properties: None,
        });
    }

    let puback = PubRec {
        pkid,
        reason: PubRecReason::Success,
        properties: None,
    };

    Ok(puback)
}

pub fn write(pubrec: &PubRec, buffer: &mut Vec<u8>) -> Result<usize, Error> {
    let len = len();
    buffer.push(0x50);
    let count = write_remaining_length(buffer, len)?;
    buffer.extend_from_slice(&pubrec.pkid.to_be_bytes());
    Ok(1 + count + len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        fn generate_pubrec_properties()(
            reason_string: Option<String>,
            user_properties: Vec<(String, String)>
        ) -> PubRecProperties {
            PubRecProperties {
                reason_string,
                user_properties,
            }
        }
    }

    fn generate_pubrec_reason() -> impl Strategy<Value = PubRecReason> {
        prop_oneof![
            Just(PubRecReason::Success),
            Just(PubRecReason::NoMatchingSubscribers),
            Just(PubRecReason::UnspecifiedError),
            Just(PubRecReason::ImplementationSpecificError),
            Just(PubRecReason::NotAuthorized),
            Just(PubRecReason::TopicNameInvalid),
            Just(PubRecReason::PacketIdentifierInUse),
            Just(PubRecReason::QuotaExceeded),
            Just(PubRecReason::PayloadFormatInvalid),
        ]
    }

    prop_compose! {
        fn generate_pubrec_packet()(
            pkid: u16,
            reason in generate_pubrec_reason(),
            properties in proptest::option::of(generate_pubrec_properties()),
        ) -> PubRec {
            PubRec {
                pkid,
                reason,
                properties,
            }
        }
    }

    proptest! {
        #[test]
        fn test_roundtrip(
            packet in generate_pubrec_packet()
        ) {
            let mut buf = vec![];
            write(&packet, &mut buf).unwrap();
            let fixed_header = parse_fixed_header(&buf).unwrap();
            let parsed_packet = read(fixed_header, &buf).unwrap();
            prop_assert_eq!(parsed_packet, PubRec {
                pkid: packet.pkid,
                reason: PubRecReason::Success,
                properties: None,
            });
        }
    }
}

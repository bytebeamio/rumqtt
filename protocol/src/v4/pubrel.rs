use super::*;

pub fn len() -> usize {
    // pkid
    2
}

pub fn read(fixed_header: FixedHeader, mut bytes: &[u8]) -> Result<PubRel, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes = &bytes[variable_header_index..];
    let pkid = read_u16(&mut bytes)?;
    if fixed_header.remaining_len == 2 {
        return Ok(PubRel {
            pkid,
            reason: PubRelReason::Success,
            properties: None,
        });
    }

    if fixed_header.remaining_len < 4 {
        return Ok(PubRel {
            pkid,
            reason: PubRelReason::Success,
            properties: None,
        });
    }

    let puback = PubRel {
        pkid,
        reason: PubRelReason::Success,
        properties: None,
    };

    Ok(puback)
}

pub fn write(pubrel: &PubRel, buffer: &mut Vec<u8>) -> Result<usize, Error> {
    let len = len();
    buffer.push(0x62);
    let count = write_remaining_length(buffer, len)?;
    buffer.extend_from_slice(&pubrel.pkid.to_be_bytes());
    Ok(1 + count + len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        fn generate_pubrel_properties()(
            reason_string: Option<String>,
            user_properties: Vec<(String, String)>
        ) -> PubRelProperties {
            PubRelProperties {
                reason_string,
                user_properties,
            }
        }
    }

    fn generate_pubrel_reason() -> impl Strategy<Value = PubRelReason> {
        prop_oneof![
            Just(PubRelReason::Success),
            Just(PubRelReason::PacketIdentifierNotFound),
        ]
    }

    prop_compose! {
        fn generate_pubrel_packet()(
            pkid: u16,
            reason in generate_pubrel_reason(),
            properties in proptest::option::of(generate_pubrel_properties()),
        ) -> PubRel {
            PubRel {
                pkid,
                reason,
                properties,
            }
        }
    }

    proptest! {
        #[test]
        fn test_roundtrip(
            packet in generate_pubrel_packet()
        ) {
            let mut buf = vec![];
            write(&packet, &mut buf).unwrap();
            let fixed_header = parse_fixed_header(&buf).unwrap();
            let parsed_packet = read(fixed_header, &buf).unwrap();
            prop_assert_eq!(parsed_packet, PubRel {
                pkid: packet.pkid,
                reason: PubRelReason::Success,
                properties: None,
            });
        }
    }
}

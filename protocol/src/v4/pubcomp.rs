use super::*;

pub fn len() -> usize {
    // pkid
    2
}

pub fn read(fixed_header: FixedHeader, mut bytes: &[u8]) -> Result<PubComp, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes = &bytes[variable_header_index..];
    let pkid = read_u16(&mut bytes)?;

    if fixed_header.remaining_len == 2 {
        return Ok(PubComp {
            pkid,
            reason: PubCompReason::Success,
            properties: None,
        });
    }

    if fixed_header.remaining_len < 4 {
        return Ok(PubComp {
            pkid,
            reason: PubCompReason::Success,
            properties: None,
        });
    }

    let puback = PubComp {
        pkid,
        reason: PubCompReason::Success,
        properties: None,
    };

    Ok(puback)
}

pub fn write(pubcomp: &PubComp, buffer: &mut Vec<u8>) -> Result<usize, Error> {
    let len = len();
    buffer.push(0x70);
    let count = write_remaining_length(buffer, len)?;
    buffer.extend_from_slice(&pubcomp.pkid.to_be_bytes());
    Ok(1 + count + len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        fn generate_pubcomp_properties()(
            reason_string: Option<String>,
            user_properties: Vec<(String, String)>
        ) -> PubCompProperties {
            PubCompProperties {
                reason_string,
                user_properties,
            }
        }
    }

    fn generate_pubcomp_reason() -> impl Strategy<Value = PubCompReason> {
        prop_oneof![
            Just(PubCompReason::Success),
            Just(PubCompReason::PacketIdentifierNotFound),
        ]
    }

    prop_compose! {
        fn generate_pubcomp_packet()(
            pkid: u16,
            reason in generate_pubcomp_reason(),
            properties in proptest::option::of(generate_pubcomp_properties()),
        ) -> PubComp {
            PubComp {
                pkid,
                reason,
                properties,
            }
        }
    }

    proptest! {
        #[test]
        fn test_roundtrip(
            packet in generate_pubcomp_packet(),
        ) {
            let mut buf = vec![];
            write(&packet, &mut buf).unwrap();
            let fixed_header = parse_fixed_header(&buf).unwrap();
            let parsed_packet = read(fixed_header, &buf).unwrap();
            prop_assert_eq!(parsed_packet, PubComp {
                pkid: packet.pkid,
                reason: PubCompReason::Success,
                properties: None,
            });
        }
    }
}

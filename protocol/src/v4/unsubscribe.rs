use super::*;

pub fn len(unsubscribe: &Unsubscribe) -> usize {
    // Packet id + length of filters (unlike subscribe, this just a string.
    // Hence 2 is prefixed for len per filter)
    let mut len = 2 + unsubscribe.filters.iter().fold(0, |s, t| 2 + s + t.len());

    len
}

pub fn read(fixed_header: FixedHeader, mut bytes: &[u8]) -> Result<Unsubscribe, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes = &bytes[variable_header_index..];

    let pkid = read_u16(&mut bytes)?;
    let mut payload_bytes = fixed_header.remaining_len - 2;
    let mut filters = Vec::with_capacity(1);

    while payload_bytes > 0 {
        let topic_filter = read_mqtt_string(&mut bytes)?;
        payload_bytes -= topic_filter.len() + 2;
        filters.push(topic_filter);
    }

    let unsubscribe = Unsubscribe {
        pkid,
        filters,
        properties: None,
    };
    Ok(unsubscribe)
}

pub fn write(unsubscribe: &Unsubscribe, buffer: &mut Vec<u8>) -> Result<usize, Error> {
    let remaining_len = 2 + unsubscribe
        .filters
        .iter()
        .fold(0, |s, topic| s + topic.len() + 2);

    buffer.push(0xA2);
    let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;
    buffer.extend_from_slice(&unsubscribe.pkid.to_be_bytes());

    for topic in unsubscribe.filters.iter() {
        write_mqtt_string(buffer, topic.as_str());
    }
    Ok(1 + remaining_len_bytes + remaining_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        fn generate_unsubscribe_properties()(
            user_properties: Vec<(String, String)>
        ) -> UnsubscribeProperties {
            UnsubscribeProperties {
                user_properties,
            }
        }
    }
    prop_compose! {
        fn generate_unsubscribe_packet()(
            pkid: u16,
            filters: Vec<String>,
            properties in prop::option::of(generate_unsubscribe_properties()),
        ) -> Unsubscribe {
            Unsubscribe {
                filters: vec![],
                pkid,
                properties,
            }
        }
    }

    proptest! {
        #[test]
        fn test_roundtrip(
            packet in generate_unsubscribe_packet()
        ) {
            let mut buf = vec![];
            write(&packet, &mut buf).unwrap();
            let fixed_header = parse_fixed_header(&buf).unwrap();
            let parsed = read(fixed_header, &buf).unwrap();
            prop_assert_eq!(parsed, Unsubscribe {
                pkid: packet.pkid,
                filters: packet.filters,
                properties: None,
            })
        }
    }
}

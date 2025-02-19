use super::*;

pub fn len(subscribe: &Subscribe) -> usize {
    // len of pkid + vec![subscribe filter len]
    2 + subscribe.filters.iter().fold(0, |s, t| s + filter::len(t))
}

pub fn read(fixed_header: FixedHeader, mut bytes: &[u8]) -> Result<Subscribe, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes = &bytes[variable_header_index..];

    let pkid = read_u16(&mut bytes)?;
    let filters = filter::read(&mut bytes)?;

    match filters.len() {
        0 => Err(Error::EmptySubscription),
        _ => Ok(Subscribe {
            pkid,
            filters,
            properties: None,
        }),
    }
}

pub fn write(subscribe: &Subscribe, buffer: &mut Vec<u8>) -> Result<usize, Error> {
    // write packet type
    buffer.push(0x82);

    // write remaining length
    let remaining_len = len(subscribe);
    let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

    // write packet id
    buffer.extend_from_slice(&subscribe.pkid.to_be_bytes());

    // write filters
    for f in subscribe.filters.iter() {
        filter::write(f, buffer);
    }

    Ok(1 + remaining_len_bytes + remaining_len)
}

mod filter {
    use super::*;

    pub fn len(filter: &Filter) -> usize {
        // filter len + filter + options
        2 + filter.path.len() + 1
    }

    pub fn read(bytes: &mut &[u8]) -> Result<Vec<Filter>, Error> {
        // variable header size = 2 (packet identifier)
        let mut filters = Vec::new();

        while !bytes.is_empty() {
            let path = read_mqtt_bytes(bytes)?;
            let path = std::str::from_utf8(&path)?.to_owned();
            let options = read_u8(bytes)?;
            let requested_qos = options & 0b0000_0011;

            filters.push(Filter {
                path,
                qos: u8_to_qos(requested_qos)?,
                nolocal: false,
                preserve_retain: false,
                retain_forward_rule: RetainForwardRule::Never,
            });
        }

        Ok(filters)
    }

    pub fn write(filter: &Filter, buffer: &mut Vec<u8>) {
        let mut options = 0;
        options |= filter.qos as u8;

        write_mqtt_string(buffer, filter.path.as_str());
        buffer.push(options);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn generate_qos() -> impl Strategy<Value = QoS> {
        prop_oneof![
            Just(QoS::AtMostOnce),
            Just(QoS::AtLeastOnce),
            Just(QoS::ExactlyOnce),
        ]
    }

    fn generate_retain_forward_rule() -> impl Strategy<Value = RetainForwardRule> {
        prop_oneof![
            Just(RetainForwardRule::OnEverySubscribe),
            Just(RetainForwardRule::OnNewSubscribe),
            Just(RetainForwardRule::Never),
        ]
    }

    prop_compose! {
        fn generate_filter()(
            path: String,
            qos in generate_qos(),
            nolocal: bool,
            preserve_retain: bool,
            retain_forward_rule in generate_retain_forward_rule(),
        ) -> Filter {
            Filter {
                path,
                qos,
                nolocal,
                preserve_retain,
                retain_forward_rule,
            }
        }
    }

    prop_compose! {
        fn generate_subscribe_properties()(
            id: Option<usize>,
            user_properties: Vec<(String, String)>
        ) -> SubscribeProperties {
            SubscribeProperties {
                id,
                user_properties,
            }
        }
    }

    prop_compose! {
        fn generate_subscribe_packet()(
            pkid: u16,
            filters in prop::collection::vec(generate_filter(), 1..100),
            properties in prop::option::of(generate_subscribe_properties()),
        ) -> Subscribe {
            Subscribe {
                pkid,
                filters,
                properties,
            }
        }
    }

    proptest! {
        #[test]
        fn test_roundtrip(
            packet in generate_subscribe_packet()
        ) {
            let mut buf = vec![];
            write(&packet, &mut buf).unwrap();
            let fixed_header = parse_fixed_header(&buf).unwrap();
            let parsed = read(fixed_header, &buf).unwrap();
            prop_assert_eq!(parsed, Subscribe {
                pkid: packet.pkid,
                properties: None,
                filters: packet.filters.into_iter().map(|f| Filter {
                    path: f.path,
                    qos: f.qos,
                    nolocal: false,
                    preserve_retain: false,
                    retain_forward_rule: RetainForwardRule::Never,
                }).collect(),
            })
        }
    }

    #[test]
    fn error_on_empty_filters() {
        let subscribe = Subscribe {
            pkid: 0,
            filters: vec![],
            properties: None,
        };

        let mut buf = vec![];
        write(&subscribe, &mut buf).is_err();
        let fixed_header = parse_fixed_header(&buf).unwrap();
        let result = read(fixed_header, &buf);
        assert_eq!(result, Err(Error::EmptySubscription));
    }
}

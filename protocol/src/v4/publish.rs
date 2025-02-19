use super::*;

pub fn len(publish: &Publish) -> usize {
    let len = 2 + publish.topic.len() + publish.payload.len();
    match publish.qos != QoS::AtMostOnce && publish.pkid != 0 {
        true => len + 2,
        _ => len,
    }
}

pub fn read(fixed_header: FixedHeader, mut bytes: &[u8]) -> Result<Publish, Error> {
    let qos_num = (fixed_header.byte1 & 0b0110) >> 1;
    let qos = u8_to_qos(qos_num)?;
    let dup = (fixed_header.byte1 & 0b1000) != 0;
    let retain = (fixed_header.byte1 & 0b0001) != 0;

    let variable_header_index = fixed_header.fixed_header_len;
    bytes = &bytes[variable_header_index..];
    let topic = read_mqtt_bytes(&mut bytes)?;

    // Packet identifier exists where QoS > 0
    let pkid = match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce | QoS::ExactlyOnce => read_u16(&mut bytes)?,
    };

    if qos != QoS::AtMostOnce && pkid == 0 {
        return Err(Error::PacketIdZero);
    }

    let publish = Publish {
        dup,
        retain,
        qos,
        pkid,
        topic,
        payload: bytes.to_vec(),
        properties: None,
    };

    Ok(publish)
}

pub fn write(publish: &Publish, buffer: &mut Vec<u8>) -> Result<usize, Error> {
    let len = publish.len();

    let dup = publish.dup as u8;
    let qos = publish.qos as u8;
    let retain = publish.retain as u8;
    buffer.push(0b0011_0000 | retain | qos << 1 | dup << 3);

    let count = write_remaining_length(buffer, len)?;
    write_mqtt_bytes(buffer, &publish.topic);

    if publish.qos != QoS::AtMostOnce {
        let pkid = publish.pkid;
        if pkid == 0 {
            return Err(Error::PacketIdZero);
        }

        buffer.extend_from_slice(&pkid.to_be_bytes());
    }

    buffer.extend_from_slice(&publish.payload);

    // TODO: Returned length is wrong in other packets. Fix it
    Ok(1 + count + len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        fn generate_publish_properties()(
            payload_format_indicator: Option<u8>,
            message_expiry_interval: Option<u32>,
            topic_alias: Option<u16>,
            response_topic: Option<String>,
            correlation_data: Option<Vec<u8>>,
            user_properties: Vec<(String, String)>,
            subscription_identifiers: Vec<usize>,
            content_type: Option<String>,
        ) -> PublishProperties {
            PublishProperties {
                payload_format_indicator,
                message_expiry_interval,
                topic_alias,
                response_topic,
                correlation_data,
                user_properties,
                subscription_identifiers,
                content_type,
            }
        }
    }

    prop_compose! {
        fn generate_publish_packet(qos_gen: impl Strategy<Value = QoS>, pkid_gen: impl Strategy<Value = u16>)(
            dup: bool,
            qos in qos_gen,
            retain: bool,
            pkid in pkid_gen,
            topic: Vec<u8>,
            payload: Vec<u8>,
            properties in proptest::option::of(generate_publish_properties()),
        ) -> Publish {
            Publish {
                dup,
                qos,
                retain,
                pkid,
                topic,
                payload,
                properties,
            }
        }
    }

    proptest! {
        #[test]
        fn test_roundtrip_qos_0(
            packet in generate_publish_packet(Just(QoS::AtMostOnce), 0..u16::MAX),
        ) {
            let mut buffer = vec![];
            let len = write(&packet, &mut buffer).unwrap();
            let fixed_header = parse_fixed_header(&buffer).unwrap();
            let result = read(fixed_header, &buffer).unwrap();
            assert_eq!(result, Publish {
                dup: packet.dup,
                qos: packet.qos,
                retain: packet.retain,
                pkid: 0,
                topic: packet.topic,
                payload: packet.payload,
                properties: None,
            });
        }

        #[test]
        fn test_roundtrip_qos_1_and_qos_2(
            publish in generate_publish_packet(prop_oneof![
                Just(QoS::AtLeastOnce),
                Just(QoS::ExactlyOnce),
            ], 1..u16::MAX),
        ) {
            let mut buffer = vec![];
            let len = write(&publish, &mut buffer).unwrap();
            let fixed_header = parse_fixed_header(&buffer).unwrap();
            let result = read(fixed_header, &buffer).unwrap();
            assert_eq!(result, Publish {
                dup: publish.dup,
                qos: publish.qos,
                retain: publish.retain,
                pkid: publish.pkid,
                topic: publish.topic,
                payload: publish.payload,
                properties: None,
            });
        }
    }

    #[test]
    fn pkid_cannot_be_zero_if_qos_not_at_most_once() {
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            pkid: 0,
            topic: "test".as_bytes().to_vec(),
            payload: vec![],
            properties: None,
        };

        let mut buffer = vec![];
        assert_eq!(write(&publish, &mut buffer), Err(Error::PacketIdZero));
    }
}

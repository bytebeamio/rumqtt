use super::*;

pub fn len() -> usize {
    // sesssion present + code
    1 + 1
}

pub fn read(fixed_header: FixedHeader, mut bytes: &[u8]) -> Result<ConnAck, Error> {
    let variable_header_index = fixed_header.fixed_header_len;

    bytes = &bytes[variable_header_index..];

    let flags = read_u8(&mut bytes)?;
    let return_code = read_u8(&mut bytes)?;

    let session_present = (flags & 0x01) == 1;
    let code = connect_return(return_code)?;
    let connack = ConnAck {
        session_present,
        code,
        properties: None,
    };

    Ok(connack)
}

pub fn write(connack: &ConnAck, buffer: &mut Vec<u8>) -> Result<usize, Error> {
    let len = len();
    buffer.push(0x20);

    let count = write_remaining_length(buffer, len)?;
    buffer.push(connack.session_present as u8);
    buffer.push(connect_code(connack.code));

    Ok(1 + count + len)
}

/// Connection return code type
fn connect_return(num: u8) -> Result<ConnectReturnCode, Error> {
    match num {
        0 => Ok(ConnectReturnCode::Success),
        1 => Ok(ConnectReturnCode::RefusedProtocolVersion),
        2 => Ok(ConnectReturnCode::BadClientId),
        3 => Ok(ConnectReturnCode::ServiceUnavailable),
        4 => Ok(ConnectReturnCode::BadUserNamePassword),
        5 => Ok(ConnectReturnCode::NotAuthorized),
        num => Err(Error::InvalidConnectReturnCode(num)),
    }
}

fn connect_code(return_code: ConnectReturnCode) -> u8 {
    match return_code {
        ConnectReturnCode::Success => 0,
        ConnectReturnCode::RefusedProtocolVersion => 1,
        ConnectReturnCode::BadClientId => 2,
        ConnectReturnCode::ServiceUnavailable => 3,
        ConnectReturnCode::BadUserNamePassword => 4,
        ConnectReturnCode::NotAuthorized => 5,
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn generate_return_code() -> impl Strategy<Value = ConnectReturnCode> {
        prop_oneof![
            Just(ConnectReturnCode::Success),
            Just(ConnectReturnCode::RefusedProtocolVersion),
            Just(ConnectReturnCode::BadClientId),
            Just(ConnectReturnCode::ServiceUnavailable),
            Just(ConnectReturnCode::BadUserNamePassword),
            Just(ConnectReturnCode::NotAuthorized),
        ]
    }

    prop_compose! {
        fn generate_connack_properties()(
            session_expiry_interval: Option<u32>,
            receive_max: Option<u16>,
            max_qos in proptest::option::of(0..=u8::MAX),
            retain_available: Option<u8>,
            max_packet_size: Option<u32>,
            assigned_client_identifier: Option<String>,
            topic_alias_max: Option<u16>,
            reason_string: Option<String>,
            user_properties: Vec<(String, String)>,
            wildcard_subscription_available: Option<u8>,
            subscription_identifiers_available: Option<u8>,
            shared_subscription_available: Option<u8>,
            server_keep_alive: Option<u16>,
            response_information: Option<String>,
            server_reference: Option<String>,
            authentication_method: Option<String>,
            authentication_data: Option<Vec<u8>>,
        ) -> ConnAckProperties {
            ConnAckProperties {
                session_expiry_interval,
                receive_max,
                max_qos,
                retain_available,
                max_packet_size,
                assigned_client_identifier,
                topic_alias_max,
                reason_string,
                user_properties,
                wildcard_subscription_available,
                subscription_identifiers_available,
                shared_subscription_available,
                server_keep_alive,
                response_information,
                server_reference,
                authentication_method,
                authentication_data,
            }
        }
    }

    prop_compose! {
        fn generate_connack_packet()(
            code in generate_return_code(),
            session_present: bool,
            properties in proptest::option::of(generate_connack_properties()),
        ) -> ConnAck {
            ConnAck {
                code,
                session_present,
                properties,
            }
        }
    }

    proptest! {
        #[test]
        fn test_roundtrip(
            packet in generate_connack_packet(),
        ) {
            let mut buf = Vec::new();
            write(&packet, &mut buf).unwrap();
            let fixed_header = parse_fixed_header(&buf).unwrap();
            let parsed_packet = read(fixed_header, &buf).unwrap();
            prop_assert_eq!(parsed_packet, ConnAck {
                code: packet.code,
                session_present: packet.session_present,
                properties: None,
            })
        }
    }
}

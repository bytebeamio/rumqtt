use super::*;

pub fn len(connect: &Connect) -> usize {
    let mut len = 2 + "MQTT".len() // protocol name
                              + 1            // protocol version
                              + 1            // connect flags
                              + 2; // keep alive

    len += 2 + connect.client_id.len();

    // last will len
    if let Some(w) = &connect.last_will {
        len += will::len(w);
    }

    // username and password len
    if let Some(l) = &connect.login {
        len += login::len(l);
    }

    len
}

pub fn read(fixed_header: FixedHeader, mut bytes: &[u8]) -> Result<Connect, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes = &bytes[variable_header_index..];

    // Variable header
    let protocol_name = read_mqtt_bytes(&mut bytes)?;
    let protocol_name = std::str::from_utf8(&protocol_name)?;
    let protocol_level = read_u8(&mut bytes)?;
    if protocol_name != "MQTT" {
        return Err(Error::InvalidProtocol);
    }

    if protocol_level != 4 {
        return Err(Error::InvalidProtocolLevel(protocol_level));
    }

    let connect_flags = read_u8(&mut bytes)?;
    let clean_session = (connect_flags & 0b10) != 0;
    let keep_alive = read_u16(&mut bytes)?;

    let client_id = read_mqtt_bytes(&mut bytes)?;
    let client_id = std::str::from_utf8(&client_id)?.to_owned();
    let last_will = will::read(connect_flags, &mut bytes)?;
    let login = login::read(connect_flags, &mut bytes)?;

    let connect = Connect {
        keep_alive,
        client_id,
        clean_session,
        login,
        last_will,
        properties: None,
    };

    Ok(connect)
}

pub fn write(connect: &Connect, buffer: &mut Vec<u8>) -> Result<usize, Error> {
    let len = len(connect);
    buffer.push(0b0001_0000);
    let count = write_remaining_length(buffer, len)?;
    write_mqtt_string(buffer, "MQTT");

    buffer.push(0x04);
    let flags_index = 1 + count + 2 + 4 + 1;

    let mut connect_flags = 0;
    if connect.clean_session {
        connect_flags |= 0x02;
    }

    buffer.push(connect_flags);
    buffer.extend_from_slice(&connect.keep_alive.to_be_bytes());
    write_mqtt_string(buffer, &connect.client_id);

    if let Some(w) = &connect.last_will {
        connect_flags |= will::write(w, buffer)?;
    }

    if let Some(l) = &connect.login {
        connect_flags |= login::write(l, buffer);
    }

    // update connect flags
    buffer[flags_index] = connect_flags;
    Ok(len)
}

mod will {
    use super::*;

    pub fn len(will: &LastWill) -> usize {
        let mut len = 0;
        len += 2 + will.topic.len() + 2 + will.message.len();
        len
    }

    pub fn read(connect_flags: u8, bytes: &mut &[u8]) -> Result<Option<LastWill>, Error> {
        let last_will = match connect_flags & 0b100 {
            0 if (connect_flags & 0b0011_1000) != 0 => {
                return Err(Error::IncorrectPacketFormat);
            }
            0 => None,
            _ => {
                let will_topic = read_mqtt_bytes(bytes)?;
                let will_message = read_mqtt_bytes(bytes)?;
                let qos_num = (connect_flags & 0b11000) >> 3;
                let will_qos = u8_to_qos(qos_num)?;
                Some(LastWill {
                    topic: will_topic,
                    message: will_message,
                    qos: will_qos,
                    retain: (connect_flags & 0b0010_0000) != 0,
                })
            }
        };

        Ok(last_will)
    }

    pub fn write(will: &LastWill, buffer: &mut Vec<u8>) -> Result<u8, Error> {
        let mut connect_flags = 0;

        connect_flags |= 0x04 | (will.qos as u8) << 3;
        if will.retain {
            connect_flags |= 0x20;
        }

        write_mqtt_bytes(buffer, &will.topic);
        write_mqtt_bytes(buffer, &will.message);
        Ok(connect_flags)
    }
}

mod login {
    use super::*;

    pub fn read(connect_flags: u8, bytes: &mut &[u8]) -> Result<Option<Login>, Error> {
        let username = match connect_flags & 0b1000_0000 {
            0 => String::new(),
            _ => {
                let username = read_mqtt_bytes(bytes)?;
                std::str::from_utf8(&username)?.to_owned()
            }
        };

        let password = match connect_flags & 0b0100_0000 {
            0 => String::new(),
            _ => {
                let password = read_mqtt_bytes(bytes)?;
                std::str::from_utf8(&password)?.to_owned()
            }
        };

        if username.is_empty() && password.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Login { username, password }))
        }
    }

    pub fn len(login: &Login) -> usize {
        let mut len = 0;

        if !login.username.is_empty() {
            len += 2 + login.username.len();
        }

        if !login.password.is_empty() {
            len += 2 + login.password.len();
        }

        len
    }

    pub fn write(login: &Login, buffer: &mut Vec<u8>) -> u8 {
        let mut connect_flags = 0;
        if !login.username.is_empty() {
            connect_flags |= 0x80;
            write_mqtt_string(buffer, &login.username);
        }

        if !login.password.is_empty() {
            connect_flags |= 0x40;
            write_mqtt_string(buffer, &login.password);
        }

        connect_flags
    }

    pub fn validate(login: &Login, username: &str, password: &str) -> bool {
        (login.username == *username) && (login.password == *password)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        fn generate_connect_properties()(
            session_expiry_interval: Option<u32>,
            receive_maximum: Option<u16>,
            max_packet_size: Option<u32>,
            topic_alias_max: Option<u16>,
            request_response_info: Option<u8>,
            request_problem_info: Option<u8>,
            user_properties: Vec<(String, String)>,
            authentication_method: Option<String>,
            authentication_data: Option<Vec<u8>>,

        ) -> ConnectProperties {
            ConnectProperties {
                session_expiry_interval,
                receive_maximum,
                max_packet_size,
                topic_alias_max,
                request_response_info,
                request_problem_info,
                user_properties,
                authentication_method,
                authentication_data,
            }
        }

    }

    prop_compose! {
        fn generate_login()(username in ".+", password in ".+") -> Login {
            Login { username, password }
        }
    }

    fn generate_qos() -> impl Strategy<Value = QoS> {
        prop_oneof![
            Just(QoS::AtMostOnce),
            Just(QoS::AtLeastOnce),
            Just(QoS::ExactlyOnce),
        ]
    }

    prop_compose! {
        fn generate_last_will()(
            topic: String,
            message: String,
            qos in generate_qos(),
            retain: bool,
        ) -> LastWill {
            LastWill {
                topic: topic.as_bytes().to_vec(),
                message: message.as_bytes().to_vec(),
                qos,
                retain,
            }
        }
    }

    prop_compose! {
        fn generate_connect_packet()(
            clean_session: bool,
            client_id: String,
            keep_alive: u16,
            properties in proptest::option::of(generate_connect_properties()),
            login in proptest::option::of(generate_login()),
            last_will in proptest::option::of(generate_last_will()),
        ) -> Connect {
            Connect {
                keep_alive,
                client_id,
                clean_session,
                last_will,
                login,
                properties,
            }
        }
    }

    proptest! {
        #[test]
        fn test_roundtrip(packet in generate_connect_packet()) {
            let mut buf = vec![];
            write(&packet, &mut buf).unwrap();
            let fixed_header = parse_fixed_header(&buf).unwrap();
            let parsed_packet = read(fixed_header, &buf).unwrap();

            prop_assert_eq!(parsed_packet, Connect {
                keep_alive: packet.keep_alive,
                clean_session: packet.clean_session,
                client_id: packet.client_id,
                last_will: packet.last_will,
                login: packet.login,
                properties: None,
            });
        }

        #[test]
        fn login_is_none_if_username_and_password_is_empty(
            client_id: String,
            clean_session: bool,
            last_will in proptest::option::of(generate_last_will()),
            properties in proptest::option::of(generate_connect_properties())
        ) {
            let packet_to_write = Connect {
                keep_alive: 0,
                client_id,
                clean_session,
                login: Some(Login {
                    username: "".into(),
                    password: "".into(),
                }),
                last_will,
                properties,
            };
            let mut buf = vec![];
            write(&packet_to_write, &mut buf).unwrap();
            let fixed_header = parse_fixed_header(&buf).unwrap();
            let parsed_packet = read(fixed_header, &buf).unwrap();

            prop_assert_eq!(parsed_packet, Connect {
                keep_alive: packet_to_write.keep_alive,
                clean_session: packet_to_write.clean_session,
                client_id: packet_to_write.client_id,
                last_will: packet_to_write.last_will,
                login: None,
                properties: None,
            });
        }
    }
}

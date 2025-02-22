use super::*;
use bytes::{Buf, Bytes};

fn len(connect: &Connect, login: &Option<Login>, will: &Option<LastWill>) -> usize {
    let mut len = 2 + "MQTT".len() // protocol name
                              + 1            // protocol version
                              + 1            // connect flags
                              + 2; // keep alive

    len += 2 + connect.client_id.len();

    // last will len
    if let Some(w) = will {
        len += will::len(w);
    }

    // username and password len
    if let Some(l) = login {
        len += login::len(l);
    }

    len
}

pub fn read(
    fixed_header: FixedHeader,
    mut bytes: Bytes,
) -> Result<(Connect, Option<Login>, Option<LastWill>), Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);

    // Variable header
    let protocol_name = read_mqtt_bytes(&mut bytes)?;
    let protocol_name = std::str::from_utf8(&protocol_name)?.to_owned();
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
    };

    Ok((connect, login, last_will))
}

pub fn write(
    connect: &Connect,
    login: &Option<Login>,
    will: &Option<LastWill>,
    buffer: &mut BytesMut,
) -> Result<usize, Error> {
    let len = len(connect, login, will);
    buffer.put_u8(0b0001_0000);
    let count = write_remaining_length(buffer, len)?;
    write_mqtt_string(buffer, "MQTT");

    buffer.put_u8(0x04);
    let flags_index = 1 + count + 2 + 4 + 1;

    let mut connect_flags = 0;
    if connect.clean_session {
        connect_flags |= 0x02;
    }

    buffer.put_u8(connect_flags);
    buffer.put_u16(connect.keep_alive);
    write_mqtt_string(buffer, &connect.client_id);

    if let Some(w) = &will {
        connect_flags |= will::write(w, buffer)?;
    }

    if let Some(l) = login {
        connect_flags |= login::write(l, buffer);
    }

    // update connect flags
    buffer[flags_index] = connect_flags;
    Ok(1 + count + len)
}

mod will {
    use super::*;

    pub fn len(will: &LastWill) -> usize {
        let mut len = 0;
        len += 2 + will.topic.len() + 2 + will.message.len();
        len
    }

    pub fn read(connect_flags: u8, mut bytes: &mut Bytes) -> Result<Option<LastWill>, Error> {
        let last_will = match connect_flags & 0b100 {
            0 if (connect_flags & 0b0011_1000) != 0 => {
                return Err(Error::IncorrectPacketFormat);
            }
            0 => None,
            _ => {
                let will_topic = read_mqtt_bytes(bytes)?;
                let will_message = read_mqtt_bytes(bytes)?;
                let qos_num = (connect_flags & 0b11000) >> 3;
                let will_qos = qos(qos_num).ok_or(Error::InvalidQoS(qos_num))?;
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

    pub fn write(will: &LastWill, buffer: &mut BytesMut) -> Result<u8, Error> {
        let mut connect_flags = 0;

        connect_flags |= 0x04 | ((will.qos as u8) << 3);
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

    pub fn read(connect_flags: u8, mut bytes: &mut Bytes) -> Result<Option<Login>, Error> {
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

    pub fn write(login: &Login, buffer: &mut BytesMut) -> u8 {
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

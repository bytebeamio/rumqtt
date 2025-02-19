use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

fn len(connack: &ConnAck, properties: &Option<ConnAckProperties>) -> usize {
    let mut len = 1  // session present
                + 1; // code

    if let Some(p) = properties {
        let properties_len = properties::len(p);
        let properties_len_len = len_len(properties_len);
        len += properties_len_len + properties_len;
    } else {
        len += 1;
    }

    len
}

pub fn read(
    fixed_header: FixedHeader,
    mut bytes: Bytes,
) -> Result<(ConnAck, Option<ConnAckProperties>), Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);

    let flags = read_u8(&mut bytes)?;
    let return_code = read_u8(&mut bytes)?;
    let properties = properties::read(&mut bytes)?;

    let session_present = (flags & 0x01) == 1;
    let code = connect_return(return_code)?;
    let connack = ConnAck {
        session_present,
        code,
    };

    Ok((connack, properties))
}

pub fn write(
    connack: &ConnAck,
    properties: &Option<ConnAckProperties>,
    buffer: &mut BytesMut,
) -> Result<usize, Error> {
    let len = len(connack, properties);
    buffer.put_u8(0x20);

    let count = write_remaining_length(buffer, len)?;
    buffer.put_u8(connack.session_present as u8);
    buffer.put_u8(connect_code(connack.code));

    if let Some(p) = properties {
        properties::write(p, buffer)?;
    } else {
        write_remaining_length(buffer, 0)?;
    }

    Ok(1 + count + len)
}

mod properties {
    use super::*;

    pub fn len(properties: &ConnAckProperties) -> usize {
        let mut len = 0;

        if properties.session_expiry_interval.is_some() {
            len += 1 + 4;
        }

        if properties.receive_max.is_some() {
            len += 1 + 2;
        }

        if properties.max_qos.is_some() {
            len += 1 + 1;
        }

        if properties.retain_available.is_some() {
            len += 1 + 1;
        }

        if properties.max_packet_size.is_some() {
            len += 1 + 4;
        }

        if let Some(id) = &properties.assigned_client_identifier {
            len += 1 + 2 + id.len();
        }

        if properties.topic_alias_max.is_some() {
            len += 1 + 2;
        }

        if let Some(reason) = &properties.reason_string {
            len += 1 + 2 + reason.len();
        }

        for (key, value) in properties.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        if properties.wildcard_subscription_available.is_some() {
            len += 1 + 1;
        }

        if properties.subscription_identifiers_available.is_some() {
            len += 1 + 1;
        }

        if properties.shared_subscription_available.is_some() {
            len += 1 + 1;
        }

        if properties.server_keep_alive.is_some() {
            len += 1 + 2;
        }

        if let Some(info) = &properties.response_information {
            len += 1 + 2 + info.len();
        }

        if let Some(reference) = &properties.server_reference {
            len += 1 + 2 + reference.len();
        }

        if let Some(authentication_method) = &properties.authentication_method {
            len += 1 + 2 + authentication_method.len();
        }

        if let Some(authentication_data) = &properties.authentication_data {
            len += 1 + 2 + authentication_data.len();
        }

        len
    }

    pub fn read(mut bytes: &mut Bytes) -> Result<Option<ConnAckProperties>, Error> {
        let mut session_expiry_interval = None;
        let mut receive_max = None;
        let mut max_qos = None;
        let mut retain_available = None;
        let mut max_packet_size = None;
        let mut assigned_client_identifier = None;
        let mut topic_alias_max = None;
        let mut reason_string = None;
        let mut user_properties = Vec::new();
        let mut wildcard_subscription_available = None;
        let mut subscription_identifiers_available = None;
        let mut shared_subscription_available = None;
        let mut server_keep_alive = None;
        let mut response_information = None;
        let mut server_reference = None;
        let mut authentication_method = None;
        let mut authentication_data = None;

        let (properties_len_len, properties_len) = length(bytes.iter())?;
        bytes.advance(properties_len_len);
        if properties_len == 0 {
            return Ok(None);
        }

        let mut cursor = 0;
        // read until cursor reaches property length. properties_len = 0 will skip this loop
        while cursor < properties_len {
            let prop = read_u8(bytes)?;
            cursor += 1;

            match property(prop)? {
                PropertyType::SessionExpiryInterval => {
                    session_expiry_interval = Some(read_u32(bytes)?);
                    cursor += 4;
                }
                PropertyType::ReceiveMaximum => {
                    receive_max = Some(read_u16(bytes)?);
                    cursor += 2;
                }
                PropertyType::MaximumQos => {
                    max_qos = Some(read_u8(bytes)?);
                    cursor += 1;
                }
                PropertyType::RetainAvailable => {
                    retain_available = Some(read_u8(bytes)?);
                    cursor += 1;
                }
                PropertyType::AssignedClientIdentifier => {
                    let id = read_mqtt_string(bytes)?;
                    cursor += 2 + id.len();
                    assigned_client_identifier = Some(id);
                }
                PropertyType::MaximumPacketSize => {
                    max_packet_size = Some(read_u32(bytes)?);
                    cursor += 4;
                }
                PropertyType::TopicAliasMaximum => {
                    topic_alias_max = Some(read_u16(bytes)?);
                    cursor += 2;
                }
                PropertyType::ReasonString => {
                    let reason = read_mqtt_string(bytes)?;
                    cursor += 2 + reason.len();
                    reason_string = Some(reason);
                }
                PropertyType::UserProperty => {
                    let key = read_mqtt_string(bytes)?;
                    let value = read_mqtt_string(bytes)?;
                    cursor += 2 + key.len() + 2 + value.len();
                    user_properties.push((key, value));
                }
                PropertyType::WildcardSubscriptionAvailable => {
                    wildcard_subscription_available = Some(read_u8(bytes)?);
                    cursor += 1;
                }
                PropertyType::SubscriptionIdentifierAvailable => {
                    subscription_identifiers_available = Some(read_u8(bytes)?);
                    cursor += 1;
                }
                PropertyType::SharedSubscriptionAvailable => {
                    shared_subscription_available = Some(read_u8(bytes)?);
                    cursor += 1;
                }
                PropertyType::ServerKeepAlive => {
                    server_keep_alive = Some(read_u16(bytes)?);
                    cursor += 2;
                }
                PropertyType::ResponseInformation => {
                    let info = read_mqtt_string(bytes)?;
                    cursor += 2 + info.len();
                    response_information = Some(info);
                }
                PropertyType::ServerReference => {
                    let reference = read_mqtt_string(bytes)?;
                    cursor += 2 + reference.len();
                    server_reference = Some(reference);
                }
                PropertyType::AuthenticationMethod => {
                    let method = read_mqtt_string(bytes)?;
                    cursor += 2 + method.len();
                    authentication_method = Some(method);
                }
                PropertyType::AuthenticationData => {
                    let data = read_mqtt_bytes(bytes)?;
                    cursor += 2 + data.len();
                    authentication_data = Some(data);
                }
                _ => return Err(Error::InvalidPropertyType(prop)),
            }
        }

        Ok(Some(ConnAckProperties {
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
        }))
    }

    pub fn write(properties: &ConnAckProperties, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = len(properties);
        write_remaining_length(buffer, len)?;

        if let Some(session_expiry_interval) = properties.session_expiry_interval {
            buffer.put_u8(PropertyType::SessionExpiryInterval as u8);
            buffer.put_u32(session_expiry_interval);
        }

        if let Some(receive_maximum) = properties.receive_max {
            buffer.put_u8(PropertyType::ReceiveMaximum as u8);
            buffer.put_u16(receive_maximum);
        }

        if let Some(qos) = properties.max_qos {
            buffer.put_u8(PropertyType::MaximumQos as u8);
            buffer.put_u8(qos);
        }

        if let Some(retain_available) = properties.retain_available {
            buffer.put_u8(PropertyType::RetainAvailable as u8);
            buffer.put_u8(retain_available);
        }

        if let Some(max_packet_size) = properties.max_packet_size {
            buffer.put_u8(PropertyType::MaximumPacketSize as u8);
            buffer.put_u32(max_packet_size);
        }

        if let Some(id) = &properties.assigned_client_identifier {
            buffer.put_u8(PropertyType::AssignedClientIdentifier as u8);
            write_mqtt_string(buffer, id);
        }

        if let Some(topic_alias_max) = properties.topic_alias_max {
            buffer.put_u8(PropertyType::TopicAliasMaximum as u8);
            buffer.put_u16(topic_alias_max);
        }

        if let Some(reason) = &properties.reason_string {
            buffer.put_u8(PropertyType::ReasonString as u8);
            write_mqtt_string(buffer, reason);
        }

        for (key, value) in properties.user_properties.iter() {
            buffer.put_u8(PropertyType::UserProperty as u8);
            write_mqtt_string(buffer, key);
            write_mqtt_string(buffer, value);
        }

        if let Some(w) = properties.wildcard_subscription_available {
            buffer.put_u8(PropertyType::WildcardSubscriptionAvailable as u8);
            buffer.put_u8(w);
        }

        if let Some(s) = properties.subscription_identifiers_available {
            buffer.put_u8(PropertyType::SubscriptionIdentifierAvailable as u8);
            buffer.put_u8(s);
        }

        if let Some(s) = properties.shared_subscription_available {
            buffer.put_u8(PropertyType::SharedSubscriptionAvailable as u8);
            buffer.put_u8(s);
        }

        if let Some(keep_alive) = properties.server_keep_alive {
            buffer.put_u8(PropertyType::ServerKeepAlive as u8);
            buffer.put_u16(keep_alive);
        }

        if let Some(info) = &properties.response_information {
            buffer.put_u8(PropertyType::ResponseInformation as u8);
            write_mqtt_string(buffer, info);
        }

        if let Some(reference) = &properties.server_reference {
            buffer.put_u8(PropertyType::ServerReference as u8);
            write_mqtt_string(buffer, reference);
        }

        if let Some(authentication_method) = &properties.authentication_method {
            buffer.put_u8(PropertyType::AuthenticationMethod as u8);
            write_mqtt_string(buffer, authentication_method);
        }

        if let Some(authentication_data) = &properties.authentication_data {
            buffer.put_u8(PropertyType::AuthenticationData as u8);
            write_mqtt_bytes(buffer, authentication_data);
        }

        Ok(())
    }
}

/// Connection return code type
fn connect_return(num: u8) -> Result<ConnectReturnCode, Error> {
    let code = match num {
        0 => ConnectReturnCode::Success,
        128 => ConnectReturnCode::UnspecifiedError,
        129 => ConnectReturnCode::MalformedPacket,
        130 => ConnectReturnCode::ProtocolError,
        131 => ConnectReturnCode::ImplementationSpecificError,
        132 => ConnectReturnCode::UnsupportedProtocolVersion,
        133 => ConnectReturnCode::ClientIdentifierNotValid,
        134 => ConnectReturnCode::BadUserNamePassword,
        135 => ConnectReturnCode::NotAuthorized,
        136 => ConnectReturnCode::ServerUnavailable,
        137 => ConnectReturnCode::ServerBusy,
        138 => ConnectReturnCode::Banned,
        140 => ConnectReturnCode::BadAuthenticationMethod,
        144 => ConnectReturnCode::TopicNameInvalid,
        149 => ConnectReturnCode::PacketTooLarge,
        151 => ConnectReturnCode::QuotaExceeded,
        153 => ConnectReturnCode::PayloadFormatInvalid,
        154 => ConnectReturnCode::RetainNotSupported,
        155 => ConnectReturnCode::QoSNotSupported,
        156 => ConnectReturnCode::UseAnotherServer,
        157 => ConnectReturnCode::ServerMoved,
        159 => ConnectReturnCode::ConnectionRateExceeded,
        num => return Err(Error::InvalidConnectReturnCode(num)),
    };

    Ok(code)
}

fn connect_code(return_code: ConnectReturnCode) -> u8 {
    match return_code {
        ConnectReturnCode::Success => 0,
        ConnectReturnCode::UnspecifiedError => 128,
        ConnectReturnCode::MalformedPacket => 129,
        ConnectReturnCode::ProtocolError => 130,
        ConnectReturnCode::ImplementationSpecificError => 131,
        ConnectReturnCode::UnsupportedProtocolVersion => 132,
        ConnectReturnCode::ClientIdentifierNotValid => 133,
        ConnectReturnCode::BadUserNamePassword => 134,
        ConnectReturnCode::NotAuthorized => 135,
        ConnectReturnCode::ServerUnavailable => 136,
        ConnectReturnCode::ServerBusy => 137,
        ConnectReturnCode::Banned => 138,
        ConnectReturnCode::BadAuthenticationMethod => 140,
        ConnectReturnCode::TopicNameInvalid => 144,
        ConnectReturnCode::PacketTooLarge => 149,
        ConnectReturnCode::QuotaExceeded => 151,
        ConnectReturnCode::PayloadFormatInvalid => 153,
        ConnectReturnCode::RetainNotSupported => 154,
        ConnectReturnCode::QoSNotSupported => 155,
        ConnectReturnCode::UseAnotherServer => 156,
        ConnectReturnCode::ServerMoved => 157,
        ConnectReturnCode::ConnectionRateExceeded => 159,
        _ => unreachable!(),
    }
}

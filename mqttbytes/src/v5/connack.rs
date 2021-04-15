use super::*;
use alloc::string::String;
use alloc::vec::Vec;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Return code in connack
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum ConnectReturnCode {
    Success = 0,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    UnsupportedProtocolVersion = 132,
    ClientIdentifierNotValid = 133,
    BadUserNamePassword = 134,
    NotAuthorized = 135,
    ServerUnavailable = 136,
    ServerBusy = 137,
    Banned = 138,
    BadAuthenticationMethod = 140,
    TopicNameInvalid = 144,
    PacketTooLarge = 149,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
    RetainNotSupported = 154,
    QoSNotSupported = 155,
    UseAnotherServer = 156,
    ServerMoved = 157,
    ConnectionRateExceeded = 159,
}

/// Acknowledgement to connect packet
#[derive(Debug, Clone, PartialEq)]
pub struct ConnAck {
    pub session_present: bool,
    pub code: ConnectReturnCode,
    pub properties: Option<ConnAckProperties>,
}

impl ConnAck {
    pub fn new(code: ConnectReturnCode, session_present: bool) -> ConnAck {
        ConnAck {
            code,
            session_present,
            properties: None,
        }
    }

    fn len(&self) -> usize {
        let mut len = 1  // session present
                        + 1; // code

        if let Some(properties) = &self.properties {
            let properties_len = properties.len();
            let properties_len_len = len_len(properties_len);
            len += properties_len_len + properties_len;
        }

        len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);

        let flags = read_u8(&mut bytes)?;
        let return_code = read_u8(&mut bytes)?;

        let session_present = (flags & 0x01) == 1;
        let code = connect_return(return_code)?;
        let connack = ConnAck {
            session_present,
            code,
            properties: ConnAckProperties::extract(&mut bytes)?,
        };

        Ok(connack)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = self.len();
        buffer.put_u8(0x20);

        let count = write_remaining_length(buffer, len)?;
        buffer.put_u8(self.session_present as u8);
        buffer.put_u8(self.code as u8);

        if let Some(properties) = &self.properties {
            properties.write(buffer)?;
        }

        Ok(1 + count + len)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnAckProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_max: Option<u16>,
    pub max_qos: Option<u8>,
    pub retain_available: Option<u8>,
    pub max_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub topic_alias_max: Option<u16>,
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
    pub wildcard_subscription_available: Option<u8>,
    pub subscription_identifiers_available: Option<u8>,
    pub shared_subscription_available: Option<u8>,
    pub server_keep_alive: Option<u16>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Bytes>,
}

impl ConnAckProperties {
    pub fn new() -> ConnAckProperties {
        ConnAckProperties {
            session_expiry_interval: None,
            receive_max: None,
            max_qos: None,
            retain_available: None,
            max_packet_size: None,
            assigned_client_identifier: None,
            topic_alias_max: None,
            reason_string: None,
            user_properties: Vec::new(),
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
            authentication_method: None,
            authentication_data: None,
        }
    }

    pub fn len(&self) -> usize {
        let mut len = 0;

        if let Some(_) = &self.session_expiry_interval {
            len += 1 + 4;
        }

        if let Some(_) = &self.receive_max {
            len += 1 + 2;
        }

        if let Some(_) = &self.max_qos {
            len += 1 + 1;
        }

        if let Some(_) = &self.retain_available {
            len += 1 + 1;
        }

        if let Some(_) = &self.max_packet_size {
            len += 1 + 4;
        }

        if let Some(id) = &self.assigned_client_identifier {
            len += 1 + 2 + id.len();
        }

        if let Some(_) = &self.topic_alias_max {
            len += 1 + 2;
        }

        if let Some(reason) = &self.reason_string {
            len += 1 + 2 + reason.len();
        }

        for (key, value) in self.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        if let Some(_) = &self.wildcard_subscription_available {
            len += 1 + 1;
        }

        if let Some(_) = &self.subscription_identifiers_available {
            len += 1 + 1;
        }

        if let Some(_) = &self.shared_subscription_available {
            len += 1 + 1;
        }

        if let Some(_) = &self.server_keep_alive {
            len += 1 + 2;
        }

        if let Some(info) = &self.response_information {
            len += 1 + 2 + info.len();
        }

        if let Some(reference) = &self.server_reference {
            len += 1 + 2 + reference.len();
        }

        if let Some(authentication_method) = &self.authentication_method {
            len += 1 + 2 + authentication_method.len();
        }

        if let Some(authentication_data) = &self.authentication_data {
            len += 1 + 2 + authentication_data.len();
        }

        len
    }

    pub fn extract(mut bytes: &mut Bytes) -> Result<Option<ConnAckProperties>, Error> {
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
            let prop = read_u8(&mut bytes)?;
            cursor += 1;

            match property(prop)? {
                PropertyType::SessionExpiryInterval => {
                    session_expiry_interval = Some(read_u32(&mut bytes)?);
                    cursor += 4;
                }
                PropertyType::ReceiveMaximum => {
                    receive_max = Some(read_u16(&mut bytes)?);
                    cursor += 2;
                }
                PropertyType::MaximumQos => {
                    max_qos = Some(read_u8(&mut bytes)?);
                    cursor += 1;
                }
                PropertyType::RetainAvailable => {
                    retain_available = Some(read_u8(&mut bytes)?);
                    cursor += 1;
                }
                PropertyType::AssignedClientIdentifier => {
                    let id = read_mqtt_string(&mut bytes)?;
                    cursor += 2 + id.len();
                    assigned_client_identifier = Some(id);
                }
                PropertyType::MaximumPacketSize => {
                    max_packet_size = Some(read_u32(&mut bytes)?);
                    cursor += 4;
                }
                PropertyType::TopicAliasMaximum => {
                    topic_alias_max = Some(read_u16(&mut bytes)?);
                    cursor += 2;
                }
                PropertyType::ReasonString => {
                    let reason = read_mqtt_string(&mut bytes)?;
                    cursor += 2 + reason.len();
                    reason_string = Some(reason);
                }
                PropertyType::UserProperty => {
                    let key = read_mqtt_string(&mut bytes)?;
                    let value = read_mqtt_string(&mut bytes)?;
                    cursor += 2 + key.len() + 2 + value.len();
                    user_properties.push((key, value));
                }
                PropertyType::WildcardSubscriptionAvailable => {
                    wildcard_subscription_available = Some(read_u8(&mut bytes)?);
                    cursor += 1;
                }
                PropertyType::SubscriptionIdentifierAvailable => {
                    subscription_identifiers_available = Some(read_u8(&mut bytes)?);
                    cursor += 1;
                }
                PropertyType::SharedSubscriptionAvailable => {
                    shared_subscription_available = Some(read_u8(&mut bytes)?);
                    cursor += 1;
                }
                PropertyType::ServerKeepAlive => {
                    server_keep_alive = Some(read_u16(&mut bytes)?);
                    cursor += 2;
                }
                PropertyType::ResponseInformation => {
                    let info = read_mqtt_string(&mut bytes)?;
                    cursor += 2 + info.len();
                    response_information = Some(info);
                }
                PropertyType::ServerReference => {
                    let reference = read_mqtt_string(&mut bytes)?;
                    cursor += 2 + reference.len();
                    server_reference = Some(reference);
                }
                PropertyType::AuthenticationMethod => {
                    let method = read_mqtt_string(&mut bytes)?;
                    cursor += 2 + method.len();
                    authentication_method = Some(method);
                }
                PropertyType::AuthenticationData => {
                    let data = read_mqtt_bytes(&mut bytes)?;
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

    fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = self.len();
        write_remaining_length(buffer, len)?;

        if let Some(session_expiry_interval) = self.session_expiry_interval {
            buffer.put_u8(PropertyType::SessionExpiryInterval as u8);
            buffer.put_u32(session_expiry_interval);
        }

        if let Some(receive_maximum) = self.receive_max {
            buffer.put_u8(PropertyType::ReceiveMaximum as u8);
            buffer.put_u16(receive_maximum);
        }

        if let Some(qos) = self.max_qos {
            buffer.put_u8(PropertyType::MaximumQos as u8);
            buffer.put_u8(qos);
        }

        if let Some(retain_available) = self.retain_available {
            buffer.put_u8(PropertyType::RetainAvailable as u8);
            buffer.put_u8(retain_available);
        }

        if let Some(max_packet_size) = self.max_packet_size {
            buffer.put_u8(PropertyType::MaximumPacketSize as u8);
            buffer.put_u32(max_packet_size);
        }

        if let Some(id) = &self.assigned_client_identifier {
            buffer.put_u8(PropertyType::AssignedClientIdentifier as u8);
            write_mqtt_string(buffer, id);
        }

        if let Some(topic_alias_max) = self.topic_alias_max {
            buffer.put_u8(PropertyType::TopicAliasMaximum as u8);
            buffer.put_u16(topic_alias_max);
        }

        if let Some(reason) = &self.reason_string {
            buffer.put_u8(PropertyType::ReasonString as u8);
            write_mqtt_string(buffer, reason);
        }

        for (key, value) in self.user_properties.iter() {
            buffer.put_u8(PropertyType::UserProperty as u8);
            write_mqtt_string(buffer, key);
            write_mqtt_string(buffer, value);
        }

        if let Some(w) = self.wildcard_subscription_available {
            buffer.put_u8(PropertyType::WildcardSubscriptionAvailable as u8);
            buffer.put_u8(w);
        }

        if let Some(s) = self.subscription_identifiers_available {
            buffer.put_u8(PropertyType::SubscriptionIdentifierAvailable as u8);
            buffer.put_u8(s);
        }

        if let Some(s) = self.shared_subscription_available {
            buffer.put_u8(PropertyType::SharedSubscriptionAvailable as u8);
            buffer.put_u8(s);
        }

        if let Some(keep_alive) = self.server_keep_alive {
            buffer.put_u8(PropertyType::ServerKeepAlive as u8);
            buffer.put_u16(keep_alive);
        }

        if let Some(info) = &self.response_information {
            buffer.put_u8(PropertyType::ResponseInformation as u8);
            write_mqtt_string(buffer, info);
        }

        if let Some(reference) = &self.server_reference {
            buffer.put_u8(PropertyType::ServerReference as u8);
            write_mqtt_string(buffer, reference);
        }

        if let Some(authentication_method) = &self.authentication_method {
            buffer.put_u8(PropertyType::AuthenticationMethod as u8);
            write_mqtt_string(buffer, authentication_method);
        }

        if let Some(authentication_data) = &self.authentication_data {
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

#[cfg(test)]
mod test {
    use super::*;
    use alloc::vec;
    use bytes::{Bytes, BytesMut};
    use pretty_assertions::assert_eq;

    fn sample() -> ConnAck {
        let properties = ConnAckProperties {
            session_expiry_interval: Some(1234),
            receive_max: Some(432),
            max_qos: Some(2),
            retain_available: Some(1),
            max_packet_size: Some(100),
            assigned_client_identifier: Some("test".to_owned()),
            topic_alias_max: Some(456),
            reason_string: Some("test".to_owned()),
            user_properties: vec![("test".to_owned(), "test".to_owned())],
            wildcard_subscription_available: Some(1),
            subscription_identifiers_available: Some(1),
            shared_subscription_available: Some(0),
            server_keep_alive: Some(1234),
            response_information: Some("test".to_owned()),
            server_reference: Some("test".to_owned()),
            authentication_method: Some("test".to_owned()),
            authentication_data: Some(Bytes::from(vec![1, 2, 3, 4])),
        };

        ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success,
            properties: Some(properties),
        }
    }

    fn sample_bytes() -> Vec<u8> {
        vec![
            0x20, // Packet type
            0x57, // Remaining length
            0x00, 0x00, // Session, code
            0x54, // Properties length
            0x11, 0x00, 0x00, 0x04, 0xd2, // Session expiry interval
            0x21, 0x01, 0xb0, // Receive maximum
            0x24, 0x02, // Maximum qos
            0x25, 0x01, // Retain available
            0x27, 0x00, 0x00, 0x00, 0x64, // Maximum packet size
            0x12, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // Assigned client identifier
            0x22, 0x01, 0xc8, // Topic alias max
            0x1f, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // Reason string
            0x26, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x04, 0x74, 0x65, 0x73,
            0x74, // user properties
            0x28, 0x01, // wildcard_subscription_available
            0x29, 0x01, // subscription_identifiers_available
            0x2a, 0x00, // shared_subscription_available
            0x13, 0x04, 0xd2, // server keep_alive
            0x1a, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // response_information
            0x1c, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // server reference
            0x15, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // authentication method
            0x16, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04, // authentication data
        ]
    }

    #[test]
    fn connack_parsing_works() {
        let mut stream = bytes::BytesMut::new();
        let packetstream = &sample_bytes();
        stream.extend_from_slice(&packetstream[..]);

        let fixed_header = parse_fixed_header(stream.iter()).unwrap();
        let connack_bytes = stream.split_to(fixed_header.frame_length()).freeze();
        let connack = ConnAck::read(fixed_header, connack_bytes).unwrap();

        assert_eq!(connack, sample());
    }

    #[test]
    fn connack_encoding_works() {
        let connack = sample();
        let mut buf = BytesMut::new();
        connack.write(&mut buf).unwrap();
        assert_eq!(&buf[..], sample_bytes());
    }
}

#![allow(dead_code)]

use std::fmt;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::*;

pub(crate) mod connect {
    use super::*;
    use bytes::Bytes;

    /// Connection packet initiated by the client
    #[derive(Debug, Clone, PartialEq)]
    pub struct Connect {
        /// Mqtt keep alive time
        pub keep_alive: u16,
        /// Client Id
        pub client_id: String,
        /// Clean session. Asks the broker to clear previous state
        pub clean_session: bool,
        /// Will that broker needs to publish when the client disconnects
        pub last_will: Option<LastWill>,
        /// Login credentials
        pub login: Option<Login>,
        /// Properties
        pub properties: Option<ConnectProperties>,
    }

    impl Connect {
        pub fn new<S: Into<String>>(id: S) -> Connect {
            Connect {
                keep_alive: 10,
                client_id: id.into(),
                clean_session: true,
                last_will: None,
                login: None,
                properties: None,
            }
        }

        pub fn len(&self) -> usize {
            let mut len = 2 + "MQTT".len() // protocol name
                              + 1            // protocol version
                              + 1            // connect flags
                              + 2; // keep alive

            len += 2 + self.client_id.len();

            // last will len
            if let Some(last_will) = &self.last_will {
                len += last_will.len();
            }

            // username and password len
            if let Some(login) = &self.login {
                len += login.len();
            }

            len
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Connect, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);

            // Variable header
            let protocol_name = read_mqtt_bytes(&mut bytes)?;
            let protocol_name = std::str::from_utf8(&protocol_name)?.to_owned();
            if protocol_name != "MQTT" {
                return Err(Error::InvalidProtocol);
            }

            let protocol_level = read_u8(&mut bytes)?;
            if protocol_level != 5 {
                return Err(Error::InvalidProtocolLevel(protocol_level));
            }

            connect_v5_part(bytes)
        }
    }

    pub(crate) fn connect_v5_part(mut bytes: Bytes) -> Result<Connect, Error> {
        let connect_flags = read_u8(&mut bytes)?;
        let clean_session = (connect_flags & 0b10) != 0;
        let keep_alive = read_u16(&mut bytes)?;

        let properties = ConnectProperties::read(&mut bytes)?;

        // Payload
        let client_id = read_mqtt_bytes(&mut bytes)?;
        let client_id = std::str::from_utf8(&client_id)?.to_owned();
        let last_will = LastWill::read(connect_flags, &mut bytes)?;
        let login = Login::read(connect_flags, &mut bytes)?;

        let connect = Connect {
            keep_alive,
            client_id,
            clean_session,
            last_will,
            login,
            properties,
        };

        Ok(connect)
    }

    /// LastWill that broker forwards on behalf of the client
    #[derive(Debug, Clone, PartialEq)]
    pub struct LastWill {
        pub topic: String,
        pub message: Bytes,
        pub qos: QoS,
        pub retain: bool,
    }

    impl LastWill {
        pub fn _new(
            topic: impl Into<String>,
            payload: impl Into<Vec<u8>>,
            qos: QoS,
            retain: bool,
        ) -> LastWill {
            LastWill {
                topic: topic.into(),
                message: Bytes::from(payload.into()),
                qos,
                retain,
            }
        }

        fn len(&self) -> usize {
            let mut len = 0;
            len += 2 + self.topic.len() + 2 + self.message.len();
            len
        }

        fn read(connect_flags: u8, bytes: &mut Bytes) -> Result<Option<LastWill>, Error> {
            let last_will = match connect_flags & 0b100 {
                0 if (connect_flags & 0b0011_1000) != 0 => {
                    return Err(Error::IncorrectPacketFormat);
                }
                0 => None,
                _ => {
                    let will_topic = read_mqtt_bytes(bytes)?;
                    let will_topic = std::str::from_utf8(&will_topic)?.to_owned();
                    let will_message = read_mqtt_bytes(bytes)?;
                    let will_qos = qos((connect_flags & 0b11000) >> 3)?;
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
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct Login {
        username: String,
        password: String,
    }

    impl Login {
        pub fn new<S: Into<String>>(u: S, p: S) -> Login {
            Login {
                username: u.into(),
                password: p.into(),
            }
        }

        fn read(connect_flags: u8, bytes: &mut Bytes) -> Result<Option<Login>, Error> {
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

        fn len(&self) -> usize {
            let mut len = 0;

            if !self.username.is_empty() {
                len += 2 + self.username.len();
            }

            if !self.password.is_empty() {
                len += 2 + self.password.len();
            }

            len
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct ConnectProperties {
        /// Expiry interval property after loosing connection
        pub session_expiry_interval: Option<u32>,
        /// Maximum simultaneous packets
        pub receive_maximum: Option<u16>,
        /// Maximum packet size
        pub max_packet_size: Option<u32>,
        /// Maximum mapping integer for a topic
        pub topic_alias_max: Option<u16>,
        pub request_response_info: Option<u8>,
        pub request_problem_info: Option<u8>,
        /// List of user properties
        pub user_properties: Vec<(String, String)>,
        /// Method of authentication
        pub authentication_method: Option<String>,
        /// Authentication data
        pub authentication_data: Option<Bytes>,
    }

    impl ConnectProperties {
        fn _new() -> ConnectProperties {
            ConnectProperties {
                session_expiry_interval: None,
                receive_maximum: None,
                max_packet_size: None,
                topic_alias_max: None,
                request_response_info: None,
                request_problem_info: None,
                user_properties: Vec::new(),
                authentication_method: None,
                authentication_data: None,
            }
        }

        fn read(bytes: &mut Bytes) -> Result<Option<ConnectProperties>, Error> {
            let mut session_expiry_interval = None;
            let mut receive_maximum = None;
            let mut max_packet_size = None;
            let mut topic_alias_max = None;
            let mut request_response_info = None;
            let mut request_problem_info = None;
            let mut user_properties = Vec::new();
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
                        receive_maximum = Some(read_u16(bytes)?);
                        cursor += 2;
                    }
                    PropertyType::MaximumPacketSize => {
                        max_packet_size = Some(read_u32(bytes)?);
                        cursor += 4;
                    }
                    PropertyType::TopicAliasMaximum => {
                        topic_alias_max = Some(read_u16(bytes)?);
                        cursor += 2;
                    }
                    PropertyType::RequestResponseInformation => {
                        request_response_info = Some(read_u8(bytes)?);
                        cursor += 1;
                    }
                    PropertyType::RequestProblemInformation => {
                        request_problem_info = Some(read_u8(bytes)?);
                        cursor += 1;
                    }
                    PropertyType::UserProperty => {
                        let key = read_mqtt_bytes(bytes)?;
                        let key = std::str::from_utf8(&key)?.to_owned();
                        let value = read_mqtt_bytes(bytes)?;
                        let value = std::str::from_utf8(&value)?.to_owned();
                        cursor += 2 + key.len() + 2 + value.len();
                        user_properties.push((key, value));
                    }
                    PropertyType::AuthenticationMethod => {
                        let method = read_mqtt_bytes(bytes)?;
                        let method = std::str::from_utf8(&method)?.to_owned();
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

            Ok(Some(ConnectProperties {
                session_expiry_interval,
                receive_maximum,
                max_packet_size,
                topic_alias_max,
                request_response_info,
                request_problem_info,
                user_properties,
                authentication_method,
                authentication_data,
            }))
        }

        fn len(&self) -> usize {
            let mut len = 0;

            if self.session_expiry_interval.is_some() {
                len += 1 + 4;
            }

            if self.receive_maximum.is_some() {
                len += 1 + 2;
            }

            if self.max_packet_size.is_some() {
                len += 1 + 4;
            }

            if self.topic_alias_max.is_some() {
                len += 1 + 2;
            }

            if self.request_response_info.is_some() {
                len += 1 + 1;
            }

            if self.request_problem_info.is_some() {
                len += 1 + 1;
            }

            for (key, value) in self.user_properties.iter() {
                len += 1 + 2 + key.len() + 2 + value.len();
            }

            if let Some(authentication_method) = &self.authentication_method {
                len += 1 + 2 + authentication_method.len();
            }

            if let Some(authentication_data) = &self.authentication_data {
                len += 1 + 2 + authentication_data.len();
            }

            len
        }
    }
}

pub(crate) mod connack {
    use super::*;
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    /// Return code in connack
    #[derive(Debug, Clone, Copy, PartialEq)]
    #[repr(u8)]
    pub enum ConnectReturnCode {
        Success = 0x00,
        UnspecifiedError = 0x80,
        MalformedPacket = 0x81,
        ProtocolError = 0x82,
        ImplementationSpecificError = 0x83,
        UnsupportedProtocolVersion = 0x84,
        ClientIdentifierNotValid = 0x85,
        BadUserNamePassword = 0x86,
        NotAuthorized = 0x87,
        ServerUnavailable = 0x88,
        ServerBusy = 0x89,
        Banned = 0x8a,
        BadAuthenticationMethod = 0x8c,
        TopicNameInvalid = 0x90,
        PacketTooLarge = 0x95,
        QuotaExceeded = 0x97,
        PayloadFormatInvalid = 0x99,
        RetainNotSupported = 0x9a,
        QoSNotSupported = 0x9b,
        UseAnotherServer = 0x9c,
        ServerMoved = 0x9d,
        ConnectionRateExceeded = 0x94,
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

        pub fn len(&self) -> usize {
            let mut len = 1  // session present
                        + 1; // code

            if let Some(properties) = &self.properties {
                let properties_len = properties.len();
                let properties_len_len = len_len(properties_len);
                len += properties_len_len + properties_len;
            } else {
                // 1 byte for 0 len
                len += 1;
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
    }

    pub fn write(
        code: ConnectReturnCode,
        session_present: bool,
        properties: Option<ConnAckProperties>,
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        // TODO: maybe we can remove double checking if properties == None ?

        let mut len = 1  // session present
                        + 1; // code
        if let Some(ref properties) = properties {
            let properties_len = properties.len();
            let properties_len_len = len_len(properties_len);
            len += properties_len_len + properties_len;
        } else {
            // 1 byte for 0 len
            len += 1;
        }

        buffer.put_u8(0x20);

        let count = write_remaining_length(buffer, len)?;

        buffer.put_u8(session_present as u8);
        buffer.put_u8(code as u8);

        if let Some(properties) = properties {
            properties.write(buffer)?;
        } else {
            // 1 byte for 0 len
            buffer.put_u8(0);
        }

        Ok(1 + count + len)
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

            if self.session_expiry_interval.is_some() {
                len += 1 + 4;
            }

            if self.receive_max.is_some() {
                len += 1 + 2;
            }

            if self.max_qos.is_some() {
                len += 1 + 1;
            }

            if self.retain_available.is_some() {
                len += 1 + 1;
            }

            if self.max_packet_size.is_some() {
                len += 1 + 4;
            }

            if let Some(id) = &self.assigned_client_identifier {
                len += 1 + 2 + id.len();
            }

            if self.topic_alias_max.is_some() {
                len += 1 + 2;
            }

            if let Some(reason) = &self.reason_string {
                len += 1 + 2 + reason.len();
            }

            for (key, value) in self.user_properties.iter() {
                len += 1 + 2 + key.len() + 2 + value.len();
            }

            if self.wildcard_subscription_available.is_some() {
                len += 1 + 1;
            }

            if self.subscription_identifiers_available.is_some() {
                len += 1 + 1;
            }

            if self.shared_subscription_available.is_some() {
                len += 1 + 1;
            }

            if self.server_keep_alive.is_some() {
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

        pub fn extract(bytes: &mut Bytes) -> Result<Option<ConnAckProperties>, Error> {
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
                        let bytes = read_mqtt_bytes(bytes)?;
                        let id = std::str::from_utf8(&bytes)?.to_owned();
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
                        let reason = read_mqtt_bytes(bytes)?;
                        let reason = std::str::from_utf8(&reason)?.to_owned();
                        cursor += 2 + reason.len();
                        reason_string = Some(reason);
                    }
                    PropertyType::UserProperty => {
                        let key = read_mqtt_bytes(bytes)?;
                        let key = std::str::from_utf8(&key)?.to_owned();
                        let value = read_mqtt_bytes(bytes)?;
                        let value = std::str::from_utf8(&value)?.to_owned();
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
                        let info = read_mqtt_bytes(bytes)?;
                        let info = std::str::from_utf8(&info)?.to_owned();
                        cursor += 2 + info.len();
                        response_information = Some(info);
                    }
                    PropertyType::ServerReference => {
                        let bytes = read_mqtt_bytes(bytes)?;
                        let reference = std::str::from_utf8(&bytes)?.to_owned();
                        cursor += 2 + reference.len();
                        server_reference = Some(reference);
                    }
                    PropertyType::AuthenticationMethod => {
                        let bytes = read_mqtt_bytes(bytes)?;
                        let method = std::str::from_utf8(&bytes)?.to_owned();
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
        match num {
            0x00 => Ok(ConnectReturnCode::Success),
            0x80 => Ok(ConnectReturnCode::UnspecifiedError),
            0x81 => Ok(ConnectReturnCode::MalformedPacket),
            0x82 => Ok(ConnectReturnCode::ProtocolError),
            0x83 => Ok(ConnectReturnCode::ImplementationSpecificError),
            0x84 => Ok(ConnectReturnCode::UnsupportedProtocolVersion),
            0x85 => Ok(ConnectReturnCode::ClientIdentifierNotValid),
            0x86 => Ok(ConnectReturnCode::BadUserNamePassword),
            0x87 => Ok(ConnectReturnCode::NotAuthorized),
            0x88 => Ok(ConnectReturnCode::ServerUnavailable),
            0x89 => Ok(ConnectReturnCode::ServerBusy),
            0x8a => Ok(ConnectReturnCode::Banned),
            0x8c => Ok(ConnectReturnCode::BadAuthenticationMethod),
            0x90 => Ok(ConnectReturnCode::TopicNameInvalid),
            0x95 => Ok(ConnectReturnCode::PacketTooLarge),
            0x97 => Ok(ConnectReturnCode::QuotaExceeded),
            0x99 => Ok(ConnectReturnCode::PayloadFormatInvalid),
            0x9a => Ok(ConnectReturnCode::RetainNotSupported),
            0x9b => Ok(ConnectReturnCode::QoSNotSupported),
            0x9c => Ok(ConnectReturnCode::UseAnotherServer),
            0x9d => Ok(ConnectReturnCode::ServerMoved),
            0x94 => Ok(ConnectReturnCode::ConnectionRateExceeded),
            num => Err(Error::InvalidConnectReturnCode(num)),
        }
    }
}

pub(crate) mod publish {
    use super::*;
    use bytes::{BufMut, Bytes, BytesMut};

    #[derive(Debug, Clone, PartialEq)]
    pub struct Publish {
        pub fixed_header: FixedHeader,
        pub raw: Bytes,
    }

    impl Publish {
        //         pub fn new<S: Into<String>, P: Into<Vec<u8>>>(topic: S, qos: QoS, payload: P) -> Publish {
        //             Publish {
        //                 dup: false,
        //                 qos,
        //                 retain: false,
        //                 pkid: 0,
        //                 topic: topic.into(),
        //                 payload: Bytes::from(payload.into()),
        //             }
        //         }

        //         pub fn from_bytes<S: Into<String>>(topic: S, qos: QoS, payload: Bytes) -> Publish {
        //             Publish {
        //                 dup: false,
        //                 qos,
        //                 retain: false,
        //                 pkid: 0,
        //                 topic: topic.into(),
        //                 payload,
        //             }
        //         }

        //         pub fn len(&self) -> usize {
        //             let mut len = 2 + self.topic.len();
        //             if self.qos != QoS::AtMostOnce && self.pkid != 0 {
        //                 len += 2;
        //             }

        //             len += self.payload.len();
        //             len
        //         }

        pub fn view_meta(&self) -> Result<(&str, u8, u16, bool, bool), Error> {
            let qos = (self.fixed_header.byte1 & 0b0110) >> 1;
            let dup = (self.fixed_header.byte1 & 0b1000) != 0;
            let retain = (self.fixed_header.byte1 & 0b0001) != 0;

            // FIXME: Remove indexes and use get method
            let stream = &self.raw[self.fixed_header.fixed_header_len..];
            let topic_len = view_u16(stream)? as usize;

            let stream = &stream[2..];
            let topic = view_str(stream, topic_len)?;

            let pkid = match qos {
                0 => 0,
                1 => {
                    let stream = &stream[topic_len..];
                    view_u16(stream)?
                }
                v => return Err(Error::InvalidQoS(v)),
            };

            if qos == 1 && pkid == 0 {
                return Err(Error::PacketIdZero);
            }

            Ok((topic, qos, pkid, dup, retain))
        }

        pub fn view_topic(&self) -> Result<&str, Error> {
            // FIXME: Remove indexes
            let stream = &self.raw[self.fixed_header.fixed_header_len..];
            let topic_len = view_u16(stream)? as usize;

            let stream = &stream[2..];
            let topic = view_str(stream, topic_len)?;
            Ok(topic)
        }

        pub fn take_topic_and_payload(mut self) -> Result<(Bytes, Bytes), Error> {
            let qos = (self.fixed_header.byte1 & 0b0110) >> 1;

            let variable_header_index = self.fixed_header.fixed_header_len;
            self.raw.advance(variable_header_index);
            let topic = read_mqtt_bytes(&mut self.raw)?;

            match qos {
                0 => (),
                1 => self.raw.advance(2),
                v => return Err(Error::InvalidQoS(v)),
            };

            let payload = self.raw;
            Ok((topic, payload))
        }

        pub fn read(fixed_header: FixedHeader, bytes: Bytes) -> Result<Self, Error> {
            let publish = Publish {
                fixed_header,
                raw: bytes,
            };

            Ok(publish)
        }
    }

    pub struct PublishBytes(pub Bytes);

    impl From<PublishBytes> for Result<Publish, Error> {
        fn from(raw: PublishBytes) -> Self {
            let fixed_header = check(raw.0.iter(), 100 * 1024 * 1024)?;
            Ok(Publish {
                fixed_header,
                raw: raw.0,
            })
        }
    }

    pub fn write(
        topic: &str,
        qos: QoS,
        pkid: u16,
        dup: bool,
        retain: bool,
        payload: &[u8],
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        let mut len = 2 + topic.len();
        if qos != QoS::AtMostOnce {
            len += 2;
        }

        len += payload.len();

        let dup = dup as u8;
        let qos = qos as u8;
        let retain = retain as u8;

        buffer.put_u8(0b0011_0000 | retain | qos << 1 | dup << 3);

        let count = write_remaining_length(buffer, len)?;
        write_mqtt_string(buffer, topic);

        if qos != 0 {
            if pkid == 0 {
                return Err(Error::PacketIdZero);
            }

            buffer.put_u16(pkid);
        }

        buffer.extend_from_slice(payload);

        // TODO: Returned length is wrong in other packets. Fix it
        Ok(1 + count + len)
    }
}

pub(crate) mod puback {
    use super::*;
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    /// Acknowledgement to QoS1 publish
    #[derive(Debug, Clone, PartialEq)]
    pub struct PubAck {
        pub pkid: u16,
        pub reason: PubAckReason,
        pub properties: Option<PubAckProperties>,
    }

    impl PubAck {
        pub fn new(pkid: u16) -> PubAck {
            PubAck {
                pkid,
                reason: PubAckReason::Success,
                properties: None,
            }
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);
            let pkid = read_u16(&mut bytes)?;

            // No reason code or properties if remaining length == 2
            if fixed_header.remaining_len == 2 {
                return Ok(PubAck {
                    pkid,
                    reason: PubAckReason::Success,
                    properties: None,
                });
            }

            // No properties len or properties if remaining len > 2 but < 4
            let ack_reason = read_u8(&mut bytes)?;
            if fixed_header.remaining_len < 4 {
                return Ok(PubAck {
                    pkid,
                    reason: reason(ack_reason)?,
                    properties: None,
                });
            }

            let puback = PubAck {
                pkid,
                reason: reason(ack_reason)?,
                properties: PubAckProperties::extract(&mut bytes)?,
            };

            Ok(puback)
        }
    }

    pub fn write(
        pkid: u16,
        reason: PubAckReason,
        properties: Option<PubAckProperties>,
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        buffer.put_u8(0x40);

        match &properties {
            Some(properties) => {
                let properties_len = properties.len();
                let properties_len_len = len_len(properties_len);
                let len = 2 + 1 + properties_len_len + properties_len;

                let count = write_remaining_length(buffer, len)?;
                buffer.put_u16(pkid);
                buffer.put_u8(reason as u8);
                properties.write(buffer)?;

                Ok(len + count + 1)
            }
            None => {
                // Unlike other packets, property length can be ignored if there are
                // no properties in acks
                //
                // TODO: maybe we should set len = 2 for PubAckReason == Success
                let len = 2 + 1;
                let count = write_remaining_length(buffer, len)?;
                buffer.put_u16(pkid);
                buffer.put_u8(reason as u8);

                Ok(len + count + 1)
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct PubAckProperties {
        pub reason_string: Option<String>,
        pub user_properties: Vec<(String, String)>,
    }

    /// Return code in connack
    #[derive(Debug, Clone, Copy, PartialEq)]
    #[repr(u8)]
    pub enum PubAckReason {
        Success = 0,
        NoMatchingSubscribers = 16,
        UnspecifiedError = 128,
        ImplementationSpecificError = 131,
        NotAuthorized = 135,
        TopicNameInvalid = 144,
        PacketIdentifierInUse = 145,
        QuotaExceeded = 151,
        PayloadFormatInvalid = 153,
    }

    impl PubAckProperties {
        pub fn len(&self) -> usize {
            let mut len = 0;

            if let Some(reason) = &self.reason_string {
                len += 1 + 2 + reason.len();
            }

            for (key, value) in self.user_properties.iter() {
                len += 1 + 2 + key.len() + 2 + value.len();
            }

            len
        }

        pub fn extract(bytes: &mut Bytes) -> Result<Option<PubAckProperties>, Error> {
            let mut reason_string = None;
            let mut user_properties = Vec::new();

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
                    PropertyType::ReasonString => {
                        let bytes = read_mqtt_bytes(bytes)?;
                        let reason = std::str::from_utf8(&bytes)?.to_owned();
                        cursor += 2 + reason.len();
                        reason_string = Some(reason);
                    }
                    PropertyType::UserProperty => {
                        let key = read_mqtt_bytes(bytes)?;
                        let key = std::str::from_utf8(&key)?.to_owned();
                        let value = read_mqtt_bytes(bytes)?;
                        let value = std::str::from_utf8(&value)?.to_owned();
                        cursor += 2 + key.len() + 2 + value.len();
                        user_properties.push((key, value));
                    }
                    _ => return Err(Error::InvalidPropertyType(prop)),
                }
            }

            Ok(Some(PubAckProperties {
                reason_string,
                user_properties,
            }))
        }

        fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
            let len = self.len();
            write_remaining_length(buffer, len)?;

            if let Some(reason) = &self.reason_string {
                buffer.put_u8(PropertyType::ReasonString as u8);
                write_mqtt_string(buffer, reason);
            }

            for (key, value) in self.user_properties.iter() {
                buffer.put_u8(PropertyType::UserProperty as u8);
                write_mqtt_string(buffer, key);
                write_mqtt_string(buffer, value);
            }

            Ok(())
        }
    }
    /// Connection return code type
    fn reason(num: u8) -> Result<PubAckReason, Error> {
        let code = match num {
            0 => PubAckReason::Success,
            16 => PubAckReason::NoMatchingSubscribers,
            128 => PubAckReason::UnspecifiedError,
            131 => PubAckReason::ImplementationSpecificError,
            135 => PubAckReason::NotAuthorized,
            144 => PubAckReason::TopicNameInvalid,
            145 => PubAckReason::PacketIdentifierInUse,
            151 => PubAckReason::QuotaExceeded,
            153 => PubAckReason::PayloadFormatInvalid,
            num => return Err(Error::InvalidConnectReturnCode(num)),
        };

        Ok(code)
    }
}

pub(crate) mod subscribe {
    use super::*;
    use bytes::{Buf, Bytes};

    /// Subscription packet
    #[derive(Clone, PartialEq)]
    pub struct Subscribe {
        pub pkid: u16,
        pub filters: Vec<SubscribeFilter>,
        pub properties: Option<SubscribeProperties>,
    }

    impl Subscribe {
        pub fn new<S: Into<String>>(path: S, qos: QoS) -> Subscribe {
            let filter = SubscribeFilter {
                path: path.into(),
                qos,
                nolocal: false,
                preserve_retain: false,
                retain_forward_rule: RetainForwardRule::OnEverySubscribe,
            };

            let filters = vec![filter];
            Subscribe {
                pkid: 0,
                filters,
                properties: None,
            }
        }

        pub fn add(&mut self, path: String, qos: QoS) -> &mut Self {
            let filter = SubscribeFilter {
                path,
                qos,
                nolocal: false,
                preserve_retain: false,
                retain_forward_rule: RetainForwardRule::OnEverySubscribe,
            };

            self.filters.push(filter);
            self
        }

        pub fn len(&self) -> usize {
            let mut len = 2 + self.filters.iter().fold(0, |s, t| s + t.len());

            if let Some(properties) = &self.properties {
                let properties_len = properties.len();
                let properties_len_len = len_len(properties_len);
                len += properties_len_len + properties_len;
            } else {
                // just 1 byte representing 0 len
                len += 1;
            }

            len
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);

            let pkid = read_u16(&mut bytes)?;
            let properties = SubscribeProperties::extract(&mut bytes)?;

            // variable header size = 2 (packet identifier)
            let mut filters = Vec::new();

            while bytes.has_remaining() {
                let path = read_mqtt_bytes(&mut bytes)?;
                let path = std::str::from_utf8(&path)?.to_owned();
                let options = read_u8(&mut bytes)?;
                let requested_qos = options & 0b0000_0011;

                let nolocal = options >> 2 & 0b0000_0001;
                let nolocal = nolocal != 0;

                let preserve_retain = options >> 3 & 0b0000_0001;
                let preserve_retain = preserve_retain != 0;

                let retain_forward_rule = (options >> 4) & 0b0000_0011;
                let retain_forward_rule = match retain_forward_rule {
                    0 => RetainForwardRule::OnEverySubscribe,
                    1 => RetainForwardRule::OnNewSubscribe,
                    2 => RetainForwardRule::Never,
                    r => return Err(Error::InvalidRetainForwardRule(r)),
                };

                filters.push(SubscribeFilter {
                    path,
                    qos: qos(requested_qos)?,
                    nolocal,
                    preserve_retain,
                    retain_forward_rule,
                });
            }

            let subscribe = Subscribe {
                pkid,
                filters,
                properties,
            };

            Ok(subscribe)
        }
    }

    pub fn write(
        filters: Vec<SubscribeFilter>,
        pkid: u16,
        properties: Option<SubscribeProperties>,
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        // write packet type
        buffer.put_u8(0x82);

        // write remaining length
        let mut len = 2 + filters.iter().fold(0, |s, t| s + t.len());

        if let Some(properties) = &properties {
            let properties_len = properties.len();
            let properties_len_len = len_len(properties_len);
            len += properties_len_len + properties_len;
        } else {
            // just 1 byte representing 0 len
            len += 1;
        }
        let remaining_len = len;
        let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

        // write packet id
        buffer.put_u16(pkid);

        match &properties {
            Some(properties) => properties.write(buffer)?,
            None => {
                write_remaining_length(buffer, 0)?;
            }
        };

        // write filters
        for filter in filters.iter() {
            filter.write(buffer);
        }

        Ok(1 + remaining_len_bytes + remaining_len)
    }

    ///  Subscription filter
    #[derive(Clone, PartialEq)]
    pub struct SubscribeFilter {
        pub path: String,
        pub qos: QoS,
        pub nolocal: bool,
        pub preserve_retain: bool,
        pub retain_forward_rule: RetainForwardRule,
    }

    impl SubscribeFilter {
        pub fn new(path: String, qos: QoS) -> SubscribeFilter {
            SubscribeFilter {
                path,
                qos,
                nolocal: false,
                preserve_retain: false,
                retain_forward_rule: RetainForwardRule::OnEverySubscribe,
            }
        }

        pub fn len(&self) -> usize {
            // filter len + filter + options
            2 + self.path.len() + 1
        }

        fn write(&self, buffer: &mut BytesMut) {
            let mut options = 0;
            options |= self.qos as u8;

            if self.nolocal {
                options |= 1 << 2;
            }

            if self.preserve_retain {
                options |= 1 << 3;
            }

            match self.retain_forward_rule {
                RetainForwardRule::OnEverySubscribe => options |= 0 << 4,
                RetainForwardRule::OnNewSubscribe => options |= 1 << 4,
                RetainForwardRule::Never => options |= 2 << 4,
            }

            write_mqtt_string(buffer, self.path.as_str());
            buffer.put_u8(options);
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct SubscribeProperties {
        pub id: Option<usize>,
        pub user_properties: Vec<(String, String)>,
    }

    impl SubscribeProperties {
        pub fn len(&self) -> usize {
            let mut len = 0;

            if let Some(id) = &self.id {
                len += 1 + len_len(*id);
            }

            for (key, value) in self.user_properties.iter() {
                len += 1 + 2 + key.len() + 2 + value.len();
            }

            len
        }

        pub fn extract(bytes: &mut Bytes) -> Result<Option<SubscribeProperties>, Error> {
            let mut id = None;
            let mut user_properties = Vec::new();

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
                    PropertyType::SubscriptionIdentifier => {
                        let (id_len, sub_id) = length(bytes.iter())?;
                        // TODO: Validate 1 +. Tests are working either way
                        cursor += 1 + id_len;
                        bytes.advance(id_len);
                        id = Some(sub_id)
                    }
                    PropertyType::UserProperty => {
                        let key = read_mqtt_bytes(bytes)?;
                        let key = std::str::from_utf8(&key)?.to_owned();
                        let value = read_mqtt_bytes(bytes)?;
                        let value = std::str::from_utf8(&value)?.to_owned();
                        cursor += 2 + key.len() + 2 + value.len();
                        user_properties.push((key, value));
                    }
                    _ => return Err(Error::InvalidPropertyType(prop)),
                }
            }

            Ok(Some(SubscribeProperties {
                id,
                user_properties,
            }))
        }

        fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
            let len = self.len();
            write_remaining_length(buffer, len)?;

            if let Some(id) = &self.id {
                buffer.put_u8(PropertyType::SubscriptionIdentifier as u8);
                write_remaining_length(buffer, *id)?;
            }

            for (key, value) in self.user_properties.iter() {
                buffer.put_u8(PropertyType::UserProperty as u8);
                write_mqtt_string(buffer, key);
                write_mqtt_string(buffer, value);
            }

            Ok(())
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum RetainForwardRule {
        OnEverySubscribe,
        OnNewSubscribe,
        Never,
    }

    impl fmt::Debug for Subscribe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "Filters = {:?}, Packet id = {:?}",
                self.filters, self.pkid
            )
        }
    }

    impl fmt::Debug for SubscribeFilter {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "Filter = {}, Qos = {:?}, Nolocal = {}, Preserve retain = {}, Forward rule = {:?}",
                self.path, self.qos, self.nolocal, self.preserve_retain, self.retain_forward_rule
            )
        }
    }
}

pub(crate) mod suback {
    use std::convert::{TryFrom, TryInto};

    use super::*;
    use bytes::{Buf, Bytes};

    /// Acknowledgement to subscribe
    #[derive(Debug, Clone, PartialEq)]
    pub struct SubAck {
        pub pkid: u16,
        pub return_codes: Vec<SubscribeReasonCode>,
        pub properties: Option<SubAckProperties>,
    }

    impl SubAck {
        pub fn new(pkid: u16, return_codes: Vec<SubscribeReasonCode>) -> SubAck {
            SubAck {
                pkid,
                return_codes,
                properties: None,
            }
        }

        pub fn len(&self) -> usize {
            let mut len = 2 + self.return_codes.len();

            match &self.properties {
                Some(properties) => {
                    let properties_len = properties.len();
                    let properties_len_len = len_len(properties_len);
                    len += properties_len_len + properties_len;
                }
                None => {
                    // just 1 byte representing 0 len
                    len += 1;
                }
            }

            len
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);

            let pkid = read_u16(&mut bytes)?;
            let properties = SubAckProperties::extract(&mut bytes)?;

            if !bytes.has_remaining() {
                return Err(Error::MalformedPacket);
            }

            let mut return_codes = Vec::new();
            while bytes.has_remaining() {
                let return_code = read_u8(&mut bytes)?;
                return_codes.push(return_code.try_into()?);
            }

            let suback = SubAck {
                pkid,
                return_codes,
                properties,
            };

            Ok(suback)
        }
    }

    pub fn write(
        return_codes: Vec<SubscribeReasonCode>,
        pkid: u16,
        properties: Option<SubAckProperties>,
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        buffer.put_u8(0x90);

        let mut len = 2 + return_codes.len();

        match &properties {
            Some(properties) => {
                let properties_len = properties.len();
                let properties_len_len = len_len(properties_len);
                len += properties_len_len + properties_len;
            }
            None => {
                // just 1 byte representing 0 len
                len += 1;
            }
        }

        let remaining_len = len;
        let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

        buffer.put_u16(pkid);

        match &properties {
            Some(properties) => properties.write(buffer)?,
            None => {
                write_remaining_length(buffer, 0)?;
            }
        };

        let p: Vec<u8> = return_codes.iter().map(|code| *code as u8).collect();
        buffer.extend_from_slice(&p);
        Ok(1 + remaining_len_bytes + remaining_len)
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct SubAckProperties {
        pub reason_string: Option<String>,
        pub user_properties: Vec<(String, String)>,
    }

    impl SubAckProperties {
        pub fn len(&self) -> usize {
            let mut len = 0;

            if let Some(reason) = &self.reason_string {
                len += 1 + 2 + reason.len();
            }

            for (key, value) in self.user_properties.iter() {
                len += 1 + 2 + key.len() + 2 + value.len();
            }

            len
        }

        pub fn extract(bytes: &mut Bytes) -> Result<Option<SubAckProperties>, Error> {
            let mut reason_string = None;
            let mut user_properties = Vec::new();

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
                    PropertyType::ReasonString => {
                        let bytes = read_mqtt_bytes(bytes)?;
                        let reason = std::str::from_utf8(&bytes)?.to_owned();
                        cursor += 2 + reason.len();
                        reason_string = Some(reason);
                    }
                    PropertyType::UserProperty => {
                        let key = read_mqtt_bytes(bytes)?;
                        let key = std::str::from_utf8(&key)?.to_owned();
                        let value = read_mqtt_bytes(bytes)?;
                        let value = std::str::from_utf8(&value)?.to_owned();
                        cursor += 2 + key.len() + 2 + value.len();
                        user_properties.push((key, value));
                    }
                    _ => return Err(Error::InvalidPropertyType(prop)),
                }
            }

            Ok(Some(SubAckProperties {
                reason_string,
                user_properties,
            }))
        }

        fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
            let len = self.len();
            write_remaining_length(buffer, len)?;

            if let Some(reason) = &self.reason_string {
                buffer.put_u8(PropertyType::ReasonString as u8);
                write_mqtt_string(buffer, reason);
            }

            for (key, value) in self.user_properties.iter() {
                buffer.put_u8(PropertyType::UserProperty as u8);
                write_mqtt_string(buffer, key);
                write_mqtt_string(buffer, value);
            }

            Ok(())
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SubscribeReasonCode {
        QoS0 = 0,
        QoS1 = 1,
        QoS2 = 2,
        Unspecified = 128,
        ImplementationSpecific = 131,
        NotAuthorized = 135,
        TopicFilterInvalid = 143,
        PkidInUse = 145,
        QuotaExceeded = 151,
        SharedSubscriptionsNotSupported = 158,
        SubscriptionIdNotSupported = 161,
        WildcardSubscriptionsNotSupported = 162,
    }

    impl TryFrom<u8> for SubscribeReasonCode {
        type Error = Error;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            let v = match value {
                0 => SubscribeReasonCode::QoS0,
                1 => SubscribeReasonCode::QoS1,
                2 => SubscribeReasonCode::QoS2,
                128 => SubscribeReasonCode::Unspecified,
                131 => SubscribeReasonCode::ImplementationSpecific,
                135 => SubscribeReasonCode::NotAuthorized,
                143 => SubscribeReasonCode::TopicFilterInvalid,
                145 => SubscribeReasonCode::PkidInUse,
                151 => SubscribeReasonCode::QuotaExceeded,
                158 => SubscribeReasonCode::SharedSubscriptionsNotSupported,
                161 => SubscribeReasonCode::SubscriptionIdNotSupported,
                162 => SubscribeReasonCode::WildcardSubscriptionsNotSupported,
                v => return Err(Error::InvalidSubscribeReasonCode(v)),
            };

            Ok(v)
        }
    }

    pub fn codes(c: Vec<u8>) -> Vec<SubscribeReasonCode> {
        c.into_iter()
            .map(|v| match qos(v).unwrap() {
                QoS::AtMostOnce => SubscribeReasonCode::QoS0,
                QoS::AtLeastOnce => SubscribeReasonCode::QoS1,
            })
            .collect()
    }
}

pub(crate) mod pingresp {
    use super::*;

    pub fn write(payload: &mut BytesMut) -> Result<usize, Error> {
        payload.put_slice(&[0xD0, 0x00]);
        Ok(2)
    }
}

/// Reads a stream of bytes and extracts next MQTT packet out of it
pub fn read_mut(stream: &mut BytesMut, max_size: usize) -> Result<Packet, Error> {
    let fixed_header = check(stream.iter(), max_size)?;

    // Test with a stream with exactly the size to check border panics
    let packet = stream.split_to(fixed_header.frame_length());
    let packet_type = fixed_header.packet_type()?;

    if fixed_header.remaining_len == 0 {
        // no payload packets
        return match packet_type {
            PacketType::PingReq => Ok(Packet::PingReq),
            PacketType::PingResp => Ok(Packet::PingResp),
            PacketType::Disconnect => Ok(Packet::Disconnect),
            _ => Err(Error::PayloadRequired),
        };
    }

    let packet = packet.freeze();
    let packet = match packet_type {
        PacketType::Connect => Packet::Connect(connect::Connect::read(fixed_header, packet)?),
        PacketType::ConnAck => Packet::ConnAck(connack::ConnAck::read(fixed_header, packet)?),
        PacketType::Publish => Packet::Publish(publish::Publish::read(fixed_header, packet)?),
        PacketType::PubAck => Packet::PubAck(puback::PubAck::read(fixed_header, packet)?),
        PacketType::Subscribe => {
            Packet::Subscribe(subscribe::Subscribe::read(fixed_header, packet)?)
        }
        PacketType::SubAck => Packet::SubAck(suback::SubAck::read(fixed_header, packet)?),
        PacketType::PingReq => Packet::PingReq,
        PacketType::PingResp => Packet::PingResp,
        PacketType::Disconnect => Packet::Disconnect,
        v => return Err(Error::UnsupportedPacket(v)),
    };

    Ok(packet)
}

/// Reads a stream of bytes and extracts next MQTT packet out of it
pub fn read(stream: &mut Bytes, max_size: usize) -> Result<Packet, Error> {
    let fixed_header = check(stream.iter(), max_size)?;

    // Test with a stream with exactly the size to check border panics
    let packet = stream.split_to(fixed_header.frame_length());
    let packet_type = fixed_header.packet_type()?;

    if fixed_header.remaining_len == 0 {
        // no payload packets
        return match packet_type {
            PacketType::PingReq => Ok(Packet::PingReq),
            PacketType::PingResp => Ok(Packet::PingResp),
            PacketType::Disconnect => Ok(Packet::Disconnect),
            _ => Err(Error::PayloadRequired),
        };
    }

    let packet = match packet_type {
        PacketType::Connect => Packet::Connect(connect::Connect::read(fixed_header, packet)?),
        PacketType::ConnAck => Packet::ConnAck(connack::ConnAck::read(fixed_header, packet)?),
        PacketType::Publish => Packet::Publish(publish::Publish::read(fixed_header, packet)?),
        PacketType::PubAck => Packet::PubAck(puback::PubAck::read(fixed_header, packet)?),
        PacketType::Subscribe => {
            Packet::Subscribe(subscribe::Subscribe::read(fixed_header, packet)?)
        }
        PacketType::SubAck => Packet::SubAck(suback::SubAck::read(fixed_header, packet)?),
        PacketType::PingReq => Packet::PingReq,
        PacketType::PingResp => Packet::PingResp,
        PacketType::Disconnect => Packet::Disconnect,
        v => return Err(Error::UnsupportedPacket(v)),
    };

    Ok(packet)
}

#[derive(Clone, Debug, PartialEq)]
pub enum Packet {
    Connect(connect::Connect),
    Publish(publish::Publish),
    ConnAck(connack::ConnAck),
    PubAck(puback::PubAck),
    PingReq,
    PingResp,
    Subscribe(subscribe::Subscribe),
    SubAck(suback::SubAck),
    Disconnect,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PropertyType {
    PayloadFormatIndicator = 1,
    MessageExpiryInterval = 2,
    ContentType = 3,
    ResponseTopic = 8,
    CorrelationData = 9,
    SubscriptionIdentifier = 11,
    SessionExpiryInterval = 17,
    AssignedClientIdentifier = 18,
    ServerKeepAlive = 19,
    AuthenticationMethod = 21,
    AuthenticationData = 22,
    RequestProblemInformation = 23,
    WillDelayInterval = 24,
    RequestResponseInformation = 25,
    ResponseInformation = 26,
    ServerReference = 28,
    ReasonString = 31,
    ReceiveMaximum = 33,
    TopicAliasMaximum = 34,
    TopicAlias = 35,
    MaximumQos = 36,
    RetainAvailable = 37,
    UserProperty = 38,
    MaximumPacketSize = 39,
    WildcardSubscriptionAvailable = 40,
    SubscriptionIdentifierAvailable = 41,
    SharedSubscriptionAvailable = 42,
}

fn property(num: u8) -> Result<PropertyType, Error> {
    let property = match num {
        1 => PropertyType::PayloadFormatIndicator,
        2 => PropertyType::MessageExpiryInterval,
        3 => PropertyType::ContentType,
        8 => PropertyType::ResponseTopic,
        9 => PropertyType::CorrelationData,
        11 => PropertyType::SubscriptionIdentifier,
        17 => PropertyType::SessionExpiryInterval,
        18 => PropertyType::AssignedClientIdentifier,
        19 => PropertyType::ServerKeepAlive,
        21 => PropertyType::AuthenticationMethod,
        22 => PropertyType::AuthenticationData,
        23 => PropertyType::RequestProblemInformation,
        24 => PropertyType::WillDelayInterval,
        25 => PropertyType::RequestResponseInformation,
        26 => PropertyType::ResponseInformation,
        28 => PropertyType::ServerReference,
        31 => PropertyType::ReasonString,
        33 => PropertyType::ReceiveMaximum,
        34 => PropertyType::TopicAliasMaximum,
        35 => PropertyType::TopicAlias,
        36 => PropertyType::MaximumQos,
        37 => PropertyType::RetainAvailable,
        38 => PropertyType::UserProperty,
        39 => PropertyType::MaximumPacketSize,
        40 => PropertyType::WildcardSubscriptionAvailable,
        41 => PropertyType::SubscriptionIdentifierAvailable,
        42 => PropertyType::SharedSubscriptionAvailable,
        num => return Err(Error::InvalidPropertyType(num)),
    };

    Ok(property)
}

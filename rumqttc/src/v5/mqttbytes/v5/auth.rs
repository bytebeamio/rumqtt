use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Auth packet
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Auth {
    pub reason: AuthReasonCode,
    pub properties: Option<AuthProperties>,
}

impl Auth {
    pub fn new(reason: AuthReasonCode, properties: Option<AuthProperties>) -> Self {
        
        Self {
            reason,
            properties,
        }
    }

    pub fn size(&self) -> usize {
        let len = self.len();
        let remaining_len_size = len_len(len);

        1 + remaining_len_size + len
    }

    fn len(&self) -> usize {
        let mut len = 1;

        if let Some(p) = &self.properties {
            let properties_len = p.len();
            let properties_len_len = len_len(properties_len);
            len += properties_len_len + properties_len;
        } else {
            // just 1 byte representing 0 len
            len += 1;
        }

        len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Auth, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);
        let code = read_u8(&mut bytes)?;
        let reason = reason(code)?;
        let properties = AuthProperties::read(&mut bytes)?;
        let auth = Auth {
            reason,
            properties,
        };

        Ok(auth)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = Self::len(self);
        buffer.put_u8(0xF0);

        let count = write_remaining_length(buffer, len)?;
        buffer.put_u8(code(self.reason));

        if let Some(p) = &self.properties {
            p.write(buffer)?;
        } else {
            write_remaining_length(buffer, 0)?;
        }

        Ok(1 + count + len)
    }
}

/// Return code in auth
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthReasonCode {
    Success,
    ContinueAuthentication,
    Reauthenticate
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AuthProperties {
    /// Method of authentication
    pub authentication_method: Option<String>,
    /// Authentication data
    pub authentication_data: Option<Bytes>,
    /// Reason for disconnection
    pub reason_string: Option<String>,
    /// List of user properties
    pub user_properties: Vec<(String, String)>,
}

impl AuthProperties {
    fn len(&self) -> usize {
        let mut len = 0;

        if let Some(authentication_method) = &self.authentication_method {
            len += 1 + 2 + authentication_method.len();
        }

        if let Some(authentication_data) = &self.authentication_data {
            len += 1 + 2 + authentication_data.len();
        }

        if let Some(reason) = &self.reason_string {
            len += 1 + 2 + reason.len();
        }

        for (key, value) in self.user_properties.iter() {
            len += 1 + 2 + key.len() + 2 + value.len();
        }

        len
    }

    pub fn read(bytes: &mut Bytes) -> Result<Option<AuthProperties>, Error> {
        let mut authentication_method = None;
        let mut authentication_data = None;
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
                _ => return Err(Error::InvalidPropertyType(prop)),
            }
        }

        Ok(Some(AuthProperties {
            authentication_method,
            authentication_data,
            reason_string,
            user_properties,
        }))
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = self.len();
        write_remaining_length(buffer, len)?;

        if let Some(authentication_method) = &self.authentication_method {
            buffer.put_u8(PropertyType::AuthenticationMethod as u8);
            write_mqtt_string(buffer, authentication_method);
        }

        if let Some(authentication_data) = &self.authentication_data {
            buffer.put_u8(PropertyType::AuthenticationData as u8);
            write_mqtt_bytes(buffer, authentication_data);
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

        Ok(())
    }
}

fn reason(num: u8) -> Result<AuthReasonCode, Error> {
    let code = match num {
        0x00 => AuthReasonCode::Success,
        0x18 => AuthReasonCode::ContinueAuthentication,
        0x19 => AuthReasonCode::Reauthenticate,
        num => return Err(Error::InvalidReason(num)),
    };

    Ok(code)
}

fn code(value: AuthReasonCode) -> u8 {
    match value {
        AuthReasonCode::Success => 0x00,
        AuthReasonCode::ContinueAuthentication => 0x18,
        AuthReasonCode::Reauthenticate => 0x19
    }
}

#[cfg(test)]
mod test {
    use super::super::test::{USER_PROP_KEY, USER_PROP_VAL};
    use super::*;
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;

    #[test]
    fn length_calculation() {
        let mut dummy_bytes = BytesMut::new();
        // Use user_properties to pad the size to exceed ~128 bytes to make the
        // remaining_length field in the packet be 2 bytes long.
        let auth_props = AuthProperties {
            authentication_method: Some("Authentication Method".into()),
            authentication_data: Some("Authentication Data".into()),
            reason_string: None,
            user_properties: vec![(USER_PROP_KEY.into(), USER_PROP_VAL.into())],
        };

        let auth_pkt = Auth::new(AuthReasonCode::ContinueAuthentication, Some(auth_props));

        let size_from_size = auth_pkt.size();
        let size_from_write = auth_pkt.write(&mut dummy_bytes).unwrap();
        let size_from_bytes = dummy_bytes.len();

        assert_eq!(size_from_write, size_from_bytes);
        assert_eq!(size_from_size, size_from_bytes);
    }
}

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{
    len_len, length, property, read_mqtt_bytes, read_mqtt_string, read_u8, write_mqtt_bytes,
    write_mqtt_string, write_remaining_length, Error, FixedHeader, PropertyType,
};

/// Auth packet reason code
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthReasonCode {
    Success,
    Continue,
    ReAuthentivate,
}

impl AuthReasonCode {
    fn read(bytes: &mut Bytes) -> Result<Self, Error> {
        let reason_code = read_u8(bytes)?;
        let code = match reason_code {
            0x00 => AuthReasonCode::Success,
            0x18 => AuthReasonCode::Continue,
            0x19 => AuthReasonCode::ReAuthentivate,
            _ => return Err(Error::MalformedPacket),
        };

        Ok(code)
    }

    fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        let reason_code = match self {
            AuthReasonCode::Success => 0x00,
            AuthReasonCode::Continue => 0x18,
            AuthReasonCode::ReAuthentivate => 0x19,
        };

        buffer.put_u8(reason_code);

        Ok(())
    }
}

/// Used to perform extended authentication exchange
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auth {
    pub code: AuthReasonCode,
    pub properties: Option<AuthProperties>,
}

impl Auth {
    fn len(&self) -> usize {
        let mut len = 1  // reason code
                    + 1; // property len

        if let Some(properties) = &self.properties {
            len += properties.len();
        }

        len
    }

    pub fn size(&self) -> usize {
        let len = self.len();
        let remaining_len_size = len_len(len);

        1 + remaining_len_size + len
    }

    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_header_len;
        bytes.advance(variable_header_index);

        let code = AuthReasonCode::read(&mut bytes)?;
        let properties = AuthProperties::read(&mut bytes)?;
        let auth = Auth { code, properties };

        Ok(auth)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        buffer.put_u8(0xF0);

        let len = self.len();
        let count = write_remaining_length(buffer, len)?;

        self.code.write(buffer)?;
        if let Some(p) = &self.properties {
            p.write(buffer)?;
        } else {
            write_remaining_length(buffer, 0)?;
        }

        Ok(1 + count + len)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AuthProperties {
    pub method: Option<String>,
    pub data: Option<Bytes>,
    pub reason: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl AuthProperties {
    fn len(&self) -> usize {
        let mut len = 0;

        if let Some(method) = &self.method {
            let m_len = method.len();
            len += 1 + m_len;
        }

        if let Some(data) = &self.data {
            let d_len = data.len();
            len += 1 + len_len(d_len) + d_len;
        }

        if let Some(reason) = &self.reason {
            let r_len = reason.len();
            len += 1 + r_len;
        }

        for (key, value) in self.user_properties.iter() {
            let p_len = key.len() + value.len();
            len += 1 + p_len;
        }

        len
    }

    pub fn read(bytes: &mut Bytes) -> Result<Option<AuthProperties>, Error> {
        let (properties_len_len, properties_len) = length(bytes.iter())?;
        bytes.advance(properties_len_len);
        if properties_len == 0 {
            return Ok(None);
        }

        let mut props = AuthProperties::default();

        let mut cursor = 0;
        // read until cursor reaches property length. properties_len = 0 will skip this loop
        while cursor < properties_len {
            let prop = read_u8(bytes)?;
            cursor += 1;

            match property(prop)? {
                PropertyType::AuthenticationMethod => {
                    let method = read_mqtt_string(bytes)?;
                    cursor += method.len();
                    props.method = Some(method);
                }
                PropertyType::AuthenticationData => {
                    let data = read_mqtt_bytes(bytes)?;
                    cursor += 2 + data.len();
                    props.data = Some(data);
                }
                PropertyType::ReasonString => {
                    let reason = read_mqtt_string(bytes)?;
                    cursor += reason.len();
                    props.reason = Some(reason);
                }
                PropertyType::UserProperty => {
                    let key = read_mqtt_string(bytes)?;
                    let value = read_mqtt_string(bytes)?;
                    cursor += 2 + key.len() + 2 + value.len();
                    props.user_properties.push((key, value));
                }
                _ => return Err(Error::InvalidPropertyType(prop)),
            }
        }

        Ok(Some(props))
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = self.len();
        write_remaining_length(buffer, len)?;

        if let Some(authentication_method) = &self.method {
            buffer.put_u8(PropertyType::AuthenticationMethod as u8);
            write_mqtt_string(buffer, authentication_method);
        }

        if let Some(authentication_data) = &self.data {
            buffer.put_u8(PropertyType::AuthenticationData as u8);
            write_mqtt_bytes(buffer, authentication_data);
        }

        if let Some(reason) = &self.reason {
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

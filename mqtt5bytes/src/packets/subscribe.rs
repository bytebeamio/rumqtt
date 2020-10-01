use super::*;
use crate::*;
use alloc::string::String;
use alloc::vec::Vec;
use bytes::{Buf, Bytes};
use core::fmt;

/// Subscription packet
#[derive(Clone, PartialEq)]
pub struct Subscribe {
    pub pkid: u16,
    pub filters: Vec<SubscribeFilter>,
    pub properties: Option<SubscribeProperties>,
}

impl Subscribe {
    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_len;
        bytes.advance(variable_header_index);
        let pkid = bytes.get_u16();
        let properties = SubscribeProperties::extract(&mut bytes)?;

        // TODO: Fix this in mqtt4bytes
        if !bytes.has_remaining() {
            return Err(Error::MalformedPacket);
        }

        // variable header size = 2 (packet identifier)
        let mut filters = Vec::new();

        while bytes.has_remaining() {
            let path = read_mqtt_string(&mut bytes)?;
            let requested_qos = bytes.get_u8();
            filters.push(SubscribeFilter {
                path,
                qos: qos(requested_qos)?,
                nolocal: true,
                preserve_retain: true,
                retain_forward_rule: RetainForwardRule::Never,
            });
        }

        let subscribe = Subscribe {
            pkid,
            filters,
            properties,
        };

        Ok(subscribe)
    }

    pub fn new<S: Into<String>>(path: S, qos: QoS) -> Subscribe {
        let filter = SubscribeFilter {
            path: path.into(),
            qos,
            nolocal: false,
            preserve_retain: false,
            retain_forward_rule: RetainForwardRule::OnEverySubscribe,
        };

        let mut filters = Vec::new();
        filters.push(filter);
        Subscribe {
            pkid: 0,
            filters,
            properties: None,
        }
    }

    pub fn empty_subscribe() -> Subscribe {
        Subscribe {
            pkid: 0,
            filters: Vec::new(),
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

    pub fn write(&self, payload: &mut BytesMut) -> Result<usize, Error> {
        let remaining_len = 2 + self.filters.iter().fold(0, |s, ref t| s + t.path.len() + 3);
        payload.put_u8(0x82);
        let remaining_len_bytes = write_remaining_length(payload, remaining_len)?;
        payload.put_u16(self.pkid);

        for filter in self.filters.iter() {
            write_mqtt_string(payload, filter.path.as_str());
            payload.put_u8(filter.qos as u8);
        }

        Ok(1 + remaining_len_bytes + remaining_len)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeProperties {
    pub id: Option<u32>,
    pub user_properties: Vec<(String, String)>,
}

impl SubscribeProperties {
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

    pub fn extract(mut bytes: &mut Bytes) -> Result<Option<SubscribeProperties>, Error> {
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
            let prop = bytes.get_u8();
            cursor += 1;

            match property(prop)? {
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
                _ => return Err(Error::InvalidPropertyType(prop)),
            }
        }

        Ok(Some(SubscribeProperties {
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

///  Subscription filter
#[derive(Clone, PartialEq)]
pub struct SubscribeFilter {
    pub path: String,
    pub qos: QoS,
    pub nolocal: bool,
    pub preserve_retain: bool,
    pub retain_forward_rule: RetainForwardRule,
}

#[derive(Clone, PartialEq)]
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
        write!(f, "Filter = {}, Qos = {:?}", self.path, self.qos)
    }
}

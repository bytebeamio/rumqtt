use super::*;
use alloc::string::String;
use alloc::vec::Vec;
use bytes::{Buf, Bytes};
use core::fmt;

/// Subscription packet
#[derive(Clone, PartialEq)]
pub struct Subscribe {
    pub pkid: u16,
    pub filters: Vec<SubscribeFilter>,
    #[cfg(v5)]
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

        let mut filters = Vec::new();
        filters.push(filter);
        Subscribe {
            pkid: 0,
            filters,
            #[cfg(v5)]
            properties: None,
        }
    }

    pub fn new_many<T>(topics: T) -> Subscribe
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        Subscribe {
            pkid: 0,
            filters: topics.into_iter().collect(),
            #[cfg(v5)]
            properties: None,
        }
    }

    pub fn empty_subscribe() -> Subscribe {
        Subscribe {
            pkid: 0,
            filters: Vec::new(),
            #[cfg(v5)]
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
        let len = 2 + self.filters.iter().fold(0, |s, t| s + t.len());

        #[cfg(v5)]
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

        // variable header size = 2 (packet identifier)
        let mut filters = Vec::new();

        while bytes.has_remaining() {
            let path = read_mqtt_string(&mut bytes)?;
            let options = read_u8(&mut bytes)?;
            let requested_qos = options & 0b0000_0011;

            let nolocal = options >> 2 & 0b0000_0001;
            let nolocal = if nolocal == 0 { false } else { true };

            let preserve_retain = options >> 3 & 0b0000_0001;
            let preserve_retain = if preserve_retain == 0 { false } else { true };

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
            #[cfg(v5)]
            properties: SubscribeProperties::extract(&mut bytes)?,
        };

        Ok(subscribe)
    }

    pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
        // write packet type
        buffer.put_u8(0x82);

        // write remaining length
        let remaining_len = self.len();
        let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

        // write packet id
        buffer.put_u16(self.pkid);

        #[cfg(v5)]
        match &self.properties {
            Some(properties) => properties.write(buffer)?,
            None => {
                write_remaining_length(buffer, 0)?;
            }
        };

        // write filters
        for filter in self.filters.iter() {
            filter.write(buffer);
        }

        Ok(1 + remaining_len_bytes + remaining_len)
    }
}

#[cfg(v5)]
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeProperties {
    pub id: Option<usize>,
    pub user_properties: Vec<(String, String)>,
}

#[cfg(v5)]
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

    pub fn extract(mut bytes: &mut Bytes) -> Result<Option<SubscribeProperties>, Error> {
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
            let prop = read_u8(&mut bytes)?;
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
                    let key = read_mqtt_string(&mut bytes)?;
                    let value = read_mqtt_string(&mut bytes)?;
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

    pub fn set_nolocal(&mut self, flag: bool) -> &mut Self {
        self.nolocal = flag;
        self
    }

    pub fn set_preserve_retain(&mut self, flag: bool) -> &mut Self {
        self.preserve_retain = flag;
        self
    }

    pub fn set_retain_forward_rule(&mut self, rule: RetainForwardRule) -> &mut Self {
        self.retain_forward_rule = rule;
        self
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

#[cfg(test)]
mod test {
    use super::*;
    use alloc::vec;
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;

    #[test]
    fn v4_subscribe_parsing_works() {
        let stream = &[
            0b1000_0010,
            20, // packet type, flags and remaining len
            0x01,
            0x04, // variable header. pkid = 260
            0x00,
            0x03,
            b'a',
            b'/',
            b'+', // payload. topic filter = 'a/+'
            0x00, // payload. qos = 0
            0x00,
            0x01,
            b'#', // payload. topic filter = '#'
            0x01, // payload. qos = 1
            0x00,
            0x05,
            b'a',
            b'/',
            b'b',
            b'/',
            b'c', // payload. topic filter = 'a/b/c'
            0x02, // payload. qos = 2
            0xDE,
            0xAD,
            0xBE,
            0xEF, // extra packets in the stream
        ];
        let mut stream = BytesMut::from(&stream[..]);
        let fixed_header = parse_fixed_header(stream.iter()).unwrap();
        let subscribe_bytes = stream.split_to(fixed_header.frame_length()).freeze();
        let packet = Subscribe::read(fixed_header, subscribe_bytes).unwrap();

        assert_eq!(
            packet,
            Subscribe {
                pkid: 260,
                filters: vec![
                    SubscribeFilter::new("a/+".to_owned(), QoS::AtMostOnce),
                    SubscribeFilter::new("#".to_owned(), QoS::AtLeastOnce),
                    SubscribeFilter::new("a/b/c".to_owned(), QoS::ExactlyOnce)
                ],
            }
        );
    }

    #[test]
    fn v4_subscribe_encoding_works() {
        let subscribe = Subscribe {
            pkid: 260,
            filters: vec![
                SubscribeFilter::new("a/+".to_owned(), QoS::AtMostOnce),
                SubscribeFilter::new("#".to_owned(), QoS::AtLeastOnce),
                SubscribeFilter::new("a/b/c".to_owned(), QoS::ExactlyOnce),
            ],
        };

        let mut buf = BytesMut::new();
        subscribe.write(&mut buf).unwrap();
        assert_eq!(
            buf,
            vec![
                0b1000_0010,
                20,
                0x01,
                0x04, // pkid = 260
                0x00,
                0x03,
                b'a',
                b'/',
                b'+', // topic filter = 'a/+'
                0x00, // qos = 0
                0x00,
                0x01,
                b'#', // topic filter = '#'
                0x01, // qos = 1
                0x00,
                0x05,
                b'a',
                b'/',
                b'b',
                b'/',
                b'c', // topic filter = 'a/b/c'
                0x02  // qos = 2
            ]
        );
    }
}

#[cfg(v5)]
#[cfg(test)]
mod test {
    use super::*;
    use alloc::vec;
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;

    fn v5_sample() -> Subscribe {
        let subscribe_properties = SubscribeProperties {
            id: Some(100),
            user_properties: vec![("test".to_owned(), "test".to_owned())],
        };

        let mut filter = SubscribeFilter::new("hello".to_owned(), QoS::AtLeastOnce);
        filter
            .set_nolocal(true)
            .set_preserve_retain(true)
            .set_retain_forward_rule(RetainForwardRule::Never);

        Subscribe {
            pkid: 42,
            filters: vec![filter],
            properties: Some(subscribe_properties),
        }
    }

    fn v5_sample_bytes() -> Vec<u8> {
        vec![
            0x82, // packet type
            0x1a, // remaining length
            0x00, 0x2a, // pkid
            0x0f, // properties len
            0x0b, 0x64, // subscription identifier
            0x26, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x04, 0x74, 0x65, 0x73,
            0x74, // user properties
            0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // filter
            0x2d, // options
        ]
    }

    #[test]
    fn v5_subscribe_parsing_works() {
        let mut stream = BytesMut::new();
        let packetstream = &v5_sample_bytes();

        stream.extend_from_slice(&packetstream[..]);

        let fixed_header = parse_fixed_header(stream.iter()).unwrap();
        let subscribe_bytes = stream.split_to(fixed_header.frame_length()).freeze();
        let subscribe = Subscribe::read(fixed_header, subscribe_bytes, Protocol::V5).unwrap();
        assert_eq!(subscribe, v5_sample());
    }

    #[test]
    fn v5_subscribe_encoding_works() {
        let publish = v5_sample();
        let mut buf = BytesMut::new();
        publish.write(&mut buf, Protocol::V5).unwrap();

        // println!("{:X?}", buf);
        // println!("{:#04X?}", &buf[..]);
        assert_eq!(&buf[..], v5_sample_bytes());
    }

    fn v5_sample2() -> Subscribe {
        let filter = SubscribeFilter::new("hello/world".to_owned(), QoS::AtLeastOnce);
        Subscribe {
            pkid: 42,
            filters: vec![filter],
            properties: None,
        }
    }

    fn v5_sample2_bytes() -> Vec<u8> {
        vec![
            0x82, 0x11, 0x00, 0x2a, 0x00, 0x00, 0x0b, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2f, 0x77,
            0x6f, 0x72, 0x6c, 0x64, 0x01,
        ]
    }

    #[test]
    fn v5_subscribe2_parsing_works() {
        let mut stream = BytesMut::new();
        let packetstream = &v5_sample2_bytes();

        stream.extend_from_slice(&packetstream[..]);

        let fixed_header = parse_fixed_header(stream.iter()).unwrap();
        let subscribe_bytes = stream.split_to(fixed_header.frame_length()).freeze();
        let subscribe = Subscribe::read(fixed_header, subscribe_bytes, Protocol::V5).unwrap();
        assert_eq!(subscribe, v5_sample2());
    }

    #[test]
    fn v5_subscribe2_encoding_works() {
        let publish = v5_sample2();
        let mut buf = BytesMut::new();
        publish.write(&mut buf, Protocol::V5).unwrap();

        // println!("{:X?}", buf);
        // println!("{:#04X?}", &buf[..]);
        assert_eq!(&buf[..], v5_sample2_bytes());
    }
}

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
        Subscribe { pkid: 0, filters }
    }

    pub fn new_many<T>(topics: T) -> Subscribe
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        Subscribe {
            pkid: 0,
            filters: topics.into_iter().collect(),
        }
    }

    pub fn empty_subscribe() -> Subscribe {
        Subscribe {
            pkid: 0,
            filters: Vec::new(),
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
        let len = 2 + self.filters.iter().fold(0, |s, t| s + t.len()); // len of pkid + vec![subscribe filter len]
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

        let subscribe = Subscribe { pkid, filters };

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

        // write filters
        for filter in self.filters.iter() {
            filter.write(buffer);
        }

        Ok(1 + remaining_len_bytes + remaining_len)
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
    fn subscribe_parsing_works() {
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
    fn subscribe_encoding_works() {
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

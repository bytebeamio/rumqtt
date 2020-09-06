use super::*;
use crate::*;
use alloc::vec::Vec;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Acknowledgement to subscribe
#[derive(Debug, Clone, PartialEq)]
pub struct SubAck {
    pub pkid: u16,
    pub return_codes: Vec<SubscribeReturnCodes>,
}

impl SubAck {
    pub fn new(pkid: u16, return_codes: Vec<SubscribeReturnCodes>) -> SubAck {
        SubAck { pkid, return_codes }
    }

    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.fixed_len;
        bytes.advance(variable_header_index);

        let pkid = bytes.get_u16();
        let mut payload_bytes = fixed_header.remaining_len - 2;
        let mut return_codes = Vec::with_capacity(payload_bytes);

        while payload_bytes > 0 {
            let return_code = bytes.get_u8();
            if return_code >> 7 == 1 {
                return_codes.push(SubscribeReturnCodes::Failure)
            } else {
                return_codes.push(SubscribeReturnCodes::Success(qos(return_code & 0x3)?));
            }
            payload_bytes -= 1
        }
        let suback = SubAck { pkid, return_codes };

        Ok(suback)
    }

    pub fn write(&self, payload: &mut BytesMut) -> Result<usize, Error> {
        payload.put_u8(0x90);
        let remaining_len = self.return_codes.len() + 2;
        let remaining_len_bytes = write_remaining_length(payload, remaining_len)?;
        payload.put_u16(self.pkid);
        let p: Vec<u8> = self
            .return_codes
            .iter()
            .map(|&code| match code {
                SubscribeReturnCodes::Success(qos) => qos as u8,
                SubscribeReturnCodes::Failure => 0x80,
            })
            .collect();

        payload.extend_from_slice(&p);
        Ok(1 + remaining_len_bytes + remaining_len)
    }
}

/// Subscription return code
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeReturnCodes {
    Success(QoS),
    Failure,
}

#[cfg(test)]
mod test {
    use super::*;
    use alloc::vec;
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;

    #[test]
    fn suback_stitching_works_correctly() {
        let stream = vec![
            0x90, 4, // packet type, flags and remaining len
            0x00, 0x0F, // variable header. pkid = 15
            0x01, 0x80, // payload. return codes [success qos1, failure]
            0xDE, 0xAD, 0xBE, 0xEF, // extra packets in the stream
        ];
        let mut stream = BytesMut::from(&stream[..]);

        let packet = mqtt_read(&mut stream, 100).unwrap();
        let packet = match packet {
            Packet::SubAck(packet) => packet,
            packet => panic!("Invalid packet = {:?}", packet),
        };

        assert_eq!(
            packet,
            SubAck {
                pkid: 15,
                return_codes: vec![
                    SubscribeReturnCodes::Success(QoS::AtLeastOnce),
                    SubscribeReturnCodes::Failure
                ]
            }
        );
    }
}

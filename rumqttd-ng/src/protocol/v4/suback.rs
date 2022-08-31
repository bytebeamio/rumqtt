use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::convert::{TryFrom, TryInto};

fn len(suback: &SubAck) -> usize {
    2 + suback.return_codes.len()
}

pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<SubAck, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);
    let pkid = read_u16(&mut bytes)?;

    if !bytes.has_remaining() {
        return Err(Error::MalformedPacket);
    }

    let mut return_codes = Vec::new();
    while bytes.has_remaining() {
        let return_code = read_u8(&mut bytes)?;
        return_codes.push(reason(return_code)?);
    }

    let suback = SubAck { pkid, return_codes };
    Ok(suback)
}

pub fn write(suback: &SubAck, buffer: &mut BytesMut) -> Result<usize, Error> {
    buffer.put_u8(0x90);
    let remaining_len = suback.len();
    let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

    buffer.put_u16(suback.pkid);
    let p: Vec<u8> = suback.return_codes.iter().map(|&c| code(c)).collect();

    buffer.extend_from_slice(&p);
    Ok(1 + remaining_len_bytes + remaining_len)
}

fn reason(code: u8) -> Result<SubscribeReasonCode, Error> {
    let v = match code {
        0 => SubscribeReasonCode::Success(QoS::AtMostOnce),
        1 => SubscribeReasonCode::Success(QoS::AtLeastOnce),
        2 => SubscribeReasonCode::Success(QoS::ExactlyOnce),
        128 => SubscribeReasonCode::Failure,
        v => return Err(Error::InvalidSubscribeReasonCode(v)),
    };

    Ok(v)
}

fn code(reason: SubscribeReasonCode) -> u8 {
    match reason {
        SubscribeReasonCode::Success(qos) => qos as u8,
        SubscribeReasonCode::Failure => 0x80,
        SubscribeReasonCode::QoS0 => 0,
        SubscribeReasonCode::QoS1 => 1,
        SubscribeReasonCode::QoS2 => 2,
        SubscribeReasonCode::Unspecified => 128,
        SubscribeReasonCode::ImplementationSpecific => 131,
        SubscribeReasonCode::NotAuthorized => 135,
        SubscribeReasonCode::TopicFilterInvalid => 143,
        SubscribeReasonCode::PkidInUse => 145,
        SubscribeReasonCode::QuotaExceeded => 151,
        SubscribeReasonCode::SharedSubscriptionsNotSupported => 158,
        SubscribeReasonCode::SubscriptionIdNotSupported => 161,
        SubscribeReasonCode::WildcardSubscriptionsNotSupported => 162,
    }
}

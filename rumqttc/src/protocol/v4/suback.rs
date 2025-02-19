use super::*;
use std::convert::{TryFrom, TryInto};

pub fn len(suback: &SubAck) -> usize {
    2 + suback.return_codes.len()
}

pub fn read(fixed_header: FixedHeader, mut bytes: &[u8]) -> Result<SubAck, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes = &bytes[variable_header_index..];
    let pkid = read_u16(&mut bytes)?;

    if bytes.is_empty() {
        return Err(Error::MalformedPacket);
    }

    let mut return_codes = Vec::new();
    while !bytes.is_empty() {
        let return_code = read_u8(&mut bytes)?;
        return_codes.push(reason(return_code)?);
    }

    let suback = SubAck {
        pkid,
        return_codes,
        properties: None,
    };
    Ok(suback)
}

pub fn write(suback: &SubAck, buffer: &mut Vec<u8>) -> Result<usize, Error> {
    buffer.push(0x90);
    let remaining_len = len(suback);
    let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

    buffer.extend_from_slice(&suback.pkid.to_be_bytes());
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
        SubscribeReasonCode::QoS0 => 0,
        SubscribeReasonCode::QoS1 => 1,
        SubscribeReasonCode::QoS2 => 2,
        // all other errors are Failure
        _ => 0x80,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{collection::vec, option, prelude::*};

    fn generate_reason_code() -> impl Strategy<Value = SubscribeReasonCode> {
        prop_oneof![
            Just(SubscribeReasonCode::Success(QoS::AtMostOnce)),
            Just(SubscribeReasonCode::Success(QoS::AtLeastOnce)),
            Just(SubscribeReasonCode::Success(QoS::ExactlyOnce)),
            Just(SubscribeReasonCode::Failure),
        ]
    }

    prop_compose! {
        fn generate_properties()(
            reason_string: Option<String>,
            user_properties: Vec<(String, String)>,
        ) -> SubAckProperties {
            SubAckProperties {
                reason_string,
                user_properties,
            }
        }
    }

    prop_compose! {
        fn generate_suback_packet()(
            pkid: u16,
            return_codes in vec(generate_reason_code(), 1..100),
            properties in option::of(generate_properties()),
        ) -> SubAck {
            SubAck { pkid, return_codes, properties }
        }
    }

    proptest! {
        #[test]
        fn test_roundtrip(suback in generate_suback_packet()) {
            let mut buffer = Vec::new();
            let n = write(&suback, &mut buffer).unwrap();
            let fixed_header = parse_fixed_header(&buffer).unwrap();
            let result = read(fixed_header, &buffer).unwrap();
            prop_assert_eq!(result, SubAck {
                pkid: suback.pkid,
                return_codes: suback.return_codes,
                properties: None,
            });
        }
    }
}

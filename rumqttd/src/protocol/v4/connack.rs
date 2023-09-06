use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

fn len() -> usize {
    // sesssion present + code
    1 + 1
}

pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<ConnAck, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);

    let flags = read_u8(&mut bytes)?;
    let return_code = read_u8(&mut bytes)?;

    let session_present = (flags & 0x01) == 1;
    let code = connect_return(return_code)?;
    let connack = ConnAck {
        session_present,
        code,
    };

    Ok(connack)
}

pub fn write(connack: &ConnAck, buffer: &mut BytesMut) -> Result<usize, Error> {
    let len = len();
    buffer.put_u8(0x20);

    let count = write_remaining_length(buffer, len)?;
    buffer.put_u8(connack.session_present as u8);
    buffer.put_u8(connect_code(connack.code));

    Ok(1 + count + len)
}

/// Connection return code type
fn connect_return(num: u8) -> Result<ConnectReturnCode, Error> {
    match num {
        0 => Ok(ConnectReturnCode::Success),
        1 => Ok(ConnectReturnCode::RefusedProtocolVersion),
        2 => Ok(ConnectReturnCode::ClientIdentifierNotValid),
        3 => Ok(ConnectReturnCode::ServiceUnavailable),
        4 => Ok(ConnectReturnCode::BadUserNamePassword),
        5 => Ok(ConnectReturnCode::NotAuthorized),
        num => Err(Error::InvalidConnectReturnCode(num)),
    }
}

fn connect_code(return_code: ConnectReturnCode) -> u8 {
    match return_code {
        ConnectReturnCode::Success => 0,
        ConnectReturnCode::RefusedProtocolVersion => 1,
        ConnectReturnCode::ClientIdentifierNotValid => 2,
        ConnectReturnCode::ServiceUnavailable => 3,
        ConnectReturnCode::BadUserNamePassword => 4,
        ConnectReturnCode::NotAuthorized => 5,
        _ => unreachable!(),
    }
}

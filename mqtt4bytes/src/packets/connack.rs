use crate::{FixedHeader, Error};
use bytes::{Bytes, Buf};

/// Return code in connack
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum ConnectReturnCode {
    Accepted = 0,
    RefusedProtocolVersion,
    RefusedIdentifierRejected,
    ServerUnavailable,
    BadUsernamePassword,
    NotAuthorized,
}

/// Acknowledgement to connect packet
#[derive(Debug, Clone, PartialEq)]
pub struct ConnAck {
    pub session_present: bool,
    pub code: ConnectReturnCode,
}

impl ConnAck {

}

impl ConnAck {
    pub fn new(code: ConnectReturnCode, session_present: bool) -> ConnAck {
        ConnAck {
            code,
            session_present,
        }
    }

    pub(crate) fn assemble(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
        let variable_header_index = fixed_header.header_len;
        bytes.advance(variable_header_index);

        if fixed_header.remaining_len != 2 {
            return Err(Error::PayloadSizeIncorrect);
        }

        let flags = bytes.get_u8();
        let return_code = bytes.get_u8();

        let session_present = (flags & 0x01) == 1;
        let code = connect_return(return_code)?;
        let connack = ConnAck {
            session_present,
            code,
        };

        Ok(connack)
    }
}

/// Connection return code type
fn connect_return(num: u8) -> Result<ConnectReturnCode, Error> {
    match num {
        0 => Ok(ConnectReturnCode::Accepted),
        1 => Ok(ConnectReturnCode::BadUsernamePassword),
        2 => Ok(ConnectReturnCode::NotAuthorized),
        3 => Ok(ConnectReturnCode::RefusedIdentifierRejected),
        4 => Ok(ConnectReturnCode::RefusedProtocolVersion),
        5 => Ok(ConnectReturnCode::ServerUnavailable),
        num => Err(Error::InvalidConnectReturnCode(num)),
    }
}

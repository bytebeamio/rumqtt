use derive_more::From;
use std::io;
use std::string::FromUtf8Error;

pub mod mqtt4;

// TODO Probably convert this to io::Error (for simplicity) and provide a function for meaningful enum?
#[derive(Debug, From)]
pub enum Error {
    InvalidConnectReturnCode(u8),
    InvalidProtocolLevel(String, u8),
    IncorrectPacketFormat,
    UnsupportedQoS,
    UnsupportedPacketType(u8),
    PayloadSizeIncorrect,
    PayloadTooLong,
    PayloadSizeLimitExceeded,
    PayloadRequired,
    #[from]
    TopicNameMustNotContainNonUtf8(FromUtf8Error),
    MalformedRemainingLength,
    #[from]
    Io(io::Error),
}

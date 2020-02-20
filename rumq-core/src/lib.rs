use derive_more::From;
use std::io;
use std::string::FromUtf8Error;

pub mod mqtt4;

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

use std::io;
use std::string::FromUtf8Error;

pub mod mqtt4;

// TODO Probably convert this to io::Error (for simplicity) and provide a function for meaningful enum?
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid connect return code `{0}`")]
    InvalidConnectReturnCode(u8),
    #[error("Invalid level `{1}` for protocol `{0}`")]
    InvalidProtocolLevel(String, u8),
    #[error("Incorrect packet format")]
    IncorrectPacketFormat,
    #[error("Unsupported QoS")]
    UnsupportedQoS,
    #[error("Unsupported packet type `{0}`")]
    UnsupportedPacketType(u8),
    #[error("Payload size incorrect")]
    PayloadSizeIncorrect,
    #[error("Payload too long")]
    PayloadTooLong,
    #[error("Payload size limit exceeded")]
    PayloadSizeLimitExceeded,
    #[error("Payload required")]
    PayloadRequired,
    #[error("Topic name must only contain valid UTF-8")]
    TopicNameMustNotContainNonUtf8(#[from] FromUtf8Error),
    #[error("Malformed remaining length")]
    MalformedRemainingLength,
    #[error("Io")]
    Io(#[from] io::Error),
}

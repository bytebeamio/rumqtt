// Defines common interface for both sync and async clients!

use flume::{SendError, TrySendError};

use super::Request;

/// Client Error
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Failed to send mqtt requests to eventloop")]
    Request(Request),
    #[error("Failed to send mqtt requests to eventloop")]
    TryRequest(Request),
}

impl From<SendError<Request>> for ClientError {
    fn from(e: SendError<Request>) -> Self {
        Self::Request(e.into_inner())
    }
}

impl From<TrySendError<Request>> for ClientError {
    fn from(e: TrySendError<Request>) -> Self {
        Self::TryRequest(e.into_inner())
    }
}

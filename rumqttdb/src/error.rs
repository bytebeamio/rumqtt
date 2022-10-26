use flume::RecvTimeoutError;
use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/o failure = {0}")]
    IO(io::Error),
    #[error("Ureq error = {0}")]
    Hyper(#[from] ureq::Error),
    #[error("bad response: {0}")]
    BadResponse(String),
    #[error("Custrom error: {0}")]
    Custom(String),
    #[error("invalid params: {0}")]
    InvalidParams(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Postgres error: {0}")]
    PgError(#[from] sqlx::Error),
    #[error("SerdeJSON error: {0}")]
    SerdeJSONError(#[from] serde_json::Error),
    #[error("Rx error = {0}")]
    Recv(#[from] RecvTimeoutError),
}

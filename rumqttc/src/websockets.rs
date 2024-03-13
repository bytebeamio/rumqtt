use http::{header::ToStrError, Response};

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Websocket response does not contain subprotocol header")]
    SubprotocolHeaderMissing,
    #[error("MQTT not in subprotocol header: {0}")]
    SubprotocolMqttMissing(String),
    #[error("Subprotocol header couldn't be converted into string representation")]
    HeaderToStr(#[from] ToStrError),
}

pub(crate) fn validate_response_headers(
    response: Response<Option<Vec<u8>>>,
) -> Result<(), ValidationError> {
    let subprotocol = response
        .headers()
        .get("Sec-WebSocket-Protocol")
        .ok_or(ValidationError::SubprotocolHeaderMissing)?
        .to_str()?;

    // Server must respond with Sec-WebSocket-Protocol header value of "mqtt"
    // https://http.dev/ws#sec-websocket-protocol
    if subprotocol.trim() != "mqtt" {
        return Err(ValidationError::SubprotocolMqttMissing(
            subprotocol.to_owned(),
        ));
    }

    Ok(())
}

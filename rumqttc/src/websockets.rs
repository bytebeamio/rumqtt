use http::Response;

#[derive(Debug, thiserror::Error)]
pub enum UrlError {
    #[error("Invalid protocol specified inside url.")]
    Protocol,
    #[error("Couldn't parse host from url.")]
    Host,
    #[error("Couldn't parse host url.")]
    Parse(#[from] http::uri::InvalidUri),
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Websocket response does not contain subprotocol header")]
    SubprotocolHeaderMissing,
    #[error("Websocket subprotocol header: {0}")]
    SubprotocolMqtt(String),
}

pub(crate) fn validate_response_headers(
    response: Response<Option<Vec<u8>>>,
) -> Result<(), ValidationError> {
    let val = response
        .headers()
        .get("Sec-WebSocket-Protocol")
        .ok_or(ValidationError::SubprotocolHeaderMissing)?;

    let sub_protocol = val
        .to_str()
        .map_err(|_| ValidationError::SubprotocolHeaderMissing)?;

    if sub_protocol != "mqtt" {
        return Err(ValidationError::SubprotocolMqtt(sub_protocol.to_owned()));
    }

    Ok(())
}

pub(crate) fn split_url(url: &str) -> Result<(String, u16), UrlError> {
    let uri = url.parse::<http::Uri>()?;
    let domain = domain(&uri).ok_or(UrlError::Protocol)?;
    let port = port(&uri).ok_or(UrlError::Host)?;
    Ok((domain, port))
}

fn domain(uri: &http::Uri) -> Option<String> {
    uri.host().map(|host| {
        // If host is an IPv6 address, it might be surrounded by brackets. These brackets are
        // *not* part of a valid IP, so they must be stripped out.
        //
        // The URI from the request is guaranteed to be valid, so we don't need a separate
        // check for the closing bracket.
        let host = if host.starts_with('[') {
            &host[1..host.len() - 1]
        } else {
            host
        };

        host.to_owned()
    })
}

fn port(uri: &http::Uri) -> Option<u16> {
    uri.port_u16().or_else(|| match uri.scheme_str() {
        Some("wss") => Some(443),
        Some("ws") => Some(80),
        _ => None,
    })
}

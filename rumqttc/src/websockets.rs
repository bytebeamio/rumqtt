use std::{pin::Pin, task::Context};

use async_tungstenite::{
    bytes::Sender,
    tungstenite::{Error, Message},
    ByteReader, ByteWriter, WebSocketReceiver, WebSocketSender, WebSocketStream,
};
use futures_util::Stream;
use http::{header::ToStrError, Response};
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite};

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

pin_project! {

/// Takes a [`WebSocketStream`] and makes it into a byte IO stream
/// compatible with the rest of rumqttc.
pub(crate) struct WsStream<S> {
    #[pin]
    read_half: ByteReader<WebSocketReceiver<S>>,
    #[pin]
    write_half: ByteWriter<WebSocketSender<S>>,
}
}

impl<S> WsStream<S>
where
    S: Unpin + futures_io::AsyncWrite + futures_io::AsyncRead,
{
    pub fn new(stream: WebSocketStream<S>) -> Self {
        let (sender, receiver) = stream.split();

        Self {
            read_half: ByteReader::new(receiver),
            write_half: ByteWriter::new(sender),
        }
    }
}

impl<S> AsyncRead for WsStream<S>
where
    WebSocketReceiver<S>: Stream<Item = Result<Message, Error>> + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.project();
        this.read_half.poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for WsStream<S>
where
    WebSocketSender<S>: Sender + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let this = self.project();
        this.write_half.poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = self.project();
        this.write_half.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = self.project();
        this.write_half.poll_shutdown(cx)
    }
}

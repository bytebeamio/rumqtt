use tokio::net::TcpStream;
use tokio_rustls::{client::TlsStream, TlsConnector};
use tokio_rustls::rustls::{ClientConfig, Certificate, PrivateKey};
use tokio::timer::{timeout, Timeout};

use derive_more::From;

use crate::MqttOptions;
use std::time::Duration;
use std::net::AddrParseError;
use std::io;
use std::sync::Arc;
use tokio_rustls::webpki::{self, DNSNameRef, InvalidDNSNameError};
use tokio_io::{AsyncRead, AsyncWrite};
use futures_util::task::Context;
use std::pin::Pin;
use rumq_core::{MqttRead, MqttWrite};
use std::io::{Cursor, BufReader};
use std::task::Poll;

pub enum NetworkStream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
    #[cfg(test)]
    Vec(Vec<u8>)
}

#[derive(From, Debug)]
pub enum Error {
    Addr(AddrParseError),
    Io(io::Error),
    Timeout(timeout::Elapsed),
    DNSName(InvalidDNSNameError),
    WebPki(webpki::Error),
    NoValidCertInChain
}

pub async fn connect(options: &MqttOptions, timeout: Duration) -> Result<NetworkStream, Error> {
    let addr = format!("{}:{}", options.broker_addr, options.port);
    let tcp = Timeout::new( async {
        TcpStream::connect(addr).await
    }, timeout).await??;

    let mut config = ClientConfig::new();

    // Add ca to root store if the connection is TLS
    // NOTE: Adding DER file isn't feasible as some of the chain information
    // is lost while converting from pem to der. This method iterates through all the
    // certs in the chain, converts each to der and adds them to root store
    // TODO: Check if there is a better way to do this
    match options.ca.as_ref() {
        Some(ca) => {
            let mut ca = BufReader::new(Cursor::new(ca));
            if config.root_store.add_pem_file(&mut ca)?.0 == 0 {
                return Err(Error::NoValidCertInChain)
            }
        }
        None => {
            let stream = NetworkStream::Tcp(tcp);
            return Ok(stream);
        }
    }

    // Add der encoded client cert and key
    if let Some(client) = options.client_auth.as_ref() {
        let cert_chain = vec![Certificate(client.0.clone())];
        let key = PrivateKey(client.1.clone());
        config.set_single_client_cert(cert_chain, key);
    }

    // Set ALPN
    if let Some(alpn) = options.alpn.as_ref() {
        config.set_protocols(&alpn);
    }

    let connector = TlsConnector::from(Arc::new(config));
    let domain = DNSNameRef::try_from_ascii_str(&options.broker_addr)?;
    let tls = connector.connect(domain, tcp).await?;
    Ok(NetworkStream::Tls(tls))
}

#[cfg(test)]
pub async fn vconnect(options: MqttOptions) -> Result<NetworkStream, Error> {
    Ok(NetworkStream::Vec(Vec::new()))
}


impl AsyncRead for NetworkStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, io::Error>> {
        match self.get_mut() {
            NetworkStream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            NetworkStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(test)]
            NetworkStream::Vec(stream) => {
                let mut stream: &[u8] = &stream;
                Pin::new(&mut stream).poll_read(cx, buf)
            }
        }
    }
}

impl AsyncWrite for NetworkStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, io::Error>> {
        match self.get_mut() {
            NetworkStream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            NetworkStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(test)]
            NetworkStream::Vec(stream) => Pin::new(stream).poll_write(cx, buf)
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            NetworkStream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            NetworkStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(test)]
            NetworkStream::Vec(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            NetworkStream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            NetworkStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(test)]
            NetworkStream::Vec(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

impl MqttRead for NetworkStream {}
impl MqttWrite for NetworkStream {}


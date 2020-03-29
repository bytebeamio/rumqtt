use tokio_rustls::{client::TlsStream, TlsConnector};
use tokio_rustls::rustls::{ClientConfig, TLSError};
use tokio_rustls::webpki::{self, DNSNameRef, InvalidDNSNameError};
use tokio_rustls::rustls::internal::pemfile::{ certs, rsa_private_keys };
use tokio::net::TcpStream;
use derive_more::From;

use crate::MqttOptions;

use std::net::AddrParseError;
use std::io;
use std::sync::Arc;
use std::io::{Cursor, BufReader};

#[derive(From, Debug)]
pub enum Error {
    Addr(AddrParseError),
    Io(io::Error),
    WebPki(webpki::Error),
    DNSName(InvalidDNSNameError),
    TLS(TLSError),
    NoValidCertInChain
}

pub async fn tls_connect(options: &MqttOptions) -> Result<TlsStream<TcpStream>, Error> {
    let addr = format!("{}:{}", options.broker_addr, options.port);
    let tcp = TcpStream::connect(addr).await?; 
    let mut config = ClientConfig::new();

    // Add ca to root store if the connection is TLS
    // NOTE: Adding DER file isn't feasible as some of the chain information
    // is lost while converting from pem to der. This method iterates through all the
    // certs in the chain, converts each to der and adds them to root store
    // TODO: Check if there is a better way to do this
    let ca = options.ca.as_ref().unwrap();
    if config.root_store.add_pem_file(&mut BufReader::new(Cursor::new(ca)))?.0 == 0 {
        return Err(Error::NoValidCertInChain)
    }

    // Add der encoded client cert and key
    if let Some(client) = options.client_auth.as_ref() {
        let certs = certs(&mut BufReader::new(Cursor::new(client.0.clone())))?;
        let mut keys = rsa_private_keys(&mut BufReader::new(Cursor::new(client.1.clone())))?;
        config.set_single_client_cert(certs, keys.remove(0))?;
    }

    // Set ALPN
    if let Some(alpn) = options.alpn.as_ref() {
        config.set_protocols(&alpn);
    }

    let connector = TlsConnector::from(Arc::new(config));
    let domain = DNSNameRef::try_from_ascii_str(&options.broker_addr)?;
    let tls = connector.connect(domain, tcp).await?;
    Ok(tls)
}

pub async fn tcp_connect(options: &MqttOptions) -> Result<TcpStream, Error> {
    let addr = format!("{}:{}", options.broker_addr, options.port);
    let tcp = TcpStream::connect(addr).await?;
    Ok(tcp)
}

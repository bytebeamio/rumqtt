use tokio::net::TcpStream;
use tokio_rustls::rustls::internal::pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use tokio_rustls::rustls::{ClientConfig, TLSError};
use tokio_rustls::webpki::{self, DNSNameRef, InvalidDNSNameError};
use tokio_rustls::{client::TlsStream, TlsConnector};

use crate::{Key, MqttOptions, TlsConfiguration};

use std::io;
use std::io::{BufReader, Cursor};
use std::net::AddrParseError;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Addr")]
    Addr(#[from] AddrParseError),
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Web Pki")]
    WebPki(#[from] webpki::Error),
    #[error("DNS name")]
    DNSName(#[from] InvalidDNSNameError),
    #[error("TLS error")]
    TLS(#[from] TLSError),
    #[error("No valid cert in chain")]
    NoValidCertInChain,
}

// The cert handling functions return unit right now, this is a shortcut
impl From<()> for Error {
    fn from(_: ()) -> Self {
        Error::NoValidCertInChain
    }
}

pub async fn tls_connector(tls_config: &TlsConfiguration) -> Result<TlsConnector, Error> {
    let config = match tls_config {
        TlsConfiguration::Simple {
            ca,
            alpn,
            client_auth,
        } => {
            let mut config = ClientConfig::new();

            // Add ca to root store if the connection is TLS
            // NOTE: Adding DER file isn't feasible as some of the chain information
            // is lost while converting from pem to der. This method iterates through all the
            // certs in the chain, converts each to der and adds them to root store
            // TODO: Check if there is a better way to do this
            if config
                .root_store
                .add_pem_file(&mut BufReader::new(Cursor::new(ca)))?
                .0
                == 0
            {
                return Err(Error::NoValidCertInChain);
            }

            // Add der encoded client cert and key
            if let Some(client) = client_auth.as_ref() {
                let certs = certs(&mut BufReader::new(Cursor::new(client.0.clone())))?;
                // load appropriate Key as per the user request. The underlying signature algorithm
                // of key generation determines the Signature Algorithm during the TLS Handskahe.
                let read_keys = match &client.1 {
                    Key::RSA(k) => rsa_private_keys(&mut BufReader::new(Cursor::new(k.clone()))),
                    Key::ECC(k) => pkcs8_private_keys(&mut BufReader::new(Cursor::new(k.clone()))),
                };
                let keys = match read_keys {
                    Ok(v) => v,
                    Err(_e) => return Err(Error::NoValidCertInChain),
                };

                // Get the first key. Error if it's not valid
                let key = match keys.first() {
                    Some(k) => k.clone(),
                    None => return Err(Error::NoValidCertInChain),
                };

                config.set_single_client_cert(certs, key)?;
            }

            // Set ALPN
            if let Some(alpn) = alpn.as_ref() {
                config.set_protocols(&alpn);
            }

            Arc::new(config)
        }
        TlsConfiguration::Rustls(tls_client_config) => tls_client_config.clone(),
    };

    Ok(TlsConnector::from(config))
}

pub async fn tls_connect(
    options: &MqttOptions,
    tls_config: &TlsConfiguration,
) -> Result<TlsStream<TcpStream>, Error> {
    let addr = options.broker_addr.as_str();
    let port = options.port;
    let connector = tls_connector(tls_config).await?;
    let domain = DNSNameRef::try_from_ascii_str(&options.broker_addr)?;
    let tcp = TcpStream::connect((addr, port)).await?;
    let tls = connector.connect(domain, tcp).await?;
    Ok(tls)
}

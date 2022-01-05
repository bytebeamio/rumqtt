use tokio::net::TcpStream;
use tokio_rustls::rustls;
use tokio_rustls::rustls::client::InvalidDnsNameError;
use tokio_rustls::rustls::{
    Certificate, ClientConfig, OwnedTrustAnchor, PrivateKey, RootCertStore, ServerName,
};
use tokio_rustls::webpki;
use tokio_rustls::{client::TlsStream, TlsConnector};

use crate::{Key, MqttOptions, TlsConfiguration};

use std::convert::TryFrom;
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
    DNSName(#[from] InvalidDnsNameError),
    #[error("TLS error")]
    TLS(#[from] rustls::Error),
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
            // Add ca to root store if the connection is TLS
            let mut root_cert_store = RootCertStore::empty();
            let certs = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(ca)))?;

            let trust_anchors = certs.iter().map_while(|cert| {
                if let Ok(ta) = webpki::TrustAnchor::try_from_cert_der(&cert[..]) {
                    Some(OwnedTrustAnchor::from_subject_spki_name_constraints(
                        ta.subject,
                        ta.spki,
                        ta.name_constraints,
                    ))
                } else {
                    None
                }
            });

            root_cert_store.add_server_trust_anchors(trust_anchors);

            if root_cert_store.is_empty() {
                return Err(Error::NoValidCertInChain);
            }

            let config = ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_cert_store);

            // Add der encoded client cert and key
            let mut config = if let Some(client) = client_auth.as_ref() {
                let certs =
                    rustls_pemfile::certs(&mut BufReader::new(Cursor::new(client.0.clone())))?;
                // load appropriate Key as per the user request. The underlying signature algorithm
                // of key generation determines the Signature Algorithm during the TLS Handskahe.
                let read_keys = match &client.1 {
                    Key::RSA(k) => rustls_pemfile::rsa_private_keys(&mut BufReader::new(
                        Cursor::new(k.clone()),
                    )),
                    Key::ECC(k) => rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(
                        Cursor::new(k.clone()),
                    )),
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

                let certs = certs.into_iter().map(|cert| Certificate(cert)).collect();

                config.with_single_cert(certs, PrivateKey(key))?
            } else {
                config.with_no_client_auth()
            };

            // Set ALPN
            if let Some(alpn) = alpn.as_ref() {
                config.alpn_protocols.extend_from_slice(alpn);
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
    let domain = ServerName::try_from(addr)?;
    let tcp = TcpStream::connect((addr, port)).await?;
    let tls = connector.connect(domain, tcp).await?;
    Ok(tls)
}

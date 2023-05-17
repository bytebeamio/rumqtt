#[cfg(feature = "use-rustls")]
use tokio_rustls::rustls;
#[cfg(feature = "use-rustls")]
use tokio_rustls::rustls::client::InvalidDnsNameError;
#[cfg(feature = "use-rustls")]
use tokio_rustls::rustls::{
    Certificate, ClientConfig, OwnedTrustAnchor, PrivateKey, RootCertStore, ServerName,
};
#[cfg(feature = "use-rustls")]
use tokio_rustls::TlsConnector as RustlsConnector;

#[cfg(feature = "use-rustls")]
use crate::Key;
#[cfg(feature = "use-rustls")]
use std::convert::TryFrom;
#[cfg(feature = "use-rustls")]
use std::io::{BufReader, Cursor};
#[cfg(feature = "use-rustls")]
use std::sync::Arc;

use crate::framed::N;
use crate::TlsConfiguration;

#[cfg(feature = "use-native-tls")]
use tokio_native_tls::TlsConnector as NativeTlsConnector;

#[cfg(feature = "use-native-tls")]
use tokio_native_tls::native_tls::{Error as NativeTlsError, Identity};

use std::io;
use std::net::AddrParseError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error parsing IP address
    #[error("Addr")]
    Addr(#[from] AddrParseError),
    /// I/O related error
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[cfg(feature = "use-rustls")]
    /// Certificate/Name validation error
    #[error("Web Pki: {0}")]
    WebPki(#[from] webpki::Error),
    /// Invalid DNS name
    #[cfg(feature = "use-rustls")]
    #[error("DNS name")]
    DNSName(#[from] InvalidDnsNameError),
    #[cfg(feature = "use-rustls")]
    /// Error from rustls module
    #[error("TLS error: {0}")]
    TLS(#[from] rustls::Error),
    #[cfg(feature = "use-rustls")]
    /// No valid certificate in chain
    #[error("No valid certificate in chain")]
    NoValidCertInChain,
    #[cfg(feature = "use-native-tls")]
    #[error("Native TLS error {0}")]
    NativeTls(#[from] NativeTlsError),
}

// // The cert handling functions return unit right now, this is a shortcut
// impl From<()> for Error {
//     fn from(_: ()) -> Self {
//         Error::NoValidCertInChain
//     }
// }

#[cfg(feature = "use-rustls")]
pub async fn rustls_connector(tls_config: &TlsConfiguration) -> Result<RustlsConnector, Error> {
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

                let certs = certs.into_iter().map(Certificate).collect();

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
        #[allow(unreachable_patterns)]
        _ => unreachable!("This cannot be called for other TLS backends than Rustls"),
    };

    Ok(RustlsConnector::from(config))
}

#[cfg(feature = "use-native-tls")]
pub async fn native_tls_connector(
    tls_config: &TlsConfiguration,
) -> Result<NativeTlsConnector, Error> {
    let connector = match tls_config {
        TlsConfiguration::SimpleNative { ca, der, password } => {
            let cert = native_tls::Certificate::from_pem(ca)?;
            let identity = Identity::from_pkcs12(der, password)?;
            native_tls::TlsConnector::builder()
                .add_root_certificate(cert)
                .identity(identity)
                .build()?
        }
        TlsConfiguration::Native => native_tls::TlsConnector::new()?,
        #[allow(unreachable_patterns)]
        _ => unreachable!("This cannot be called for other TLS backends than Native TLS"),
    };

    Ok(connector.into())
}

pub async fn tls_connect(
    addr: &str,
    _port: u16,
    tls_config: &TlsConfiguration,
    tcp: Box<dyn N>,
) -> Result<Box<dyn N>, Error> {
    let tls: Box<dyn N> = match tls_config {
        #[cfg(feature = "use-rustls")]
        TlsConfiguration::Simple { .. } | TlsConfiguration::Rustls(_) => {
            let connector = rustls_connector(tls_config).await?;
            let domain = ServerName::try_from(addr)?;
            Box::new(connector.connect(domain, tcp).await?)
        }
        #[cfg(feature = "use-native-tls")]
        TlsConfiguration::Native | TlsConfiguration::SimpleNative { .. } => {
            let connector = native_tls_connector(tls_config).await?;
            Box::new(connector.connect(addr, tcp).await?)
        }
        #[allow(unreachable_patterns)]
        _ => panic!("Unknown or not enabled TLS backend configuration"),
    };
    Ok(tls)
}

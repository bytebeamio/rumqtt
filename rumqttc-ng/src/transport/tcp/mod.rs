mod connection;

#[cfg(feature = "use-rustls")]
use {
    rustls_pemfile::Item,
    tokio_rustls::rustls::{
        self,
        pki_types::{InvalidDnsNameError, ServerName},
        ClientConfig, RootCertStore,
    },
    tokio_rustls::TlsConnector as RustlsConnector,
    std::convert::TryFrom,
    std::io::{BufReader, Cursor},
    std::sync::Arc,
};

#[cfg(feature = "use-native-tls")]
use {
    tokio_native_tls::TlsConnector as NativeTlsConnector,
    tokio_native_tls::native_tls::{Error as NativeTlsError, Identity},
};

use tokio::io::{AsyncRead, AsyncWrite};
use std::io;
use std::net::AddrParseError;
use super::RustlsConfig;

struct TcpSettings {
    host: String,
    port: u16,
    #[cfg(feature = "use-rustls")]
    /// Rustls TLS configuration
    rustls_config: Option<RustlsConfig>,
    #[cfg(feature = "use-native-tls")]
    /// Native TLS configuration
    native_tls_config: Option<NativeTlsConfig>,
}

pub async fn connect(settings: &TcpSettings) -> Result<Box<dyn AsyncReadWrite>, Error> {
    // First establish TCP connection
    let tcp = tokio::net::TcpStream::connect((settings.host.as_str(), settings.port)).await?;
    let tcp: Box<dyn AsyncReadWrite> = Box::new(tcp);

    // If no TLS config is present, return TCP connection
    #[cfg(feature = "use-rustls")]
    if let Some(rustls_config) = &settings.rustls_config {
        match rustls_config {
            RustlsConfig::Simple { ca, alpn, client_auth } => {
                let connector = create_simple_rustls_connector(ca, alpn, client_auth).await?;
                let domain = ServerName::try_from(settings.host.as_str())?.to_owned();
                return Ok(Box::new(connector.connect(domain, tcp).await?));
            }
            RustlsConfig::Custom(config) => {
                let connector = create_custom_rustls_connector(config).await?;
                let domain = ServerName::try_from(settings.host.as_str())?.to_owned();
                return Ok(Box::new(connector.connect(domain, tcp).await?));
            }
        }
    }

    #[cfg(feature = "use-native-tls")]
    if let Some(native_config) = &settings.native_tls_config {
        let connector = native_tls_connector(native_config).await?;
        return Ok(Box::new(connector.connect(&settings.host, tcp).await?));
    }

    Ok(tcp)
}

#[cfg(feature = "use-rustls")]
async fn create_simple_rustls_connector(
    ca: &[u8],
    alpn: &Option<Vec<Vec<u8>>>,
    client_auth: &Option<(Vec<u8>, Vec<u8>)>,
) -> Result<RustlsConnector, Error> {
    let mut root_cert_store = RootCertStore::empty();
    let certs = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(ca)))
        .collect::<Result<Vec<_>, _>>()?;

    root_cert_store.add_parsable_certificates(certs);

    if root_cert_store.is_empty() {
        return Err(Error::NoValidCertInChain);
    }

    let config = ClientConfig::builder().with_root_certificates(root_cert_store);
    let mut config = if let Some(client) = client_auth {
        create_client_auth_config(config, &client.0, &client.1)?
    } else {
        config.with_no_client_auth()
    };

    if let Some(alpn) = alpn {
        config.alpn_protocols.extend_from_slice(alpn);
    }

    Ok(RustlsConnector::from(Arc::new(config)))
}

#[cfg(feature = "use-rustls")]
async fn create_custom_rustls_connector(config: &Arc<ClientConfig>) -> Result<RustlsConnector, Error> {
    Ok(RustlsConnector::from(config.clone()))
}

#[cfg(feature = "use-native-tls")]
pub async fn native_tls_connector(
    tls_config: &TlsConfiguration,
) -> Result<NativeTlsConnector, Error> {
    let connector = match tls_config {
        TlsConfiguration::SimpleNative { ca, client_auth } => {
            let cert = native_tls::Certificate::from_pem(ca)?;

            let mut connector_builder = native_tls::TlsConnector::builder();
            connector_builder.add_root_certificate(cert);

            if let Some((der, password)) = client_auth {
                let identity = Identity::from_pkcs12(der, password)?;
                connector_builder.identity(identity);
            }

            connector_builder.build()?
        }
        TlsConfiguration::Native => native_tls::TlsConnector::new()?,
        TlsConfiguration::NativeConnector(connector) => connector.to_owned(),
        #[allow(unreachable_patterns)]
        _ => unreachable!("This cannot be called for other TLS backends than Native TLS"),
    };

    Ok(connector.into())
}

#[cfg(feature = "use-rustls")]
fn create_client_auth_config(
    builder: rustls::ConfigBuilder<rustls::ClientConfig, rustls::client::WantsClientCert>,
    cert: &[u8],
    key: &[u8],
) -> Result<rustls::ClientConfig, Error> {
    let certs = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(cert)))
        .collect::<Result<Vec<_>, _>>()?;

    if certs.is_empty() {
        return Err(Error::NoValidCertInChain);
    }

    // Create buffer for key file and read until we find a valid key
    let mut key_buffer = BufReader::new(Cursor::new(key));
    let key = loop {
        let item = rustls_pemfile::read_one(&mut key_buffer)?;
        match item {
            Some(Item::Sec1Key(key)) => break key.into(),
            Some(Item::Pkcs1Key(key)) => break key.into(),
            Some(Item::Pkcs8Key(key)) => break key.into(),
            None => return Err(Error::NoValidKey),
            _ => {}
        }
    };

    Ok(builder.with_client_auth_cert(certs, key)?)
}


pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

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
    /// No valid CA cert found
    #[error("No valid CA certificate provided")]
    NoValidCertInChain,
    #[cfg(feature = "use-rustls")]
    /// No valid client cert found
    #[error("No valid certificate for client authentication in chain")]
    NoValidClientCertInChain,
    #[cfg(feature = "use-rustls")]
    /// No valid key found
    #[error("No valid key in chain")]
    NoValidKeyInChain,
    #[cfg(feature = "use-native-tls")]
    #[error("Native TLS error {0}")]
    NativeTls(#[from] NativeTlsError),
    #[error("No valid key found in the provided key file")]
    NoValidKey,
}

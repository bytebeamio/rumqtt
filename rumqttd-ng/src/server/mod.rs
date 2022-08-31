use std::fs::File;

#[cfg(feature = "native-tls")]
use std::io::Read;
#[cfg(feature = "native-tls")]
use tokio_native_tls::native_tls;
#[cfg(feature = "native-tls")]
use tokio_native_tls::native_tls::Error as NativeTlsError;

use rustls_pemfile::{certs, rsa_private_keys};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::rustls::{
    server::AllowAnyAuthenticatedClient, Certificate, Error as RustlsError, PrivateKey,
};

use std::io::{self, BufReader};
use std::sync::Arc;

mod broker;

pub use self::broker::Broker;

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[error("I/O {0}")]
    Io(#[from] io::Error),
    #[cfg(feature = "native-tls")]
    #[error("Native TLS error {0}")]
    NativeTls(#[from] NativeTlsError),
    #[error("Rustls error {0}")]
    Rustls(#[from] RustlsError),
    #[error("Server cert file {0} not found")]
    ServerCertNotFound(String),
    #[error("Invalid server cert file {0}")]
    InvalidServerCert(String),
    #[error("Invalid CA cert file {0}")]
    InvalidCACert(String),
    #[error("Invalid server key file {0}")]
    InvalidServerKey(String),
    #[error("Server private key file {0} not found")]
    ServerKeyNotFound(String),
    #[error("CA file {0} no found")]
    CaFileNotFound(String),
    #[cfg(not(feature = "native-tls"))]
    NativeTlsNotEnabled,
    #[error("Invalid tenant id = {0}")]
    InvalidTenantId(String),
    #[error("Invalid tenant certificate")]
    InvalidTenant,
    #[error("Tenant id missing in certificate")]
    MissingTenantId,
    #[error("Tenant id missing in certificate")]
    CertificateParse,
}

/// Extract uid from certificate's subject organization field
fn extract_tenant_id(der: &Vec<u8>) -> Result<String, Error> {
    let (_, cert) =
        x509_parser::parse_x509_certificate(der).map_err(|_| Error::CertificateParse)?;
    let tenant_id = match cert.subject().iter_organization().next() {
        Some(org) => match org.as_str() {
            Ok(val) => val.to_string(),
            Err(_) => return Err(Error::InvalidTenant),
        },
        None => return Err(Error::MissingTenantId),
    };

    if tenant_id.chars().any(|c| !c.is_alphanumeric()) {
        return Err(Error::InvalidTenantId(tenant_id));
    }

    Ok(tenant_id)
}

#[allow(dead_code)]
enum TLSAcceptor {
    Rustls {
        acceptor: tokio_rustls::TlsAcceptor,
    },
    #[cfg(feature = "native-tls")]
    NativeTLS {
        acceptor: tokio_native_tls::TlsAcceptor,
    },
}

#[cfg(feature = "native-tls")]
fn create_nativetls_acceptor(
    pkcs12_path: &String,
    pkcs12_pass: &String,
) -> Result<TLSAcceptor, Error> {
    // Get certificates
    let cert_file = File::open(&pkcs12_path);
    let mut cert_file = cert_file.map_err(|_| Error::ServerCertNotFound(pkcs12_path.clone()))?;

    // Read cert into memory
    let mut buf = Vec::new();
    cert_file
        .read_to_end(&mut buf)
        .map_err(|_| Error::InvalidServerCert(pkcs12_path.clone()))?;

    // Get the identity
    let identity = native_tls::Identity::from_pkcs12(&buf, &pkcs12_pass)
        .map_err(|_| Error::InvalidServerCert(pkcs12_path.clone()))?;

    // Builder
    let builder = native_tls::TlsAcceptor::builder(identity).build()?;

    // Create acceptor
    let acceptor = tokio_native_tls::TlsAcceptor::from(builder);
    Ok(TLSAcceptor::NativeTLS { acceptor })
}

#[allow(dead_code)]
#[cfg(not(feature = "native-tls"))]
fn create_nativetls_acceptor(
    _pkcs12_path: &String,
    _pkcs12_pass: &String,
) -> Result<TLSAcceptor, Error> {
    Err(Error::NativeTlsNotEnabled)
}

fn create_rustls_accpetor(
    cert_path: &String,
    key_path: &String,
    ca_path: &String,
) -> Result<TLSAcceptor, Error> {
    let (certs, key) = {
        // Get certificates
        let cert_file = File::open(&cert_path);
        let cert_file = cert_file.map_err(|_| Error::ServerCertNotFound(cert_path.clone()))?;
        let certs = certs(&mut BufReader::new(cert_file));
        let certs = certs.map_err(|_| Error::InvalidServerCert(cert_path.to_string()))?;
        let certs = certs
            .iter()
            .map(|cert| Certificate(cert.to_owned()))
            .collect();

        // Get private key
        let key_file = File::open(&key_path);
        let key_file = key_file.map_err(|_| Error::ServerKeyNotFound(key_path.clone()))?;
        let keys = rsa_private_keys(&mut BufReader::new(key_file));
        let keys = keys.map_err(|_| Error::InvalidServerKey(key_path.clone()))?;

        // Get the first key
        let key = match keys.first() {
            Some(k) => k.clone(),
            None => return Err(Error::InvalidServerKey(key_path.clone())),
        };

        (certs, PrivateKey(key))
    };

    // client authentication with a CA. CA isn't required otherwise
    let server_config = {
        use tokio_rustls::rustls::{RootCertStore, ServerConfig};

        let ca_file = File::open(ca_path);
        let ca_file = ca_file.map_err(|_| Error::CaFileNotFound(ca_path.clone()))?;
        let ca_file = &mut BufReader::new(ca_file);
        let ca_certs = rustls_pemfile::certs(ca_file)?;
        let ca_cert = ca_certs
            .first()
            .map(|c| Certificate(c.to_owned()))
            .ok_or(Error::InvalidCACert(ca_path.to_string()))?;

        let mut store = RootCertStore::empty();
        store
            .add(&ca_cert)
            .map_err(|_| Error::InvalidCACert(ca_path.to_string()))?;

        ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(AllowAnyAuthenticatedClient::new(store))
            .with_single_cert(certs, key)?
    };

    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    Ok(TLSAcceptor::Rustls { acceptor })
}

pub trait IO: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Sync + Unpin> IO for T {}

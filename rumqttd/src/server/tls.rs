use std::fs::File;

#[cfg(feature = "use-native-tls")]
use std::io::Read;
use tokio::net::TcpStream;
#[cfg(feature = "use-native-tls")]
use tokio_native_tls::native_tls;
#[cfg(feature = "use-native-tls")]
use tokio_native_tls::native_tls::Error as NativeTlsError;

#[cfg(feature = "use-rustls")]
use tokio_rustls::rustls::{
    server::AllowAnyAuthenticatedClient, Certificate, Error as RustlsError, PrivateKey,
    RootCertStore, ServerConfig,
};

#[cfg(feature = "use-rustls")]
use std::{io::BufReader, sync::Arc};

use crate::link::network::N;
use crate::TlsConfig;

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[error("I/O {0}")]
    Io(#[from] std::io::Error),
    #[cfg(feature = "use-native-tls")]
    #[error("Native TLS error {0}")]
    NativeTls(#[from] NativeTlsError),
    #[error("No peer certificate")]
    NoPeerCertificate,
    #[cfg(feature = "use-rustls")]
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
    #[cfg(not(feature = "use-native-tls"))]
    NativeTlsNotEnabled,
    #[cfg(not(feature = "use-rustls"))]
    RustlsNotEnabled,
    #[error("Invalid tenant id = {0}")]
    InvalidTenantId(String),
    #[error("Invalid tenant certificate")]
    InvalidTenant,
    #[error("Tenant id missing in certificate")]
    MissingTenantId,
    #[error("Tenant id missing in certificate")]
    CertificateParse,
}

#[cfg(feature = "use-rustls")]
/// Extract uid from certificate's subject organization field
fn extract_tenant_id(der: &[u8]) -> Result<Option<String>, Error> {
    let (_, cert) =
        x509_parser::parse_x509_certificate(der).map_err(|_| Error::CertificateParse)?;
    let tenant_id = match cert.subject().iter_organization().next() {
        Some(org) => match org.as_str() {
            Ok(val) => val.to_string(),
            Err(_) => return Err(Error::InvalidTenant),
        },
        None => {
            #[cfg(feature = "validate-tenant-prefix")]
            return Err(Error::MissingTenantId);
            #[cfg(not(feature = "validate-tenant-prefix"))]
            return Ok(None);
        }
    };

    if tenant_id.chars().any(|c| !c.is_alphanumeric()) {
        return Err(Error::InvalidTenantId(tenant_id));
    }

    Ok(Some(tenant_id))
}

#[allow(dead_code)]
pub enum TLSAcceptor {
    #[cfg(feature = "use-rustls")]
    Rustls { acceptor: tokio_rustls::TlsAcceptor },
    #[cfg(feature = "use-native-tls")]
    NativeTLS {
        acceptor: tokio_native_tls::TlsAcceptor,
    },
}

impl TLSAcceptor {
    pub fn new(config: &TlsConfig) -> Result<Self, Error> {
        match config {
            #[cfg(feature = "use-rustls")]
            TlsConfig::Rustls {
                capath,
                certpath,
                keypath,
            } => Self::rustls(capath, certpath, keypath),
            #[cfg(feature = "use-native-tls")]
            TlsConfig::NativeTls {
                pkcs12path,
                pkcs12pass,
            } => Self::native_tls(pkcs12path, pkcs12pass),
            #[cfg(not(feature = "use-rustls"))]
            TlsConfig::Rustls { .. } => Err(Error::RustlsNotEnabled),
            #[cfg(not(feature = "use-native-tls"))]
            TlsConfig::NativeTls { .. } => Err(Error::NativeTlsNotEnabled),
        }
    }

    pub async fn accept(&self, stream: TcpStream) -> Result<(Option<String>, Box<dyn N>), Error> {
        match self {
            #[cfg(feature = "use-rustls")]
            TLSAcceptor::Rustls { acceptor } => {
                let stream = acceptor.accept(stream).await?;
                let (_, session) = stream.get_ref();
                let peer_certificates = session
                    .peer_certificates()
                    .ok_or(Error::NoPeerCertificate)?;
                let tenant_id = extract_tenant_id(&peer_certificates[0].0)?;
                let network = Box::new(stream);
                Ok((tenant_id, network))
            }
            #[cfg(feature = "use-native-tls")]
            TLSAcceptor::NativeTLS { acceptor } => {
                let stream = acceptor.accept(stream).await?;
                // native-tls doesn't support client certificate verification
                // let session = stream.get_ref();
                // let peer_certificate = session
                //     .peer_certificate()?
                //     .ok_or(Error::NoPeerCertificate)?
                //     .to_der()?;
                // let tenant_id = extract_tenant_id(&peer_certificate)?;
                let network = Box::new(stream);
                Ok((None, network))
            }
        }
    }

    #[cfg(feature = "use-native-tls")]
    fn native_tls(pkcs12_path: &String, pkcs12_pass: &str) -> Result<Self, Error> {
        // Get certificates
        let cert_file = File::open(pkcs12_path);
        let mut cert_file =
            cert_file.map_err(|_| Error::ServerCertNotFound(pkcs12_path.clone()))?;

        // Read cert into memory
        let mut buf = Vec::new();
        cert_file
            .read_to_end(&mut buf)
            .map_err(|_| Error::InvalidServerCert(pkcs12_path.clone()))?;

        // Get the identity
        let identity = native_tls::Identity::from_pkcs12(&buf, pkcs12_pass)
            .map_err(|_| Error::InvalidServerCert(pkcs12_path.clone()))?;

        // Builder
        let builder = native_tls::TlsAcceptor::builder(identity).build()?;

        // Create acceptor
        let acceptor = tokio_native_tls::TlsAcceptor::from(builder);
        Ok(TLSAcceptor::NativeTLS { acceptor })
    }

    #[cfg(feature = "use-rustls")]
    fn rustls(
        ca_path: &String,
        cert_path: &String,
        key_path: &String,
    ) -> Result<TLSAcceptor, Error> {
        let (certs, key) = {
            // Get certificates
            let cert_file = File::open(cert_path);
            let cert_file = cert_file.map_err(|_| Error::ServerCertNotFound(cert_path.clone()))?;
            let certs = rustls_pemfile::certs(&mut BufReader::new(cert_file));
            let certs = certs.map_err(|_| Error::InvalidServerCert(cert_path.to_string()))?;
            let certs = certs
                .iter()
                .map(|cert| Certificate(cert.to_owned()))
                .collect();

            // Get private key
            let key_file = File::open(key_path);
            let key_file = key_file.map_err(|_| Error::ServerKeyNotFound(key_path.clone()))?;
            let keys = rustls_pemfile::rsa_private_keys(&mut BufReader::new(key_file));
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
            let ca_file = File::open(ca_path);
            let ca_file = ca_file.map_err(|_| Error::CaFileNotFound(ca_path.clone()))?;
            let ca_file = &mut BufReader::new(ca_file);
            let ca_certs = rustls_pemfile::certs(ca_file)?;
            let ca_cert = ca_certs
                .first()
                .map(|c| Certificate(c.to_owned()))
                .ok_or_else(|| Error::InvalidCACert(ca_path.to_string()))?;

            let mut store = RootCertStore::empty();
            store
                .add(&ca_cert)
                .map_err(|_| Error::InvalidCACert(ca_path.to_string()))?;

            ServerConfig::builder()
                .with_safe_defaults()
                .with_client_cert_verifier(Arc::new(AllowAnyAuthenticatedClient::new(store)))
                .with_single_cert(certs, key)?
        };

        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        Ok(TLSAcceptor::Rustls { acceptor })
    }
}

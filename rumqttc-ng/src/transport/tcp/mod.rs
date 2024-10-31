use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::rustls::ClientConfig;


mod connection;
mod tls;

pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

/// TLS configuration method
#[derive(Clone, Debug)]
#[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
pub enum TlsConfiguration {
    #[cfg(feature = "use-rustls")]
    Simple {
        /// ca certificate
        ca: Vec<u8>,
        /// alpn settings
        alpn: Option<Vec<Vec<u8>>>,
        /// tls client_authentication
        client_auth: Option<(Vec<u8>, Vec<u8>)>,
    },
    #[cfg(feature = "use-native-tls")]
    SimpleNative {
        /// ca certificate
        ca: Vec<u8>,
        /// pkcs12 binary der and
        /// password for use with der
        client_auth: Option<(Vec<u8>, String)>,
    },
    #[cfg(feature = "use-rustls")]
    /// Injected rustls ClientConfig for TLS, to allow more customisation.
    Rustls(Arc<ClientConfig>),
    #[cfg(feature = "use-native-tls")]
    /// Use default native-tls configuration
    Native,
    #[cfg(feature = "use-native-tls")]
    /// Injected native-tls TlsConnector for TLS, to allow more customisation.
    NativeConnector(TlsConnector),
}
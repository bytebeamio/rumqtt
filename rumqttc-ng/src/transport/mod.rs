mod tcp;

#[cfg(feature = "use-rustls")]
use tokio_rustls::rustls::ClientConfig;

#[cfg(feature = "use-rustls")]
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum TransportEvent {
    Reconnection(usize),
    IncomingData,
    OutgoingAck,
    ConnectionClosed,
}

#[derive(Debug, Clone)]
pub enum TransportSettings {
    Mock,
    Tcp {
        host: String,
        port: u16,
        #[cfg(feature = "use-rustls")]
        /// Rustls TLS configuration
        rustls_config: Option<RustlsConfig>,
        #[cfg(feature = "use-native-tls")]
        /// Native TLS configuration
        native_tls_config: Option<NativeTlsConfig>,
    },
    Ws {
        url: String,
        cert_file: String,
        key_file: String,
    },
    Quic {
        host: String,
        port: u16,
        cert_file: String,
        key_file: String,
    },
}

#[derive(Debug, Clone)]
pub struct ConnectionSettings {
    pub max_read_buf_size: usize,
    pub max_write_buf_size: usize,
    pub timeout: u64,
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        Self {
            max_read_buf_size: 1024,
            max_write_buf_size: 1024,
            timeout: 10,
        }
    }
}

#[derive(Debug)]
pub enum Control {
    Disconnect,
    Stats,
}

#[cfg(feature = "use-rustls")]
#[derive(Clone, Debug)]
pub enum RustlsConfig {
    /// Simple configuration with CA cert and optional client auth
    Simple {
        /// CA certificate
        ca: Vec<u8>,
        /// ALPN settings
        alpn: Option<Vec<Vec<u8>>>,
        /// TLS client authentication
        client_auth: Option<(Vec<u8>, Vec<u8>)>,
    },
    /// Custom rustls ClientConfig for more customization
    Custom(Arc<ClientConfig>),
}

#[cfg(feature = "use-native-tls")]
#[derive(Clone, Debug)]
pub enum NativeTlsConfig {
    /// Simple configuration with CA cert and optional client auth
    Simple {
        /// CA certificate
        ca: Vec<u8>,
        /// PKCS12 binary DER and password
        client_auth: Option<(Vec<u8>, String)>,
    },
    /// Use default native-tls configuration
    Default,
    /// Custom native-tls TlsConnector for more customization
    Custom(TlsConnector),
}
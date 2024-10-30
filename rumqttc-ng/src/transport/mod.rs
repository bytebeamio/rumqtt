mod tcp;

pub use tcp::*;



#[derive(Debug, Clone)]
pub enum TransportSettings {
    Mock,
    // TODO(swanx): change host & port to single _bind_ address.
    Tcp {
        host: String,
        port: u16,
    },
    Tls {
        host: String,
        port: u16,
        ca_cert: Option<String>,
        cert_file: String,
        key_file: String,
    },
    Ws {
        url: String,
    },
    Wss {
        url: String,
        cert_file: String,
        key_file: String,
    },
    Http {},
    Https {},
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
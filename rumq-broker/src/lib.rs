#[macro_use]
extern crate log;

use derive_more::From;
use futures_util::future::join_all;
use tokio_util::codec::Framed;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::task;
use tokio::time::{self, Elapsed};
use tokio_rustls::rustls::internal::pemfile::{certs, rsa_private_keys};
use tokio_rustls::rustls::TLSError;
use tokio_rustls::rustls::{AllowAnyAuthenticatedClient, NoClientAuth, RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;
use futures_util::sink::Sink;
use futures_util::stream::Stream;
use rumq_core::mqtt4::{codec, Packet};

use serde::Deserialize;

use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::thread;

mod connection;
mod state;
mod router;

pub use rumq_core as core;
pub use router::{RouterMessage, Connection};

#[derive(From, Debug)]
pub enum Error {
    Io(io::Error),
    Mqtt(rumq_core::Error),
    Timeout(Elapsed),
    State(state::Error),
    Tls(TLSError),
    NoServerCert,
    NoServerPrivateKey,
    NoCAFile,
    NoServerCertFile,
    NoServerKeyFile,
    Disconnected,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    servers:    Vec<ServerSettings>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerSettings {
    pub port: u16,
    pub connection_timeout_ms: u16,
    pub next_connection_delay_ms: u64,
    pub max_client_id_len: usize,
    pub max_connections: usize,
    pub disk_persistence: bool,
    pub throttle_delay_ms: u64,
    pub disk_retention_size: usize,
    pub disk_retention_time_sec: usize,
    pub auto_save_interval_sec: u16,
    pub max_payload_size: usize,
    pub max_inflight_topic_size: usize,
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

async fn tls_connection<P: AsRef<Path>>(ca_path: Option<P>, cert_path: P, key_path: P) -> Result<TlsAcceptor, Error> {
    // client authentication with a CA. CA isn't required otherwise
    let mut server_config = if let Some(ca_path) = ca_path {
        let mut root_cert_store = RootCertStore::empty();
        root_cert_store.add_pem_file(&mut BufReader::new(File::open(ca_path)?)).map_err(|_| Error::NoCAFile)?;
        ServerConfig::new(AllowAnyAuthenticatedClient::new(root_cert_store))
    } else {
        ServerConfig::new(NoClientAuth::new())
    };

    let certs = certs(&mut BufReader::new(File::open(cert_path)?)).map_err(|_| Error::NoServerCertFile)?;
    let mut keys = rsa_private_keys(&mut BufReader::new(File::open(key_path)?)).map_err(|_| Error::NoServerKeyFile)?;

    server_config.set_single_cert(certs, keys.remove(0))?;
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    Ok(acceptor)
}

async fn accept_loop(config: Arc<ServerSettings>, router_tx: Sender<(String, router::RouterMessage)>) -> Result<(), Error> {
    let addr = format!("0.0.0.0:{}", config.port);
    let connection_config = config.clone();

    let acceptor = if let Some(cert_path) = config.cert_path.clone() {
        let key_path = config.key_path.clone().ok_or(Error::NoServerPrivateKey)?;
        Some(tls_connection(config.ca_path.clone(), cert_path, key_path).await?)
    } else {
        None
    };

    info!("Waiting for connections on {}", addr);
    // eventloop which accepts connections
    let mut listener = TcpListener::bind(addr).await?;
    let accept_loop_delay = Duration::from_millis(config.next_connection_delay_ms);
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                error!("Tcp connection error = {:?}", e);
                continue;
            }
        };

        info!("Accepting from: {}", addr);

        let config = connection_config.clone();
        let router_tx = router_tx.clone();

        if let Some(acceptor) = &acceptor {
            let stream = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(e) => {
                    error!("Tls connection error = {:?}", e);
                    continue;
                }
            };

            let framed = Framed::new(stream, codec::MqttCodec::new(config.max_payload_size));
            task::spawn( async {
                match connection::eventloop(config, framed, router_tx).await {
                    Ok(id) => info!("Connection eventloop done!!. Id = {:?}", id),
                    Err(e) => error!("Connection eventloop error = {:?}", e),
                }
            });
        } else {
            let framed = Framed::new(stream, codec::MqttCodec::new(config.max_payload_size));
            task::spawn( async {
                match connection::eventloop(config, framed, router_tx).await {
                    Ok(id) => info!("Connection eventloop done!!. Id = {:?}", id),
                    Err(e) => error!("Connection eventloop error = {:?}", e),
                }
            });
        };

        time::delay_for(accept_loop_delay).await;
    }
}


pub trait Network: Stream<Item = Result<Packet, rumq_core::Error>> + Sink<Packet, Error = io::Error> + Unpin + Send {}
impl<T> Network for T where T: Stream<Item = Result<Packet, rumq_core::Error>> + Sink<Packet, Error = io::Error> + Unpin + Send {}

#[tokio::main(core_threads = 1)]
async fn router(rx: Receiver<(String, router::RouterMessage)>) {
    let mut router = router::Router::new(rx);
    if let Err(e) = router.start().await {
        error!("Router stopped. Error = {:?}", e);
    }
}

pub struct Broker {
    config: Config,
    router_handle: Sender<(String, router::RouterMessage)>,
}

pub fn new(config: Config) -> Broker {
    let (router_tx, router_rx) = channel(100);

    thread::spawn(move || {
        router(router_rx)
    });

    Broker {
        config,
        router_handle: router_tx
    }
}

impl Broker {
    pub fn new_router_handle(&self) -> Sender<(String, router::RouterMessage)> {
        self.router_handle.clone()
    }

    pub async fn start(&mut self) -> Vec<Result<(), task::JoinError>> {
        let mut servers = Vec::new();
        let server_configs = self.config.servers.split_off(0);

        for server in server_configs.into_iter() {
            let config = Arc::new(server);
            let fut = accept_loop(config, self.router_handle.clone());
            let o = task::spawn(async {
                error!("Accept loop returned = {:?}", fut.await);
            });

            servers.push(o);
        }

        join_all(servers).await
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn accept_loop_rate_limits_incoming_connections() {}

    #[test]
    fn accept_loop_should_not_allow_more_than_maximum_connections() {}

    #[test]
    fn accept_loop_should_accept_new_connection_when_a_client_disconnects_after_max_connections() {}

    #[test]
    fn client_loop_should_error_if_connect_packet_is_not_received_in_time() {}
}

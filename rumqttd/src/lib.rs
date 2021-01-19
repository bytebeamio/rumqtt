#[macro_use]
extern crate log;

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};

use mqttbytes::v4::Packet;
use rumqttlog::*;
use tokio::time::error::Elapsed;

use crate::remotelink::RemoteLink;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::task;
use tokio::time;
use tokio_rustls::rustls::internal::pemfile::{certs, rsa_private_keys};
use tokio_rustls::rustls::{
    AllowAnyAuthenticatedClient, NoClientAuth, RootCertStore, ServerConfig, TLSError,
};
use tokio_rustls::TlsAcceptor;

pub mod async_locallink;
mod consolelink;
mod locallink;
mod network;
mod remotelink;
mod state;

use crate::consolelink::ConsoleLink;
pub use crate::locallink::{LinkError, LinkRx, LinkTx};
use crate::network::Network;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[error("I/O {0}")]
    Io(#[from] io::Error),
    #[error("Connection error {0}")]
    Connection(#[from] remotelink::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Channel send error")]
    Send(#[from] SendError<(Id, Event)>),
    #[error("TLS error {0}")]
    Tls(#[from] TLSError),
    #[error("No server cert")]
    NoServerCert,
    #[error("No server private key")]
    NoServerKey,
    #[error("No ca file")]
    NoCAFile,
    #[error("No server cert file")]
    NoServerCertFile,
    #[error("No server key file")]
    NoServerKeyFile,
    Disconnected,
    NetworkClosed,
    WrongPacket(Packet),
}

type Id = usize;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Config {
    pub id: usize,
    pub router: rumqttlog::Config,
    pub servers: HashMap<String, ServerSettings>,
    pub cluster: Option<HashMap<String, MeshSettings>>,
    pub replicator: Option<ConnectionSettings>,
    pub console: Option<ConsoleSettings>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerSettings {
    pub port: u16,
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub next_connection_delay_ms: u64,
    pub connections: ConnectionSettings,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConnectionSettings {
    pub connection_timeout_ms: u16,
    pub max_client_id_len: usize,
    pub throttle_delay_ms: u64,
    pub max_payload_size: usize,
    pub max_inflight_count: u16,
    pub max_inflight_size: usize,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshSettings {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsoleSettings {
    pub port: u16,
}

impl Default for ServerSettings {
    fn default() -> Self {
        panic!("Server settings should be derived from a configuration file")
    }
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        panic!("Server settings should be derived from a configuration file")
    }
}

impl Default for ConsoleSettings {
    fn default() -> Self {
        panic!("Console settings should be derived from configuration file")
    }
}

pub struct Broker {
    config: Arc<Config>,
    router_tx: Sender<(Id, Event)>,
    router: Option<Router>,
}

impl Broker {
    pub fn new(config: Config) -> Broker {
        let config = Arc::new(config);
        let router_config = Arc::new(config.router.clone());
        let (router, router_tx) = Router::new(router_config);
        Broker {
            config,
            router_tx,
            router: Some(router),
        }
    }

    pub fn router_handle(&self) -> Sender<(Id, Event)> {
        self.router_tx.clone()
    }

    pub fn link(&self, client_id: &str) -> Result<LinkTx, LinkError> {
        // Register this connection with the router. Router replies with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)

        let tx = LinkTx::new(client_id, self.router_tx.clone());
        Ok(tx)
    }

    pub fn start(&mut self) -> Result<(), Error> {
        // spawn the router in a separate thread
        let mut router = self.router.take().unwrap();
        let router_thread = thread::Builder::new().name("rumqttd-router".to_owned());
        router_thread.spawn(move || router.start())?;

        // spawn servers in a separate thread
        for (id, config) in self.config.servers.clone() {
            let server_name = format!("rumqttd-server-{}", id);
            let server_thread = thread::Builder::new().name(server_name);
            let server = Server::new(id, config, self.router_tx.clone());
            server_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();
                runtime.block_on(async {
                    if let Err(e) = server.start().await {
                        error!("Accept loop error: {:?}", e.to_string());
                    }
                });
            })?;
        }

        // Run console in current thread, if it is configured.
        if self.config.console.is_some() {
            let console = ConsoleLink::new(self.config.clone(), self.router_tx.clone());
            let console = Arc::new(console);
            consolelink::start(console);
        }

        Ok(())
    }
}

struct Server {
    id: String,
    config: ServerSettings,
    router_tx: Sender<(Id, Event)>,
}

impl Server {
    pub fn new(id: String, config: ServerSettings, router_tx: Sender<(Id, Event)>) -> Server {
        Server {
            id,
            config,
            router_tx,
        }
    }

    async fn tls(&self) -> Result<Option<TlsAcceptor>, Error> {
        let (certs, mut keys) = match &self.config.cert_path {
            Some(cert_path) => {
                let mut cert_file = BufReader::new(File::open(&cert_path)?);
                let key_path = self.config.key_path.as_ref().ok_or(Error::NoServerKey)?;
                let mut key_file = BufReader::new(File::open(&key_path)?);
                let certs = certs(&mut cert_file).map_err(|_| Error::NoServerCertFile)?;
                let keys = rsa_private_keys(&mut key_file).map_err(|_| Error::NoServerKeyFile)?;
                (certs, keys)
            }
            None => return Ok(None),
        };

        // client authentication with a CA. CA isn't required otherwise
        let mut server_config = match &self.config.ca_path {
            Some(ca_path) => {
                let mut ca_store = RootCertStore::empty();
                let mut ca_file = BufReader::new(File::open(ca_path)?);
                ca_store
                    .add_pem_file(&mut ca_file)
                    .map_err(|_| Error::NoCAFile)?;

                ServerConfig::new(AllowAnyAuthenticatedClient::new(ca_store))
            }
            None => ServerConfig::new(NoClientAuth::new()),
        };

        server_config.set_single_cert(certs, keys.remove(0))?;
        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        Ok(Some(acceptor))
    }

    async fn start(&self) -> Result<(), Error> {
        let addr = format!("0.0.0.0:{}", self.config.port);

        let listener = TcpListener::bind(&addr).await?;
        let delay = Duration::from_millis(self.config.next_connection_delay_ms);
        let mut count = 0;

        let config = Arc::new(self.config.connections.clone());
        let acceptor = self.tls().await?;
        let max_incoming_size = config.max_payload_size;

        info!("Waiting for connections on {}. Server = {}", addr, self.id);
        loop {
            let (stream, addr) = listener.accept().await?;
            let network = match &acceptor {
                Some(acceptor) => {
                    info!("{}. Accepting TLS connection from: {}", count, addr);
                    Network::new(acceptor.accept(stream).await?, max_incoming_size)
                }
                None => {
                    info!("{}. Accepting TCP connection from: {}", count, addr);
                    Network::new(stream, max_incoming_size)
                }
            };

            count += 1;

            let config = config.clone();
            let router_tx = self.router_tx.clone();
            task::spawn(async {
                let connector = Connector::new(config, router_tx);
                if let Err(e) = connector.new_connection(network).await {
                    error!("Dropping link task!! Result = {:?}", e);
                }
            });

            time::sleep(delay).await;
        }
    }
}

struct Connector {
    config: Arc<ConnectionSettings>,
    router_tx: Sender<(Id, Event)>,
}

impl Connector {
    fn new(config: Arc<ConnectionSettings>, router_tx: Sender<(Id, Event)>) -> Connector {
        Connector { config, router_tx }
    }

    /// A new network connection should wait for mqtt connect packet. This handling should be handled
    /// asynchronously to avoid listener from not blocking new connections while this connection is
    /// waiting for mqtt connect packet. Also this honours connection wait time as per config to prevent
    /// denial of service attacks (rogue clients which only does network connection without sending
    /// mqtt connection packet to make make the server reach its concurrent connection limit)
    async fn new_connection(&self, network: Network) -> Result<(), Error> {
        let config = self.config.clone();
        let router_tx = self.router_tx.clone();

        // Start the link
        let (client_id, id, mut link) = RemoteLink::new(config, router_tx, network).await?;
        let (execute_will, pending) = match link.start().await {
            // Connection get close. This shouldn't usually happen
            Ok(_) => {
                error!("Link stopped!! Id = {}, Client Id = {}", id, client_id);
                (true, link.state.clean())
            }
            // We are representing clean close as Abort in `Network`
            Err(remotelink::Error::Io(e)) if e.kind() == io::ErrorKind::ConnectionAborted => {
                info!("Link closed!! Id = {}, Client Id = {}", id, client_id);
                (true, link.state.clean())
            }
            // Client requested disconnection.
            Err(remotelink::Error::Disconnect) => {
                info!("Disconnected!! Id = {}, Client Id = {}", id, client_id);
                (false, link.state.clean())
            }
            // Any other error
            Err(e) => {
                error!("Stopped!! Id = {}, Client Id = {}, {:?}", id, client_id, e);
                (true, link.state.clean())
            }
        };

        let disconnect = Disconnection::new(client_id, execute_will, pending);
        let disconnect = Event::Disconnect(disconnect);
        let message = (id, disconnect);
        self.router_tx.send(message)?;
        Ok(())
    }
}

pub trait IO: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Sync + Unpin> IO for T {}

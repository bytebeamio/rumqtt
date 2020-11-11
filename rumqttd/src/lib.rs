#[macro_use]
extern crate log;

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};

use mqtt4bytes::Packet;
use rumqttlog::*;
use tokio::time::error::Elapsed;

use crate::remotelink::RemoteLink;
pub use rumqttlog::Config as RouterConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::task;
use tokio::time;

mod consolelink;
mod locallink;
mod network;
mod remotelink;
mod replicationlink;
mod state;

use crate::consolelink::ConsoleLink;
pub use crate::locallink::{LinkError, LinkRx, LinkTx};
use crate::network::Network;
use crate::replicationlink::Mesh;
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Connection error")]
    Connection(#[from] remotelink::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Channel send error")]
    Send(#[from] SendError<(Id, Event)>),
    Disconnected,
    NetworkClosed,
    WrongPacket(Packet),
}

type Id = usize;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Config {
    id: usize,
    router: rumqttlog::Config,
    servers: HashMap<String, ServerSettings>,
    cluster: Option<HashMap<String, MeshSettings>>,
    replicator: Option<ConnectionSettings>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerSettings {
    pub port: u16,
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
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshSettings {
    pub host: String,
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

        // spawn console thread
        let console = ConsoleLink::new(self.config.clone(), self.router_tx.clone());
        let console = Arc::new(console);
        let console_thread = thread::Builder::new().name("rumqttd-console".to_owned());
        console_thread.spawn(move || consolelink::start(console))?;

        // replication mesh
        let mut mesh = Mesh::new(self.config.clone(), self.router_tx.clone());
        let console_thread = thread::Builder::new().name("rumqttd-replicator".to_owned());
        console_thread.spawn(move || mesh.start())?;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let (_id, server) = self.config.servers.clone().into_iter().next().unwrap();
        let router_tx = self.router_tx.clone();

        rt.block_on(async {
            if let Err(e) = accept_loop(Arc::new(server), router_tx).await {
                error!("Accept loop error: {:?}", e);
            }
        });

        Ok(())
    }
}

async fn accept_loop(
    config: Arc<ServerSettings>,
    router_tx: Sender<(Id, Event)>,
) -> Result<(), Error> {
    let addr = format!("0.0.0.0:{}", config.port);
    info!("Waiting for connections on {}", addr);

    let listener = TcpListener::bind(addr).await?;
    let accept_loop_delay = Duration::from_millis(config.next_connection_delay_ms);
    let mut count = 0;

    let config = Arc::new(config.connections.clone());
    loop {
        let (stream, addr) = listener.accept().await?;
        count += 1;
        info!("{}. Accepting from: {}", count, addr);

        let config = config.clone();
        let router_tx = router_tx.clone();
        task::spawn(async {
            let connector = Connector::new(config, router_tx);

            // TODO Remove all max packet size hard codes
            let network = Network::new(stream, 10 * 1024);
            if let Err(e) = connector.new_connection(network).await {
                error!("Dropping link task!! Result = {:?}", e);
            }
        });

        time::sleep(accept_loop_delay).await;
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
            Err(remotelink::Error::Disconnect) => {
                error!("Disconnected!! Id = {}, Client Id = {}", id, client_id);
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

#[macro_use]
extern crate log;

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};

use mqtt4bytes::Packet;
use rumqttlog::*;
use tokio::time::Elapsed;

use crate::remotelink::RemoteLink;
pub use rumqttlog::Config as RouterConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::task;
use tokio::time;

mod locallink;
mod network;
mod remotelink;
mod state;

pub use crate::locallink::{LinkError, LinkRx, LinkTx};
use crate::network::Network;

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

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Config {
    servers: Vec<ServerSettings>,
    router: rumqttlog::Config,
}

type Id = usize;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerSettings {
    pub port: u16,
    pub connection_timeout_ms: u16,
    pub next_connection_delay_ms: u64,
    pub max_client_id_len: usize,
    pub max_connections: usize,
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

impl Default for ServerSettings {
    fn default() -> Self {
        ServerSettings {
            port: 1883,
            connection_timeout_ms: 100,
            next_connection_delay_ms: 0,
            max_client_id_len: 10,
            max_connections: 100,
            throttle_delay_ms: 0,
            max_payload_size: 2 * 1024,
            max_inflight_count: 100,
            max_inflight_size: 100 * 1024,
            ca_path: None,
            cert_path: None,
            key_path: None,
            username: None,
            password: None,
        }
    }
}

pub struct Broker {
    config: Config,
    router_tx: Sender<(Id, Event)>,
    router: Option<Router>,
}

impl Broker {
    pub fn new(config: Config) -> Broker {
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
        let mut router = self.router.take().unwrap();
        let name = "rumqttd-router".to_owned();
        let thread = thread::Builder::new().name(name);

        // spawn the router in a separate thread
        thread.spawn(move || router.start())?;
        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()?;

        let server = self.config.clone().servers.into_iter().next().unwrap();
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

    let mut listener = TcpListener::bind(addr).await?;
    let accept_loop_delay = Duration::from_millis(config.next_connection_delay_ms);
    let mut count = 0;

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

        time::delay_for(accept_loop_delay).await;
    }
}

struct Connector {
    config: Arc<ServerSettings>,
    router_tx: Sender<(Id, Event)>,
}

impl Connector {
    fn new(config: Arc<ServerSettings>, router_tx: Sender<(Id, Event)>) -> Connector {
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

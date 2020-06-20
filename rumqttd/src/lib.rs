/*
#[macro_use]
extern crate log;

pub mod link;
pub mod tracker;

use serde::{Serialize, Deserialize};
use std::{thread, io};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use tokio::time;
use tokio::task;
use std::sync::Arc;

use rumqttlog::{Router, RouterInMessage, Connection, Sender, bounded};
use rumqttlog::mqtt4bytes::{self, MqttCodec, Packet, ConnAck, ConnectReturnCode};
use tokio::time::Elapsed;
use crate::link::Link;
use tokio::stream::StreamExt;

pub use rumqttlog::Config as RouterConfig;
use futures_util::SinkExt;

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("MQTT protocol error")]
    Mqtt(#[from] mqtt4bytes::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    Disconnected,
    NetworkClosed,
    WrongPacket(Packet)
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Config {
    servers: Vec<ServerSettings>,
    router: rumqttlog::Config,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerSettings {
    pub port: u16,
    pub connection_timeout_ms: u16,
    pub next_connection_delay_ms: u64,
    pub max_client_id_len: usize,
    pub max_connections: usize,
    pub throttle_delay_ms: u64,
    pub max_payload_size: usize,
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
            ca_path: None,
            cert_path: None,
            key_path: None,
            username: None,
            password: None
        }
    }
}

pub struct Broker {
    config: Config,
    router_tx: Sender<(String, RouterInMessage)>,
    router: Option<Router>
}

impl Broker {
    pub fn new(config: Config) -> Broker {
        let (router, router_tx) = Router::new(config.router.clone());
        Broker { config, router_tx, router: Some(router) }
    }

    pub fn new_router_handle(&self) -> Sender<(String, RouterInMessage)> {
        self.router_tx.clone()
    }

    async fn accept_loop(&self, config: Arc<ServerSettings>) -> Result<(), Error> {
        let addr = format!("0.0.0.0:{}", config.port);
        info!("Waiting for connections on {}", addr);

        let mut listener = TcpListener::bind(addr).await?;
        let accept_loop_delay = Duration::from_millis(config.next_connection_delay_ms);

        loop {
            let (stream, addr) = listener.accept().await?;
            info!("Accepting from: {}", addr);
            let config = config.clone();
            let router_tx = self.router_tx.clone();
            let framed = Framed::new(stream,MqttCodec::new(config.max_payload_size));
            task::spawn(async {
                let connector = Connector { config, router_tx };
                let o = connector.new_connection(framed).await;
                error!("Dropping link task!! Result = {:?}", o);
            });
            time::delay_for(accept_loop_delay).await;
        }
    }

    #[tokio::main(core_threads = 1)]
    pub async fn start(&mut self) -> Result<(), Error> {
        let r = self.router.take().unwrap();
        thread::spawn(move || router(r));

        let server = self.config.clone().servers.into_iter().next().unwrap();
        if let Err(e) = self.accept_loop(Arc::new(server)).await {
            error!("Accept loop error: {:?}", e);
        }

        Ok(())
    }
}

#[tokio::main(core_threads = 1)]
async fn router(mut router: Router) {
    router.start().await;
}

struct Connector {
    config: Arc<ServerSettings>,
    router_tx: Sender<(String, RouterInMessage)>
}


impl Connector {
    /// A new network connection should wait for mqtt connect packet. This handling should be handled
    /// asynchronously to avoid listener from not blocking new connections while this connection is
    /// waiting for mqtt connect packet. Also this honours connection wait time as per config to prevent
    /// denial of service attacks (rogue clients which only does network connection without sending
    /// mqtt connection packet to make make the server reach its concurrent connection limit)
    async fn new_connection<S: IO>(&self, framed: Framed<S, MqttCodec>) -> Result<(), Error> {
        let mut framed = framed;
        // Wait for MQTT connect packet and error out if it's not received in time to prevent
        // DOS attacks by filling total connections that the server can handle with idle open
        // connections which results in server rejecting new connections
        let timeout = Duration::from_millis(self.config.connection_timeout_ms.into());
        let connect = time::timeout(timeout, async {
            let connect = match framed.next().await.ok_or(Error::NetworkClosed)?? {
                Packet::Connect(connect) => connect,
                packet => return Err(Error::WrongPacket(packet))
            };

            Ok::<_, Error>(connect)
        }).await??;


        // Register this connection with the router. Router replys with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let (link_tx, link_rx) = bounded(4);
        let client_id = connect.client_id.clone();
        let connection = Connection::new(client_id, link_tx);
        let message = (0, RouterInMessage::Connect(connection));
        let mut router_tx = self.router_tx.clone();
        router_tx.send(message).await.unwrap();

        // Send connection acknowledgement back to the client
        let connack = ConnAck::new(ConnectReturnCode::Accepted, false);
        let packet = Packet::ConnAck(connack);
        framed.send(packet).await?;

        // Start the link
        let mut link = Link::new(&client_id, self.router_tx.clone(), link_rx);
        let o = link.start(framed).await;
        error!("Link stopped!! Result = {:?}", o);

        // TODO When a new connection request is sent to the router, router should ack with error
        // TODO if it exceeds maximum allowed active connections
        Ok(())
    }
}

pub trait IO: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> IO for T {}

 */

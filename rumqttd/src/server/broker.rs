// use crate::link::bridge;
use crate::link::console::ConsoleLink;
use crate::link::network::{Network, N};
use crate::link::remote::{self, RemoteLink};
#[cfg(feature = "websockets")]
use crate::link::shadow::{self, ShadowLink};
use crate::protocol::v4::V4;
use crate::protocol::v5::V5;
#[cfg(feature = "websockets")]
use crate::protocol::ws::Ws;
use crate::protocol::Protocol;
#[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
use crate::server::tls::{self, TLSAcceptor};
use crate::{meters, ConnectionSettings, GetMeter, Meter};
use flume::{RecvError, SendError, Sender};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, field, info, Instrument, Span};
#[cfg(feature = "websockets")]
use websocket_codec::MessageCodec;

use metrics::register_gauge;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::time::Duration;
use std::{io, thread};

use crate::link::console;
use crate::link::local::{self, Link, LinkRx, LinkTx};
use crate::router::{Disconnection, Event, Router};
use crate::{Config, ConnectionId, ServerSettings};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::error::Elapsed;
use tokio::{task, time};

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[error("I/O {0}")]
    Io(#[from] io::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
    #[error("Certs error = {0}")]
    Certs(#[from] tls::Error),
    #[error("Accept error = {0}")]
    Accept(String),
    #[error("Remote error = {0}")]
    Remote(#[from] remote::Error),
}

pub struct Broker {
    config: Arc<Config>,
    router_tx: Sender<(ConnectionId, Event)>,
}

impl Broker {
    pub fn new(config: Config) -> Broker {
        let config = Arc::new(config);
        let router_config = config.router.clone();
        let router: Router = Router::new(config.id, router_config);

        // Setup cluster if cluster settings are configured
        match config.cluster.clone() {
            Some(_cluster_config) => {
                // let node_id = cluster_config.node_id;
                // let listen = cluster_config.listen;
                // let seniors = cluster_config.seniors;
                // let mut cluster = RemoteCluster::new(node_id, &listen, seniors);
                // Broker::setup_remote_cluster(&mut router, node_id, &mut cluster);

                // Start router first and then cluster in the background
                let router_tx = router.spawn();
                // cluster.spawn();
                Broker { config, router_tx }
            }
            None => {
                let router_tx = router.spawn();
                Broker { config, router_tx }
            }
        }
    }

    // pub fn new_local_cluster(
    //     config: Config,
    //     node_id: NodeId,
    //     seniors: Vec<(NodeId, Sender<ReplicationData>)>,
    // ) -> (Broker, Sender<ReplicationData>) {
    //     let config = Arc::new(config);
    //     let router_config = config.router.clone();

    //     let mut router = Router::new(config.id, router_config);
    //     let (mut cluster, tx) = LocalCluster::new(node_id, seniors);
    //     Broker::setup_local_cluster(&mut router, node_id, &mut cluster);

    //     // Start router first and then cluster in the background
    //     let router_tx = router.spawn();
    //     cluster.spawn();

    //     (Broker { config, router_tx }, tx)
    // }

    // fn setup_remote_cluster(router: &mut Router, node_id: NodeId, cluster: &mut RemoteCluster) {
    //     // Retrieve pre-configured list of router <-> replica link from router
    //     // and add the link to cluster
    //     let nodes: Vec<NodeId> = (0..MAX_NODES).filter(|v| *v != node_id).collect();
    //     for node_id in nodes {
    //         let link = router.get_replica_handle(node_id);
    //         cluster.add_replica_router_handle(node_id, link);
    //     }
    // }

    // fn setup_local_cluster(router: &mut Router, node_id: NodeId, cluster: &mut LocalCluster) {
    //     // Retrieve pre-configured list of router <-> replica link from router
    //     // and add the link to cluster
    //     let nodes: Vec<NodeId> = (0..MAX_NODES).filter(|v| *v != node_id).collect();
    //     for node_id in nodes {
    //         let link = router.get_replica_handle(node_id);
    //         cluster.add_replica_router_handle(node_id, link);
    //     }
    // }

    // Link to get meters
    pub fn meters(&self) -> Result<meters::MetersLink, meters::LinkError> {
        let link = meters::MetersLink::new(self.router_tx.clone())?;
        Ok(link)
    }

    pub fn link(&self, client_id: &str) -> Result<(LinkTx, LinkRx), local::LinkError> {
        // Register this connection with the router. Router replies with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let (link_tx, link_rx, _ack) =
            Link::new(None, client_id, self.router_tx.clone(), true, None, false)?;
        Ok((link_tx, link_rx))
    }

    #[tracing::instrument(skip(self))]
    pub fn start(&mut self) -> Result<(), Error> {
        // spawn bridge in a separate thread
        // if let Some(bridge_config) = self.config.bridge.clone() {
        //     let router_tx = self.router_tx.clone();
        //     thread::Builder::new()
        //         .name("bridge-thread".to_string())
        //         .spawn(move || {
        //             let mut runtime = tokio::runtime::Builder::new_current_thread();
        //             let runtime = runtime.enable_all().build().unwrap();
        //             let router_tx = router_tx.clone();

        //             runtime.block_on(async move {
        //                 if let Err(e) = bridge::bridge_launch(bridge_config, router_tx).await {
        //                     error!("bridge: {:?}", e);
        //                 };
        //             });
        //         })?;
        // }

        // spawn servers in a separate thread
        for (_, config) in self.config.v4.clone() {
            let server_thread = thread::Builder::new().name(config.name.clone());
            let server = Server::new(config, self.router_tx.clone(), V4);
            server_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();

                runtime.block_on(async {
                    if let Err(e) = server.start(false).await {
                        error!(error=?e, "Remote link error");
                    }
                });
            })?;
        }

        for (_, config) in self.config.v5.clone() {
            let server_thread = thread::Builder::new().name(config.name.clone());
            let server = Server::new(config, self.router_tx.clone(), V5);
            server_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();

                runtime.block_on(async {
                    if let Err(e) = server.start(false).await {
                        error!(error=?e, "Remote link error");
                    }
                });
            })?;
        }

        #[cfg(feature = "websockets")]
        for (_, config) in self.config.ws.clone() {
            let server_thread = thread::Builder::new().name(config.name.clone());
            let server = Server::new(
                config,
                self.router_tx.clone(),
                Ws {
                    codec: MessageCodec::server(),
                },
            );
            server_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();

                runtime.block_on(async {
                    if let Err(e) = server.start(true).await {
                        error!(error=?e, "Remote link error");
                    }
                });
            })?;
        }

        if let Some(prometheus_setting) = &self.config.prometheus {
            let port = prometheus_setting.port;
            let timeout = prometheus_setting.interval;
            let metrics_thread = thread::Builder::new().name("Metrics".to_owned());
            let meter_link = self.meters().unwrap();
            metrics_thread.spawn(move || {
                let builder = PrometheusBuilder::new()
                    .with_http_listener(SocketAddr::new("127.0.0.1".parse().unwrap(), port));
                builder.install().unwrap();

                let total_publishes = register_gauge!("metrics.router.total_publishes");
                let total_connections = register_gauge!("metrics.router.total_connections");
                let failed_publishes = register_gauge!("metrics.router.failed_publishes");
                loop {
                    let request = GetMeter::Router;
                    let metrics = match meter_link.get(request) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Data link receive error = {:?}", e);
                            break;
                        }
                    };

                    match metrics {
                        Meter::Router(_, ref r) => {
                            total_connections.set(r.total_connections as f64);
                            total_publishes.set(r.total_publishes as f64);
                            failed_publishes.set(r.failed_publishes as f64);
                        }
                        _ => panic!("We only request for router metrics"),
                    }
                    std::thread::sleep(Duration::from_secs(timeout));
                }
            })?;
        }

        // for (_, config) in self.config.shadows.clone() {
        //     let server_thread = thread::Builder::new().name(config.name.clone());
        //     let server = Server::new(config, self.router_tx.clone());
        //     server_thread.spawn(move || {
        //         let mut runtime = tokio::runtime::Builder::new_current_thread();
        //         let runtime = runtime.enable_all().build().unwrap();

        //         runtime.block_on(async {
        //             if let Err(e) = server.start(true).await {
        //                 error!("Accept loop error: {:?}", e.to_string());
        //             }
        //         });
        //     })?;
        // }

        let console_link = ConsoleLink::new(self.config.console.clone(), self.router_tx.clone());

        let console_link = Arc::new(console_link);
        console::start(console_link);

        Ok(())
    }
}

struct Server<P> {
    config: ServerSettings,
    router_tx: Sender<(ConnectionId, Event)>,
    protocol: P,
}

impl<P: Protocol + Clone + Send + 'static> Server<P> {
    pub fn new(
        config: ServerSettings,
        router_tx: Sender<(ConnectionId, Event)>,
        protocol: P,
    ) -> Server<P> {
        Server {
            config,
            router_tx,
            protocol,
        }
    }

    // Depending on TLS or not create a new Network
    async fn tls_accept(&self, stream: TcpStream) -> Result<(Box<dyn N>, Option<String>), Error> {
        #[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
        match &self.config.tls {
            Some(c) => {
                let (tenant_id, network) = TLSAcceptor::new(c)?.accept(stream).await?;
                Ok((network, tenant_id))
            }
            None => Ok((Box::new(stream), None)),
        }
        #[cfg(not(any(feature = "use-rustls", feature = "use-native-tls")))]
        Ok((Box::new(stream), None))
    }

    async fn start(&self, shadow: bool) -> Result<(), Error> {
        let listener = TcpListener::bind(&self.config.listen).await?;
        let delay = Duration::from_millis(self.config.next_connection_delay_ms);
        let mut count: usize = 0;

        let config = Arc::new(self.config.connections.clone());
        info!(
            config = self.config.name,
            listen_addr = self.config.listen.to_string(),
            "Listening for remote connections",
        );
        loop {
            // Await new network connection.
            let (stream, addr) = match listener.accept().await {
                Ok((s, r)) => (s, r),
                Err(e) => {
                    error!(error=?e, "Unable to accept socket.");
                    continue;
                }
            };

            let (network, tenant_id) = match self.tls_accept(stream).await {
                Ok(o) => o,
                Err(e) => {
                    error!(error=?e, "Tls accept error");
                    continue;
                }
            };

            info!(
                name=?self.config.name, ?addr, count,"accept"
            );

            let config = config.clone();
            let router_tx = self.router_tx.clone();
            count += 1;

            let protocol = self.protocol.clone();
            match shadow {
                #[cfg(feature = "websockets")]
                true => task::spawn(shadow_connection(config, router_tx, network).instrument(
                    tracing::error_span!(
                        "shadow_connection",
                        client_id = field::Empty,
                        connection_id = field::Empty
                    ),
                )),
                _ => task::spawn(
                    remote(config, tenant_id, router_tx, network, protocol).instrument(
                        tracing::error_span!(
                            "remote_link",
                            client_id = field::Empty,
                            connection_id = field::Empty
                        ),
                    ),
                ),
            };

            time::sleep(delay).await;
        }
    }
}

/// A new network connection should wait for mqtt connect packet. This handling should be handled
/// asynchronously to avoid listener from not blocking new connections while this connection is
/// waiting for mqtt connect packet. Also this honours connection wait time as per config to prevent
/// denial of service attacks (rogue clients which only does network connection without sending
/// mqtt connection packet to make make the server reach its concurrent connection limit)
async fn remote<P: Protocol>(
    config: Arc<ConnectionSettings>,
    tenant_id: Option<String>,
    router_tx: Sender<(ConnectionId, Event)>,
    stream: Box<dyn N>,
    protocol: P,
) {
    let network = Network::new(stream, config.max_payload_size, 100, protocol);
    // Start the link
    let mut link = match RemoteLink::new(config, router_tx.clone(), tenant_id, network).await {
        Ok(l) => l,
        Err(e) => {
            error!(error=?e, "Remote link error");
            return;
        }
    };

    let client_id = link.client_id.clone();
    let connection_id = link.connection_id;
    let mut execute_will = false;

    Span::current().record("client_id", &client_id);
    Span::current().record("connection_id", connection_id);

    match link.start().await {
        // Connection get close. This shouldn't usually happen
        Ok(_) => error!("connection-stop"),
        // No need to send a disconnect message when disconnetion
        // originated internally in the router
        Err(remote::Error::Link(e)) => {
            error!(error=?e, "router-drop");
            return;
        }
        // Any other error
        Err(e) => {
            error!(error=?e,"Disconnected!!");
            execute_will = true;
        }
    };

    let disconnect = Disconnection {
        id: client_id,
        execute_will,
        pending: vec![],
    };

    let disconnect = Event::Disconnect(disconnect);
    let message = (connection_id, disconnect);
    router_tx.send(message).ok();
}

#[cfg(feature = "websockets")]
async fn shadow_connection(
    config: Arc<ConnectionSettings>,
    router_tx: Sender<(ConnectionId, Event)>,
    stream: Box<dyn N>,
) {
    // Start the link
    let mut link = match ShadowLink::new(config, router_tx.clone(), stream).await {
        Ok(l) => l,
        Err(e) => {
            error!(reason=?e, "Remote link error");
            return;
        }
    };

    let client_id = link.client_id.clone();
    let connection_id = link.connection_id;

    Span::current().record("client_id", &client_id);
    Span::current().record("connection_id", connection_id);

    match link.start().await {
        // Connection get close. This shouldn't usually happen
        Ok(_) => error!("connection-stop"),
        // No need to send a disconnect message when disconnetion
        // originated internally in the router
        Err(shadow::Error::Link(e)) => {
            error!(reason=?e, "router-drop");
            return;
        }
        Err(e) => error!(?e),
    };

    let disconnect = Disconnection {
        id: client_id,
        execute_will: false,
        pending: vec![],
    };

    let disconnect = Event::Disconnect(disconnect);
    let message = (connection_id, disconnect);
    router_tx.send(message).ok();
}

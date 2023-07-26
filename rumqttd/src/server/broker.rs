use crate::link::alerts::{self};
use crate::link::console::ConsoleLink;
use crate::link::network::{Network, N};
use crate::link::remote::{self, RemoteLink};
use crate::link::{bridge, timer};
use crate::local::LinkBuilder;
use crate::protocol::v4::V4;
use crate::protocol::v5::V5;
use crate::protocol::Protocol;
#[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
use crate::server::tls::{self, TLSAcceptor};
use crate::{meters, ConnectionSettings, Meter};
use flume::{RecvError, SendError, Sender};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tracing::{error, field, info, Instrument};

#[cfg(feature = "websocket")]
use async_tungstenite::tokio::accept_hdr_async;
#[cfg(feature = "websocket")]
use async_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
#[cfg(feature = "websocket")]
use async_tungstenite::tungstenite::http::HeaderValue;
#[cfg(feature = "websocket")]
use ws_stream_tungstenite::WsStream;

use metrics::register_gauge;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::time::Duration;
use std::{io, thread};

use crate::link::console;
use crate::link::local::{self, LinkRx, LinkTx};
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

        // Setup cluster if cluster settings are configured.
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

    // Link to get alerts
    pub fn alerts(&self) -> Result<alerts::AlertsLink, alerts::LinkError> {
        let link = alerts::AlertsLink::new(self.router_tx.clone())?;
        Ok(link)
    }

    pub fn link(&self, client_id: &str) -> Result<(LinkTx, LinkRx), local::LinkError> {
        // Register this connection with the router. Router replies with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex. max connection limit).
        let (link_tx, link_rx, _ack) =
            LinkBuilder::new(client_id, self.router_tx.clone()).build()?;
        Ok((link_tx, link_rx))
    }

    #[tracing::instrument(skip(self))]
    pub fn start(&mut self) -> Result<(), Error> {
        if let Some(metrics_config) = self.config.metrics.clone() {
            let timer_thread = thread::Builder::new().name("timer".to_owned());
            let router_tx = self.router_tx.clone();
            timer_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();

                runtime.block_on(async move {
                    timer::start(metrics_config, router_tx).await;
                });
            })?;
        }

        // Spawn bridge in a separate thread.
        if let Some(bridge_config) = self.config.bridge.clone() {
            let bridge_thread = thread::Builder::new().name(bridge_config.name.clone());
            let router_tx = self.router_tx.clone();
            bridge_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();

                runtime.block_on(async move {
                    if let Err(e) = bridge::start(bridge_config, router_tx, V4).await {
                        error!(error=?e, "Bridge Link error");
                    };
                });
            })?;
        }

        // Spawn servers in a separate thread.
        for (_, config) in self.config.v4.clone() {
            let server_thread = thread::Builder::new().name(config.name.clone());
            let server = Server::new(config, self.router_tx.clone(), V4);
            server_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();

                runtime.block_on(async {
                    if let Err(e) = server.start(LinkType::Remote).await {
                        error!(error=?e, "Server error - V4");
                    }
                });
            })?;
        }

        if let Some(v5_config) = &self.config.v5 {
            for (_, config) in v5_config.clone() {
                let server_thread = thread::Builder::new().name(config.name.clone());
                let server = Server::new(config, self.router_tx.clone(), V5);
                server_thread.spawn(move || {
                    let mut runtime = tokio::runtime::Builder::new_current_thread();
                    let runtime = runtime.enable_all().build().unwrap();

                    runtime.block_on(async {
                        if let Err(e) = server.start(LinkType::Remote).await {
                            error!(error=?e, "Server error - V5");
                        }
                    });
                })?;
            }
        }

        #[cfg(feature = "websocket")]
        if let Some(ws_config) = &self.config.ws {
            for (_, config) in ws_config.clone() {
                let server_thread = thread::Builder::new().name(config.name.clone());
                //TODO: Add support for V5 procotol with websockets. Registered in config or on ServerSettings
                let server = Server::new(config, self.router_tx.clone(), V4);
                server_thread.spawn(move || {
                    let mut runtime = tokio::runtime::Builder::new_current_thread();
                    let runtime = runtime.enable_all().build().unwrap();

                    runtime.block_on(async {
                        if let Err(e) = server.start(LinkType::Websocket).await {
                            error!(error=?e, "Server error - WS");
                        }
                    });
                })?;
            }
        }

        if let Some(prometheus_setting) = &self.config.prometheus {
            let timeout = prometheus_setting.interval;
            // If port is specified use it instead of listen.
            // NOTE: This means listen is ignored when `port` is specified.
            // `port` will be removed in future release in favour of `listen`
            let addr = {
                #[allow(deprecated)]
                match prometheus_setting.port {
                    Some(port) => SocketAddr::new("127.0.0.1".parse().unwrap(), port),
                    None => prometheus_setting.listen.unwrap_or(SocketAddr::new(
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                        9042,
                    )),
                }
            };
            let metrics_thread = thread::Builder::new().name("Metrics".to_owned());
            let meter_link = self.meters().unwrap();
            metrics_thread.spawn(move || {
                let builder = PrometheusBuilder::new().with_http_listener(addr);
                builder.install().unwrap();

                let total_publishes = register_gauge!("metrics.router.total_publishes");
                let total_connections = register_gauge!("metrics.router.total_connections");
                let failed_publishes = register_gauge!("metrics.router.failed_publishes");
                loop {
                    if let Ok(metrics) = meter_link.recv() {
                        for m in metrics {
                            match m {
                                Meter::Router(_, ref r) => {
                                    total_connections.set(r.total_connections as f64);
                                    total_publishes.set(r.total_publishes as f64);
                                    failed_publishes.set(r.failed_publishes as f64);
                                }
                                _ => continue,
                            }
                        }
                    }

                    std::thread::sleep(Duration::from_secs(timeout));
                }
            })?;
        }

        let console_link = ConsoleLink::new(self.config.console.clone(), self.router_tx.clone());

        let console_link = Arc::new(console_link);
        let mut runtime = tokio::runtime::Builder::new_current_thread();
        let runtime = runtime.enable_all().build().unwrap();
        runtime.block_on(console::start(console_link));

        Ok(())
    }
}

#[derive(Copy, Clone)]
pub enum LinkType {
    #[cfg(feature = "websocket")]
    Websocket,
    Remote,
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

    async fn start(&self, link_type: LinkType) -> Result<(), Error> {
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
                name=?self.config.name, ?addr, count, tenant=?tenant_id, "accept"
            );

            let config = config.clone();
            let router_tx = self.router_tx.clone();
            count += 1;

            let protocol = self.protocol.clone();
            match link_type {
                #[cfg(feature = "websocket")]
                LinkType::Websocket => {
                    let stream = match accept_hdr_async(network, WSCallback).await {
                        Ok(s) => Box::new(WsStream::new(s)),
                        Err(e) => {
                            error!(error=?e, "Websocket failed handshake");
                            continue;
                        }
                    };
                    task::spawn(
                        remote(config, tenant_id.clone(), router_tx, stream, protocol).instrument(
                            tracing::info_span!(
                                "websocket_link",
                                client_id = field::Empty,
                                connection_id = field::Empty
                            ),
                        ),
                    )
                }
                LinkType::Remote => task::spawn(
                    remote(config, tenant_id.clone(), router_tx, network, protocol).instrument(
                        tracing::error_span!(
                            "remote_link",
                            ?tenant_id,
                            client_id = field::Empty,
                            connection_id = field::Empty,
                        ),
                    ),
                ),
            };

            time::sleep(delay).await;
        }
    }
}

/// Configures the Websocket connection to indicate the correct protocol
/// by adding the "sec-websocket-protocol" with value of "mqtt" to the response header
#[cfg(feature = "websocket")]
struct WSCallback;
#[cfg(feature = "websocket")]
impl Callback for WSCallback {
    fn on_request(
        self,
        _request: &Request,
        mut response: Response,
    ) -> Result<Response, ErrorResponse> {
        response
            .headers_mut()
            .insert("sec-websocket-protocol", HeaderValue::from_static("mqtt"));
        Ok(response)
    }
}

/// A new network connection should wait for a mqtt connect packet. This should be handled
/// asynchronously to avoid blocking other new connections while this connection is
/// waiting for mqtt connect packet. Also this honours connection wait time as per config to prevent
/// denial of service attacks (rogue clients which only establish network connections without
/// sending a mqtt connection packet to make the server reach its concurrent connection limit).
async fn remote<P: Protocol>(
    config: Arc<ConnectionSettings>,
    tenant_id: Option<String>,
    router_tx: Sender<(ConnectionId, Event)>,
    stream: Box<dyn N>,
    protocol: P,
) {
    let network = Network::new(
        stream,
        config.max_payload_size,
        config.max_inflight_count,
        protocol,
    );
    // Start the link
    let mut link =
        match RemoteLink::new(config, router_tx.clone(), tenant_id.clone(), network).await {
            Ok(l) => l,
            Err(e) => {
                error!(error=?e, "Remote link error");
                return;
            }
        };

    let client_id = link.client_id.to_owned();
    let connection_id = link.connection_id;
    let mut execute_will = false;

    match link.start().await {
        // Connection got closed. This shouldn't usually happen.
        Ok(_) => error!("connection-stop"),
        // No need to send a disconnect message when disconnection
        // originated internally in the router.
        Err(remote::Error::Link(e)) => {
            error!(error=?e, "router-drop");
            return;
        }
        // Any other error
        Err(e) => {
            error!(error=?e, "Disconnected!!");
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

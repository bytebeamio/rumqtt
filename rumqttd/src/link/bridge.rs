use bytes::BytesMut;
use flume::Sender;

use std::{
    fs,
    io::{self, BufReader, Cursor},
    net::{AddrParseError, SocketAddr, ToSocketAddrs},
    path::Path,
    sync::Arc,
    time::Duration,
};

use tokio::{
    net::{lookup_host, TcpStream},
    time::{sleep, sleep_until, Instant},
};
use tokio_rustls::{
    client::TlsStream,
    rustls::{ClientConfig, Error as TLSError},
    webpki::{self, DnsNameRef, InvalidDnsNameError},
    TlsConnector,
};
use tracing::*;

use crate::{
    link::{
        local::{Link, LinkError},
        network::Network,
    },
    protocol::{self, Connect, Packet, Protocol, QoS, RetainForwardRule, Subscribe},
    router::Event,
    BridgeConfig, ClientAuth, ConnectionId, Transport,
};

pub async fn start<P>(
    config: BridgeConfig,
    router_tx: Sender<(ConnectionId, Event)>,
    protocol: P,
) -> Result<(), BridgeError>
where
    P: Protocol + Clone + Send + 'static,
{
    let (tx, _rx, _ack) = Link::new(None, &config.name, router_tx, true, None, true)?;

    let addr = match lookup_host(config.addr).await?.next() {
        Some(addr) => addr,
        None => return Err(BridgeError::InvalidUrl),
    };

    'outer: loop {
        let mut network = match network_connect(&config, &addr, protocol.clone()).await {
            Ok(v) => v,
            Err(e) => {
                warn!("bridge error: {}, retrying", e);
                sleep(Duration::from_secs(config.reconnection_delay)).await;
                continue;
            }
        };
        info!("bridge: connected to {}", &addr);
        if let Err(e) = network_init(&config, &mut network).await {
            warn!("bridge: unable to init connection, reconnecting - {}", e);
            sleep(Duration::from_secs(config.reconnection_delay)).await;
            continue;
        }
    }
}

async fn network_connect<P: Protocol>(
    config: &BridgeConfig,
    addr: &SocketAddr,
    protocol: P,
) -> Result<Network<P>, BridgeError> {
    match &config.transport {
        Transport::Tcp => {
            let socket = match TcpStream::connect(addr).await {
                Ok(v) => v,
                Err(e) => return Err(BridgeError::Io(e)),
            };
            Ok(Network::new(
                Box::new(socket),
                config.connections.max_payload_size,
                100,
                protocol,
            ))
        }
        Transport::Tls { ca, client_auth } => {
            unimplemented!();
            // let socket = match tls_connect(config.url.clone(), config.port, &ca, client_auth).await
            // {
            //     Ok(v) => v,
            //     Err(e) => return Err((e, "unable to form TCP/TLS connection")),
            // };
            // Ok(Network::new(Box::new(socket), 5120))
        }
    }
}

async fn network_init<P: Protocol>(
    config: &BridgeConfig,
    network: &mut Network<P>,
) -> Result<(), BridgeError> {
    let connect = Connect {
        keep_alive: 10,
        client_id: config.name.clone(),
        clean_session: true,
    };
    let packet = Some(Packet::Connect(connect, None, None, None, None));

    send_and_recv(network, packet, |packet| {
        matches!(packet, Packet::ConnAck(..))
    })
    .await?;

    // connecting to other router
    let qos = match config.qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => return Err(BridgeError::InvalidQos),
    };

    let filters = vec![protocol::Filter {
        path: config.sub_path.clone(),
        qos,
        nolocal: false,
        preserve_retain: false,
        retain_forward_rule: RetainForwardRule::Never,
    }];

    let subscribe = Subscribe { pkid: 0, filters };
    let packet = Some(Packet::Subscribe(subscribe, None));
    send_and_recv(network, packet, |packet| {
        matches!(packet, Packet::SubAck(..))
    })
    .await
}

async fn send_and_recv<F: FnOnce(Packet) -> bool, P: Protocol>(
    network: &mut Network<P>,
    send_packet: Option<Packet>,
    accept_recv: F,
) -> Result<(), BridgeError> {
    network.write(send_packet).await.unwrap();
    match accept_recv(network.read().await?) {
        true => Ok(()),
        false => Err(BridgeError::InvalidPacket),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("Addr - {0}")]
    Addr(#[from] AddrParseError),
    #[error("I/O - {0}")]
    Io(#[from] io::Error),
    #[error("Web Pki - {0}")]
    WebPki(#[from] tokio_rustls::webpki::Error),
    #[error("DNS name - {0}")]
    DNSName(#[from] InvalidDnsNameError),
    #[error("TLS error - {0}")]
    TLS(#[from] TLSError),
    #[error("No valid cert in chain")]
    NoValidCertInChain,
    #[error("local link - {0}")]
    Link(#[from] LinkError),
    #[error("invalid qos")]
    InvalidQos,
    #[error("invalid key")]
    InvalidKey,
    #[error("invalid url")]
    InvalidUrl,
    #[error("invalid packet")]
    InvalidPacket,
}

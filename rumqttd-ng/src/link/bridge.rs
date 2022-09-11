use std::{
    fs,
    io::{self, BufReader, Cursor},
    net::{AddrParseError, SocketAddr, ToSocketAddrs},
    path::Path,
    sync::Arc,
    time::Duration,
};

use bytes::BytesMut;
use flume::Sender;
use log::*;
use tokio::{
    net::TcpStream,
    time::{sleep, sleep_until, Instant},
};
use tokio_rustls::{
    client::TlsStream,
    rustls::{internal::pemfile::*, ClientConfig, TLSError},
    webpki::{self, DNSNameRef, InvalidDNSNameError},
    TlsConnector,
};

use crate::{
    link::{
        local::{Link, LinkError},
        network::Network,
    },
    router::Event,
    BridgeConfig, ClientAuth, ConnectionId, Transport, protocol::{v4::pingreq, Packet},
};

pub async fn bridge_launch(
    config: BridgeConfig,
    router_tx: Sender<(ConnectionId, Event)>,
) -> Result<(), BridgeError> {
    // connect to local router
    let (mut tx, _rx) = Link::new("bridge", router_tx, true)?;

    // connecting to other router
    let qos = match u8_to_qos(config.qos) {
        Ok(v) => v,
        Err(_e) => {
            return Err(BridgeError::InvalidQos);
        }
    };
    let addr = match (config.url.clone(), config.port).to_socket_addrs()?.next() {
        Some(addr) => addr,
        None => return Err(BridgeError::InvalidUrl),
    };

    'outer: loop {
        let mut network = match netowrk_connect(&config, &addr).await {
            Ok(v) => v,
            Err((e, s)) => {
                warn!("bridge: {}, retrying - {}", s, e);
                sleep(Duration::from_secs(config.reconnection_delay)).await;
                continue;
            }
        };
        info!("bridge: connected to {}", config.url);

        if let Err(e) = network_init(&mut network, &config.sub_path, qos).await {
            warn!("bridge: unable to init connection, reconnecting - {}", e);
            sleep(Duration::from_secs(config.reconnection_delay)).await;
            continue;
        }

        let mut buf = BytesMut::with_capacity(2);
        pingreq::write(&mut buf).unwrap();
        debug!("bridge: recved suback from {}", config.url);

        let mut ping_time = Instant::now();
        let mut timeout = sleep_until(ping_time + Duration::from_secs(config.ping_delay));
        let mut ping_unacked = false;

        loop {
            tokio::select! {
                packet_res = network.read() => {
                    // resetting timeout because tokio::select! consumes the old timeout future
                    timeout = sleep_until(ping_time + Duration::from_secs(config.ping_delay));
                    let packet = match packet_res {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("bridge: unable to read from network stream, reconnecting - {}", e);
                            sleep(Duration::from_secs(config.reconnection_delay)).await;
                            continue 'outer;
                        }
                    };

                    match packet {
                        Packet::Publish(publish) => {
                            tx.send(Packet::Publish(publish)).await?;
                        }
                        Packet::PingResp => ping_unacked = false,
                        packet => warn!("bridge: expected publish, got {:?}", packet),
                    }
                }
                _ = timeout => {
                    // retry connection if ping not acked till next timeout
                    if ping_unacked {
                        warn!("bridge: no response to ping, reconnecting");
                        sleep(Duration::from_secs(config.reconnection_delay)).await;
                        continue 'outer;
                    }

                    if let Err(e) = network.write_all(&buf).await {
                        warn!("bridge: unable to write PINGRES to network stream, reconnecting - {}", e);
                        sleep(Duration::from_secs(config.reconnection_delay)).await;
                        continue 'outer;
                    };
                    ping_unacked = true;

                    ping_time = Instant::now();
                    // resetting timeout because tokio::select! consumes the old timeout future
                    timeout = sleep_until(ping_time + Duration::from_secs(config.ping_delay));
                }
            }
        }
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
    DNSName(#[from] InvalidDNSNameError),
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

// The cert handling functions return unit right now, this is a shortcut
impl From<()> for BridgeError {
    fn from(_: ()) -> Self {
        BridgeError::NoValidCertInChain
    }
}

async fn netowrk_connect(
    config: &BridgeConfig,
    addr: &SocketAddr,
) -> Result<Network, (BridgeError, &'static str)> {
    match &config.transport {
        Transport::Tcp => {
            let socket = match TcpStream::connect(addr).await {
                Ok(v) => v,
                Err(e) => return Err((BridgeError::Io(e), "unable to form TCP connection")),
            };
            Ok(Network::new(Box::new(socket), 5120))
        }
        Transport::Tls { ca, client_auth } => {
            let socket = match tls_connect(config.url.clone(), config.port, &ca, client_auth).await
            {
                Ok(v) => v,
                Err(e) => return Err((e, "unable to form TCP/TLS connection")),
            };
            Ok(Network::new(Box::new(socket), 5120))
        }
    }
}

async fn send_and_recv<F: FnOnce(Packet) -> bool>(
    network: &mut Network,
    send_packet: &[u8],
    accept_recv: F,
) -> Result<(), BridgeError> {
    network.write_all(send_packet).await?;
    match accept_recv(network.read().await?) {
        true => Ok(()),
        false => Err(BridgeError::InvalidPacket),
    }
}

async fn network_init(network: &mut Network, sub_path: &str, qos: QoS) -> Result<(), BridgeError> {
    let mut buf = BytesMut::new();
    Connect::new("bytebeam-bridge").write(&mut buf).unwrap();
    send_and_recv(network, &buf, |packet| matches!(packet, Packet::ConnAck(_))).await?;

    buf.clear();
    let _len = subscribe::write(
        vec![SubscribeFilter {
            path: sub_path.to_owned(),
            qos,
        }],
        1,
        &mut buf,
    );
    send_and_recv(network, &buf, |packet| matches!(packet, Packet::SubAck(_))).await
}

// TODO: Try to replicate rumqttc
pub async fn tls_connect<P: AsRef<Path>>(
    broker_addr: String,
    port: u16,
    ca_file: P,
    client_auth_opt: &Option<ClientAuth>,
) -> Result<TlsStream<TcpStream>, BridgeError> {
    let mut config = ClientConfig::new();
    config
        .root_store
        .add_pem_file(&mut BufReader::new(Cursor::new(fs::read(ca_file)?)))?;

    if let Some(ClientAuth {
        certs: certs_path,
        key: key_path,
    }) = client_auth_opt
    {
        let read_certs = certs(&mut BufReader::new(Cursor::new(fs::read(certs_path)?)))?;

        let read_keys = match rustls_pemfile::read_one(&mut BufReader::new(Cursor::new(fs::read(
            key_path,
        )?)))? {
            Some(rustls_pemfile::Item::RSAKey(_)) => {
                rsa_private_keys(&mut BufReader::new(Cursor::new(fs::read(key_path)?)))?
            }
            Some(rustls_pemfile::Item::PKCS8Key(_)) => {
                pkcs8_private_keys(&mut BufReader::new(Cursor::new(fs::read(key_path)?)))?
            }
            None | Some(_) => return Err(BridgeError::InvalidKey),
        };

        let read_key = match read_keys.first() {
            Some(v) => v.clone(),
            None => return Err(BridgeError::InvalidKey),
        };

        config.set_single_client_cert(read_certs, read_key)?;
    }

    let connector = TlsConnector::from(Arc::new(config));
    let addr = broker_addr.clone();
    let domain = DNSNameRef::try_from_ascii_str(&broker_addr)?;
    let tcp = TcpStream::connect((addr, port)).await?;
    let tls = connector.connect(domain, tcp).await?;
    Ok(tls)
}

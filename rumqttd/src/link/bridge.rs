use flume::Sender;

use std::{
    fs,
    io::{self, BufReader, Cursor},
    net::AddrParseError,
    path::Path,
    sync::Arc,
    time::Duration,
};

use tokio::{
    net::TcpStream,
    time::{sleep, sleep_until, Instant},
};
use tokio_rustls::{
    rustls::{
        client::InvalidDnsNameError, Certificate, ClientConfig, Error as TLSError,
        OwnedTrustAnchor, PrivateKey, RootCertStore, ServerName,
    },
    webpki, TlsConnector,
};
use tracing::*;

use crate::{
    link::{
        local::{Link, LinkError},
        network::Network,
    },
    protocol::{
        self, Connect, Packet, PingReq, PingResp, Protocol, QoS, RetainForwardRule, Subscribe,
    },
    router::{Ack, Event},
    BridgeConfig, ClientAuth, ConnectionId, Notification, Transport,
};

use super::network::{self, N};

pub async fn start<P>(
    config: BridgeConfig,
    router_tx: Sender<(ConnectionId, Event)>,
    protocol: P,
) -> Result<(), BridgeError>
where
    P: Protocol + Clone + Send + 'static,
{
    let (mut tx, mut rx, _ack) = Link::new(None, &config.name, router_tx, true, None, true)?;

    'outer: loop {
        let mut network = match network_connect(&config, &config.addr, protocol.clone()).await {
            Ok(v) => v,
            Err(e) => {
                warn!("bridge error: {}, retrying", e);
                sleep(Duration::from_secs(config.reconnection_delay)).await;
                continue;
            }
        };
        info!("bridge: connected to {}", &config.addr);
        if let Err(e) = network_init(&config, &mut network).await {
            warn!("bridge: unable to init connection, reconnecting - {}", e);
            sleep(Duration::from_secs(config.reconnection_delay)).await;
            continue;
        }

        let ping_req = Packet::PingReq(PingReq);
        debug!("bridge: recved suback from {}", &config.addr);

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
                        Packet::Publish(publish, publish_prop) => {
                            tx.send(Packet::Publish(publish, publish_prop)).await?;
                        }
                        Packet::PingResp(_) => ping_unacked = false,
                        packet => warn!("bridge: expected publish, got {:?}", packet),
                    }
                }
                o = rx.next() => {
                    let notif = match o {
                        Ok(notif) => notif,
                        Err(e) => {
                            warn!("bridge: unable to write PINGRES to network stream, reconnecting - {}", e);
                            sleep(Duration::from_secs(config.reconnection_delay)).await;
                            continue 'outer;
                        }
                    };
                    if let Some(notif) = notif {
                        match notif {
                            Notification::DeviceAck(ack) => {
                                write_ack_to_network(ack, &mut network).await?;
                            },
                            _ => unreachable!("We should only get device acks"),
                        }

                    }
                    timeout = sleep_until(ping_time + Duration::from_secs(config.ping_delay));

                }
                _ = timeout => {
                    // retry connection if ping not acked till next timeout
                    if ping_unacked {
                        warn!("bridge: no response to ping, reconnecting");
                        sleep(Duration::from_secs(config.reconnection_delay)).await;
                        continue 'outer;
                    }

                    if let Err(e) = network.write(ping_req.clone()).await {
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
async fn write_ack_to_network<P: Protocol>(
    ack: Ack,
    network: &mut Network<P>,
) -> Result<(), BridgeError> {
    match ack {
        Ack::ConnAck(_, _) => (),
        Ack::PubAck(ack) => {
            let puback = Packet::PubAck(ack, None);
            network.write(puback).await?;
        }
        Ack::PubAckWithProperties(_, _) => todo!(),
        Ack::SubAck(_) => (),
        Ack::SubAckWithProperties(_, _) => todo!(),
        Ack::PubRec(ack) => {
            let pubrec = Packet::PubRec(ack, None);
            network.write(pubrec).await?;
        }
        Ack::PubRecWithProperties(_, _) => todo!(),
        Ack::PubRel(ack) => {
            let pubrel = Packet::PubRel(ack, None);
            network.write(pubrel).await?;
        }
        Ack::PubRelWithProperties(_, _) => todo!(),
        Ack::PubComp(ack) => {
            let pubcomp = Packet::PubComp(ack, None);
            network.write(pubcomp).await?;
        }
        Ack::PubCompWithProperties(_, _) => todo!(),
        Ack::UnsubAck(_) => (),
        Ack::PingResp(_) => {
            let pingresp = Packet::PingResp(PingResp);
            network.write(pingresp).await?;
        }
    }
    Ok(())
}

async fn network_connect<P: Protocol>(
    config: &BridgeConfig,
    addr: &str,
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
            let tcp_stream = match TcpStream::connect(addr).await {
                Ok(v) => v,
                Err(e) => return Err(BridgeError::Io(e)),
            };
            let host = addr.split(':').next().unwrap();
            let socket = match tls_connect(&host, &ca, client_auth, tcp_stream).await {
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            Ok(Network::new(
                Box::new(socket),
                config.connections.max_payload_size,
                100,
                protocol,
            ))
        }
    }
}
pub async fn tls_connect<P: AsRef<Path>>(
    host: &str,
    ca_file: P,
    client_auth_opt: &Option<ClientAuth>,
    tcp: TcpStream,
) -> Result<Box<dyn N>, BridgeError> {
    let mut root_cert_store = RootCertStore::empty();

    let ca_certs = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(fs::read(ca_file)?)))?;
    let trust_anchors = ca_certs.iter().map_while(|cert| {
        if let Ok(ta) = webpki::TrustAnchor::try_from_cert_der(&cert[..]) {
            Some(OwnedTrustAnchor::from_subject_spki_name_constraints(
                ta.subject,
                ta.spki,
                ta.name_constraints,
            ))
        } else {
            None
        }
    });

    root_cert_store.add_server_trust_anchors(trust_anchors);

    if root_cert_store.is_empty() {
        return Err(BridgeError::NoValidCertInChain);
    }

    let config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_cert_store);

    let config = if let Some(ClientAuth {
        certs: certs_path,
        key: key_path,
    }) = client_auth_opt
    {
        let read_certs =
            rustls_pemfile::certs(&mut BufReader::new(Cursor::new(fs::read(certs_path)?)))?;

        let read_keys = match rustls_pemfile::read_one(&mut BufReader::new(Cursor::new(fs::read(
            key_path,
        )?)))? {
            Some(rustls_pemfile::Item::RSAKey(_)) => rustls_pemfile::rsa_private_keys(
                &mut BufReader::new(Cursor::new(fs::read(key_path)?)),
            )?,
            Some(rustls_pemfile::Item::PKCS8Key(_)) => rustls_pemfile::pkcs8_private_keys(
                &mut BufReader::new(Cursor::new(fs::read(key_path)?)),
            )?,
            None | Some(_) => return Err(BridgeError::NoValidCertInChain),
        };

        let read_key = match read_keys.first() {
            Some(v) => v.clone(),
            None => return Err(BridgeError::NoValidCertInChain),
        };

        let certs = read_certs.into_iter().map(Certificate).collect();
        config.with_single_cert(certs, PrivateKey(read_key))?
    } else {
        config.with_no_client_auth()
    };

    let connector = TlsConnector::from(Arc::new(config));
    let domain = ServerName::try_from(host).unwrap();
    Ok(Box::new(connector.connect(domain, tcp).await?))
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
    let packet = Packet::Connect(connect, None, None, None, None);

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
    let packet = Packet::Subscribe(subscribe, None);
    send_and_recv(network, packet, |packet| {
        matches!(packet, Packet::SubAck(..))
    })
    .await
}

async fn send_and_recv<F: FnOnce(Packet) -> bool, P: Protocol>(
    network: &mut Network<P>,
    send_packet: Packet,
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
    #[error("Network - {0}")]
    Network(#[from] network::Error),
    #[error("Web Pki - {0}")]
    WebPki(#[from] tokio_rustls::webpki::Error),
    #[error("DNS name - {0}")]
    DNSName(#[from] InvalidDnsNameError),
    #[error("TLS error - {0}")]
    TLS(#[from] TLSError),
    #[error("local link - {0}")]
    Link(#[from] LinkError),
    #[error("invalid qos")]
    InvalidQos,
    #[error("invalid packet")]
    InvalidPacket,
    #[error("invalid trust_anchor")]
    NoValidCertInChain,
}

//! Demonstrates running the MQTT client over a simulated network via a custom
//! [`Connector`]. Both the client and a minimal MQTT broker run as turmoil
//! hosts, so no real sockets are used — the connect/subscribe/deliver round-trip
//! is exercised entirely inside the simulation, for both the v4 and v5 clients.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use rumqttc::{async_trait, AsyncReadWrite, Connector, NetworkOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use turmoil::net::{TcpListener, TcpStream};

const MAX_SIZE: usize = 10 * 1024;
const TOPIC: &str = "hello/world";
const PAYLOAD: &[u8] = &[1, 2, 3, 4];

/// A [`Connector`] that dials the broker over turmoil's simulated network
/// instead of the default `tokio::net` TCP socket.
struct TurmoilConnector;

#[async_trait]
impl Connector for TurmoilConnector {
    async fn connect(
        &self,
        addr: &str,
        _network_options: &NetworkOptions,
    ) -> io::Result<Box<dyn AsyncReadWrite>> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Box::new(stream))
    }
}

fn network_options_with_connector() -> NetworkOptions {
    let mut network_options = NetworkOptions::new();
    network_options.set_connector(Arc::new(TurmoilConnector));
    network_options
}

// ---------------------------------------------------------------------------
// MQTT v4
// ---------------------------------------------------------------------------

mod v4 {
    use super::*;
    use rumqttc::mqttbytes::v4::{
        ConnAck, ConnectReturnCode, Packet, Publish, SubAck, SubscribeReasonCode,
    };
    use rumqttc::mqttbytes::{Error, QoS};
    use rumqttc::{AsyncClient, Event, Incoming, MqttOptions};

    async fn send(stream: &mut TcpStream, packet: Packet) -> io::Result<()> {
        let mut buf = BytesMut::new();
        packet
            .write(&mut buf, MAX_SIZE)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        stream.write_all(&buf).await
    }

    /// A minimal MQTT v4 broker: acks CONNECT, answers SUBSCRIBE with a SUBACK
    /// and then delivers one publish, and replies to pings.
    pub async fn run_broker() -> Result<(), BoxError> {
        let listener = TcpListener::bind(("0.0.0.0", 1883)).await?;
        loop {
            let (mut stream, _) = listener.accept().await?;
            let mut read = BytesMut::new();
            let mut chunk = [0u8; 1024];

            loop {
                match Packet::read(&mut read, MAX_SIZE) {
                    Ok(Packet::Connect(_)) => {
                        send(
                            &mut stream,
                            Packet::ConnAck(ConnAck::new(ConnectReturnCode::Success, false)),
                        )
                        .await?;
                    }
                    Ok(Packet::Subscribe(subscribe)) => {
                        let suback = SubAck::new(
                            subscribe.pkid,
                            vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
                        );
                        send(&mut stream, Packet::SubAck(suback)).await?;

                        let mut publish = Publish::new(TOPIC, QoS::AtLeastOnce, PAYLOAD);
                        publish.pkid = 1;
                        send(&mut stream, Packet::Publish(publish)).await?;
                    }
                    Ok(Packet::PingReq) => send(&mut stream, Packet::PingResp).await?,
                    Ok(_) => {}
                    Err(Error::InsufficientBytes(_)) => {
                        let n = stream.read(&mut chunk).await?;
                        if n == 0 {
                            break;
                        }
                        read.extend_from_slice(&chunk[..n]);
                    }
                    Err(e) => return Err(Box::new(e)),
                }
            }
        }
    }

    pub async fn run_client() -> Result<(), BoxError> {
        let mut mqttoptions = MqttOptions::new("turmoil-client-v4", "broker", 1883);
        mqttoptions.set_keep_alive(Duration::from_secs(5));

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
        eventloop.set_network_options(network_options_with_connector());

        client.subscribe(TOPIC, QoS::AtLeastOnce).await?;

        loop {
            if let Event::Incoming(Incoming::Publish(publish)) = eventloop.poll().await? {
                assert_eq!(publish.topic, TOPIC);
                assert_eq!(&publish.payload[..], PAYLOAD);
                return Ok(());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// MQTT v5
// ---------------------------------------------------------------------------

mod v5 {
    use super::*;
    use rumqttc::v5::mqttbytes::v5::{
        ConnAck, ConnectReturnCode, Packet, PingResp, Publish, SubAck, SubscribeReasonCode,
    };
    use rumqttc::v5::mqttbytes::{Error, QoS};
    use rumqttc::v5::{AsyncClient, Event, MqttOptions};

    async fn send(stream: &mut TcpStream, packet: Packet) -> io::Result<()> {
        let mut buf = BytesMut::new();
        packet
            .write(&mut buf, None)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        stream.write_all(&buf).await
    }

    /// A minimal MQTT v5 broker mirroring the v4 one.
    pub async fn run_broker() -> Result<(), BoxError> {
        let listener = TcpListener::bind(("0.0.0.0", 1883)).await?;
        loop {
            let (mut stream, _) = listener.accept().await?;
            let mut read = BytesMut::new();
            let mut chunk = [0u8; 1024];

            loop {
                match Packet::read(&mut read, None) {
                    Ok(Packet::Connect(..)) => {
                        let connack = ConnAck {
                            session_present: false,
                            code: ConnectReturnCode::Success,
                            properties: None,
                        };
                        send(&mut stream, Packet::ConnAck(connack)).await?;
                    }
                    Ok(Packet::Subscribe(subscribe)) => {
                        let suback = SubAck {
                            pkid: subscribe.pkid,
                            return_codes: vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
                            properties: None,
                        };
                        send(&mut stream, Packet::SubAck(suback)).await?;

                        let publish = Publish {
                            dup: false,
                            qos: QoS::AtLeastOnce,
                            retain: false,
                            topic: Bytes::from_static(TOPIC.as_bytes()),
                            pkid: 1,
                            payload: Bytes::from_static(PAYLOAD),
                            properties: None,
                        };
                        send(&mut stream, Packet::Publish(publish)).await?;
                    }
                    Ok(Packet::PingReq(_)) => send(&mut stream, Packet::PingResp(PingResp)).await?,
                    Ok(_) => {}
                    Err(Error::InsufficientBytes(_)) => {
                        let n = stream.read(&mut chunk).await?;
                        if n == 0 {
                            break;
                        }
                        read.extend_from_slice(&chunk[..n]);
                    }
                    Err(e) => return Err(Box::new(e)),
                }
            }
        }
    }

    pub async fn run_client() -> Result<(), BoxError> {
        let mut mqttoptions = MqttOptions::new("turmoil-client-v5", "broker", 1883);
        mqttoptions.set_keep_alive(Duration::from_secs(5));
        mqttoptions.set_network_options(network_options_with_connector());

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

        client.subscribe(TOPIC, QoS::AtLeastOnce).await?;

        loop {
            if let Event::Incoming(Packet::Publish(publish)) = eventloop.poll().await? {
                assert_eq!(&publish.topic[..], TOPIC.as_bytes());
                assert_eq!(&publish.payload[..], PAYLOAD);
                return Ok(());
            }
        }
    }
}

type BoxError = Box<dyn std::error::Error>;

#[test]
fn v4_connect_and_receive_over_turmoil() {
    let mut sim = turmoil::Builder::new().build();
    sim.host("broker", v4::run_broker);
    sim.client("client", v4::run_client());
    sim.run().unwrap();
}

#[test]
fn v5_connect_and_receive_over_turmoil() {
    let mut sim = turmoil::Builder::new().build();
    sim.host("broker", v5::run_broker);
    sim.client("client", v5::run_client());
    sim.run().unwrap();
}

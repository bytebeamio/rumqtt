use derive_more::From;
use futures_util::future::FutureExt;
use futures_util::select;
use futures_util::stream::StreamExt;
use rumq_core::{connack, MqttRead, MqttWrite, Packet, Connect, ConnectReturnCode};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::TrySendError;
use tokio::time;

use crate::router::RouterMessage;
use crate::Network;
use crate::ServerSettings;

use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, From)]
pub enum Error {
    Core(rumq_core::Error),
    Timeout(time::Elapsed),
    Mpsc(TrySendError<RouterMessage>),
    /// Received a wrong packet while waiting for another packet
    WrongPacket,
    /// Invalid client ID
    InvalidClientId,
    Disconnect,
    SlowRouter,
    NoReceiver
}

pub async fn eventloop(config: Arc<ServerSettings>, stream: impl Network, mut router_tx: Sender<RouterMessage>) -> Result<String, Error> {
    // TODO: persistent new connection should get state back from the graveyard and send pending packets
    // along with outstanding publishes that the router received when the client connection is offline
    let mut connection = Connection::new(config, stream, router_tx.clone()).await?;
    let id = connection.id.clone();

    if let Err(err) = connection.run().await {
        error!("Connection error = {:?}", err);
     
        if let Error::Disconnect = err {
            router_tx.try_send(RouterMessage::Disconnect(id.clone()))?;
        } else {
            router_tx.try_send(RouterMessage::Death(id.clone()))?;
        }
    }

    Ok(id)
}

struct Connection<S> {
    id:         String,
    keep_alive: Duration,
    stream:     S,
    this_rx:    Receiver<RouterMessage>,
    router_tx:  Sender<RouterMessage>,
}

impl<S: Network> Connection<S> {
    async fn new(config: Arc<ServerSettings>, mut stream: S, mut router_tx: Sender<RouterMessage>) -> Result<Connection<S>, Error> {
        let (this_tx, this_rx) = channel(100);
        let timeout = Duration::from_millis(config.connection_timeout_ms.into());
        let (connect, connack) = time::timeout(timeout, async {
            let packet = stream.mqtt_read().await?;
            let o = handle_incoming_connect(packet)?;
            Ok::<_, Error>(o)
        })
        .await??;

        // write connack packet
        stream.mqtt_write(&connack).await?;

        let id = connect.client_id.clone();
        let keep_alive = Duration::from_secs(connect.keep_alive as u64);

        // construct connect router message with cliend id and handle to this connection
        let routermessage = RouterMessage::Connect((connect, this_tx));
        router_tx.try_send(routermessage)?;
        let connection = Connection { id, keep_alive, stream, this_rx, router_tx };
        Ok(connection)
    }

    async fn run(&mut self) -> Result<(), Error> {
        // TODO: Enable and monitor perf. Default buffer size of 8K. Might've to periodically flush?
        // Q. What happens when socket receives only 4K of data and there is no new data?
        // Will the userspace not receive this data indefinitely. I don't see any timeouts?
        // Carl says calls to read only issue one syscall. read_exact will "wait for that
        // amount to be read
        // Verify BufReader code
        // https://docs.rs/tokio/0.2.6/src/tokio/io/util/buf_reader.rs.html#117
        // let mut stream = BufStream::new(stream);
        let id = &self.id;
        // eventloop which processes packets and router messages
        loop {
            let stream = &mut self.stream;
            let keep_alive = self.keep_alive + self.keep_alive.mul_f32(0.5);

            // TODO: Use Delay::reset to not construct this timeout future everytime
            let packet = time::timeout(keep_alive, async {
                let packet = stream.mqtt_read().await?;
                Ok::<_, Error>(packet)
            });

            select! {
                // read packets from network and generate network reply and router message
                o = packet.fuse() => {
                    let packet = o??;
                    let message = match packet {
                        Packet::Publish(publish) => RouterMessage::Publish(publish),
                        Packet::Subscribe(subscribe) => RouterMessage::Subscribe((id.clone(), subscribe)),
                        Packet::Disconnect => {
                            return Err(Error::Disconnect)
                        }
                        _ => {
                            error!("Incorrect packet = {:?} received", packet);
                            return Err(Error::WrongPacket)
                        }
                    };

                    match self.router_tx.try_send(message) {
                        Err(TrySendError::Full(_)) => {
                            error!("Slow router. Disconnecting");
                            return Err(Error::SlowRouter)
                        }
                        Err(TrySendError::Closed(_)) => {
                            error!("Slow router. Disconnecting");
                            return Err(Error::NoReceiver)
                        }
                        Ok(_) => ()
                    }
                }

                // read packets from router and generate packets to write to network
                o = self.this_rx.next().fuse() => {
                    if let Some(packet) = o {
                        match packet {
                            RouterMessage::Publish(publish) => {
                                let packet = Packet::Publish(publish);
                                self.stream.mqtt_write(&packet).await?;
                            }
                            RouterMessage::PubAck(pkid) => {
                                let packet = Packet::Puback(pkid);
                                self.stream.mqtt_write(&packet).await?;
                            }
                            RouterMessage::SubAck(suback) => {
                                let packet = Packet::Suback(suback);
                                self.stream.mqtt_write(&packet).await?;
                            }
                            _ => {
                                error!("Incorrect packet = {:?} received", packet);
                                return Err(Error::WrongPacket)
                            }
                        }
                    }
                }
            };
        }
    }
}

pub fn handle_incoming_connect(packet: Packet) -> Result<(Connect, Packet), Error> {
    let mut connect = match packet {
        Packet::Connect(connect) => connect,
        packet => {
            error!("Invalid packet. Expecting connect. Received = {:?}", packet);
            return Err(Error::WrongPacket);
        }
    };

    // this broker expects a keepalive. 0 keepalives are promoted to 10 minutes
    if connect.keep_alive == 0 {
        warn!("0 keepalive. Promoting it to 10 minutes");
        connect.keep_alive = 10 * 60;
    }

    if connect.client_id.starts_with(' ') || connect.client_id.is_empty() {
        error!("Client id shouldn't start with space (or) shouldn't be emptys");
        return Err(Error::InvalidClientId);
    }

    let connack = connack(ConnectReturnCode::Accepted, false);
    // TODO: Handle session present
    let reply = Packet::Connack(connack);

    Ok((connect, reply))
}

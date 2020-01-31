use derive_more::From;
use futures_util::future::FutureExt;
use futures_util::select;
use futures_util::stream::StreamExt;
use rumq_core::{connack, MqttRead, MqttWrite, Packet, Connect, ConnectReturnCode};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::TrySendError;
use tokio::stream::iter;
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
    Mpsc(TrySendError<(String, RouterMessage)>),
    /// Received a wrong packet while waiting for another packet
    WrongPacket,
    /// Invalid client ID
    InvalidClientId,
    SlowRouter,
    NoReceiver,
    NotConnack
}

pub async fn eventloop(config: Arc<ServerSettings>, stream: impl Network, mut router_tx: Sender<(String, RouterMessage)>) -> Result<String, Error> {
    let mut connection = Connection::new(config, stream, router_tx.clone()).await?;
    let id = connection.id.clone();

    if let Err(err) = connection.run().await {
        error!("Connection error = {:?}", err);
        router_tx.try_send((id.clone(), RouterMessage::Death(id.clone())))?;
    }

    Ok(id)
}

struct Connection<S> {
    id:         String,
    keep_alive: Duration,
    stream:     S,
    this_rx:    Receiver<RouterMessage>,
    router_tx:  Sender<(String, RouterMessage)>,
}

impl<S: Network> Connection<S> {
    async fn new(config: Arc<ServerSettings>, mut stream: S, mut router_tx: Sender<(String, RouterMessage)>) -> Result<Connection<S>, Error> {
        let (this_tx, this_rx) = channel(100);
        let timeout = Duration::from_millis(config.connection_timeout_ms.into());
        let connect = time::timeout(timeout, async {
            let packet = stream.mqtt_read().await?;
            let o = handle_incoming_connect(packet)?;
            Ok::<_, Error>(o)
        })
        .await??;


        let id = connect.client_id.clone();
        let keep_alive = Duration::from_secs(connect.keep_alive as u64);

        // construct connect router message with cliend id and handle to this connection
        let routermessage = RouterMessage::Connect((connect, this_tx));
        router_tx.try_send((id.clone(), routermessage))?;
        let connection = Connection { id, keep_alive, stream, this_rx, router_tx };
        Ok(connection)
    }

    fn forward_to_router(&mut self, id: &str, message: RouterMessage) -> Result<(), Error> {
        match self.router_tx.try_send((id.to_owned(), message)) {
            Err(TrySendError::Full(_)) => Err(Error::SlowRouter),
            Err(TrySendError::Closed(_)) => Err(Error::NoReceiver),
            Ok(_) => Ok(()) 
        }
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
        let id = self.id.to_owned();
        
        let message = match self.this_rx.next().await {
            Some(m) => m,
            None => {
                info!("Tx closed!! Stopping the connection");
                return Ok(()) 
            }
        };


        let connectionack = match message {
            RouterMessage::Pending(connack) => connack,
            _ => return Err(Error::NotConnack)
        };
        
        // eventloop which pending packets from the last session 
        if let Some(mut pending) = connectionack {
            let connack = connack(ConnectReturnCode::Accepted, true);
            let packet = Packet::Connack(connack);
            self.stream.mqtt_write(&packet).await?;
            
            let mut pending = iter(pending.drain(..)).map(Packet::Publish);
            loop {
                let stream = &mut self.stream;
                let keep_alive = self.keep_alive + self.keep_alive.mul_f32(0.5);

                let packet = time::timeout(keep_alive, async {
                    let packet = stream.mqtt_read().await?;
                    Ok::<_, Error>(packet)
                });

                select! {
                    // read packets from network and generate network reply and router message
                    o = packet.fuse() => {
                        let message = match o?? {
                            Packet::Pingreq => self.stream.mqtt_write(&Packet::Pingresp).await?,
                            packet => {
                                let message = RouterMessage::Packet(packet);
                                self.forward_to_router(&id, message)?;
                            }
                        };
                    }

                    // read packets from the router and write to network
                    // router can close the connection by dropping tx handle. this should stop this
                    // eventloop without sending the death notification
                    o = pending.next().fuse() => match o {
                        Some(packet) => self.stream.mqtt_write(&packet).await?,
                        None => {
                            debug!("Done processing previous session and offline messages");
                            break
                        } 
                    }
                };
            }
        } else {
            let connack = connack(ConnectReturnCode::Accepted, false);
            let packet = Packet::Connack(connack);
            self.stream.mqtt_write(&packet).await?;
        }
        
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
                    let message = match o?? {
                        Packet::Pingreq => self.stream.mqtt_write(&Packet::Pingresp).await?,
                        packet => {
                            let message = RouterMessage::Packet(packet);
                            self.forward_to_router(&id, message)?;
                        }
                    };
                }

                // read packets from the router and write to network
                // router can close the connection by dropping tx handle. this should stop this
                // eventloop without sending the death notification
                o = self.this_rx.next().fuse() => match o {
                    Some(message) => {
                        match message {
                            RouterMessage::Packet(packet) => self.stream.mqtt_write(&packet).await?,
                            _ => return Err(Error::WrongPacket)
                        }
                    }
                    None => {
                        info!("Tx closed!! Stopping the connection");
                        return Ok(())
                    }
                }
            };
        }
    }
}

pub fn handle_incoming_connect(packet: Packet) -> Result<Connect, Error> {
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

    Ok(connect)
}

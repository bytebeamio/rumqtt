use derive_more::From;
use rumq_core::{connack, Packet, Connect, ConnectReturnCode};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tokio::stream::iter;
use tokio::stream::StreamExt;
use tokio::time;
use tokio::select;
use futures_util::sink::Sink;
use futures_util::sink::SinkExt;
use futures_util::stream::Stream;

use crate::router::{self, RouterMessage};
use crate::ServerSettings;

use std::sync::Arc;
use std::time::Duration;
use std::io;

#[derive(Debug, From)]
pub enum Error {
    Io(io::Error),
    Core(rumq_core::Error),
    Timeout(time::Elapsed),
    Send(SendError<(String, RouterMessage)>),
    /// Received a wrong packet while waiting for another packet
    WrongPacket,
    /// Invalid client ID
    InvalidClientId,
    NotConnack,
    StreamDone
}

pub async fn eventloop<S: Stream<Item = Result<Packet, rumq_core::Error>> + Sink<Packet, Error = io::Error> + Unpin>(config: Arc<ServerSettings>, stream: S, mut router_tx: Sender<(String, RouterMessage)>) -> Result<String, Error> {
    let mut connection = Connection::new(config, stream, router_tx.clone()).await?;
    let id = connection.id.clone();

    if let Err(err) = connection.run().await {
        error!("Connection error = {:?}. Id = {}", err, id);
        router_tx.send((id.clone(), RouterMessage::Death(id.clone()))).await?;
    }

    Ok(id)
}

pub struct Connection<S> {
    id:         String,
    keep_alive: Duration,
    stream:     S,
    this_rx:    Receiver<RouterMessage>,
    router_tx:  Sender<(String, RouterMessage)>,
}

impl<S: Stream<Item = Result<Packet, rumq_core::Error>> + Sink<Packet, Error = io::Error> + Unpin> Connection<S> {
    async fn new(config: Arc<ServerSettings>, mut stream: S, mut router_tx: Sender<(String, RouterMessage)>) -> Result<Connection<S>, Error> {
        let (this_tx, this_rx) = channel(100);
        let timeout = Duration::from_millis(config.connection_timeout_ms.into());
        let connect = time::timeout(timeout, async {
            let packet = stream.next().await.ok_or(Error::StreamDone)??;
            let o = handle_incoming_connect(packet)?;
            Ok::<_, Error>(o)
        })
        .await??;


        let id = connect.client_id.clone();
        let keep_alive = Duration::from_secs(connect.keep_alive as u64);

        // construct connect router message with cliend id and handle to this connection
        let routermessage = RouterMessage::Connect(router::Connection::new(connect, this_tx));
        router_tx.send((id.clone(), routermessage)).await?;
        let connection = Connection { id, keep_alive, stream, this_rx, router_tx };
        Ok(connection)
    }

    async fn run(&mut self) -> Result<(), Error> {
        let stream = &mut self.stream;
        let id = self.id.to_owned();

        let message = match self.this_rx.next().await {
            Some(m) => m,
            None => {
                info!("Tx closed!! Stopping the connection");
                return Ok(()) 
            }
        };


        let mut pending = match message {
            RouterMessage::Pending(connack) => connack,
            _ => return Err(Error::NotConnack)
        };

        // eventloop which pending packets from the last session 
        if pending.len() > 0 {
            let connack = connack(ConnectReturnCode::Accepted, true);
            let packet = Packet::Connack(connack);
            stream.send(packet).await?;

            let mut pending = iter(pending.drain(..)).map(Packet::Publish);
            loop {
                // let stream = &mut self.stream;
                let keep_alive = self.keep_alive + self.keep_alive.mul_f32(0.5);

                let packet = time::timeout(keep_alive, async {
                    let packet = stream.next().await.ok_or(Error::StreamDone)??;
                    Ok::<_, Error>(packet)
                });

                select! {
                    // read packets from network and generate network reply and router message
                    o = packet => {
                        match o?? {
                            Packet::Pingreq => stream.send(Packet::Pingresp).await?,
                            packet => {
                                let message = RouterMessage::Packet(packet);
                                self.router_tx.send((id.to_owned(), message)).await?;
                            }
                        };
                    }

                    // read packets from the router and write to network
                    // router can close the connection by dropping tx handle. this should stop this
                    // eventloop without sending the death notification
                    o = pending.next() => match o {
                        Some(packet) => stream.send(packet).await?,
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
            self.stream.send(packet).await?;
        }

        // eventloop which processes packets and router messages
        loop {
            let stream = &mut self.stream;
            let keep_alive = self.keep_alive + self.keep_alive.mul_f32(0.5);

            // TODO: Use Delay::reset to not construct this timeout future everytime
            let packet = time::timeout(keep_alive, async {
                let packet = stream.next().await.ok_or(Error::StreamDone)??;
                Ok::<_, Error>(packet)
            });
            
            let this_rx = &mut self.this_rx;
            let message = async {
                this_rx.next().await
            };

            select! {
                // read packets from network and generate network reply and router message
                o = packet => {
                    match o?? {
                        Packet::Pingreq => self.stream.send(Packet::Pingresp).await?,
                        packet => {
                            let message = RouterMessage::Packet(packet);
                            self.router_tx.send((id.to_owned(), message)).await?;
                        }
                    };
                }

                // read packets from the router and write to network
                // router can close the connection by dropping tx handle. this should stop this
                // eventloop without sending the death notification
                o = message => match o {
                    Some(message) => {
                        match message {
                            RouterMessage::Packet(packet) => self.stream.send(packet).await?,
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

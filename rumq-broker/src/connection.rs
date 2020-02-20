use derive_more::From;
use rumq_core::mqtt4::{connack, Packet, Connect, ConnectReturnCode};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tokio::stream::iter;
use tokio::stream::StreamExt;
use tokio::time;
use tokio::time::Elapsed;
use tokio::time::Instant;
use tokio::select;
use futures_util::sink::SinkExt;
use futures_util::stream::Stream;

use crate::router::{self, RouterMessage};
use crate::ServerSettings;
use crate::Network;

use std::sync::Arc;
use std::time::Duration;
use std::io;

#[derive(Debug, From)]
pub enum Error {
    Io(io::Error),
    Core(rumq_core::Error),
    Timeout(Elapsed),
    KeepAlive,
    Send(SendError<(String, RouterMessage)>),
    /// Received a wrong packet while waiting for another packet
    WrongPacket,
    /// Invalid client ID
    InvalidClientId,
    NotConnack,
    StreamDone
}

pub async fn eventloop<S: Network>(config: Arc<ServerSettings>, stream: S, mut router_tx: Sender<(String, RouterMessage)>) -> Result<String, Error> {
    let mut connection = Connection::new(config, stream, router_tx.clone()).await?;
    let id = connection.id.clone();

    if let Err(err) = connection.run().await {
        error!("Connection error = {:?}. Id = {}", err, id);
        router_tx.send((id.clone(), RouterMessage::Death(id.clone()))).await?;
    }

    Ok(id)
}

pub struct Connection<S> {
    config: Arc<ServerSettings>,
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
        let connection = Connection { config, id, keep_alive, stream, this_rx, router_tx };
        Ok(connection)
    }


    async fn run(&mut self) -> Result<(), Error> {
        let keep_alive = self.keep_alive + self.keep_alive.mul_f32(0.5);
        let id = self.id.clone();

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
            let keep_alive = self.keep_alive + self.keep_alive.mul_f32(0.5);

            self.stream.send(packet).await?;

            let mut pending = iter(pending.drain(..)).map(|publish| RouterMessage::Packet(Packet::Publish(publish)));
            let mut incoming = time::throttle(Duration::from_millis(self.config.throttle_delay_ms), &mut self.stream);
            let mut timeout = time::delay_for(keep_alive);

            loop {
                let (done, routermessage) = select(&mut incoming, &mut pending, keep_alive, &mut timeout).await?;
                if let Some(message) = routermessage {
                    self.router_tx.send((id.clone(), message)).await?;
                }

                if done {
                    break
                }
            }
        } else {
            let connack = connack(ConnectReturnCode::Accepted, false);
            let packet = Packet::Connack(connack);
            self.stream.send(packet).await?;
        }

        // eventloop which processes packets and router messages
        let mut incoming = &mut self.stream;
        let mut incoming = time::throttle(Duration::from_millis(self.config.throttle_delay_ms), &mut incoming);

        loop {
            let mut timeout = time::delay_for(keep_alive);
            let (done, routermessage) = select(&mut incoming, &mut self.this_rx, keep_alive, &mut timeout).await?; 
            if let Some(message) = routermessage {
                self.router_tx.send((id.clone(), message)).await?;
            }

            if done {
                break
            }
        }

        Ok(())
    }
}

use tokio::time::Throttle;
use tokio::time::Delay;

/// selects incoming packets from the network stream and router message stream
/// Forwards router messages to network
/// bool field can be used to instruct outer loop to stop processing messages
async fn select<S: Network>(
    stream: &mut Throttle<S>, 
    mut outgoing: impl Stream<Item = RouterMessage> + Unpin,
    keep_alive: Duration,
    mut timeout: &mut Delay) -> Result<(bool, Option<RouterMessage>), Error> {

    let keepalive = &mut timeout;
    select! {
        _ = keepalive => return Err(Error::KeepAlive),
        o = stream.next() => {
            timeout.reset(Instant::now() + keep_alive);
            let o = match o {
                Some(o) => o,
                None => {
                    let done = true;
                    let packet = None;
                    return Ok((done, packet))
                }
            };

            match o? {
                Packet::Pingreq => stream.get_mut().send(Packet::Pingresp).await?,
                packet => {
                    let message = Some(RouterMessage::Packet(packet));
                    let done = false;
                    return Ok((done, message))
                }
            }
        }
        o = outgoing.next() => match o {
            Some(RouterMessage::Packet(packet)) => stream.get_mut().send(packet).await?,
            Some(RouterMessage::Packets(packets)) => {
                // TODO: Make these vectorized
                for packet in packets.into_iter() {
                    stream.get_mut().send(packet).await?
                }
            }
            Some(message) => {
                warn!("Invalid router message = {:?}", message);
                return Ok((false, None))
            }
            None => {
                let message = None;
                let done = true;
                return Ok((done, message))
            }
        }
    }

    Ok((false, None))
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

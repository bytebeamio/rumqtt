use crate::network::Network;
use crate::state::{self, State};
use crate::{network, Id, ServerSettings};
use mqtt4bytes::{qos, ConnAck, Connect, ConnectReturnCode, Publish};
use rumqttlog::{
    Connection, ConnectionAck, Event, Notification, Receiver, RecvError, SendError, Sender,
};

use std::io;
use std::sync::Arc;
use tokio::time::{Duration, Elapsed};
use tokio::{select, time};

pub struct RemoteLink {
    config: Arc<ServerSettings>,
    connect: Connect,
    id: Id,
    network: Network,
    state: State,
    router_tx: Sender<(Id, Event)>,
    link_rx: Receiver<Notification>,
    total: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Network error {0}")]
    Network(#[from] network::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("State error")]
    State(#[from] state::Error),
    #[error("Unexpected router message")]
    RouterMessage(Notification),
    #[error("Connack error {0}")]
    ConnAck(String),
    #[error("Keep alive time exceeded")]
    KeepAlive,
    #[error("Channel send error")]
    Send(#[from] SendError<(Id, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Payload count greater than max inflight")]
    TooManyPayloads(usize),
    #[error("Disconnect request")]
    Disconnect,
}

impl RemoteLink {
    pub async fn new(
        config: Arc<ServerSettings>,
        router_tx: Sender<(Id, Event)>,
        mut network: Network,
    ) -> Result<(String, Id, RemoteLink), Error> {
        // Wait for MQTT connect packet and error out if it's not received in time to prevent
        // DOS attacks by filling total connections that the server can handle with idle open
        // connections which results in server rejecting new connections
        let timeout = Duration::from_millis(config.connection_timeout_ms.into());
        let mut connect = time::timeout(timeout, async {
            let connect = network.read_connect().await?;
            Ok::<_, Error>(connect)
        })
        .await??;

        // Register this connection with the router. Router replys with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let client_id = connect.client_id.clone();
        let (mut connection, link_rx) = Connection::new_remote(&client_id, 10);

        // Add last will to conneciton
        if let Some(will) = connect.last_will.take() {
            connection.set_will(will);
        }

        let message = (0, Event::Connect(connection));
        router_tx.send(message).unwrap();

        // Send connection acknowledgement back to the client
        let connack = ConnAck::new(ConnectReturnCode::Accepted, false);
        network.connack(connack).await?;

        // TODO When a new connection request is sent to the router, router should ack with error
        // TODO if it exceeds maximum allowed active connections
        // Right now link identifies failure with dropped rx in router, which is probably ok for now
        let id = match link_rx.recv()? {
            Notification::ConnectionAck(ack) => match ack {
                ConnectionAck::Success(id) => id,
                ConnectionAck::Failure(reason) => return Err(Error::ConnAck(reason)),
            },
            message => return Err(Error::RouterMessage(message)),
        };

        let max_inflight_count = config.max_inflight_count;
        Ok((
            client_id,
            id,
            RemoteLink {
                config,
                connect,
                id,
                network,
                state: State::new(max_inflight_count),
                router_tx,
                link_rx,
                total: 0,
            },
        ))
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        self.network.set_keepalive(self.connect.keep_alive);

        // Note:
        // Shouldn't result in bounded queue deadlocks because of blocking n/w send
        loop {
            select! {
                o = self.network.readb(&mut self.state) => {
                    let disconnect = o?;
                    self.handle_network_data().await?;

                    if disconnect {
                        return Err(Error::Disconnect)
                    }
                }
                // Receive from router when previous when state isn't in collision
                // due to previously received data request
                message = self.link_rx.async_recv(), if !self.state.pause_outgoing() => {
                    let message = message?;
                    self.handle_router_response(message).await?;
                }
            }
        }
    }

    async fn handle_network_data(&mut self) -> Result<(), Error> {
        let data = self.state.take_incoming();
        self.network.flush(self.state.write_mut()).await?;

        if !data.is_empty() {
            trace!(
                "{:11} {:14} Id = {}, Count = {}",
                "data",
                "remote",
                self.id,
                data.len()
            );

            let message = Event::Data(data);
            self.router_tx.send((self.id, message))?;
        }

        Ok(())
    }

    async fn handle_router_response(&mut self, message: Notification) -> Result<(), Error> {
        match message {
            Notification::ConnectionAck(_) => {}
            Notification::Acks(reply) => {
                trace!(
                    "{:11} {:14} Id = {}, Count = {}",
                    "acks",
                    "reply",
                    self.id,
                    reply.acks.len()
                );

                for (_pkid, ack) in reply.acks.into_iter() {
                    self.state.outgoing_ack(ack)?;
                }
            }
            Notification::Data(reply) => {
                trace!(
                    "{:11} {:14} Id = {}, Topic = {}, Offsets = {:?}, Count = {}",
                    "data",
                    "reply",
                    self.id,
                    reply.topic,
                    reply.cursors,
                    reply.payload.len()
                );

                let payload_count = reply.payload.len();
                if payload_count > self.config.max_inflight_count as usize {
                    return Err(Error::TooManyPayloads(payload_count));
                }

                self.total += payload_count;
                for p in reply.payload {
                    let publish = Publish::from_bytes(&reply.topic, qos(reply.qos).unwrap(), p);
                    self.state.outgoing_publish(publish)?;
                }
            }
            Notification::Pause => {
                let message = (self.id, Event::Ready);
                self.router_tx.send(message)?;
            }
        }

        self.network.flush(self.state.write_mut()).await?;
        Ok(())
    }
}

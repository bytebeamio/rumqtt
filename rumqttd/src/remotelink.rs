use crate::network::Network;
use crate::state::{self, State};
use crate::{Id, ServerSettings};
use mqtt4bytes::{ConnAck, Connect, ConnectReturnCode, Packet, Publish, QoS};
use rumqttlog::{
    Connection, ConnectionAck, Receiver, RecvError, Event,
    Notification, SendError, Sender,
};
use std::io;
use std::sync::Arc;
use tokio::time::{Duration, Elapsed, Instant};
use tokio::{select, time};

pub struct RemoteLink {
    config: Arc<ServerSettings>,
    connect: Connect,
    id: Id,
    network: Network,
    state: State,
    router_tx: Sender<(Id, Event)>,
    link_rx: Receiver<Notification>,
    acks_required: usize,
    stored_message: Option<Notification>,
    total: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    // #[error("Packet not supported yet")]
    // UnsupportedPacket(Packet),
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
        let connect = time::timeout(timeout, async {
            let connect = network.read_connect().await?;
            Ok::<_, Error>(connect)
        })
        .await??;

        // Register this connection with the router. Router replys with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let client_id = connect.client_id.clone();
        let (connection, link_rx) = Connection::new_remote(&client_id, 10);
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
                acks_required: 0,
                stored_message: None,
                total: 0,
            },
        ))
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let keep_alive = Duration::from_secs(self.connect.keep_alive.into());
        let keep_alive = keep_alive + keep_alive.mul_f32(0.5);
        let mut timeout = time::delay_for(keep_alive);

        // DESIGN: Shouldn't result in bounded queue deadlocks because of blocking n/w send
        //         Router shouldn't drop messages
        // NOTE: Right now we request data by topic, instead if can request data
        // of multiple topics at once, we can have better utilization of
        // network and system calls for n publisher and 1 subscriber workloads
        // as data from multiple topics can be batched (for a given connection)
        loop {
            let keep_alive2 = &mut timeout;
            if self.acks_required == 0 {
                if let Some(message) = self.stored_message.take() {
                    self.handle_router_response(message).await?;
                }
            }

            select! {
                _ = keep_alive2 => return Err(Error::KeepAlive),
                packets = self.network.readb() => {
                    timeout.reset(Instant::now() + keep_alive);
                    let packets = packets?;
                    self.handle_network_data(packets).await?;
                }
                // Receive from router when previous when state isn't in collision
                // due to previously recived data request
                message = self.link_rx.async_recv(), if self.acks_required == 0 => {
                    let message = message?;
                    self.handle_router_response(message).await?;
                }
            }
        }
    }

    async fn handle_router_response(&mut self, message: Notification) -> Result<(), Error> {
        match message {
            Notification::Acks(reply) => {
                trace!(
                    "{:11} {:14} Id = {}, Count = {}",
                    "acks",
                    "reply",
                    self.id,
                    reply.acks.len()
                );

                for (_pkid, ack) in reply.acks.into_iter() {
                    self.network.fill(ack)?;
                }
            }
            Notification::ConnectionAck(_) => {}
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

                // Save this message and set collision flag to not receive any
                // messages from router until there are acks on network. When
                // correct number of acks are received to ensure stored message
                // wont collide, collision flag is reset and the stored message
                // is retrieved by looping logic to be sent to network again
                let no_collision_count = self.state.no_collision_count(reply.payload.len());
                if no_collision_count > 0 {
                    self.acks_required = no_collision_count;
                    self.stored_message = Some(Notification::Data(reply));
                    return Ok(());
                }

                self.total += payload_count;
                // dbg!(self.total);
                for p in reply.payload {
                    let publish = Publish::from_bytes(&reply.topic, QoS::AtLeastOnce, p);
                    let publish = self.state.handle_router_data(publish)?;
                    let publish = Packet::Publish(publish);
                    self.network.fill(publish)?;
                }
            }
            Notification::Pause => {
                let message = (self.id, Event::Ready);
                self.router_tx.send(message)?;
            }
        }

        // FIXME Early returns above will prevent router send and network write
        self.network.flush().await?;
        Ok(())
    }

    async fn handle_network_data(&mut self, incoming: Vec<Packet>) -> Result<(), Error> {
        let mut data = Vec::new();

        for packet in incoming {
            // debug!("Id = {}[{}], Packet packet = {:?}", self.connect.client_id, self.id, packet);
            match packet {
                Packet::PubAck(ack) => {
                    if self.acks_required > 0 {
                        self.acks_required -= 1;
                    }

                    self.state.handle_network_puback(ack)?;
                }
                Packet::Publish(publish) => {
                    // collect publishes from this batch
                    let incoming = Packet::Publish(publish);
                    data.push(incoming);
                }
                Packet::Subscribe(subscribe) => {
                    trace!(
                        "{:11} {:14} Id = {}, Topics = {:?}",
                        "subscribe",
                        "commit",
                        self.id,
                        subscribe.topics
                    );
                    let incoming = Packet::Subscribe(subscribe);
                    data.push(incoming);
                }
                Packet::PingReq => {
                    self.network.fill(Packet::PingResp)?;
                }
                Packet::Disconnect => {
                    // TODO Add correct disconnection handling
                }
                packet => {
                    error!("Packet = {:?} not supported yet", packet);
                    // return Err(Error::UnsupportedPacket(packet))
                }
            }
        }

        // FIXME Early returns above will prevent router send and network write
        self.network.flush().await?;
        if !data.is_empty() {
            trace!(
                "{:11} {:14} Id = {}, Count = {}",
                "data",
                "commit",
                self.id,
                data.len()
            );
            let message = Event::Data(data);
            self.router_tx.send((self.id, message))?;
        }

        Ok(())
    }
}


use rumqttc::{Network, ConnAck, ConnectReturnCode, Publish, QoS, Request, PubAck, Incoming, SubscribeReturnCodes, SubAck, Connect};
use rumqttlog::{Sender, Receiver, RouterInMessage, RouterOutMessage, tracker::Tracker, SendError, RecvError, ConnectionAck, Connection};
use tokio::{select, time};
use std::io;
use crate::{Id, ServerSettings};
use crate::state::{self, State};
use std::sync::Arc;
use tokio::time::{Instant, Duration, Elapsed};

pub struct RemoteLink {
    config: Arc<ServerSettings>,
    connect: Connect,
    id: Id,
    network: Network,
    tracker: Tracker,
    state: State,
    router_tx: Sender<(Id, RouterInMessage)>,
    link_rx: Receiver<RouterOutMessage>,
    acks_required: usize,
    stored_message: Option<RouterOutMessage>,
    total: usize
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    // #[error("Packet not supported yet")]
    // UnsupportedPacket(Incoming),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("State error")]
    State(#[from] state::Error),
    #[error("Unexpected router message")]
    RouterMessage(RouterOutMessage),
    #[error("Connack error {0}")]
    ConnAck(String),
    #[error("Keep alive time exceeded")]
    KeepAlive,
    #[error("Channel send error")]
    Send(#[from] SendError<(Id, RouterInMessage)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Payload count greater than max inflight")]
    TooManyPayloads(usize)
 }

impl RemoteLink {
    pub async fn new(
        config: Arc<ServerSettings>,
        router_tx: Sender<(Id, RouterInMessage)>,
        mut network: Network
    ) -> Result<(String, Id, RemoteLink), Error> {
        // Wait for MQTT connect packet and error out if it's not received in time to prevent
        // DOS attacks by filling total connections that the server can handle with idle open
        // connections which results in server rejecting new connections
        let timeout = Duration::from_millis(config.connection_timeout_ms.into());
        let connect = time::timeout(timeout, async {
            let connect = network.read_connect().await?;
            Ok::<_, Error>(connect)
        }).await??;

        // Register this connection with the router. Router replys with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let client_id = connect.client_id.clone();
        let (connection, link_rx) = Connection::new(&client_id, 10);
        let message = (0, RouterInMessage::Connect(connection));
        router_tx.send(message).await.unwrap();

        // Send connection acknowledgement back to the client
        let connack = ConnAck::new(ConnectReturnCode::Accepted, false);
        network.connack(connack).await?;

        // TODO When a new connection request is sent to the router, router should ack with error
        // TODO if it exceeds maximum allowed active connections
        // Right now link identifies failure with dropped rx in router, which is probably ok for now
        let id = match link_rx.recv().await? {
            RouterOutMessage::ConnectionAck(ack) =>  match ack {
                ConnectionAck::Success(id) => id,
                ConnectionAck::Failure(reason) => return Err(Error::ConnAck(reason))
            }
            message => return Err(Error::RouterMessage(message))
        };

        let max_inflight_count = config.max_inflight_count;
        Ok((client_id, id, RemoteLink {
            config,
            connect,
            id,
            network,
            tracker: Tracker::new(max_inflight_count as usize),
            state: State::new(max_inflight_count),
            router_tx,
            link_rx,
            acks_required: 0,
            stored_message: None,
            total: 0
        }))
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
                message = self.link_rx.recv(), if self.acks_required == 0 => {
                    let message = message?;
                    self.handle_router_response(message).await?;
                }
                Some(message) = tracker_next(&mut self.tracker),
                if self.tracker.has_next() && self.tracker.inflight() < 10 => {
                    trace!("{:11} {:14} Id = {}, Message = {:?}", "tacker", "next", self.id, message);
                    self.router_tx.send((self.id, message)).await?;
                }
            }
        }
    }

    async fn handle_router_response(&mut self, message: RouterOutMessage) -> Result<(), Error> {
        match message {
            RouterOutMessage::TopicsReply(reply) => {
                trace!("{:11} {:14} Id = {}, Count = {}", "topics", "reply", self.id, reply.topics.len());
                self.tracker.track_new_topics(&reply)
            }
            RouterOutMessage::ConnectionAck(_) => {}
            RouterOutMessage::DataReply(reply) => {
                trace!("{:11} {:14} Id = {}, Topic = {}, Count = {}", "data", "reply", self.id, reply.topic, reply.payload.len());
                let payload_count = reply.payload.len();
                if payload_count > self.config.max_inflight_count as usize {
                    return Err(Error::TooManyPayloads(payload_count))
                }

                // Save this message and set collision flag to not receive any
                // messages from router until there are acks on network. When
                // correct number of acks are received to ensure stored message
                // wont collide, collision flag is reset and the stored message
                // is retrieved by looping logic to be sent to network again
                let no_collision_count = self.state.no_collision_count(reply.payload.len());
                if no_collision_count  > 0 {
                    self.acks_required = no_collision_count;
                    self.stored_message = Some(RouterOutMessage::DataReply(reply));
                    return Ok(())
                }

                self.tracker.update_data_tracker(&reply);
                self.total += payload_count;
                // dbg!(self.total);
                for p in reply.payload {
                    let publish = Publish::from_bytes(&reply.topic, QoS::AtLeastOnce, p);
                    let publish = self.state.handle_router_data(publish)?;
                    let publish = Request::Publish(publish);
                    self.network.fill2(publish)?;
                }
            }
            RouterOutMessage::AcksReply(reply) => {
                trace!("{:11} {:14} Id = {}, Topic = {}, Count = {}", "acks", "reply", self.id, reply.topic, reply.pkids.len());
                self.tracker.update_watermarks_tracker(&reply);
                for ack in reply.pkids.into_iter().rev() {
                    let ack = PubAck::new(ack);
                    let ack = Request::PubAck(ack);
                    self.network.fill2(ack)?;
                }
            }
            RouterOutMessage::AllTopicsReply(reply) => {
                trace!("{:11} {:14} Id = {}, Count = {}", "alltopics", "reply", self.id, reply.topics.len());
                self.tracker.track_all_topics(&reply);
            }
        }

        // FIXME Early returns above will prevent router send and network write
        self.network.flush().await?;
        Ok(())
    }

    async fn handle_network_data(&mut self, incoming: Vec<Incoming>) -> Result<(), Error> {
        let mut publishes = Vec::new();

        for packet in incoming {
            // debug!("Id = {}[{}], Incoming packet = {:?}", self.connect.client_id, self.id, packet);
            match packet {
                Incoming::PubAck(ack) => {
                    if self.acks_required > 0 {
                        self.acks_required -= 1;
                    }

                    self.state.handle_network_puback(ack)?;
                }
                Incoming::Publish(publish) => {
                    self.tracker.track_watermark(&publish.topic);
                    // collect publishes from this batch
                    publishes.push(publish);
                }
                Incoming::Subscribe(subscribe) => {
                    trace!("{:11} {:14} Id = {}, Topics = {:?}", "subscribe", "commit", self.id, subscribe.topics);
                    let mut return_codes = Vec::new();
                    for filter in subscribe.topics {
                        let code = SubscribeReturnCodes::Success(filter.qos);
                        return_codes.push(code);
                        self.tracker.add_subscription(&filter.topic_path);
                    }

                    // println!("{:?}", self.tracker);
                    let suback = SubAck::new(subscribe.pkid, return_codes);
                    let suback = Request::SubAck(suback);
                    self.network.fill2(suback)?;
                }
                Incoming::PingReq => {
                    self.network.fill2(Request::PingResp)?;
                }
                Incoming::Disconnect => {
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
        if !publishes.is_empty() {
            trace!("{:11} {:14} Id = {}, Count = {}", "data", "commit", self.id, publishes.len());
            let message = RouterInMessage::ConnectionData(publishes);
            self.router_tx.send((self.id, message)).await?;
        }

        Ok(())
    }
}

async fn tracker_next(tracker: &mut Tracker) -> Option<RouterInMessage> {
    tracker.next()
}

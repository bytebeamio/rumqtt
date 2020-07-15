use rumqttc::*;
use rumqttlog::{Sender, Receiver, RouterInMessage, RouterOutMessage, tracker::Tracker, SendError, RecvError};
use tokio::{select, time};
use std::io;
use crate::{Id, ServerSettings};
use crate::state::{self, State};
use std::sync::Arc;
use tokio::time::{Instant, Duration};

pub struct Link {
    config: Arc<ServerSettings>,
    connect: Connect,
    id: Id,
    network: Network,
    tracker: Tracker,
    state: State,
    router_tx: Sender<(Id, RouterInMessage)>,
    link_rx: Receiver<RouterOutMessage>
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Eventloop {0}")]
    EventLoop(#[from] ConnectionError),
    #[error("Packet not supported yet")]
    UnsupportedPacket(Incoming),
    #[error("State error")]
    State(#[from] state::Error),
    #[error("Keep alive time exceeded")]
    KeepAlive,
    #[error("Channel send error")]
    Send(#[from] SendError<(Id, RouterInMessage)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
 }

impl Link {
    pub async fn new(
        config: Arc<ServerSettings>,
        connect: Connect,
        id: Id,
        network: Network,
        router_tx: Sender<(Id, RouterInMessage)>,
        link_rx: Receiver<RouterOutMessage>
    ) -> Link {
        Link {
            config,
            connect,
            id,
            network,
            tracker: Tracker::new(),
            state: State::new(100),
            router_tx,
            link_rx
        }
    }


    pub async fn start(&mut self) -> Result<(), Error> {
        let keep_alive = Duration::from_secs(self.connect.keep_alive.into());
        let keep_alive = keep_alive + keep_alive.mul_f32(0.5);
        let mut timeout = time::delay_for(keep_alive);
        let mut inflight = 0;

        loop {
            let keep_alive2 = &mut timeout;
            select! {
                _ = keep_alive2 => return Err(Error::KeepAlive),
                packets = self.network.readb() => {
                    timeout.reset(Instant::now() + keep_alive);
                    let packets = packets?;
                    self.handle_network_data(packets).await?;
                }
                message = self.link_rx.recv() => {
                    debug!("{:?}", message);
                    let message = message?;
                    self.handle_router_response(message).await?;
                }
                Some(message) = self.tracker.next(), if inflight < 3 && self.tracker.has_next() => {
                    // NOTE Right now we are request data by topic, instead if can request
                    // data of multiple topics at once, we can have better utilization of
                    // network and system calls for n publisher and 1 subscriber workloads
                    // as data from multiple topics can be batched (for a given connection)
                    debug!("{:?}", message);
                    inflight += 1;
                    self.router_tx.send((self.id, message)).await?;
                }
            }
        }
    }

    async fn handle_router_response(&mut self, message: RouterOutMessage) -> Result<(), Error> {
        match message {
            RouterOutMessage::TopicsReply(reply) => {
                self.tracker.track_more_topics(&reply)
            }
            RouterOutMessage::ConnectionAck(_) => {}
            RouterOutMessage::DataAck(_) => {}
            RouterOutMessage::DataReply(reply) => {
                for p in reply.payload {
                    let publish = Publish::from_bytes(&reply.topic, QoS::AtLeastOnce, p);
                    let publish = self.state.handle_router_data(publish)?;
                    let publish = Request::Publish(publish);
                    self.network.fill2(publish)?;
                }
            }
            RouterOutMessage::WatermarksReply(_) => {}
        }

        // FIXME Early returns above will prevent router send and network write
        self.network.flush().await?;
        Ok(())
    }

    async fn handle_network_data(&mut self, incoming: Vec<Incoming>) -> Result<(), Error> {
        let mut publishes = Vec::new();

        for packet in incoming {
            debug!("Id = {}[{}], Incoming packet = {:?}", self.connect.client_id, self.id, packet);
            match packet {
                Incoming::PubAck(ack) => {
                    self.state.handle_network_puback(ack)?;
                }
                Incoming::Publish(publish) => {
                    publishes.push(publish)
                }
                Incoming::Subscribe(subscribe) => {
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
            let message = RouterInMessage::Data(publishes);
            self.router_tx.send((self.id, message)).await?;
        }

        Ok(())
    }
}
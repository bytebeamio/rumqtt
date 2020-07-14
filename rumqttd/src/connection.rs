use rumqttc::*;
use rumqttlog::{Sender, RouterInMessage, tracker::Tracker};
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
    KeepAlive
 }

impl Link {
    pub async fn new(
        config: Arc<ServerSettings>,
        connect: Connect,
        id: Id,
        network: Network,
        router_tx: Sender<(Id, RouterInMessage)>
    ) -> Link {
        Link {
            config,
            connect,
            id,
            network,
            tracker: Tracker::new(),
            state: State::new(100),
            router_tx
        }
    }


    pub async fn start(&mut self) -> Result<(), Error> {
        let keep_alive = Duration::from_secs(self.connect.keep_alive.into());
        let keep_alive = keep_alive + keep_alive.mul_f32(0.5);
        let mut timeout = time::delay_for(keep_alive);

        loop {
            let keep_alive2 = &mut timeout;
            select! {
                _ = keep_alive2 => return Err(Error::KeepAlive),
                packets = self.network.readb() => {
                    timeout.reset(Instant::now() + keep_alive);
                    let packets = packets?;
                    self.handle_network_data(packets).await?;
                }
            }
        }
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

                    let suback = SubAck::new(subscribe.pkid, return_codes);
                    let suback = Request::SubAck(suback);
                    self.network.fill2(suback)?;
                }
                Incoming::PingReq => {
                    self.network.fill2(Request::PingResp)?;
                }
                packet => {
                    return Err(Error::UnsupportedPacket(packet))
                }
            }
        }

        self.network.flush().await?;
        let message = Some(RouterInMessage::Data(publishes));
        Ok(())
    }
}
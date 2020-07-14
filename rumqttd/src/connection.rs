use rumqttc::*;
use rumqttlog::{Sender, RouterInMessage, tracker::Tracker};
use std::io;
use crate::Id;
use crate::state::{self, State};

pub struct Link {
    id: Id,
    sid: String,
    network: Network,
    tracker: Tracker,
    state: State,
    router_tx: Sender<(Id, RouterInMessage)>
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
    State(#[from] state::Error)
 }

impl Link {
    pub async fn new(
        id: Id,
        sid: String,
        network: Network,
        router_tx: Sender<(Id, RouterInMessage)>
    ) -> Link {
        Link {
            id,
            sid,
            network,
            tracker: Tracker::new(),
            state: State::new(100),
            router_tx
        }
    }


    pub async fn start(&mut self) -> Result<(), Error> {
        loop {
            let v = self.network.readb().await?;
            self.handle_network_data(v).await?;
        }
    }

    async fn handle_network_data(&mut self, incoming: Vec<Incoming>) -> Result<(), Error> {
        let mut publishes = Vec::new();

        for packet in incoming {
            debug!("Id = {:?}[{}], Incoming packet = {:?}", self.sid, self.id, packet);
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
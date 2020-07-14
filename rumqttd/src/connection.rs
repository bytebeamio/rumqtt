use rumqttc::{MqttOptions, Network, EventLoop, ConnectionError};
use rumqttlog::{Sender, RouterInMessage};
use std::io;
use crate::Id;

pub struct Link {
    eventloop: EventLoop
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Eventloop {0}")]
    EventLoop(#[from] ConnectionError),
 }

impl Link {
    pub async fn new(network: Network) -> Link {
        let options = MqttOptions::new("dummy", "dummy", 0000);
        let mut eventloop = EventLoop::new(options, 10).await;
        eventloop.set_network(network);

        Link {
            eventloop
        }
    }

    pub async fn start(&mut self, _router_tx: Sender<(Id, RouterInMessage)>) -> Result<(), Error> {
        loop {
            let (notification, _) = self.eventloop.pollv().await?;
            println!("{:?}", notification);
        }
    }
}
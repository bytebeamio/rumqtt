use std::collections::HashMap;

use async_channel::{bounded, Receiver, RecvError, SendError, Sender};
use mqttbytes::{
    v4::{Packet, Publish},
    QoS,
};
use tokio::select;

use crate::{AsyncClient, ClientError, ConnectionError, Event, EventLoop, MqttOptions};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to handle request to Client")]
    Client(#[from] ClientError),
    #[error("Failed to handle request to Eventloop")]
    Connection(#[from] ConnectionError),
    #[error("Failed to recv")]
    Recv(#[from] RecvError),
    #[error("Failed to send subscription request")]
    Subscribe(#[from] SendError<Subscription>),
    #[error("Failed to send publish msg")]
    Publish(#[from] SendError<Publish>),
}

type Subscription = (String, Sender<Publish>);

pub struct ReqHandler {
    client: AsyncClient,
    sub_tx: Sender<Subscription>,
}

impl ReqHandler {
    pub fn new(options: MqttOptions, cap: usize) -> (ReqHandler, SubHandler) {
        let (client, eventloop) = AsyncClient::new(options, cap);
        let (sub_tx, sub_rx) = bounded(10);

        (
            ReqHandler { client, sub_tx },
            SubHandler {
                eventloop,
                sub_rx,
                subscribers: HashMap::new(),
            },
        )
    }

    pub async fn subscriber<S: Into<String>>(
        &self,
        topic: S,
        qos: QoS,
    ) -> Result<Subscriber, Error> {
        let topic = topic.into();
        self.client.subscribe(topic.clone(), qos).await?;

        let (pub_tx, pub_rx) = bounded(10);
        self.sub_tx.send((topic.clone(), pub_tx)).await?;

        Ok(Subscriber { pub_rx })
    }

    pub fn publisher<S: Into<String>>(&self, topic: S) -> Publisher {
        Publisher {
            topic: topic.into(),
            client: self.client.clone(),
            qos: QoS::AtLeastOnce,
            retain: false,
        }
    }
}

pub struct SubHandler {
    eventloop: EventLoop,
    sub_rx: Receiver<Subscription>,
    subscribers: HashMap<String, Sender<Publish>>,
}

impl SubHandler {
    // Start a loop to handle subscriptions
    pub async fn start(&mut self) -> Result<(), Error> {
        loop {
            select! {
                e = self.eventloop.poll() => {
                    if let Event::Incoming(Packet::Publish(publish)) = e? {
                        if let Some(pub_tx) = self.subscribers.get(&publish.topic) {
                            pub_tx.send(publish).await?;
                        }
                    }
                }

                s = self.sub_rx.recv() => {
                    let (topic, pub_tx) = s?;
                    self.subscribers.insert(topic, pub_tx);
                }
            }
        }
    }
}

pub struct Subscriber {
    pub_rx: Receiver<Publish>,
}

impl Subscriber {
    pub async fn next(&self) -> Result<Publish, Error> {
        Ok(self.pub_rx.recv().await?)
    }
}

pub struct Publisher {
    topic: String,
    client: AsyncClient,
    pub qos: QoS,
    pub retain: bool,
}

impl Publisher {
    pub async fn publish<V: Into<Vec<u8>>>(&self, payload: V) -> Result<(), Error> {
        self.client
            .publish(&self.topic, self.qos, self.retain, payload)
            .await?;
        Ok(())
    }

    pub fn try_publish<V: Into<Vec<u8>>>(&self, payload: V) -> Result<(), Error> {
        self.client
            .try_publish(&self.topic, self.qos, self.retain, payload)?;
        Ok(())
    }
}

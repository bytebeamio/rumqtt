use std::collections::HashMap;

use async_channel::{bounded, Receiver, RecvError, SendError, Sender};
use mqttbytes::{
    v4::{Packet, Publish, Unsubscribe},
    QoS,
};
use tokio::select;

use crate::{
    AsyncClient, ClientError, ConnectionError, Event, EventLoop, MqttOptions, Outgoing, Request,
};

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
    #[error("Failed to send unsubscribe request")]
    Unsubscribe(#[from] SendError<String>),
    #[error("Failed to send publish msg")]
    Publish(#[from] SendError<Publish>),
    #[error("Failed to send request")]
    Request(#[from] SendError<Request>),
}

type Subscription = (String, Sender<Publish>);

pub struct ReqHandler {
    client: AsyncClient,
    sub_tx: Sender<Subscription>,
    unsub_tx: Sender<String>,
    shutdown_tx: Sender<()>,
}

impl ReqHandler {
    pub fn new(options: MqttOptions, cap: usize) -> (ReqHandler, SubHandler) {
        let (client, eventloop) = AsyncClient::new(options, cap);
        let (sub_tx, sub_rx) = bounded(10);
        let (unsub_tx, unsub_rx) = bounded(10);
        let (shutdown_tx, shutdown_rx) = bounded(1);

        (
            ReqHandler {
                client,
                sub_tx,
                unsub_tx,
                shutdown_tx,
            },
            SubHandler {
                eventloop,
                sub_rx,
                subscribers: HashMap::new(),
                unsub_rx,
                shutdown_rx,
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

        Ok(Subscriber {
            topic,
            pub_rx,
            unsub_tx: self.unsub_tx.clone(),
        })
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

impl Drop for ReqHandler {
    fn drop(&mut self) {
        self.shutdown_tx.try_send(()).unwrap();
    }
}

pub struct SubHandler {
    eventloop: EventLoop,
    sub_rx: Receiver<Subscription>,
    subscribers: HashMap<String, Sender<Publish>>,
    unsub_rx: Receiver<String>,
    shutdown_rx: Receiver<()>,
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

                u = self.unsub_rx.recv() => {
                    let topic = u?;
                    self.subscribers.remove(&topic);
                    let req = Request::Unsubscribe(Unsubscribe::new(topic));
                    self.eventloop.handle().send(req).await?;
                }

                _ = self.shutdown_rx.recv() => {
                    self.disconnect().await?;
                    return Ok(())
                }
            }
        }
    }

    async fn disconnect(&mut self) -> Result<(), Error> {
        self.eventloop.handle().send(Request::Disconnect).await?;

        loop {
            if let Event::Outgoing(Outgoing::Disconnect) = self.eventloop.poll().await? {
                return Ok(());
            }
        }
    }
}

pub struct Subscriber {
    topic: String,
    pub_rx: Receiver<Publish>,
    unsub_tx: Sender<String>,
}

impl Subscriber {
    pub async fn next(&self) -> Result<Publish, Error> {
        Ok(self.pub_rx.recv().await?)
    }

    pub async fn unsubscribe(&mut self) -> Result<(), Error> {
        self.unsub_tx.send(self.topic.clone()).await?;
        Ok(())
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

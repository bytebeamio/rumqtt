use async_channel::{RecvError, SendError};
use mqttbytes::{
    v4::{Packet, Publish},
    QoS,
};

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
    #[error("Failed to send request")]
    Request(#[from] SendError<Request>),
}

pub struct MqttHandler {
    client: AsyncClient,
    eventloop: EventLoop,
}

impl MqttHandler {
    pub fn new(options: MqttOptions, cap: usize) -> Self {
        let (client, eventloop) = AsyncClient::new(options, cap);

        Self { client, eventloop }
    }

    pub async fn disconnect(&mut self) -> Result<(), Error> {
        self.client.disconnect().await?;

        loop {
            if let Event::Outgoing(Outgoing::Disconnect) = self.eventloop.poll().await? {
                return Ok(());
            }
        }
    }

    pub fn publisher<S: Into<String>>(self, topic: S, qos: QoS, retain: bool) -> Publisher {
        Publisher {
            handler: self,
            topic: topic.into(),
            qos,
            retain,
        }
    }

    pub async fn subscriber<S: Into<String>>(
        self,
        topic: S,
        qos: QoS,
    ) -> Result<Subscriber, Error> {
        let topic = topic.into();
        self.client.subscribe(&topic, qos).await?;

        Ok(Subscriber {
            handler: self,
            topic,
        })
    }
}

impl Drop for MqttHandler {
    fn drop(&mut self) {
        pollster::block_on(self.disconnect()).unwrap()
    }
}

pub struct Publisher {
    handler: MqttHandler,
    pub topic: String,
    pub qos: QoS,
    pub retain: bool,
}

impl Publisher {
    pub async fn publish<V: Into<Vec<u8>>>(&mut self, payload: V) -> Result<(), Error> {
        self.handler
            .client
            .publish(&self.topic, self.qos, self.retain, payload)
            .await?;

        loop {
            if let Event::Outgoing(Outgoing::Publish(_)) = self.handler.eventloop.poll().await? {
                return Ok(());
            }
        }
    }

    pub fn try_publish<V: Into<Vec<u8>>>(&mut self, payload: V) -> Result<(), Error> {
        self.handler
            .client
            .try_publish(&self.topic, self.qos, self.retain, payload)?;

        loop {
            if let Event::Outgoing(Outgoing::Publish(_)) =
                pollster::block_on(self.handler.eventloop.poll())?
            {
                return Ok(());
            }
        }
    }
}

pub struct Subscriber {
    handler: MqttHandler,
    topic: String,
}

impl Subscriber {
    pub async fn next(&mut self) -> Result<Publish, Error> {
        loop {
            if let Event::Incoming(Packet::Publish(publish)) = self.handler.eventloop.poll().await?
            {
                if publish.topic == self.topic {
                    return Ok(publish);
                }
            }
        }
    }

    pub async fn unsubscribe(&mut self) -> Result<(), Error> {
        self.handler.client.unsubscribe(&self.topic).await?;

        loop {
            if let Event::Outgoing(Outgoing::Unsubscribe(_)) = self.handler.eventloop.poll().await?
            {
                return Ok(());
            }
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        pollster::block_on(self.unsubscribe()).unwrap()
    }
}

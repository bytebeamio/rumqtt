use bytes::Bytes;
use flume::Sender;

use crate::v5::mqttbytes::{Publish, PublishProperties, QoS};

use super::{mqttbytes::valid_topic, ClientError, Request};

#[derive(Debug)]
pub struct Publisher {
    topic: String,
    // for sending publish req to EventLoop
    request_tx: flume::Sender<Request>,
    properties: PublishProperties,
}

impl Publisher {
    pub fn new(topic: String, request_tx: Sender<Request>) -> Self {
        Publisher {
            topic,
            request_tx,
            properties: PublishProperties::default(),
        }
    }

    pub fn topic_alias(&mut self, alias: u16) {
        self.properties.topic_alias = Some(alias);
    }

    /// Sends a MQTT Publish to the `EventLoop`.
    pub async fn publish<P>(
        &mut self,
        qos: QoS,
        retain: bool,
        payload: P,
    ) -> Result<(), ClientError>
    where
        P: Into<Bytes>,
    {
        let mut publish = Publish::new(&self.topic, qos, payload);
        publish.retain = retain;
        let publish = Request::Publish(publish, Some(self.properties.clone()));
        if !valid_topic(&self.topic) {
            return Err(ClientError::Request(publish));
        }
        self.request_tx.send_async(publish).await?;

        // if we have sent topic with our alias once, we can set topic to be empty
        if self.properties.topic_alias.is_some() && !self.topic.is_empty() {
            self.topic.clear();
        }
        Ok(())
    }
}

use bytes::Bytes;
use flume::Sender;

use crate::v5::{
    mqttbytes::{valid_topic, Publish, PublishProperties, QoS},
    ClientError, Request,
};

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

    pub fn topic_alias(mut self, alias: u16) -> Self {
        self.properties.topic_alias = Some(alias);
        self
    }

    // Sends a MQTT Publish to the `EventLoop`
    // pub fn publish<S, P>(
    //     &self,
    //     topic: S,
    //     qos: QoS,
    //     retain: bool,
    //     payload: P,
    // ) -> Result<(), ClientError>
    // where
    //     S: Into<String>,
    //     P: Into<Bytes>,
    // {
    //     pollster::block_on(self.client.publish(topic, qos, retain, payload))?;
    //     Ok(())
    // }
    //
    // pub fn try_publish<S, P>(
    //     &self,
    //     topic: S,
    //     qos: QoS,
    //     retain: bool,
    //     payload: P,
    // ) -> Result<(), ClientError>
    // where
    //     S: Into<String>,
    //     P: Into<Bytes>,
    // {
    //     self.client.try_publish(topic, qos, retain, payload)?;
    //     Ok(())
    // }
}

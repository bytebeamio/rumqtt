use bytes::Bytes;

use crate::v5::{mqttbytes::QoS, unsync::Publisher as AsyncPublisher, ClientError};

#[derive(Debug)]
pub struct Publisher {
    async_pub: AsyncPublisher,
}

impl Publisher {
    pub fn new(async_pub: AsyncPublisher) -> Self {
        Publisher { async_pub }
    }

    pub fn topic_alias(mut self, alias: u16) -> Self {
        self.async_pub = self.async_pub.topic_alias(alias);
        self
    }

    // Sends a MQTT Publish to the `EventLoop`
    pub fn publish<P>(&mut self, qos: QoS, retain: bool, payload: P) -> Result<(), ClientError>
    where
        P: Into<Bytes>,
    {
        pollster::block_on(self.async_pub.publish(qos, retain, payload))?;
        Ok(())
    }

    pub fn try_publish<P>(&mut self, qos: QoS, retain: bool, payload: P) -> Result<(), ClientError>
    where
        P: Into<Bytes>,
    {
        self.async_pub.try_publish(qos, retain, payload)
    }
}

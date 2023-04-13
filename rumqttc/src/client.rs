//! This module offers a high level abstraction to communicate with the `EventLoop`.
use crate::mqttbytes::{v4::*, QoS};
use crate::{valid_topic, EventLoop, MqttOptions, Request};

use bytes::Bytes;
use flume::{SendError, Sender, TrySendError};

/// Client Error
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Failed to send mqtt requests to eventloop")]
    Request(Request),
    #[error("Failed to send mqtt requests to eventloop")]
    TryRequest(Request),
}

impl From<SendError<Request>> for ClientError {
    fn from(e: SendError<Request>) -> Self {
        Self::Request(e.into_inner())
    }
}

impl From<TrySendError<Request>> for ClientError {
    fn from(e: TrySendError<Request>) -> Self {
        Self::TryRequest(e.into_inner())
    }
}

/// An asynchronous client, communicates with MQTT `EventLoop`.
///
/// This is cloneable and can be used to send [`publish`](`Client::publish`), [`subscribe`](`Client::subscribe`),
/// [`unsubscribe`](`Client::unsubscribe`) through the `EventLoop`/`Connection`, which is to be polled parallelly.
#[derive(Clone, Debug)]
pub struct Client {
    request_tx: Sender<Request>,
}

impl Client {
    /// Construct a Client and `EventLoop`.
    ///
    /// `cap` specifies the capacity of the bounded async channel.
    ///
    /// **NOTE**: The `EventLoop` must be regularly polled in order to send, receive and process packets
    /// from the broker, i.e. move ahead.
    pub fn new(options: MqttOptions, cap: usize) -> (Client, EventLoop) {
        let eventloop = EventLoop::new(options, cap);
        let request_tx = eventloop.requests_tx.clone();

        let client = Client { request_tx };

        (client, eventloop)
    }

    /// Create a new `Client` from a pair of async channel `Sender`s. This is mostly useful for
    /// creating a test instance.
    pub fn from_senders(request_tx: Sender<Request>) -> Client {
        Client { request_tx }
    }

    pub fn send(&self, request: Request) -> Result<(), ClientError> {
        self.request_tx.send(request)?;
        Ok(())
    }

    pub async fn send_async(&self, request: Request) -> Result<(), ClientError> {
        self.request_tx.send_async(request).await?;
        Ok(())
    }

    pub fn try_send(&self, request: Request) -> Result<(), ClientError> {
        self.request_tx.try_send(request)?;
        Ok(())
    }

    /// Sends a MQTT Publish to the `EventLoop`.
    pub async fn publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let topic = topic.into();
        let mut publish = Publish::new(&topic, qos, payload);
        publish.retain = retain;
        let publish = Request::Publish(publish);
        if !valid_topic(&topic) {
            return Err(ClientError::Request(publish));
        }
        self.send_async(publish).await?;
        Ok(())
    }

    /// Sends a MQTT Publish to the `EventLoop`.
    pub fn publish_sync<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let topic = topic.into();
        let mut publish = Publish::new(&topic, qos, payload);
        publish.retain = retain;
        let publish = Request::Publish(publish);
        if !valid_topic(&topic) {
            return Err(ClientError::Request(publish));
        }
        self.send(publish)?;
        Ok(())
    }

    /// Attempts to send a MQTT Publish to the `EventLoop`.
    pub fn try_publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let topic = topic.into();
        let mut publish = Publish::new(&topic, qos, payload);
        publish.retain = retain;
        let publish = Request::Publish(publish);
        if !valid_topic(&topic) {
            return Err(ClientError::TryRequest(publish));
        }
        self.try_send(publish)?;
        Ok(())
    }

    /// Sends a MQTT PubAck to the `EventLoop`. Only needed in if `manual_acks` flag is set.
    pub async fn ack(&self, publish: &Publish) -> Result<(), ClientError> {
        let ack = get_ack_req(publish);

        if let Some(ack) = ack {
            self.send_async(ack).await?;
        }
        Ok(())
    }

    /// Sends a MQTT PubAck to the `EventLoop`. Only needed in if `manual_acks` flag is set.
    pub fn ack_sync(&self, publish: &Publish) -> Result<(), ClientError> {
        let ack = get_ack_req(publish);

        if let Some(ack) = ack {
            self.send(ack)?;
        }
        Ok(())
    }

    /// Attempts to send a MQTT PubAck to the `EventLoop`. Only needed in if `manual_acks` flag is set.
    pub fn try_ack(&self, publish: &Publish) -> Result<(), ClientError> {
        let ack = get_ack_req(publish);
        if let Some(ack) = ack {
            self.try_send(ack)?;
        }
        Ok(())
    }

    /// Sends a MQTT Publish to the `EventLoop`
    pub async fn publish_bytes<S>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: Bytes,
    ) -> Result<(), ClientError>
    where
        S: Into<String>,
    {
        let mut publish = Publish::from_bytes(topic, qos, payload);
        publish.retain = retain;
        let publish = Request::Publish(publish);
        self.send_async(publish).await?;
        Ok(())
    }

    /// Sends a MQTT Subscribe to the `EventLoop`
    pub async fn subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let subscribe = Subscribe::new(topic.into(), qos);
        let request = Request::Subscribe(subscribe);
        self.send_async(request).await?;
        Ok(())
    }

    /// Sends a MQTT Subscribe to the `EventLoop`
    pub fn subscribe_sync<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let subscribe = Subscribe::new(topic.into(), qos);
        let request = Request::Subscribe(subscribe);
        self.send(request)?;
        Ok(())
    }

    /// Attempts to send a MQTT Subscribe to the `EventLoop`
    pub fn try_subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let subscribe = Subscribe::new(topic.into(), qos);
        let request = Request::Subscribe(subscribe);
        self.try_send(request)?;
        Ok(())
    }

    /// Sends a MQTT Subscribe for multiple topics to the `EventLoop`
    pub async fn subscribe_many<T>(&self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let subscribe = Subscribe::new_many(topics);
        let request = Request::Subscribe(subscribe);
        self.send_async(request).await?;
        Ok(())
    }

    /// Sends a MQTT Subscribe for multiple topics to the `EventLoop`
    pub fn subscribe_many_sync<T>(&self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let subscribe = Subscribe::new_many(topics);
        let request = Request::Subscribe(subscribe);
        self.send(request)?;
        Ok(())
    }

    /// Attempts to send a MQTT Subscribe for multiple topics to the `EventLoop`
    pub fn try_subscribe_many<T>(&self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let subscribe = Subscribe::new_many(topics);
        let request = Request::Subscribe(subscribe);
        self.try_send(request)?;
        Ok(())
    }

    /// Sends a MQTT Unsubscribe to the `EventLoop`
    pub async fn unsubscribe<S: Into<String>>(&self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic.into());
        let request = Request::Unsubscribe(unsubscribe);
        self.send_async(request).await?;
        Ok(())
    }

    /// Sends a MQTT Unsubscribe to the `EventLoop`
    pub fn unsubscribe_sync<S: Into<String>>(&self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic.into());
        let request = Request::Unsubscribe(unsubscribe);
        self.send(request)?;
        Ok(())
    }

    /// Attempts to send a MQTT Unsubscribe to the `EventLoop`
    pub fn try_unsubscribe<S: Into<String>>(&self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic.into());
        let request = Request::Unsubscribe(unsubscribe);
        self.try_send(request)?;
        Ok(())
    }

    /// Sends a MQTT disconnect to the `EventLoop`
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        let request = Request::Disconnect;
        self.send_async(request).await?;
        Ok(())
    }

    /// Sends a MQTT disconnect to the `EventLoop`
    pub fn disconnect_sync(&self) -> Result<(), ClientError> {
        let request = Request::Disconnect;
        self.send(request)?;
        Ok(())
    }

    /// Attempts to send a MQTT disconnect to the `EventLoop`
    pub fn try_disconnect(&self) -> Result<(), ClientError> {
        let request = Request::Disconnect;
        self.try_send(request)?;
        Ok(())
    }
}

fn get_ack_req(publish: &Publish) -> Option<Request> {
    let ack = match publish.qos {
        QoS::AtMostOnce => return None,
        QoS::AtLeastOnce => Request::PubAck(PubAck::new(publish.pkid)),
        QoS::ExactlyOnce => Request::PubRec(PubRec::new(publish.pkid)),
    };
    Some(ack)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn calling_iter_twice_on_connection_shouldnt_panic() {
        use std::time::Duration;

        let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
        let will = LastWill::new("hello/world", "good bye", QoS::AtMostOnce, false);
        mqttoptions
            .set_keep_alive(Duration::from_secs(5))
            .set_last_will(will);

        let (_, eventloop) = Client::new(mqttoptions, 10);
        let mut connection = eventloop.sync();
        let _ = connection.iter();
        let _ = connection.iter();
    }
}

// use std::collections::HashMap;
use super::publisher::Publisher;
use crate::v5::mqttbytes::{Filter, PubAck, PubRec, Publish, QoS, Subscribe, Unsubscribe};
use crate::v5::{ClientError, EventLoop, MqttOptions, Request};

use flume::Sender;

/// An asynchronous client, communicates with MQTT `EventLoop`.
///
/// This is cloneable and can be used to asynchronously [`publish`](`AsyncClient::publish`),
/// [`subscribe`](`AsyncClient::subscribe`) through the `EventLoop`, which is to be polled parallelly.
///
/// **NOTE**: The `EventLoop` must be regularly polled in order to send, receive and process packets
/// from the broker, i.e. move ahead.
#[derive(Clone, Debug)]
pub struct Client {
    request_tx: Sender<Request>,
    // alias_mapping: HashMap<String, u16>,
}

impl Client {
    /// Create a new `AsyncClient`.
    ///
    /// `cap` specifies the capacity of the bounded async channel.
    pub fn new(options: MqttOptions, cap: usize) -> (Client, EventLoop) {
        let eventloop = EventLoop::new(options, cap);
        let request_tx = eventloop.requests_tx.clone();
        // let alias_mapping = HashMap::new();

        let client = Client {
            request_tx,
            // alias_mapping,
        };

        (client, eventloop)
    }

    /// Create a new `AsyncClient` from a pair of async channel `Sender`s. This is mostly useful for
    /// creating a test instance.
    pub fn from_senders(request_tx: Sender<Request>) -> Client {
        // let alias_mapping = HashMap::new();
        Client {
            request_tx,
            // alias_mapping
        }
    }

    pub fn publisher<S>(&self, topic: S) -> Publisher
    where
        S: Into<String>,
    {
        let topic = topic.into();
        Publisher::new(topic, self.request_tx.clone())
    }

    /// Sends a MQTT PubAck to the `EventLoop`. Only needed in if `manual_acks` flag is set.
    pub async fn ack(&self, publish: &Publish) -> Result<(), ClientError> {
        let ack = get_ack_req(publish);

        if let Some(ack) = ack {
            self.request_tx.send_async(ack).await?;
        }
        Ok(())
    }

    /// Attempts to send a MQTT PubAck to the `EventLoop`. Only needed in if `manual_acks` flag is set.
    pub fn try_ack(&self, publish: &Publish) -> Result<(), ClientError> {
        let ack = get_ack_req(publish);
        if let Some(ack) = ack {
            self.request_tx.try_send(ack)?;
        }
        Ok(())
    }

    /// Sends a MQTT Subscribe to the `EventLoop`
    pub async fn subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let filter = Filter::new(topic, qos);
        let subscribe = Subscribe::new(filter);
        let request = Request::Subscribe(subscribe);
        self.request_tx.send_async(request).await?;
        Ok(())
    }

    /// Attempts to send a MQTT Subscribe to the `EventLoop`
    pub fn try_subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let filter = Filter::new(topic, qos);
        let subscribe = Subscribe::new(filter);
        let request = Request::Subscribe(subscribe);
        self.request_tx.try_send(request)?;
        Ok(())
    }

    /// Sends a MQTT Subscribe for multiple topics to the `EventLoop`
    pub async fn subscribe_many<T>(&self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = Filter>,
    {
        let subscribe = Subscribe::new_many(topics);
        let request = Request::Subscribe(subscribe);
        self.request_tx.send_async(request).await?;
        Ok(())
    }

    /// Attempts to send a MQTT Subscribe for multiple topics to the `EventLoop`
    pub fn try_subscribe_many<T>(&self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = Filter>,
    {
        let subscribe = Subscribe::new_many(topics);
        let request = Request::Subscribe(subscribe);
        self.request_tx.try_send(request)?;
        Ok(())
    }

    /// Sends a MQTT Unsubscribe to the `EventLoop`
    pub async fn unsubscribe<S: Into<String>>(&self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic);
        let request = Request::Unsubscribe(unsubscribe);
        self.request_tx.send_async(request).await?;
        Ok(())
    }

    /// Attempts to send a MQTT Unsubscribe to the `EventLoop`
    pub fn try_unsubscribe<S: Into<String>>(&self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic);
        let request = Request::Unsubscribe(unsubscribe);
        self.request_tx.try_send(request)?;
        Ok(())
    }

    /// Sends a MQTT disconnect to the `EventLoop`
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        let request = Request::Disconnect;
        self.request_tx.send_async(request).await?;
        Ok(())
    }

    /// Attempts to send a MQTT disconnect to the `EventLoop`
    pub fn try_disconnect(&self) -> Result<(), ClientError> {
        let request = Request::Disconnect;
        self.request_tx.try_send(request)?;
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
    use crate::v5::{mqttbytes::LastWill, sync};

    use super::*;

    #[test]
    fn calling_iter_twice_on_connection_shouldnt_panic() {
        use std::time::Duration;

        let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
        let will = LastWill::new("hello/world", "good bye", QoS::AtMostOnce, false);
        mqttoptions
            .set_keep_alive(Duration::from_secs(5))
            .set_last_will(will);

        let (_, mut connection) = sync::Client::new(mqttoptions, 10);
        let _ = connection.iter();
        let _ = connection.iter();
    }
}

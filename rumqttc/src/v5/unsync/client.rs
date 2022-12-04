// use std::collections::HashMap;
use std::time::Duration;

use super::publisher::Publisher;
use crate::v5::mqttbytes::{Filter, PubAck, PubRec, Publish, QoS, Subscribe, Unsubscribe};
use crate::v5::{ClientError, ConnectionError, Event, EventLoop, MqttOptions, Request};

use flume::Sender;
use futures::FutureExt;
use tokio::runtime::Runtime;
use tokio::time::timeout;

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

    /// Attempts to send a MQTT Publish to the `EventLoop`.
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
    //     let topic = topic.into();
    //     let mut publish = Publish::new(&topic, qos, payload);
    //     publish.retain = retain;
    //     let publish = Request::Publish(publish);
    //     if !valid_topic(&topic) {
    //         return Err(ClientError::TryRequest(publish));
    //     }
    //     self.request_tx.try_send(publish)?;
    //     Ok(())
    // }

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

    /// Sends a MQTT Publish to the `EventLoop`
    // pub async fn publish_bytes<S>(
    //     &self,
    //     topic: S,
    //     qos: QoS,
    //     retain: bool,
    //     payload: Bytes,
    // ) -> Result<(), ClientError>
    // where
    //     S: Into<String>,
    // {
    //     let topic = topic.into();
    //     let mut publish = Publish::new(&topic, qos, payload);
    //     publish.retain = retain;
    //     let publish = Request::Publish(publish);
    //     if !valid_topic(&topic) {
    //         return Err(ClientError::TryRequest(publish));
    //     }
    //     self.request_tx.send_async(publish).await?;
    //     Ok(())
    // }

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

/// Error type returned by [`Connection::recv`]
#[derive(Debug, Eq, PartialEq)]
pub struct RecvError;

/// Error type returned by [`Connection::try_recv`]
#[derive(Debug, Eq, PartialEq)]
pub enum TryRecvError {
    /// User has closed requests channel
    Disconnected,
    /// Did not resolve
    Empty,
}

/// Error type returned by [`Connection::recv_timeout`]
#[derive(Debug, Eq, PartialEq)]
pub enum RecvTimeoutError {
    /// User has closed requests channel
    Disconnected,
    /// Recv request timedout
    Timeout,
}

///  MQTT connection. Maintains all the necessary state
pub struct Connection {
    pub eventloop: EventLoop,
    runtime: Runtime,
}

impl Connection {
    fn new(eventloop: EventLoop, runtime: Runtime) -> Connection {
        Connection { eventloop, runtime }
    }

    /// Returns an iterator over this connection. Iterating over this is all that's
    /// necessary to make connection progress and maintain a robust connection.
    /// Just continuing to loop will reconnect
    /// **NOTE** Don't block this while iterating
    // ideally this should be named iter_mut because it requires a mutable reference
    // Also we can implement IntoIter for this to make it easy to iterate over it
    #[must_use = "Connection should be iterated over a loop to make progress"]
    pub fn iter(&mut self) -> Iter<'_> {
        Iter { connection: self }
    }

    /// Attempt to fetch an incoming [`Event`] on the [`EvenLoop`], returning an error
    /// if all clients/users have closed requests channel.
    ///
    /// [`EvenLoop`]: super::EventLoop
    pub fn recv(&mut self) -> Result<Result<Event, ConnectionError>, RecvError> {
        let f = self.eventloop.poll();
        let event = self.runtime.block_on(f);

        resolve_event(event).ok_or(RecvError)
    }

    /// Attempt to fetch an incoming [`Event`] on the [`EvenLoop`], returning an error
    /// if none immediately present or all clients/users have closed requests channel.
    ///
    /// [`EvenLoop`]: super::EventLoop
    pub fn try_recv(&mut self) -> Result<Result<Event, ConnectionError>, TryRecvError> {
        let f = self.eventloop.poll();
        // Enters the runtime context so we can poll the future, as required by `now_or_never()`.
        // ref: https://docs.rs/tokio/latest/tokio/runtime/struct.Runtime.html#method.enter
        let _guard = self.runtime.enter();
        let event = f.now_or_never().ok_or(TryRecvError::Empty)?;

        resolve_event(event).ok_or(TryRecvError::Disconnected)
    }

    /// Attempt to fetch an incoming [`Event`] on the [`EvenLoop`], returning an error
    /// if all clients/users have closed requests channel or the timeout has expired.
    ///
    /// [`EvenLoop`]: super::EventLoop
    pub fn recv_timeout(
        &mut self,
        duration: Duration,
    ) -> Result<Result<Event, ConnectionError>, RecvTimeoutError> {
        let f = self.eventloop.poll();
        let event = self
            .runtime
            .block_on(async { timeout(duration, f).await })
            .map_err(|_| RecvTimeoutError::Timeout)?;

        resolve_event(event).ok_or(RecvTimeoutError::Disconnected)
    }
}

fn resolve_event(event: Result<Event, ConnectionError>) -> Option<Result<Event, ConnectionError>> {
    match event {
        Ok(v) => Some(Ok(v)),
        // closing of request channel should stop the iterator
        Err(ConnectionError::RequestsDone) => {
            trace!("Done with requests");
            None
        }
        Err(e) => Some(Err(e)),
    }
}

/// Iterator which polls the `EventLoop` for connection progress
pub struct Iter<'a> {
    connection: &'a mut Connection,
}

impl Iterator for Iter<'_> {
    type Item = Result<Event, ConnectionError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.connection.recv().ok()
    }
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

//! This module offers a high level synchronous and asynchronous abstraction to
//! async eventloop.
use crate::v5::{packet::*, ConnectionError, Event, EventLoop, MqttOptions, Request};

use async_channel::{SendError, Sender, TrySendError};
use bytes::Bytes;
use std::{
    collections::VecDeque,
    mem,
    sync::{Arc, Mutex},
};
use tokio::runtime::{self, Runtime};

/// Client Error
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Failed to send cancel request to eventloop")]
    Cancel(SendError<()>),
    #[error("Failed to send mqtt request to eventloop, the evenloop has been closed")]
    EventloopClosed,
    #[error("Failed to send mqtt request to evenloop, to requests buffer is full right now")]
    RequestsFull,
    #[error("Serialization error")]
    Mqtt4(mqttbytes::Error),
}

/// `AsyncClient` to communicate with MQTT `Eventloop`
/// This is cloneable and can be used to asynchronously Publish, Subscribe.
#[derive(Clone, Debug)]
pub struct AsyncClient {
    request_buf: Arc<Mutex<VecDeque<Request>>>,
    request_buf_capacity: usize,
    request_tx: Sender<()>,
    cancel_tx: Sender<()>,
}

impl AsyncClient {
    /// Create a new `AsyncClient`
    pub fn new(options: MqttOptions, cap: usize) -> (AsyncClient, EventLoop) {
        let mut eventloop = EventLoop::new(options, cap);
        let request_buf = eventloop.buf().clone();
        let request_tx = eventloop.handle();
        let cancel_tx = eventloop.cancel_handle();

        let client = AsyncClient {
            request_buf,
            request_buf_capacity: cap,
            request_tx,
            cancel_tx,
        };

        (client, eventloop)
    }

    /// Create a new `AsyncClient` from a pair of async channel `Sender`s. This is mostly useful for
    /// creating a test instance.
    pub fn from_senders(
        request_buf: Arc<Mutex<VecDeque<Request>>>,
        request_tx: Sender<()>,
        cancel_tx: Sender<()>,
        cap: usize,
    ) -> AsyncClient {
        AsyncClient {
            request_buf,
            request_buf_capacity: cap,
            request_tx,
            cancel_tx,
        }
    }

    /// Sends a MQTT Publish to the eventloop
    pub async fn publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<u16, ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let mut publish = Publish::new(topic, qos, payload);
        publish.retain = retain;
        let pkid = publish.pkid;
        self.send_and_notify(Request::Publish(publish)).await?;
        Ok(pkid)
    }

    /// Sends a MQTT Publish to the eventloop
    pub fn try_publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<u16, ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let mut publish = Publish::new(topic, qos, payload);
        publish.retain = retain;
        let pkid = publish.pkid;
        self.try_send_and_notify(Request::Publish(publish))?;
        Ok(pkid)
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub async fn ack(&self, publish: &Publish) -> Result<(), ClientError> {
        let ack = get_ack_req(publish);

        if let Some(ack) = ack {
            self.send_and_notify(ack).await?;
        }
        Ok(())
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub fn try_ack(&self, publish: &Publish) -> Result<(), ClientError> {
        let ack = get_ack_req(publish);
        if let Some(ack) = ack {
            self.try_send_and_notify(ack)?;
        }
        Ok(())
    }

    /// Sends a MQTT Publish to the eventloop
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
        self.send_and_notify(Request::Publish(publish)).await
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub async fn subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let subscribe = Subscribe::new(topic.into(), qos);
        self.send_and_notify(Request::Subscribe(subscribe)).await
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn try_subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let subscribe = Subscribe::new(topic.into(), qos);
        self.try_send_and_notify(Request::Subscribe(subscribe))
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub async fn subscribe_many<T>(&self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let subscribe = Subscribe::new_many(topics);
        self.send_and_notify(Request::Subscribe(subscribe)).await
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub fn try_subscribe_many<T>(&self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let subscribe = Subscribe::new_many(topics);
        self.try_send_and_notify(Request::Subscribe(subscribe))
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub async fn unsubscribe<S: Into<String>>(&self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic.into());
        self.send_and_notify(Request::Unsubscribe(unsubscribe))
            .await
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn try_unsubscribe<S: Into<String>>(&self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic.into());
        self.try_send_and_notify(Request::Unsubscribe(unsubscribe))
    }

    /// Sends a MQTT disconnect to the eventloop
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.send_and_notify(Request::Disconnect).await
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn try_disconnect(&self) -> Result<(), ClientError> {
        self.try_send_and_notify(Request::Disconnect)
    }

    /// Stops the eventloop right away
    pub async fn cancel(&self) -> Result<(), ClientError> {
        self.cancel_tx.send(()).await.map_err(ClientError::Cancel)
    }

    async fn send_and_notify(&self, request: Request) -> Result<(), ClientError> {
        let mut request_buf = self.request_buf.lock().unwrap();
        if request_buf.len() == self.request_buf_capacity {
            return Err(ClientError::RequestsFull);
        }
        request_buf.push_back(request);
        if let Err(SendError(_)) = self.request_tx.send(()).await {
            return Err(ClientError::EventloopClosed);
        };
        Ok(())
    }

    fn try_send_and_notify(&self, request: Request) -> Result<(), ClientError> {
        let mut request_buf = self.request_buf.lock().unwrap();
        if request_buf.len() == self.request_buf_capacity {
            return Err(ClientError::RequestsFull);
        }
        request_buf.push_back(request);
        if let Err(TrySendError::Closed(_)) = self.request_tx.try_send(()) {
            return Err(ClientError::EventloopClosed);
        }
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

/// `Client` to communicate with MQTT eventloop `Connection`.
///
/// Client is cloneable and can be used to synchronously Publish, Subscribe.
/// Asynchronous channel handle can also be extracted if necessary
#[derive(Clone)]
pub struct Client {
    client: AsyncClient,
}

impl Client {
    /// Create a new `Client`
    pub fn new(options: MqttOptions, cap: usize) -> (Client, Connection) {
        let (client, eventloop) = AsyncClient::new(options, cap);
        let client = Client { client };
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let connection = Connection::new(eventloop, runtime);
        (client, connection)
    }

    /// Sends a MQTT Publish to the eventloop
    pub fn publish<S, V>(
        &mut self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<u16, ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        pollster::block_on(self.client.publish(topic, qos, retain, payload))
    }

    pub fn try_publish<S, V>(
        &mut self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<u16, ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        self.client.try_publish(topic, qos, retain, payload)
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub fn ack(&self, publish: &Publish) -> Result<(), ClientError> {
        pollster::block_on(self.client.ack(publish))
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub fn try_ack(&self, publish: &Publish) -> Result<(), ClientError> {
        self.client.try_ack(publish)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn subscribe<S: Into<String>>(&mut self, topic: S, qos: QoS) -> Result<(), ClientError> {
        pollster::block_on(self.client.subscribe(topic, qos))
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn try_subscribe<S: Into<String>>(
        &mut self,
        topic: S,
        qos: QoS,
    ) -> Result<(), ClientError> {
        self.client.try_subscribe(topic, qos)
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub fn subscribe_many<T>(&mut self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        pollster::block_on(self.client.subscribe_many(topics))
    }

    pub fn try_subscribe_many<T>(&mut self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        self.client.try_subscribe_many(topics)
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn unsubscribe<S: Into<String>>(&mut self, topic: S) -> Result<(), ClientError> {
        pollster::block_on(self.client.unsubscribe(topic))
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn try_unsubscribe<S: Into<String>>(&mut self, topic: S) -> Result<(), ClientError> {
        self.client.try_unsubscribe(topic)
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn disconnect(&mut self) -> Result<(), ClientError> {
        pollster::block_on(self.client.disconnect())
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn try_disconnect(&mut self) -> Result<(), ClientError> {
        self.client.try_disconnect()
    }

    /// Stops the eventloop right away
    pub fn cancel(&mut self) -> Result<(), ClientError> {
        pollster::block_on(self.client.cancel())
    }
}

///  MQTT connection. Maintains all the necessary state
pub struct Connection {
    pub eventloop: EventLoop,
    runtime: Option<Runtime>,
}

impl Connection {
    fn new(eventloop: EventLoop, runtime: Runtime) -> Connection {
        Connection {
            eventloop,
            runtime: Some(runtime),
        }
    }

    /// Returns an iterator over this connection. Iterating over this is all that's
    /// necessary to make connection progress and maintain a robust connection.
    /// Just continuing to loop will reconnect
    /// **NOTE** Don't block this while iterating
    #[must_use = "Connection should be iterated over a loop to make progress"]
    pub fn iter(&mut self) -> Iter {
        let runtime = self.runtime.take().unwrap();
        Iter {
            connection: self,
            runtime,
        }
    }
}

/// Iterator which polls the eventloop for connection progress
pub struct Iter<'a> {
    connection: &'a mut Connection,
    runtime: runtime::Runtime,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<Event, ConnectionError>;

    fn next(&mut self) -> Option<Self::Item> {
        let f = self.connection.eventloop.poll();
        match self.runtime.block_on(f) {
            Ok(v) => Some(Ok(v)),
            // closing of request channel should stop the iterator
            Err(ConnectionError::RequestsDone) => {
                trace!("Done with requests");
                None
            }
            Err(ConnectionError::Cancel) => {
                trace!("Cancellation request received");
                None
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'a> Drop for Iter<'a> {
    fn drop(&mut self) {
        // TODO: Don't create new runtime in drop
        let runtime = runtime::Builder::new_current_thread().build().unwrap();
        self.connection.runtime = Some(mem::replace(&mut self.runtime, runtime));
    }
}

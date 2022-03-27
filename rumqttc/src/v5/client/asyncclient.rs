use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use flume::{SendError, Sender, TrySendError};

use crate::v5::{
    client::get_ack_req,
    packet::{Publish, Subscribe, SubscribeFilter, Unsubscribe},
    ClientError, EventLoop, MqttOptions, QoS, Request,
};

/// `AsyncClient` to communicate with MQTT `Eventloop`
/// This is cloneable and can be used to asynchronously Publish, Subscribe.
#[derive(Debug)]
pub struct AsyncClient {
    pub(crate) outgoing_buf: Arc<Mutex<VecDeque<Request>>>,
    pub(crate) outgoing_buf_capacity: usize,
    pub(crate) pkid_counter: u16,
    pub(crate) max_inflight: u16,
    pub(crate) request_tx: Sender<()>,
}

impl AsyncClient {
    /// Create a new `AsyncClient`
    pub fn new(options: MqttOptions, cap: usize) -> (AsyncClient, EventLoop) {
        let eventloop = EventLoop::new(options, cap);
        let outgoing_buf = eventloop.request_buf().clone();
        let request_tx = eventloop.handle();
        let max_inflight = eventloop.state.max_inflight;

        let client = AsyncClient {
            outgoing_buf,
            outgoing_buf_capacity: cap,
            pkid_counter: 0,
            max_inflight,
            request_tx,
        };

        (client, eventloop)
    }

    /// Sends a MQTT Publish to the eventloop
    pub async fn publish<S, V>(
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
        let mut publish = Publish::new(topic, qos, payload);
        publish.retain = retain;
        let pkid = if qos != QoS::AtMostOnce {
            self.increment_pkid()
        } else {
            0
        };
        publish.pkid = pkid;
        self.push_and_async_notify(Request::Publish(publish))
            .await?;
        Ok(pkid)
    }

    /// Sends a MQTT Publish to the eventloop
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
        let mut publish = Publish::new(topic, qos, payload);
        publish.retain = retain;
        let pkid = if qos != QoS::AtMostOnce {
            self.increment_pkid()
        } else {
            0
        };
        publish.pkid = pkid;
        self.push_and_try_notify(Request::Publish(publish))?;
        Ok(pkid)
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub async fn ack(&mut self, publish: &Publish) -> Result<(), ClientError> {
        if let Some(ack) = get_ack_req(publish.qos, publish.pkid) {
            self.push_and_async_notify(ack).await?;
        }
        Ok(())
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub fn try_ack(&mut self, publish: &Publish) -> Result<(), ClientError> {
        if let Some(ack) = get_ack_req(publish.qos, publish.pkid) {
            self.push_and_try_notify(ack)?;
        }
        Ok(())
    }

    /// Sends a MQTT Publish to the eventloop
    pub async fn publish_bytes<S>(
        &mut self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: Bytes,
    ) -> Result<u16, ClientError>
    where
        S: Into<String>,
    {
        let mut publish = Publish::from_bytes(topic, qos, payload);
        publish.retain = retain;
        let pkid = if qos != QoS::AtMostOnce {
            self.increment_pkid()
        } else {
            0
        };
        publish.pkid = pkid;
        self.push_and_async_notify(Request::Publish(publish))
            .await?;
        Ok(pkid)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub async fn subscribe<S: Into<String>>(
        &mut self,
        topic: S,
        qos: QoS,
    ) -> Result<u16, ClientError> {
        let mut subscribe = Subscribe::new(topic.into(), qos);
        let pkid = self.increment_pkid();
        subscribe.pkid = pkid;
        self.push_and_async_notify(Request::Subscribe(subscribe))
            .await?;
        Ok(pkid)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn try_subscribe<S: Into<String>>(
        &mut self,
        topic: S,
        qos: QoS,
    ) -> Result<u16, ClientError> {
        let mut subscribe = Subscribe::new(topic.into(), qos);
        let pkid = self.increment_pkid();
        subscribe.pkid = pkid;
        self.push_and_try_notify(Request::Subscribe(subscribe))?;
        Ok(pkid)
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub async fn subscribe_many<T>(&mut self, topics: T) -> Result<u16, ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let mut subscribe = Subscribe::new_many(topics);
        let pkid = self.increment_pkid();
        subscribe.pkid = pkid;
        self.push_and_async_notify(Request::Subscribe(subscribe))
            .await?;
        Ok(pkid)
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub fn try_subscribe_many<T>(&mut self, topics: T) -> Result<u16, ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let mut subscribe = Subscribe::new_many(topics);
        let pkid = self.increment_pkid();
        subscribe.pkid = pkid;
        self.push_and_try_notify(Request::Subscribe(subscribe))?;
        Ok(pkid)
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub async fn unsubscribe<S: Into<String>>(&mut self, topic: S) -> Result<u16, ClientError> {
        let mut unsubscribe = Unsubscribe::new(topic.into());
        let pkid = self.increment_pkid();
        unsubscribe.pkid = pkid;
        self.push_and_async_notify(Request::Unsubscribe(unsubscribe))
            .await?;
        Ok(pkid)
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn try_unsubscribe<S: Into<String>>(&mut self, topic: S) -> Result<u16, ClientError> {
        let mut unsubscribe = Unsubscribe::new(topic.into());
        let pkid = self.increment_pkid();
        unsubscribe.pkid = pkid;
        self.push_and_try_notify(Request::Unsubscribe(unsubscribe))?;
        Ok(pkid)
    }

    /// Sends a MQTT disconnect to the eventloop
    pub async fn disconnect(&mut self) -> Result<(), ClientError> {
        self.push_and_async_notify(Request::Disconnect).await
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn try_disconnect(&mut self) -> Result<(), ClientError> {
        self.push_and_try_notify(Request::Disconnect)
    }

    async fn push_and_async_notify(&self, request: Request) -> Result<(), ClientError> {
        {
            let mut request_buf = self.outgoing_buf.lock().unwrap();
            if request_buf.len() == self.outgoing_buf_capacity {
                return Err(ClientError::RequestsFull);
            }
            request_buf.push_back(request);
        }
        if let Err(SendError(_)) = self.request_tx.send_async(()).await {
            return Err(ClientError::EventloopClosed);
        };
        Ok(())
    }

    pub(crate) fn push_and_notify(&self, request: Request) -> Result<(), ClientError> {
        let mut request_buf = self.outgoing_buf.lock().unwrap();
        if request_buf.len() == self.outgoing_buf_capacity {
            return Err(ClientError::RequestsFull);
        }
        request_buf.push_back(request);
        if let Err(SendError(_)) = self.request_tx.send(()) {
            return Err(ClientError::EventloopClosed);
        };
        Ok(())
    }

    fn push_and_try_notify(&self, request: Request) -> Result<(), ClientError> {
        let mut request_buf = self.outgoing_buf.lock().unwrap();
        if request_buf.len() == self.outgoing_buf_capacity {
            return Err(ClientError::RequestsFull);
        }
        request_buf.push_back(request);
        if let Err(TrySendError::Disconnected(_)) = self.request_tx.try_send(()) {
            return Err(ClientError::EventloopClosed);
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn increment_pkid(&mut self) -> u16 {
        self.pkid_counter = if self.pkid_counter == self.max_inflight {
            1
        } else {
            self.pkid_counter + 1
        };
        self.pkid_counter
    }
}

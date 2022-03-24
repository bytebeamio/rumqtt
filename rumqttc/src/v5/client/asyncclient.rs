use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc, Mutex,
    },
};

use bytes::Bytes;
use flume::{SendError, Sender, TrySendError};

use crate::v5::{
    client::{get_ack_req, Publisher, Subscriber},
    packet::{Publish, Subscribe, SubscribeFilter, Unsubscribe},
    ClientError, EventLoop, MqttOptions, QoS, Request,
};

/// `AsyncClient` to communicate with MQTT `Eventloop`
/// This is cloneable and can be used to asynchronously Publish, Subscribe.
#[derive(Clone, Debug)]
pub struct AsyncClient {
    request_buf: Arc<Mutex<VecDeque<Request>>>,
    sub_events_buf: Arc<Mutex<VecDeque<Publish>>>,
    pkid_counter: Arc<AtomicU16>,
    max_inflight: u16,
    sub_events_buf_cache: VecDeque<Publish>,
    request_buf_capacity: usize,
    request_tx: Sender<()>,
}

impl AsyncClient {
    /// Create a new `AsyncClient`
    pub fn new(options: MqttOptions, cap: usize) -> (AsyncClient, EventLoop) {
        let eventloop = EventLoop::new(options, cap);
        let request_buf = eventloop.request_buf().clone();
        let sub_events_buf = eventloop.sub_events_buf().clone();
        let sub_events_buf_cache = VecDeque::with_capacity(cap);
        let request_tx = eventloop.handle();
        let max_inflight = eventloop.state.max_inflight;
        let pkid_counter = eventloop.state.pkid_counter().clone();

        let client = AsyncClient {
            request_buf,
            request_buf_capacity: cap,
            sub_events_buf,
            pkid_counter,
            max_inflight,
            sub_events_buf_cache,
            request_tx,
        };

        (client, eventloop)
    }

    /// Create a new `AsyncClient` from a pair of async channel `Sender`s. This is mostly useful for
    /// creating a test instance.
    pub fn from_senders(
        request_buf: Arc<Mutex<VecDeque<Request>>>,
        sub_events_buf: Arc<Mutex<VecDeque<Publish>>>,
        pkid_counter: Arc<AtomicU16>,
        max_inflight: u16,
        request_tx: Sender<()>,
        cap: usize,
    ) -> AsyncClient {
        AsyncClient {
            request_buf,
            request_buf_capacity: cap,
            pkid_counter,
            max_inflight,
            sub_events_buf,
            sub_events_buf_cache: VecDeque::with_capacity(cap),
            request_tx,
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
        let pkid = self.increment_pkid();
        publish.pkid = pkid;
        self.send_async_and_notify(Request::Publish(publish))
            .await?;
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
        let pkid = self.increment_pkid();
        publish.pkid = pkid;
        self.try_send_and_notify(Request::Publish(publish))?;
        Ok(pkid)
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub async fn ack(&self, publish: &Publish) -> Result<(), ClientError> {
        if let Some(ack) = get_ack_req(publish.qos, publish.pkid) {
            self.send_async_and_notify(ack).await?;
        }
        Ok(())
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub fn try_ack(&self, publish: &Publish) -> Result<(), ClientError> {
        if let Some(ack) = get_ack_req(publish.qos, publish.pkid) {
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
    ) -> Result<u16, ClientError>
    where
        S: Into<String>,
    {
        let mut publish = Publish::from_bytes(topic, qos, payload);
        publish.retain = retain;
        let pkid = self.increment_pkid();
        publish.pkid = pkid;
        self.send_async_and_notify(Request::Publish(publish)).await?;
        Ok(pkid)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub async fn subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let subscribe = Subscribe::new(topic.into(), qos);
        self.send_async_and_notify(Request::Subscribe(subscribe))
            .await
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
        self.send_async_and_notify(Request::Subscribe(subscribe))
            .await
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
        self.send_async_and_notify(Request::Unsubscribe(unsubscribe))
            .await
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn try_unsubscribe<S: Into<String>>(&self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic.into());
        self.try_send_and_notify(Request::Unsubscribe(unsubscribe))
    }

    /// Sends a MQTT disconnect to the eventloop
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.send_async_and_notify(Request::Disconnect).await
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn try_disconnect(&self) -> Result<(), ClientError> {
        self.try_send_and_notify(Request::Disconnect)
    }

    async fn send_async_and_notify(&self, request: Request) -> Result<(), ClientError> {
        {
            let mut request_buf = self.request_buf.lock().unwrap();
            if request_buf.len() == self.request_buf_capacity {
                return Err(ClientError::RequestsFull);
            }
            request_buf.push_back(request);
        }
        if let Err(SendError(_)) = self.request_tx.send_async(()).await {
            return Err(ClientError::EventloopClosed);
        };
        Ok(())
    }

    pub(crate) fn send_and_notify(&self, request: Request) -> Result<(), ClientError> {
        let mut request_buf = self.request_buf.lock().unwrap();
        if request_buf.len() == self.request_buf_capacity {
            return Err(ClientError::RequestsFull);
        }
        request_buf.push_back(request);
        if let Err(SendError(_)) = self.request_tx.send(()) {
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
        if let Err(TrySendError::Disconnected(_)) = self.request_tx.try_send(()) {
            return Err(ClientError::EventloopClosed);
        }
        Ok(())
    }

    pub fn next_publish(&mut self) -> Option<Publish> {
        if let Some(publish) = self.sub_events_buf_cache.pop_front() {
            return Some(publish);
        }

        std::mem::swap(
            &mut self.sub_events_buf_cache,
            &mut *self.sub_events_buf.lock().unwrap(),
        );
        self.sub_events_buf_cache.pop_front()
    }

    pub async fn split(
        self,
        publish_topic: impl Into<String>,
        publish_qos: QoS,
    ) -> Result<(Publisher, Subscriber), ClientError> {
        let publisher = Publisher {
            request_buf: self.request_buf.clone(),
            request_buf_capacity: self.request_buf_capacity,
            pkid_counter: self.pkid_counter,
            max_inflight: self.max_inflight,
            request_tx: self.request_tx.clone(),
            publish_topic: publish_topic.into(),
            publish_qos,
        };
        let subscriber = Subscriber {
            request_buf: self.request_buf,
            sub_events_buf: self.sub_events_buf,
            sub_events_buf_cache: self.sub_events_buf_cache,
            request_buf_capacity: self.request_buf_capacity,
            request_tx: self.request_tx,
        };
        Ok((publisher, subscriber))
    }

    fn increment_pkid(&self) -> u16 {
        let mut cur_pkid = self.pkid_counter.load(Ordering::SeqCst);
        loop {
            let new_pkid = if cur_pkid > self.max_inflight {
                1
            } else {
                cur_pkid + 1
            };
            match self.pkid_counter.compare_exchange(
                cur_pkid,
                new_pkid,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_prev_pkid) => break new_pkid,
                Err(actual_pkid) => cur_pkid = actual_pkid,
            }
        }
    }
}

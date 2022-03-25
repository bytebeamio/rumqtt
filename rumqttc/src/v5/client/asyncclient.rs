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
    client::get_ack_req,
    packet::{Publish, Subscribe, SubscribeFilter, Unsubscribe},
    ClientError, EventLoop, MqttOptions, QoS, Request,
};

/// `AsyncClient` to communicate with MQTT `Eventloop`
/// This is cloneable and can be used to asynchronously Publish, Subscribe.
#[derive(Clone, Debug)]
pub struct AsyncClient {
    pub(crate) outgoing_buf: Arc<Mutex<VecDeque<Request>>>,
    pub(crate) outgoing_buf_capacity: usize,
    pub(crate) pkid_counter: Arc<AtomicU16>,
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
        let pkid_counter = eventloop.state.pkid_counter().clone();

        let client = AsyncClient {
            outgoing_buf,
            outgoing_buf_capacity: cap,
            pkid_counter,
            max_inflight,
            request_tx,
        };

        (client, eventloop)
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
    pub async fn ack(&self, publish: &Publish) -> Result<(), ClientError> {
        if let Some(ack) = get_ack_req(publish.qos, publish.pkid) {
            self.push_and_async_notify(ack).await?;
        }
        Ok(())
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub fn try_ack(&self, publish: &Publish) -> Result<(), ClientError> {
        if let Some(ack) = get_ack_req(publish.qos, publish.pkid) {
            self.push_and_try_notify(ack)?;
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
    pub async fn subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let subscribe = Subscribe::new(topic.into(), qos);
        self.push_and_async_notify(Request::Subscribe(subscribe))
            .await
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn try_subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let subscribe = Subscribe::new(topic.into(), qos);
        self.push_and_try_notify(Request::Subscribe(subscribe))
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub async fn subscribe_many<T>(&self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let subscribe = Subscribe::new_many(topics);
        self.push_and_async_notify(Request::Subscribe(subscribe))
            .await
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub fn try_subscribe_many<T>(&self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let subscribe = Subscribe::new_many(topics);
        self.push_and_try_notify(Request::Subscribe(subscribe))
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub async fn unsubscribe<S: Into<String>>(&self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic.into());
        self.push_and_async_notify(Request::Unsubscribe(unsubscribe))
            .await
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn try_unsubscribe<S: Into<String>>(&self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic.into());
        self.push_and_try_notify(Request::Unsubscribe(unsubscribe))
    }

    /// Sends a MQTT disconnect to the eventloop
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.push_and_async_notify(Request::Disconnect).await
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn try_disconnect(&self) -> Result<(), ClientError> {
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

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use flume::{SendError, Sender, TrySendError};

use crate::v5::{
    client::get_ack_req, ClientError, Publish, QoS, Request, Subscribe, SubscribeFilter,
    Unsubscribe,
};

#[derive(Debug, Clone)]
pub struct Subscriber {
    pub(crate) outgoing_buf: Arc<Mutex<VecDeque<Request>>>,
    pub(crate) incoming_buf: Arc<Mutex<VecDeque<Publish>>>,
    pub(crate) incoming_buf_cache: VecDeque<Publish>,
    pub(crate) request_buf_capacity: usize,
    pub(crate) request_tx: Sender<()>,
}

impl Subscriber {
    pub fn next_publish(&mut self) -> Option<Publish> {
        if let Some(publish) = self.incoming_buf_cache.pop_front() {
            return Some(publish);
        }

        std::mem::swap(
            &mut self.incoming_buf_cache,
            &mut *self.incoming_buf.lock().unwrap(),
        );
        self.incoming_buf_cache.pop_front()
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub async fn ack(&self, qos: QoS, pkid: u16) -> Result<(), ClientError> {
        if let Some(ack) = get_ack_req(qos, pkid) {
            self.send_async_and_notify(ack).await?;
        }
        Ok(())
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub fn try_ack(&self, qos: QoS, pkid: u16) -> Result<(), ClientError> {
        if let Some(ack) = get_ack_req(qos, pkid) {
            self.try_send_and_notify(ack)?;
        }
        Ok(())
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

    async fn send_async_and_notify(&self, request: Request) -> Result<(), ClientError> {
        {
            let mut request_buf = self.outgoing_buf.lock().unwrap();
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

    fn try_send_and_notify(&self, request: Request) -> Result<(), ClientError> {
        let mut request_buf = self.outgoing_buf.lock().unwrap();
        if request_buf.len() == self.request_buf_capacity {
            return Err(ClientError::RequestsFull);
        }
        request_buf.push_back(request);
        if let Err(TrySendError::Disconnected(_)) = self.request_tx.try_send(()) {
            return Err(ClientError::EventloopClosed);
        }
        Ok(())
    }
}

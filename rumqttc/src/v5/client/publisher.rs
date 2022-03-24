use std::{
    collections::VecDeque,
    sync::{Arc, atomic::{AtomicU16, Ordering}, Mutex},
};

use bytes::Bytes;
use flume::{SendError, Sender, TrySendError};

use crate::v5::{packet::Publish, ClientError, QoS, Request};

pub struct Publisher {
    pub(crate) incoming_buf: Arc<Mutex<VecDeque<Request>>>,
    pub(crate) incoming_buf_capacity: usize,
    pub(crate) pkid_counter: Arc<AtomicU16>,
    pub(crate) max_inflight: u16,
    pub(crate) request_tx: Sender<()>,
    pub(crate) publish_topic: String,
    pub(crate) publish_qos: QoS,
}

impl Publisher {
    /// Sends a MQTT Publish to the eventloop
    pub async fn publish(
        &self,
        retain: bool,
        payload: impl Into<Vec<u8>>,
    ) -> Result<u16, ClientError> {
        let mut publish = Publish::new(&self.publish_topic, self.publish_qos, payload);
        publish.retain = retain;
        let pkid = self.increment_pkid();
        publish.pkid = pkid;
        self.send_async_and_notify(Request::Publish(publish))
            .await?;
        Ok(pkid)
    }

    /// Sends a MQTT Publish to the eventloop
    pub fn try_publish(
        &self,
        retain: bool,
        payload: impl Into<Vec<u8>>,
    ) -> Result<u16, ClientError> {
        let mut publish = Publish::new(&self.publish_topic, self.publish_qos, payload);
        publish.retain = retain;
        let pkid = self.increment_pkid();
        publish.pkid = pkid;
        self.try_send_and_notify(Request::Publish(publish))?;
        Ok(pkid)
    }

    /// Sends a MQTT Publish to the eventloop
    pub async fn publish_bytes(&self, retain: bool, payload: Bytes) -> Result<u16, ClientError> {
        let mut publish = Publish::from_bytes(&self.publish_topic, self.publish_qos, payload);
        let pkid = self.increment_pkid();
        publish.pkid = pkid;
        publish.retain = retain;
        self.send_async_and_notify(Request::Publish(publish)).await?;
        Ok(pkid)
    }

    async fn send_async_and_notify(&self, request: Request) -> Result<(), ClientError> {
        {
            let mut request_buf = self.incoming_buf.lock().unwrap();
            if request_buf.len() == self.incoming_buf_capacity {
                return Err(ClientError::RequestsFull);
            }
            request_buf.push_back(request);
        }
        if let Err(SendError(_)) = self.request_tx.send_async(()).await {
            return Err(ClientError::EventloopClosed);
        };
        Ok(())
    }

    /// Sends a MQTT disconnect to the eventloop
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.send_async_and_notify(Request::Disconnect).await
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn try_disconnect(&self) -> Result<(), ClientError> {
        self.try_send_and_notify(Request::Disconnect)
    }

    fn try_send_and_notify(&self, request: Request) -> Result<(), ClientError> {
        let mut request_buf = self.incoming_buf.lock().unwrap();
        if request_buf.len() == self.incoming_buf_capacity {
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

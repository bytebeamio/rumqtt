use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use flume::{SendError, Sender, TrySendError};

use crate::v5::{packet::Publish, ClientError, QoS, Request};

pub struct Publisher {
    pub(crate) request_buf: Arc<Mutex<VecDeque<Request>>>,
    pub(crate) request_buf_capacity: usize,
    pub(crate) request_tx: Sender<()>,
    pub(crate) cancel_tx: Sender<()>,
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
        let pkid = publish.pkid;
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
        let pkid = publish.pkid;
        self.try_send_and_notify(Request::Publish(publish))?;
        Ok(pkid)
    }

    /// Sends a MQTT Publish to the eventloop
    pub async fn publish_bytes(&self, retain: bool, payload: Bytes) -> Result<(), ClientError> {
        let mut publish = Publish::from_bytes(&self.publish_topic, self.publish_qos, payload);
        publish.retain = retain;
        self.send_async_and_notify(Request::Publish(publish)).await
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

    /// Sends a MQTT disconnect to the eventloop
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.send_async_and_notify(Request::Disconnect).await
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn try_disconnect(&self) -> Result<(), ClientError> {
        self.try_send_and_notify(Request::Disconnect)
    }

    /// Stops the eventloop right away
    pub async fn cancel(&self) -> Result<(), ClientError> {
        self.cancel_tx
            .send_async(())
            .await
            .map_err(ClientError::Cancel)
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
}

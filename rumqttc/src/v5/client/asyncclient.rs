use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use flume::{SendError, Sender, TrySendError};

use crate::v5::{
    client::get_ack_req,
    outgoing_buf::OutgoingBuf,
    packet::{Publish, Subscribe, SubscribeFilter, Unsubscribe},
    ClientError, ConnectionError, EventLoop, MqttOptions, Notifier, QoS, Request,
    SubscribeProperties, UnsubscribeProperties,
};

/// `AsyncClient` to communicate with an MQTT `Eventloop`
/// This is cloneable and can be used to asynchronously Publish, Subscribe.
#[derive(Clone, Debug)]
pub struct AsyncClient {
    pub(crate) outgoing_buf: Arc<Mutex<OutgoingBuf>>,
    pub(crate) request_tx: Sender<()>,
}

impl AsyncClient {
    /// Create a new `AsyncClient`
    pub fn new(options: MqttOptions, cap: usize) -> (AsyncClient, EventLoop) {
        let eventloop = EventLoop::new(options, cap);
        let outgoing_buf = eventloop.state.outgoing_buf.clone();
        let request_tx = eventloop.handle();

        let client = AsyncClient {
            outgoing_buf,
            request_tx,
        };

        (client, eventloop)
    }

    /// Create an `AsyncClient` and it's associated [`Notifier`]
    pub async fn connect(options: MqttOptions, cap: usize) -> (AsyncClient, Notifier) {
        let (client, mut eventloop) = AsyncClient::new(options, cap);
        let incoming_buf = eventloop.state.incoming_buf.clone();
        let disconnected = eventloop.state.disconnected.clone();
        let incoming_buf_cache = VecDeque::with_capacity(cap);
        let notifier = Notifier::new(incoming_buf, incoming_buf_cache, disconnected);

        tokio::spawn(async move {
            loop {
                // TODO: maybe do something like retries for some specific errors? or maybe give user
                // options to configure these retries?
                let event = eventloop.poll().await;
                if let Err(e) = &event {
                    println!("{}", e);
                }

                if let Err(ConnectionError::Disconnected) = event {
                    break;
                }
            }
        });

        (client, notifier)
    }

    fn make_request(&self, mut request: Request, increment: bool) -> Result<u16, ClientError> {
        let mut request_buf = self.outgoing_buf.lock().unwrap();
        if request_buf.buf.len() == request_buf.capacity {
            return Err(ClientError::RequestsFull);
        }

        let pkid = if increment {
            let pkid = request_buf.increment_pkid();
            set_pkid(&mut request, pkid);
            pkid
        } else {
            0
        };
        request_buf.buf.push_back(request);

        Ok(pkid)
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
        let pkid = self.make_request(Request::Publish(publish), qos != QoS::AtMostOnce)?;
        self.notify_async().await?;
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
        let pkid = self.make_request(Request::Publish(publish), qos != QoS::AtMostOnce)?;
        self.try_notify()?;
        Ok(pkid)
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub async fn ack(&self, publish: &Publish) -> Result<(), ClientError> {
        if let Some(ack) = get_ack_req(publish.qos, publish.pkid) {
            self.make_request(ack, false)?;
            self.notify_async().await?;
        }
        Ok(())
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub fn try_ack(&self, publish: &Publish) -> Result<(), ClientError> {
        if let Some(ack) = get_ack_req(publish.qos, publish.pkid) {
            self.make_request(ack, false)?;
            self.try_notify()?;
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
        let pkid = self.make_request(Request::Publish(publish), qos != QoS::AtMostOnce)?;
        self.notify_async().await?;
        Ok(pkid)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub async fn subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<u16, ClientError> {
        self.subscribe_with(topic, qos, None).await
    }

    /// Sends a MQTT Subscribe to the eventloop with options
    pub async fn subscribe_with<S: Into<String>, P: Into<Option<SubscribeProperties>>>(
        &self,
        topic: S,
        qos: QoS,
        props: P,
    ) -> Result<u16, ClientError> {
        let mut subscribe = Subscribe::new(topic.into(), qos);
        subscribe.properties = props.into();
        let pkid = {
            let mut request_buf = self.outgoing_buf.lock().unwrap();
            if request_buf.buf.len() == request_buf.capacity {
                return Err(ClientError::RequestsFull);
            }
            let pkid = request_buf.increment_pkid();
            subscribe.pkid = pkid;
            request_buf.buf.push_back(Request::Subscribe(subscribe));
            pkid
        };
        self.notify_async().await?;
        Ok(pkid)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn try_subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<u16, ClientError> {
        self.try_subscribe_with(topic, qos, None)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn try_subscribe_with<S: Into<String>, P: Into<Option<SubscribeProperties>>>(
        &self,
        topic: S,
        qos: QoS,
        props: P,
    ) -> Result<u16, ClientError> {
        let mut subscribe = Subscribe::new(topic.into(), qos);
        subscribe.properties = props.into();
        let pkid = {
            let mut request_buf = self.outgoing_buf.lock().unwrap();
            if request_buf.buf.len() == request_buf.capacity {
                return Err(ClientError::RequestsFull);
            }
            let pkid = request_buf.increment_pkid();
            subscribe.pkid = pkid;
            request_buf.buf.push_back(Request::Subscribe(subscribe));
            pkid
        };
        self.try_notify()?;
        Ok(pkid)
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub async fn subscribe_many<T>(&self, topics: T) -> Result<u16, ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        self.subscribe_with_many(None, topics).await
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop, with properties
    pub async fn subscribe_with_many<P: Into<Option<SubscribeProperties>>, T>(
        &self,
        props: P,
        topics: T,
    ) -> Result<u16, ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let mut subscribe = Subscribe::new_many(topics)?;
        subscribe.properties = props.into();
        let pkid = {
            let mut request_buf = self.outgoing_buf.lock().unwrap();
            if request_buf.buf.len() == request_buf.capacity {
                return Err(ClientError::RequestsFull);
            }
            let pkid = request_buf.increment_pkid();
            subscribe.pkid = pkid;
            request_buf.buf.push_back(Request::Subscribe(subscribe));
            pkid
        };
        self.notify_async().await?;
        Ok(pkid)
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub fn try_subscribe_many<T>(&self, topics: T) -> Result<u16, ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        self.try_subscribe_with_many(None, topics)
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub fn try_subscribe_with_many<P, T>(&self, props: P, topics: T) -> Result<u16, ClientError>
    where
        P: Into<Option<SubscribeProperties>>,
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let mut subscribe = Subscribe::new_many(topics)?;
        subscribe.properties = props.into();
        let pkid = {
            let mut request_buf = self.outgoing_buf.lock().unwrap();
            if request_buf.buf.len() == request_buf.capacity {
                return Err(ClientError::RequestsFull);
            }
            let pkid = request_buf.increment_pkid();
            subscribe.pkid = pkid;
            request_buf.buf.push_back(Request::Subscribe(subscribe));
            pkid
        };
        self.try_notify()?;
        Ok(pkid)
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub async fn unsubscribe<S: Into<String>>(&self, topic: S) -> Result<u16, ClientError> {
        self.unsubscribe_with(topic, None).await
    }

    /// Sends a MQTT Unsubscribe to the eventloop, with properties
    pub async fn unsubscribe_with<S: Into<String>, P: Into<Option<UnsubscribeProperties>>>(
        &self,
        topic: S,
        props: P,
    ) -> Result<u16, ClientError> {
        let mut unsubscribe = Unsubscribe::new(topic.into());
        unsubscribe.properties = props.into();
        let pkid = {
            let mut request_buf = self.outgoing_buf.lock().unwrap();
            if request_buf.buf.len() == request_buf.capacity {
                return Err(ClientError::RequestsFull);
            }
            let pkid = request_buf.increment_pkid();
            unsubscribe.pkid = pkid;
            request_buf.buf.push_back(Request::Unsubscribe(unsubscribe));
            pkid
        };
        self.notify_async().await?;
        Ok(pkid)
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn try_unsubscribe<S: Into<String>>(&self, topic: S) -> Result<u16, ClientError> {
        self.try_unsubscribe_with(topic, None)
    }

    /// Sends a MQTT Unsubscribe to the eventloop, with properties
    pub fn try_unsubscribe_with<S: Into<String>, P: Into<Option<UnsubscribeProperties>>>(
        &self,
        topic: S,
        props: P,
    ) -> Result<u16, ClientError> {
        let mut unsubscribe = Unsubscribe::new(topic.into());
        unsubscribe.properties = props.into();
        let pkid = {
            let mut request_buf = self.outgoing_buf.lock().unwrap();
            if request_buf.buf.len() == request_buf.capacity {
                return Err(ClientError::RequestsFull);
            }
            let pkid = request_buf.increment_pkid();
            unsubscribe.pkid = pkid;
            request_buf.buf.push_back(Request::Unsubscribe(unsubscribe));
            pkid
        };
        self.try_notify()?;
        Ok(pkid)
    }

    /// Sends a MQTT disconnect to the eventloop
    #[inline]
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.make_request(Request::Disconnect, false)?;
        self.notify_async().await
    }

    /// Sends a MQTT disconnect to the eventloop
    #[inline]
    pub fn try_disconnect(&self) -> Result<(), ClientError> {
        self.make_request(Request::Disconnect, false)?;
        self.try_notify()
    }

    #[inline]
    async fn notify_async(&self) -> Result<(), ClientError> {
        if let Err(SendError(_)) = self.request_tx.send_async(()).await {
            return Err(ClientError::EventloopClosed);
        };
        Ok(())
    }

    #[inline]
    pub(crate) fn notify(&self) -> Result<(), ClientError> {
        if let Err(SendError(_)) = self.request_tx.send(()) {
            return Err(ClientError::EventloopClosed);
        };
        Ok(())
    }

    #[inline]
    fn try_notify(&self) -> Result<(), ClientError> {
        if let Err(TrySendError::Disconnected(_)) = self.request_tx.try_send(()) {
            return Err(ClientError::EventloopClosed);
        }
        Ok(())
    }
}

#[inline]
pub fn set_pkid(request: &mut Request, pkid: u16) {
    match request {
        Request::Publish(p) => {
            p.pkid = pkid;
        }
        Request::PubAck(p) => {
            p.pkid = pkid;
        }
        Request::PubRec(p) => {
            p.pkid = pkid;
        }
        Request::PubComp(p) => {
            p.pkid = pkid;
        }
        Request::PubRel(p) => {
            p.pkid = pkid;
        }
        Request::Subscribe(p) => {
            p.pkid = pkid;
        }
        Request::SubAck(p) => {
            p.pkid = pkid;
        }
        Request::Unsubscribe(p) => {
            p.pkid = pkid;
        }
        Request::UnsubAck(p) => {
            p.pkid = pkid;
        }
        _ => {}
    }
}
